//// Reporting for a short-lived task that owns a workflow execution.
//// `run_owned` returns its classified outcome while the task lives. If the
//// task exits abnormally, the independent receiver waits for compensation and
//// calls `on_stopped` with the same result and a data-safe summary. The receiver
//// monitors the task until it exits, even after returning a result: use this
//// boundary inside a per-invocation worker, not a long-lived server process.
//// `rollback_within` bounds waiting when the owner dies before reporting whether
//// it started a coordinator; normal execution follows `execution.Config`.
//// Receiver readiness has a five-second bound. A receiver that exits or
//// misses that deadline produces a definite failure before the workflow starts.
//// Its startup channel and monitor are discarded without consuming other mail.
//// The notification callback runs in a guarded worker, bounded by the same
//// `rollback_within` duration; a crash or timeout leaves delivery unconfirmed.

import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import saga
import saga/execution
import saga/internal/ffi
import saga/internal/reporting_startup
import saga/outcome.{Definitely, Unknown}

/// What the receiver learns about the Saga run.
type Delivery(o, e, u) {
  Delivered(execution.Outcome(o, e, u))
  /// The coordinator exited without an outcome.
  RunLost
}

/// What the task tells the receiver after starting the Saga run.
type Start {
  Started(coordinator: Pid)
  NotStarted
}

/// Runs the workflow in the calling task, which owns the Saga run, and
/// waits for its outcome through the receiver.
pub fn run_owned(
  workflow: saga.Workflow(input, output, error, undo_error),
  input: input,
  config: execution.Config,
  explain: fn(error) -> String,
  on_stopped: fn(Result(output, outcome.Failure), String) -> Nil,
  rollback_within: Duration,
) -> Result(output, outcome.Failure) {
  let milliseconds = duration.to_milliseconds(rollback_within)
  case milliseconds >= 1 && milliseconds <= 4_294_967_295 {
    False ->
      Error(Definitely("rollback_within must be between 1 ms and 2^32 - 1 ms"))
    True -> run(workflow, input, config, explain, on_stopped, milliseconds)
  }
}

fn run(workflow, input, config, explain, on_stopped, rollback_within) {
  let judge = fn(delivery) { classify(delivery, explain) }
  let task = process.self()
  let forward = process.new_subject()
  let ready = process.new_subject()
  let #(send_ready, close_ready) = ffi.aliased_sender(ready)
  let receiver =
    process.spawn_unlinked(fn() {
      let report = process.new_subject()
      let start = process.new_subject()
      send_ready(#(report, start))
      receive(Receiver(
        task_monitor: Some(process.monitor(task)),
        forward:,
        report:,
        start:,
        coordinator: None,
        settle: fn(delivery) {
          let summary = case delivery {
            Delivered(outcome) -> "Saga reported " <> outcome.summary(outcome)
            RunLost -> "the workflow run was lost"
          }
          notify(rollback_within, fn() { on_stopped(judge(delivery), summary) })
        },
        rollback_within:,
      ))
    })
  let receiver_monitor = process.monitor(receiver)
  use #(report, start) <- result.try(
    reporting_startup.await(
      receiver,
      receiver_monitor,
      ready,
      close_ready,
      5000,
    )
    |> result.map_error(fn(error) {
      Definitely(case error {
        reporting_startup.ReceiverExited ->
          "the workflow receiver exited before it became ready"
        reporting_startup.TimedOut ->
          "the workflow receiver did not become ready within 5 seconds"
      })
    }),
  )
  case execution.start_reporting(workflow, input, config, to: report) {
    Error(execution.InvalidConfig(errors)) -> {
      process.send(start, NotStarted)
      Error(Definitely(
        "the workflow is misconfigured: "
        <> string.join(list.map(errors, execution.describe_config_error), "; "),
      ))
    }
    Error(execution.ExecutionLost(_)) -> {
      process.send(start, NotStarted)
      Error(Unknown("the workflow run was lost"))
    }
    Ok(started) -> {
      process.send(start, Started(execution.pid(started)))
      let delivered =
        process.new_selector()
        |> process.select_map(forward, Ok)
        |> process.select_specific_monitor(receiver_monitor, fn(_) {
          Error(Nil)
        })
        |> process.selector_receive_forever
      case delivered {
        Ok(delivery) -> judge(delivery)
        Error(Nil) -> Error(Unknown("the workflow's outcome was lost"))
      }
    }
  }
}

type Receiver(o, e, u) {
  Receiver(
    /// `None` once the task has exited.
    task_monitor: Option(process.Monitor),
    forward: Subject(Delivery(o, e, u)),
    report: Subject(execution.Outcome(o, e, u)),
    start: Subject(Start),
    coordinator: Option(process.Monitor),
    settle: fn(Delivery(o, e, u)) -> Nil,
    rollback_within: Int,
  )
}

type Event(o, e, u) {
  Reported(execution.Outcome(o, e, u))
  Starting(Start)
  TaskExited(process.ExitReason)
  CoordinatorExited
}

/// Owns the report subject. Forwards what it learns to the task while the
/// task lives; once the task was stopped, settles the call with it. Saga
/// sends the outcome before its coordinator exits, so a coordinator exit
/// with no outcome means the run was lost. When the task was stopped before
/// telling whether a run started, the receiver waits `rollback_within` for
/// an outcome and then gives up; the caller must retain the uncertain effect.
fn receive(receiver: Receiver(o, e, u)) -> Nil {
  let selector =
    process.new_selector()
    |> process.select_map(receiver.report, Reported)
    |> process.select_map(receiver.start, Starting)
  let selector = case receiver.task_monitor {
    Some(monitor) ->
      process.select_specific_monitor(selector, monitor, fn(down) {
        case down {
          process.ProcessDown(reason:, ..) -> TaskExited(reason)
          process.PortDown(reason:, ..) -> TaskExited(reason)
        }
      })
    None -> selector
  }
  let selector = case receiver.coordinator {
    Some(monitor) ->
      process.select_specific_monitor(selector, monitor, fn(_) {
        CoordinatorExited
      })
    None -> selector
  }
  let event = case receiver.task_monitor, receiver.coordinator {
    // The task is gone and never said whether a run started.
    None, None ->
      process.selector_receive(selector, receiver.rollback_within)
      |> result.replace_error(Nil)
    _, _ -> Ok(process.selector_receive_forever(selector))
  }
  case event {
    Error(Nil) -> Nil
    Ok(Starting(NotStarted)) -> Nil
    Ok(Starting(Started(pid))) ->
      receive(Receiver(..receiver, coordinator: Some(process.monitor(pid))))
    Ok(Reported(outcome)) -> deliver(receiver, Delivered(outcome))
    Ok(CoordinatorExited) -> deliver(receiver, RunLost)
    // The task returned: it had the outcome.
    Ok(TaskExited(process.Normal)) -> Nil
    Ok(TaskExited(_)) -> receive(Receiver(..receiver, task_monitor: None))
  }
}

/// Hands `delivery` to the task if it lives. A task that then exits
/// abnormally may not have reported it, so the call is settled with it: a
/// settlement after the task reported is refused and changes nothing.
fn deliver(receiver: Receiver(o, e, u), delivery: Delivery(o, e, u)) -> Nil {
  case receiver.task_monitor {
    None -> receiver.settle(delivery)
    Some(monitor) -> {
      process.send(receiver.forward, delivery)
      let exited =
        process.new_selector()
        |> process.select_specific_monitor(monitor, fn(down) {
          case down {
            process.ProcessDown(reason:, ..) -> reason
            process.PortDown(reason:, ..) -> reason
          }
        })
        |> process.selector_receive_forever
      case exited {
        process.Normal -> Nil
        _ -> receiver.settle(delivery)
      }
    }
  }
}

fn classify(
  delivery: Delivery(output, error, undo_error),
  explain: fn(error) -> String,
) -> Result(output, outcome.Failure) {
  case delivery {
    RunLost -> Error(Unknown("the workflow run was lost"))
    Delivered(outcome) -> outcome.classify(outcome, explain)
  }
}

fn notify(within: Int, callback: fn() -> Nil) -> Nil {
  let worker =
    process.spawn(fn() {
      let _ = ffi.rescue(callback)
      Nil
    })
  let monitor = process.monitor(worker)
  let finished =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
    |> process.selector_receive(within)
  case finished {
    Ok(Nil) -> Nil
    Error(Nil) -> {
      process.unlink(worker)
      process.kill(worker)
    }
  }
  process.demonitor_process(monitor)
}
