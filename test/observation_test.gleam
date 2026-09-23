import gleam/erlang/process.{type Subject}
import gleam/list
import gleeunit/should
import saga
import saga/execution
import saga/observation
import sinal
import support/probe

pub type DemoError {
  Boom
}

pub type DemoUndoError {
  UndoBoom
}

/// A small process that records emitted event names in emission order, so
/// tests can assert an exact sequence without a shared mutable list.
type Collector {
  Collector(subject: Subject(CollectorMessage))
}

type CollectorMessage {
  Record(name: String)
  Snapshot(reply: Subject(List(String)))
}

fn new_collector() -> Collector {
  let ready = process.new_subject()
  process.spawn(fn() {
    let subject = process.new_subject()
    process.send(ready, subject)
    collector_loop(subject, [])
  })
  let assert Ok(subject) = process.receive(ready, 1000)
  Collector(subject)
}

fn collector_loop(
  subject: Subject(CollectorMessage),
  acc: List(String),
) -> Nil {
  let message = process.receive_forever(subject)
  case message {
    Record(name) -> collector_loop(subject, list.append(acc, [name]))
    Snapshot(reply) -> {
      process.send(reply, acc)
      collector_loop(subject, acc)
    }
  }
}

fn record(collector: Collector, name: String) -> Nil {
  process.send(collector.subject, Record(name))
}

fn events(collector: Collector) -> List(String) {
  process.call(collector.subject, 1000, Snapshot)
}

fn all_subscriptions(collector: Collector) -> sinal.SubscriptionPlan {
  sinal.subscriptions([
    sinal.subscription(observation.run_started(), fn(_m, _d) {
      record(collector, "run_start")
    }),
    sinal.subscription(observation.run_stopped(), fn(_m, d) {
      record(collector, "run_stop:" <> outcome_kind_string(d.outcome))
    }),
    sinal.subscription(observation.step_started(), fn(_m, d) {
      record(collector, "step_start:" <> d.step)
    }),
    sinal.subscription(observation.step_stopped(), fn(_m, d) {
      record(
        collector,
        "step_stop:" <> d.step <> ":" <> attempt_kind_string(d.result),
      )
    }),
    sinal.subscription(observation.compensation_stopped(), fn(_m, d) {
      record(
        collector,
        "compensate_stop:" <> d.step <> ":" <> decision_kind_string(d.decision),
      )
    }),
    sinal.subscription(observation.undo_stopped(), fn(_m, d) {
      record(
        collector,
        "undo_stop:" <> d.step <> ":" <> undo_kind_string(d.result),
      )
    }),
  ])
}

fn outcome_kind_string(kind: observation.OutcomeKind) -> String {
  case kind {
    observation.OutcomeCompleted -> "completed"
    observation.OutcomeFailed -> "failed"
    observation.OutcomeCancelled -> "cancelled"
    observation.OutcomeUnresolved -> "unresolved"
  }
}

fn attempt_kind_string(kind: observation.AttemptKind) -> String {
  case kind {
    observation.AttemptSucceeded -> "succeeded"
    observation.AttemptFailed -> "failed"
    observation.AttemptCrashed -> "crashed"
    observation.AttemptTimedOut -> "timed_out"
    observation.AttemptInterrupted -> "interrupted"
  }
}

fn decision_kind_string(kind: observation.DecisionKind) -> String {
  case kind {
    observation.DecisionRetry -> "retry"
    observation.DecisionContinue -> "continue"
    observation.DecisionAbort -> "abort"
    observation.DecisionHold -> "hold"
    observation.DecisionCrashed -> "crashed"
    observation.DecisionTimedOut -> "timed_out"
  }
}

fn undo_kind_string(kind: observation.UndoKind) -> String {
  case kind {
    observation.UndoUndone -> "undone"
    observation.UndoFailedKind -> "failed"
    observation.UndoCrashedKind -> "crashed"
    observation.UndoTimedOutKind -> "timed_out"
  }
}

/// A completed run emits `run_start`, one `step_start`/`step_stop` pair per
/// step, and `run_stop:completed`.
pub fn observation_events_test() {
  let collector = new_collector()
  let assert Ok(workflow) =
    saga.define("obs_completed", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })

  let assert Ok(sinal.SubscriptionCompletion(Ok(execution.Completed(0)), [])) =
    sinal.with_subscriptions(all_subscriptions(collector), fn() {
      execution.run(workflow, 0, execution.config())
    })

  events(collector)
  |> should.equal([
    "run_start", "step_start:s", "step_stop:s:succeeded", "run_stop:completed",
  ])
}

/// A failed run with a completed step whose undo fails: `run_stop:failed`
/// is emitted, and the undo's outcome is observed via `undo_stop`.
pub fn observation_events_failed_with_undo_failure_test() {
  let collector = new_collector()
  let assert Ok(workflow) =
    saga.define("obs_failed", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("a", fn(x: Int) { Ok(x) })
          |> saga.undo(fn(_i, _o) { Error(UndoBoom) }),
        )
      a |> saga.perform(saga.step("b", fn(_x: Int) { Error(Boom) }))
    })

  let assert Ok(sinal.SubscriptionCompletion(Ok(result), [])) =
    sinal.with_subscriptions(all_subscriptions(collector), fn() {
      execution.run(workflow, 0, execution.config())
    })
  let assert execution.Failed(_cause, _settlement) = result

  let recorded = events(collector)
  list.contains(recorded, "run_stop:failed") |> should.be_true
  list.contains(recorded, "undo_stop:a:failed") |> should.be_true
  list.contains(recorded, "step_stop:b:failed") |> should.be_true
}

/// A cancelled run emits `run_stop:cancelled`.
pub fn observation_events_cancelled_test() {
  let collector = new_collector()
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("obs_cancelled", fn(input) {
      input
      |> saga.perform(
        saga.step("blocked", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        }),
      )
    })

  let config = execution.Config(..execution.config(), settle_timeout: 0)

  let assert Ok(sinal.SubscriptionCompletion(_work_result, [])) =
    sinal.with_subscriptions(all_subscriptions(collector), fn() {
      probe.with_run(workflow, 0, config, fn(exec) {
        let assert Ok(_pid) = probe.wait_entered(gate, 2000)
        execution.cancel(exec)
        let assert Ok(execution.Cancelled(_reason, _settlement)) =
          execution.await(exec, 2000)
        Nil
      })
    })

  list.contains(events(collector), "run_stop:cancelled") |> should.be_true
}

/// A raising Sinal handler is isolated by Sinal itself (it is detached, and
/// the failure is reported through `on_failure`, never re-raised into the
/// coordinator) — the run's outcome is unaffected.
pub fn raising_handler_does_not_change_outcome_test() {
  let assert Ok(workflow) =
    saga.define("obs_raising", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })

  let raising_subscription =
    sinal.subscription(observation.run_started(), fn(_m, _d) {
      panic as "boom in handler"
    })

  let assert Ok(sinal.SubscriptionCompletion(Ok(execution.Completed(0)), _)) =
    sinal.with_subscriptions(sinal.subscriptions([raising_subscription]), fn() {
      execution.run(workflow, 0, execution.config())
    })
}
