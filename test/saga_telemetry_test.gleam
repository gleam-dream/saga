import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleeunit/should
import saga
import saga/execution
import saga/telemetry
import sinal
import sinal/correlation.{type Correlation}
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
    sinal.subscription(telemetry.run_started(), fn(_m, _d) {
      record(collector, "run_start")
    }),
    sinal.subscription(telemetry.run_stopped(), fn(_m, d) {
      record(collector, "run_stop:" <> outcome_kind_string(d.outcome))
    }),
    sinal.subscription(telemetry.step_started(), fn(_m, d) {
      record(collector, "step_start:" <> d.step)
    }),
    sinal.subscription(telemetry.step_stopped(), fn(_m, d) {
      record(
        collector,
        "step_stop:" <> d.step <> ":" <> attempt_kind_string(d.result),
      )
    }),
    sinal.subscription(telemetry.compensation_stopped(), fn(_m, d) {
      record(
        collector,
        "compensate_stop:" <> d.step <> ":" <> decision_kind_string(d.decision),
      )
    }),
    sinal.subscription(telemetry.undo_stopped(), fn(_m, d) {
      record(
        collector,
        "undo_stop:" <> d.step <> ":" <> undo_kind_string(d.result),
      )
    }),
  ])
}

fn outcome_kind_string(kind: telemetry.OutcomeKind) -> String {
  case kind {
    telemetry.OutcomeCompleted -> "completed"
    telemetry.OutcomeCompletedWithUnknownEffects -> "completed_unknown"
    telemetry.OutcomeFailed -> "failed"
    telemetry.OutcomeCancelled -> "cancelled"
    telemetry.OutcomeUnresolved -> "unresolved"
  }
}

fn attempt_kind_string(kind: telemetry.AttemptKind) -> String {
  case kind {
    telemetry.AttemptSucceeded -> "succeeded"
    telemetry.AttemptFailed -> "failed"
    telemetry.AttemptUnknown -> "unknown"
    telemetry.AttemptCrashed -> "crashed"
    telemetry.AttemptTimedOut -> "timed_out"
    telemetry.AttemptInterrupted -> "interrupted"
  }
}

fn decision_kind_string(kind: telemetry.DecisionKind) -> String {
  case kind {
    telemetry.DecisionRetry -> "retry"
    telemetry.DecisionContinue -> "continue"
    telemetry.DecisionAbort -> "abort"
    telemetry.DecisionHold -> "hold"
    telemetry.DecisionCrashed -> "crashed"
    telemetry.DecisionTimedOut -> "timed_out"
  }
}

fn undo_kind_string(kind: telemetry.UndoKind) -> String {
  case kind {
    telemetry.UndoUndone -> "undone"
    telemetry.UndoFailedKind -> "failed"
    telemetry.UndoCrashedKind -> "crashed"
    telemetry.UndoTimedOutKind -> "timed_out"
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

  let config = execution.config() |> execution.with_settle_timeout(0)

  let assert Ok(sinal.SubscriptionCompletion(_work_result, [])) =
    sinal.with_subscriptions(all_subscriptions(collector), fn() {
      probe.with_run(workflow, 0, config, fn(exec) {
        let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
        execution.cancel(exec)
        let assert Ok(execution.Cancelled(_reason, _settlement)) =
          execution.await(exec, 10_000)
        Nil
      })
    })

  list.contains(events(collector), "run_stop:cancelled") |> should.be_true
}

/// A step-timeout kill with a `compensate` decider attached still emits its
/// own `step_stop:<name>:timed_out` for the killed attempt — distinct from
/// the decider's own `compensate_stop` event — and with the killed attempt's
/// real elapsed duration (close to its configured `timeout`), not the near-0
/// duration of the (much faster) decider that runs afterwards.
pub fn step_timeout_with_decider_emits_step_stopped_test() {
  let gate = probe.new_gate()
  let durations = process.new_subject()
  let counter = probe.new_counter()

  let subscriptions =
    sinal.subscriptions([
      sinal.subscription(telemetry.step_stopped(), fn(m, d) {
        case d.result {
          telemetry.AttemptTimedOut ->
            process.send(durations, #(d.step, m.duration))
          _ -> Nil
        }
      }),
    ])

  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(x: Int) {
          probe.counter_enter(counter)
          case probe.total_entries(counter) {
            1 -> {
              probe.enter(gate)
              Ok(x)
            }
            _ -> Ok(x)
          }
        })
        |> saga.timeout(50)
        |> saga.compensate(max_attempts: 2, with: fn(_i, failure, _a) {
          case failure {
            saga.TimedOut -> saga.Retry
            _ -> panic as "expected TimedOut"
          }
        }),
      )
    })

  let assert Ok(sinal.SubscriptionCompletion(Ok(_result), [])) =
    sinal.with_subscriptions(subscriptions, fn() {
      execution.run(workflow, 0, execution.config())
    })

  let assert Ok(#(step, duration)) = process.receive(durations, 1000)
  step |> should.equal("flaky")
  // The killed attempt was blocked on `gate` for at least its 50ms
  // `timeout` before being killed; a near-0 duration here would mean the
  // event was (wrongly) emitted using the decider's own much-faster
  // duration instead of the killed attempt's real elapsed time.
  { duration >= 40 } |> should.be_true
}

/// The plain (no `compensate` decider) step-timeout path also reports the
/// killed attempt's real elapsed duration, not a near-0 duration from a
/// freshly-taken timestamp overwriting the original `started_at`.
pub fn step_timeout_without_decider_reports_real_duration_test() {
  let gate = probe.new_gate()
  let durations = process.new_subject()

  let subscriptions =
    sinal.subscriptions([
      sinal.subscription(telemetry.step_stopped(), fn(m, d) {
        case d.result {
          telemetry.AttemptTimedOut ->
            process.send(durations, #(d.step, m.duration))
          _ -> Nil
        }
      }),
    ])

  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("blocked", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        })
        |> saga.timeout(50),
      )
    })

  let assert Ok(sinal.SubscriptionCompletion(Ok(_result), [])) =
    sinal.with_subscriptions(subscriptions, fn() {
      execution.run(workflow, 0, execution.config())
    })

  let assert Ok(#(step, duration)) = process.receive(durations, 1000)
  step |> should.equal("blocked")
  { duration >= 40 } |> should.be_true
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
    sinal.subscription(telemetry.run_started(), fn(_m, _d) {
      panic as "boom in handler"
    })

  let assert Ok(sinal.SubscriptionCompletion(Ok(execution.Completed(0)), _)) =
    sinal.with_subscriptions(sinal.subscriptions([raising_subscription]), fn() {
      execution.run(workflow, 0, execution.config())
    })
}

/// `execution.with_correlation` reaches every event of the run, and a local
/// run carries no durable execution id.
pub fn correlation_reaches_every_event_test() {
  let seen = process.new_subject()
  let assert Ok(order) = correlation.from_string("order-42")
  let assert Ok(workflow) =
    saga.define("correlated", fn(input) {
      input
      |> saga.perform(
        saga.step("a", fn(x: Int) { Ok(x) })
        |> saga.undo(fn(_input, _output) { Ok(Nil) }),
      )
      |> saga.perform(saga.step("b", fn(_x: Int) { Error(Boom) }))
    })
  let plan =
    sinal.subscriptions([
      sinal.subscription(telemetry.run_started(), fn(_m, d) {
        process.send(seen, #("run_start", d.correlation, d.execution))
      }),
      sinal.subscription(telemetry.run_stopped(), fn(_m, d) {
        process.send(seen, #("run_stop", d.correlation, d.execution))
      }),
      sinal.subscription(telemetry.step_started(), fn(_m, d) {
        process.send(seen, #("step_start", d.correlation, d.execution))
      }),
      sinal.subscription(telemetry.step_stopped(), fn(_m, d) {
        process.send(seen, #("step_stop", d.correlation, d.execution))
      }),
      sinal.subscription(telemetry.undo_stopped(), fn(_m, d) {
        process.send(seen, #("undo_stop", d.correlation, d.execution))
      }),
    ])
  let config = execution.config() |> execution.with_correlation(order)
  let assert Ok(sinal.SubscriptionCompletion(Ok(execution.Failed(..)), [])) =
    sinal.with_subscriptions(plan, fn() { execution.run(workflow, 1, config) })
  let received = drain(seen, [])
  list.map(received, fn(event) { event.0 })
  |> should.equal([
    "run_start", "step_start", "step_stop", "step_start", "step_stop",
    "undo_stop", "run_stop",
  ])
  list.all(received, fn(event) { event.1 == Some(order) && event.2 == None })
  |> should.be_true
}

/// Without a correlation the field is absent, and `execution.kind` gives
/// `CompletedWithUnknownEffects` its own kind, as `run_stop` does.
pub fn outcome_kind_is_shared_with_run_stop_test() {
  let kinds = process.new_subject()
  let attempts = probe.new_counter()
  let assert Ok(workflow) =
    saga.define("kinds", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(x: Int) {
          probe.counter_enter(attempts)
          case probe.total_entries(attempts) {
            1 -> panic as "first attempt crashes"
            _ -> Ok(x)
          }
        })
        |> saga.compensate(max_attempts: 2, with: fn(_input, _failure, _a) {
          saga.Retry
        }),
      )
    })
  let plan =
    sinal.subscriptions([
      sinal.subscription(telemetry.run_stopped(), fn(_m, d) {
        process.send(kinds, #(d.outcome, d.correlation))
      }),
    ])
  let assert Ok(sinal.SubscriptionCompletion(Ok(outcome), [])) =
    sinal.with_subscriptions(plan, fn() {
      execution.run(workflow, 1, execution.config())
    })
  let assert execution.CompletedWithUnknownEffects(1, [_]) = outcome
  execution.kind(outcome)
  |> should.equal(telemetry.OutcomeCompletedWithUnknownEffects)
  process.receive(kinds, 1000)
  |> should.equal(Ok(#(telemetry.OutcomeCompletedWithUnknownEffects, None)))
  telemetry.outcome_kind_name(execution.kind(outcome))
  |> should.equal("completed_with_unknown_effects")
}

pub fn describe_cause_names_the_step_test() {
  let address =
    saga.StepAddress(scope: ["checkout"], name: "charge", occurrence: 1)
  execution.describe_cause(execution.StepTimedOut(address))
  |> should.equal("step checkout/charge timed out")
  execution.describe_cause(execution.DeadlineExceeded)
  |> should.equal("the run passed its deadline")
}

fn drain(
  subject: Subject(#(String, Option(Correlation), Option(String))),
  acc: List(#(String, Option(Correlation), Option(String))),
) -> List(#(String, Option(Correlation), Option(String))) {
  case process.receive(subject, 0) {
    Ok(event) -> drain(subject, [event, ..acc])
    Error(Nil) -> list.reverse(acc)
  }
}
