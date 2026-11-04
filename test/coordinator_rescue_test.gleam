//// An unexpected raise in the coordinator's own message handling ends the
//// run through the same settlement and rollback as a step failure, with
//// redacted evidence, instead of losing the run and its undo journal.

import gleam/erlang/process
import gleam/list
import gleam/string
import gleam/time/duration
import gleeunit/should
import saga
import saga/execution
import saga/internal/coordinator
import saga/telemetry
import sinal
import support/probe

@external(erlang, "coordinator_rescue_ffi", "control")
fn control(
  execution: execution.Execution(o, e, u),
) -> process.Subject(coordinator.Control(o, e, u))

@external(erlang, "coordinator_rescue_ffi", "coerce")
fn coerce(value: a) -> b

@external(erlang, "coordinator_rescue_ffi", "cause_round_trips")
fn cause_round_trips() -> Bool

/// `reserve` commits with an undo, `ship` is in flight, and the coordinator
/// then raises while handling a message (a progress request whose reply is
/// not a subject, the stand-in for any internal defect). The run fails with
/// `CoordinatorCrashed`, `ship` is interrupted, and `reserve` is undone.
/// The evidence names the exception class and where it was raised, never
/// the terms it was raised with.
pub fn coordinator_raise_fails_the_run_and_undoes_committed_steps_test() {
  let undone = process.new_subject()
  let gate = probe.new_gate()
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("reserve", fn(x: Int) { Ok(x) })
        |> saga.undo(fn(request: saga.UndoRequest(Int, Int)) {
          process.send(undone, request.output)
          Ok(Nil)
        }),
      )
      |> saga.perform(
        saga.step("ship", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        }),
      )
    })
  let config =
    execution.config()
    |> execution.with_settle_timeout(duration.milliseconds(0))
  let assert Ok(run) = execution.start(workflow, 7, config)
  let assert Ok(_) = probe.wait_entered(gate, 2000)

  process.send(
    control(run),
    coordinator.ProgressRequest(coerce(#("secret-token", 42))),
  )

  let assert Ok(execution.Failed(
    execution.CoordinatorCrashed(crash),
    settlement,
  )) = execution.await(run, duration.seconds(5))
  process.receive(undone, 1000) |> should.equal(Ok(7))
  settlement.undone
  |> should.equal([saga.StepAddress([], "reserve", 1)])
  settlement.interrupted
  |> should.equal([saga.StepAddress([], "ship", 1)])
  crash.class |> should.equal(saga.ErrorClass)
  string.contains(crash.reason, "secret-token") |> should.be_false
  string.contains(crash.reason, "42") |> should.be_false
  string.contains(crash.reason, "/") |> should.be_true
}

/// A durable run saves its trigger and outcome in every checkpoint, so the
/// checkpoint schema carries `CoordinatorCrashed` like every other cause.
pub fn coordinator_crash_is_saved_in_checkpoints_test() {
  cause_round_trips() |> should.be_true
}

/// A second raise, after the first has started settling, ends the run at
/// once instead of trusting the coordinator to walk the journal: the
/// in-flight attempt is interrupted and the completed undoable step is
/// reported as not undone, with the coordinator's crash.
pub fn second_coordinator_raise_reports_the_journal_left_in_place_test() {
  let gate = probe.new_gate()
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("reserve", fn(x: Int) { Ok(x) })
        |> saga.undo(fn(_request) { Ok(Nil) }),
      )
      |> saga.perform(
        saga.step("ship", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        }),
      )
    })
  // A long settle window keeps `ship` in flight between the two raises.
  let config =
    execution.config()
    |> execution.with_settle_timeout(duration.seconds(30))
  let assert Ok(run) = execution.start(workflow, 7, config)
  let assert Ok(_) = probe.wait_entered(gate, 2000)
  let poison = coordinator.ProgressRequest(coerce(Nil))
  process.send(control(run), poison)
  process.send(control(run), poison)

  let assert Ok(execution.Failed(
    execution.CoordinatorCrashed(_),
    execution.Settlement(
      undone: [],
      undo_failures: [execution.UndoCrashed(reserve, _)],
      interrupted: [ship],
      sibling_failures: [execution.CoordinatorCrashed(_)],
      ..,
    ),
  )) = execution.await(run, duration.seconds(5))
  reserve |> should.equal(saga.StepAddress([], "reserve", 1))
  ship |> should.equal(saga.StepAddress([], "ship", 1))
}

/// After a raise, the coordinator kills every linked worker it never
/// recorded. In a durable runner the processes linked before the run
/// started (its storage guard and claim heartbeat) are not workers and are
/// kept; a recorded worker is kept for settlement. The durable control
/// subject is not reachable from tests, so this checks the selection the
/// coordinator applies to its links.
pub fn untracked_workers_exclude_baseline_and_recorded_pids_test() {
  let guard = process.spawn_unlinked(fn() { process.sleep_forever() })
  let heartbeat = process.spawn_unlinked(fn() { process.sleep_forever() })
  let recorded = process.spawn_unlinked(fn() { process.sleep_forever() })
  let orphan = process.spawn_unlinked(fn() { process.sleep_forever() })
  coordinator.untracked_workers(
    linked: [guard, recorded, heartbeat, orphan],
    baseline: [guard, heartbeat],
    recorded: [recorded],
  )
  |> should.equal([orphan])
  list.each([guard, heartbeat, recorded, orphan], process.kill)
}

fn stop_events(events: process.Subject(String)) -> sinal.SubscriptionPlan {
  sinal.subscriptions([
    sinal.subscription(telemetry.step_started(), fn(_m, d) {
      process.send(events, "step_start:" <> d.step)
    }),
    sinal.subscription(telemetry.step_stopped(), fn(_m, d) {
      let result = case d.result {
        telemetry.AttemptInterrupted -> "interrupted"
        _ -> "other"
      }
      process.send(events, "step_stop:" <> d.step <> ":" <> result)
    }),
    sinal.subscription(telemetry.undo_stopped(), fn(_m, d) {
      let result = case d.result {
        telemetry.UndoCrashedKind -> "crashed"
        _ -> "other"
      }
      process.send(events, "undo_stop:" <> d.step <> ":" <> result)
    }),
  ])
}

fn received(
  events: process.Subject(String),
  acc: List(String),
) -> List(String) {
  case process.receive(events, 0) {
    Ok(event) -> received(events, [event, ..acc])
    Error(Nil) -> list.reverse(acc)
  }
}

/// A second raise kills the in-flight attempt and emits its `step_stopped`
/// as `interrupted`, like the settle window does, so every started step
/// span is closed.
pub fn second_raise_closes_the_interrupted_attempt_span_test() {
  let events = process.new_subject()
  let gate = probe.new_gate()
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("reserve", fn(x: Int) { Ok(x) })
        |> saga.undo(fn(_request) { Ok(Nil) }),
      )
      |> saga.perform(
        saga.step("ship", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        }),
      )
    })
  let config =
    execution.config()
    |> execution.with_settle_timeout(duration.seconds(30))
  let assert Ok(sinal.SubscriptionCompletion(outcome, [])) =
    sinal.with_subscriptions(stop_events(events), fn() {
      let assert Ok(run) = execution.start(workflow, 7, config)
      let assert Ok(_) = probe.wait_entered(gate, 2000)
      let poison = coordinator.ProgressRequest(coerce(Nil))
      process.send(control(run), poison)
      process.send(control(run), poison)
      execution.await(run, duration.seconds(5))
    })
  let assert Ok(execution.Failed(execution.CoordinatorCrashed(_), _)) = outcome
  received(events, [])
  |> should.equal([
    "step_start:reserve", "step_stop:reserve:other", "step_start:ship",
    "step_stop:ship:interrupted",
  ])
}

/// A second raise during rollback kills the undo in flight and emits its
/// `undo_stopped` as crashed, matching the `UndoCrashed` it reports.
pub fn second_raise_closes_the_killed_undo_span_test() {
  let events = process.new_subject()
  let gate = probe.new_gate()
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("reserve", fn(x: Int) { Ok(x) })
        |> saga.undo(fn(_request) {
          probe.enter(gate)
          Ok(Nil)
        }),
      )
      |> saga.perform(saga.step("charge", fn(_x: Int) { Error("declined") }))
    })
  let assert Ok(sinal.SubscriptionCompletion(outcome, [])) =
    sinal.with_subscriptions(stop_events(events), fn() {
      let assert Ok(run) = execution.start(workflow, 7, execution.config())
      let assert Ok(_) = probe.wait_entered(gate, 2000)
      let poison = coordinator.ProgressRequest(coerce(Nil))
      process.send(control(run), poison)
      process.send(control(run), poison)
      execution.await(run, duration.seconds(5))
    })
  let assert Ok(execution.Failed(
    execution.StepFailed(_, "declined"),
    execution.Settlement(undo_failures: [execution.UndoCrashed(_, _)], ..),
  )) = outcome
  received(events, [])
  |> should.equal([
    "step_start:reserve", "step_stop:reserve:other", "step_start:charge",
    "step_stop:charge:other", "undo_stop:reserve:crashed",
  ])
}
