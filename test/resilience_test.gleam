/// Regression tests for independent-review findings on mailbox hygiene,
/// crash-class fidelity, `progress` after a run ends, and retry-vs-settling
/// causes. Ported in spirit from the reviewer's REPRO4/REPRO5/REPRO6.
import gleam/erlang/process
import gleam/list
import gleam/string
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

// ---------------------------------------------------------------------------
// REPRO4 / finding 5: no monitor `Down` leak into the caller's mailbox.
// ---------------------------------------------------------------------------

pub fn run_does_not_leak_monitor_down_test() {
  probe.flush_mailbox()
  let assert Ok(workflow) =
    saga.define("l", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })
  let before = probe.mailbox_length()
  let assert Ok(execution.Completed(0)) =
    execution.run(workflow, 0, execution.config())
  let assert Ok(execution.Completed(0)) =
    execution.run(workflow, 0, execution.config())
  let assert Ok(execution.Completed(0)) =
    execution.run(workflow, 0, execution.config())
  // No polling needed: `run` only returns after the coordinator's outcome
  // (and, per the fix, its demonitor-with-flush) has already happened.
  probe.mailbox_length() |> should.equal(before)
}

pub fn await_does_not_leak_monitor_down_test() {
  probe.flush_mailbox()
  let assert Ok(workflow) =
    saga.define("l2", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })
  let before = probe.mailbox_length()
  let assert Ok(exec) = execution.start(workflow, 0, execution.config())
  let assert Ok(execution.Completed(0)) = execution.await(exec, 2000)
  probe.mailbox_length() |> should.equal(before)
}

// ---------------------------------------------------------------------------
// REPRO5 / finding 8: `progress` on a finished run reports `ExecutionEnded`
// promptly, not a timeout.
// ---------------------------------------------------------------------------

pub fn progress_after_end_reports_execution_ended_test() {
  let assert Ok(workflow) =
    saga.define("p", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })
  let assert Ok(exec) = execution.start(workflow, 0, execution.config())
  let assert Ok(execution.Completed(0)) = execution.await(exec, 1000)
  case execution.progress(exec, timeout: 2000) {
    Error(execution.ExecutionEnded) -> Nil
    other ->
      panic as { "expected ExecutionEnded, got " <> string.inspect(other) }
  }
}

// ---------------------------------------------------------------------------
// REPRO6 / finding 7: a native `throw` is reported with `ThrowClass`, not
// folded into `ErrorClass`.
// ---------------------------------------------------------------------------

pub fn throw_reported_as_throw_class_test() {
  let assert Ok(workflow) =
    saga.define("t", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(_x: Int) -> Result(Int, Nil) { probe.native_throw() }),
      )
    })

  let assert Ok(execution.Failed(cause, _settlement)) =
    execution.run(workflow, 0, execution.config())
  case cause {
    execution.StepCrashed(_step, crash) ->
      crash.class |> should.equal(saga.ThrowClass)
    _ -> panic as "expected StepCrashed"
  }
}

// Note: `exit_reason_to_string`'s `Abnormal(reason)` branch now formats
// `reason` with `string.inspect` instead of discarding it (see
// `saga/execution.gleam` and `saga/internal/coordinator.gleam`). This is
// not covered by a black-box test here: both the coordinator (which traps
// exits from the moment it starts) and its tasks only ever observably exit
// with `Normal`, `Killed` (untrappable, via `process.kill`), or a crash
// already wrapped by `ffi.rescue` (reported through `Crash`, not
// `ExitReason`) from this package's own public surface — there is no
// reachable, non-flaky way to make either exit with an arbitrary
// `Abnormal(reason)` without a white-box hook into the coordinator.

// ---------------------------------------------------------------------------
// Finding 13: a retry refused because settling already began is a distinct
// cause from RetryLimitReached.
// ---------------------------------------------------------------------------

pub fn retry_refused_while_settling_is_distinct_cause_test() {
  let decider_gate = probe.new_gate()
  let fail_gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("retryable", fn(_x: Int) -> Result(Int, Nil) { Error(Nil) })
          |> saga.compensate(max_attempts: 5, with: fn(_i, _f, _a) {
            // Blocks until settling has already begun (via `fail_gate`
            // below), so this decision is received by the coordinator only
            // after the run has a different, unrelated terminal trigger.
            probe.enter(decider_gate)
            saga.RetryAfter(1)
          }),
        )
      let b =
        input
        |> saga.perform(
          saga.step("failing", fn(_x: Int) -> Result(Int, Nil) {
            probe.enter(fail_gate)
            Error(Nil)
          }),
        )
      saga.both(a, b)
    })

  let assert Ok(exec) = execution.start(workflow, 0, execution.config())
  // Both attempts start together (`both` schedules independent nodes): wait
  // for the decider to be blocked, and for `failing` to be blocked, then
  // release `failing` first so its own terminal failure begins settling
  // while the decider is still deciding on `retryable`.
  let assert Ok(_pid) = probe.wait_entered(decider_gate, 2000)
  let assert Ok(_pid2) = probe.wait_entered(fail_gate, 2000)
  probe.open(fail_gate)
  // Give settling a moment to actually begin before releasing the decider.
  process.sleep(100)
  probe.open(decider_gate)

  let assert Ok(execution.Failed(cause, settlement)) =
    execution.await(exec, 3000)
  case cause {
    execution.StepFailed(step, Nil) -> step.name |> should.equal("failing")
    _ -> panic as "expected the primary cause to be `failing`'s StepFailed"
  }
  list.any(settlement.sibling_failures, fn(sibling) {
    case sibling {
      execution.RetrySuperseded(step, _last) -> step.name == "retryable"
      _ -> False
    }
  })
  |> should.be_true
}

// ---------------------------------------------------------------------------
// Finding 6: real durations, and AttemptInterrupted on a settle-sweep kill.
// ---------------------------------------------------------------------------

fn collect_step_stop_durations(
  collector: process.Subject(#(String, observation.AttemptKind, Int)),
) -> sinal.SubscriptionPlan {
  sinal.subscriptions([
    sinal.subscription(observation.step_stopped(), fn(m, d) {
      process.send(collector, #(d.step, d.result, m.duration))
    }),
  ])
}

pub fn step_stopped_reports_real_duration_test() {
  let collector = process.new_subject()
  let assert Ok(workflow) =
    saga.define("dur", fn(input) {
      input
      |> saga.perform(
        saga.step("slow", fn(x: Int) {
          process.sleep(60)
          Ok(x)
        }),
      )
    })

  let assert Ok(sinal.SubscriptionCompletion(Ok(execution.Completed(0)), [])) =
    sinal.with_subscriptions(collect_step_stop_durations(collector), fn() {
      execution.run(workflow, 0, execution.config())
    })

  let assert Ok(#("slow", observation.AttemptSucceeded, duration)) =
    process.receive(collector, 500)
  // Not a hard-coded 0: the step body slept 60ms, so its reported duration
  // must be a meaningfully positive measurement.
  { duration >= 30 } |> should.be_true
}

pub fn settle_sweep_kill_emits_attempt_interrupted_test() {
  let collector = process.new_subject()
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("interrupt", fn(input) {
      let blocked =
        input
        |> saga.perform(
          saga.step("blocked", fn(x: Int) {
            probe.enter(gate)
            Ok(x)
          }),
        )
      let failing =
        input
        |> saga.perform(
          saga.step("failing", fn(_x: Int) -> Result(Int, Nil) { Error(Nil) }),
        )
      saga.both(blocked, failing)
    })

  let config = execution.Config(..execution.config(), settle_timeout: 100)
  let assert Ok(sinal.SubscriptionCompletion(Ok(_result), [])) =
    sinal.with_subscriptions(collect_step_stop_durations(collector), fn() {
      execution.run(workflow, 0, config)
    })

  drain_until_interrupted(collector, 10)
}

fn drain_until_interrupted(
  collector: process.Subject(#(String, observation.AttemptKind, Int)),
  remaining: Int,
) -> Nil {
  case remaining <= 0 {
    True -> panic as "expected an AttemptInterrupted step_stopped event"
    False ->
      case process.receive(collector, 200) {
        Error(_) -> panic as "expected an AttemptInterrupted step_stopped event"
        Ok(#("blocked", observation.AttemptInterrupted, _duration)) -> Nil
        Ok(_other) -> drain_until_interrupted(collector, remaining - 1)
      }
  }
}
