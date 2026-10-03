//// Every action of a run — a step attempt, a compensation decision, an undo
//// — ends either with a known result (it returned) or with an unknown
//// effect (it crashed or exited, timed out, or was interrupted). These tests
//// pin that the final `Outcome` alone names every action of the second kind,
//// for each outcome kind, through `execution.unknown_effects`.

import gleam/list
import gleeunit/should
import saga
import saga/execution
import support/probe

pub type DemoError {
  Boom
}

pub type DemoUndoError {
  UndoBoom
}

type Kind {
  Crashed
  TimedOut
  Interrupted
}

/// The outcome's unknown effects as `#(step name, action, kind)`, dropping
/// the crash detail.
fn effects(
  outcome: execution.Outcome(o, e, u),
) -> List(#(String, execution.Action, Kind)) {
  list.map(execution.unknown_effects(outcome), fn(effect) {
    #(effect.step.name, effect.action, case effect.ending {
      execution.ActionCrashed(_) -> Crashed
      execution.ActionTimedOut -> TimedOut
      execution.ActionInterrupted -> Interrupted
    })
  })
}

fn at(name: String) -> saga.StepAddress {
  saga.StepAddress(scope: [], name: name, occurrence: 1)
}

// ---------------------------------------------------------------------------
// Completed
// ---------------------------------------------------------------------------

/// Retrying typed errors to success leaves nothing unknown: the outcome is a
/// plain `Completed`, even though the step has a recovery decider.
pub fn retried_typed_errors_complete_with_known_effects_test() {
  let counter = probe.new_counter()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(_x: Int) {
          probe.counter_enter(counter)
          case probe.total_entries(counter) < 3 {
            True -> Error(Boom)
            False -> Ok(42)
          }
        })
        |> saga.compensate(max_attempts: 5, with: fn(_i, _f, _a) { saga.Retry }),
      )
    })

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  outcome |> should.equal(execution.Completed(42))
  execution.unknown_effects(outcome) |> should.equal([])
}

/// An attempt that crashed and was retried to success leaves the crashed
/// attempt's effect unknown: the run completes with that attempt named.
pub fn crash_retried_to_success_is_an_unknown_effect_test() {
  let counter = probe.new_counter()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(x: Int) {
          probe.counter_enter(counter)
          case probe.total_entries(counter) {
            1 -> panic as "effect performed, then crashed"
            _ -> Ok(x + 1)
          }
        })
        |> saga.compensate(max_attempts: 2, with: fn(_i, failure, _a) {
          case failure {
            saga.Crashed(_) -> saga.Retry
            _ -> panic as "expected Crashed"
          }
        }),
      )
    })

  let assert Ok(outcome) = execution.run(workflow, 1, execution.config())
  let assert execution.CompletedWithUnknownEffects(2, unknown) = outcome
  let assert [execution.UnknownEffect(step, execution.StepAttempt(1), ending)] =
    unknown
  step |> should.equal(at("flaky"))
  let assert execution.ActionCrashed(saga.Crash(saga.ErrorClass, _)) = ending
  execution.unknown_effects(outcome) |> should.equal(unknown)
}

/// A crash the decider answers with `Continue` is still an unknown effect:
/// the replacement output is known, the crashed attempt is not.
pub fn crash_continued_is_an_unknown_effect_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(_x: Int) -> Result(Int, DemoError) {
          panic as "crashed"
        })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          saga.Continue(7, saga.NoUndo)
        }),
      )
    })

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  let assert execution.CompletedWithUnknownEffects(7, _) = outcome
  effects(outcome)
  |> should.equal([#("flaky", execution.StepAttempt(1), Crashed)])
}

/// A timed-out attempt retried to success is named with its attempt number
/// and a timeout ending.
pub fn timeout_retried_to_success_is_an_unknown_effect_test() {
  let gate = probe.new_gate()
  let counter = probe.new_counter()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("slow", fn(x: Int) {
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
        |> saga.compensate(max_attempts: 2, with: fn(_i, _f, _a) { saga.Retry }),
      )
    })

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  let assert execution.CompletedWithUnknownEffects(0, _) = outcome
  effects(outcome)
  |> should.equal([#("slow", execution.StepAttempt(1), TimedOut)])
}

// ---------------------------------------------------------------------------
// Failed
// ---------------------------------------------------------------------------

/// A step with a decider whose single attempt returns a typed error and is
/// aborted fails with nothing unknown.
pub fn typed_error_aborted_fails_with_known_effects_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("charge", fn(_x: Int) -> Result(Int, DemoError) {
          Error(Boom)
        })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          saga.Abort(Boom)
        }),
      )
    })

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  let assert execution.Failed(execution.StepFailed(_, Boom), settlement) =
    outcome
  settlement.unknown_effects |> should.equal([])
  execution.unknown_effects(outcome) |> should.equal([])
}

/// The only attempt performs its effect and crashes; the decider aborts
/// with a typed error. The cause is `StepFailed`, as for an ordinary typed
/// failure, but the crashed attempt is named as an unknown effect.
pub fn crash_aborted_with_typed_error_is_an_unknown_effect_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("charge", fn(_x: Int) -> Result(Int, DemoError) {
          panic as "charged, then crashed"
        })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          saga.Abort(Boom)
        }),
      )
    })

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  let assert execution.Failed(execution.StepFailed(step, Boom), settlement) =
    outcome
  step |> should.equal(at("charge"))
  effects(outcome)
  |> should.equal([#("charge", execution.StepAttempt(1), Crashed)])
  settlement.unknown_effects |> should.equal(execution.unknown_effects(outcome))
}

/// Step `a` fails with a typed error while sibling `b` performs its effect
/// and crashes in the settle window; `b`'s decider aborts with a typed
/// error. `b` is a `StepFailed` sibling failure, and its crashed attempt is
/// an unknown effect.
pub fn sibling_crash_in_settle_window_is_an_unknown_effect_test() {
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      let a =
        input
        |> saga.perform(saga.step("a", fn(_x: Int) { Error(Boom) }))
      let b =
        input
        |> saga.perform(
          saga.step("b", fn(_x: Int) -> Result(Int, DemoError) {
            probe.enter(gate)
            panic as "effect performed, then crashed"
          })
          |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
            saga.Abort(Boom)
          }),
        )
      saga.both(a, b)
    })

  let config = execution.config() |> execution.with_max_concurrency(2)
  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    probe.open(gate)
    let assert Ok(outcome) = execution.await(exec, 10_000)
    let assert execution.Failed(execution.StepFailed(first, Boom), settlement) =
      outcome
    first |> should.equal(at("a"))
    settlement.sibling_failures
    |> should.equal([execution.StepFailed(at("b"), Boom)])
    effects(outcome)
    |> should.equal([#("b", execution.StepAttempt(1), Crashed)])
  })
}

/// Retry exhaustion names each attempt that crashed, by number, and not the
/// attempt that returned a typed error.
pub fn retry_limit_names_each_crashed_attempt_test() {
  let counter = probe.new_counter()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(_x: Int) -> Result(Int, DemoError) {
          probe.counter_enter(counter)
          case probe.total_entries(counter) {
            2 -> Error(Boom)
            _ -> panic as "crashed"
          }
        })
        |> saga.compensate(max_attempts: 3, with: fn(_i, _f, _a) { saga.Retry }),
      )
    })

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  let assert execution.Failed(execution.RetryLimitReached(_, _), _) = outcome
  effects(outcome)
  |> should.equal([
    #("flaky", execution.StepAttempt(1), Crashed),
    #("flaky", execution.StepAttempt(3), Crashed),
  ])
}

/// A compensation decision that crashes, or outlives `cleanup_timeout`, is
/// an unknown effect of that decision, numbered by the attempt it decided.
pub fn compensation_crash_and_timeout_are_unknown_effects_test() {
  let assert Ok(crashing) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(_x: Int) -> Result(Int, DemoError) { Error(Boom) })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          panic as "decider crashed"
        }),
      )
    })
  let assert Ok(outcome) = execution.run(crashing, 0, execution.config())
  let assert execution.Failed(execution.StepCrashed(..), _) = outcome
  effects(outcome)
  |> should.equal([#("s", execution.StepCompensation(1), Crashed)])

  let gate = probe.new_gate()
  let assert Ok(hanging) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(_x: Int) -> Result(Int, DemoError) { Error(Boom) })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          probe.enter(gate)
          saga.Abort(Boom)
        }),
      )
    })
  let config = execution.config() |> execution.with_cleanup_timeout(50)
  let assert Ok(outcome) = execution.run(hanging, 0, config)
  let assert execution.Failed(execution.StepTimedOut(..), _) = outcome
  effects(outcome)
  |> should.equal([#("s", execution.StepCompensation(1), TimedOut)])
}

/// An undo that returns an error is a known result; one that crashes or
/// outlives `cleanup_timeout` is an unknown effect of that undo.
pub fn undo_crash_and_timeout_are_unknown_effects_test() {
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      let refused =
        input
        |> saga.perform(
          saga.step("refused", fn(x: Int) { Ok(x) })
          |> saga.undo(fn(_i, _o) { Error(UndoBoom) }),
        )
      let crashing =
        refused
        |> saga.perform(
          saga.step("crashing", fn(x: Int) { Ok(x) })
          |> saga.undo(fn(_i, _o) { panic as "undo crashed" }),
        )
      let hanging =
        crashing
        |> saga.perform(
          saga.step("hanging", fn(x: Int) { Ok(x) })
          |> saga.undo(fn(_i, _o) {
            probe.enter(gate)
            Ok(Nil)
          }),
        )
      hanging
      |> saga.perform(saga.step("last", fn(_x: Int) { Error(Boom) }))
    })

  let config = execution.config() |> execution.with_cleanup_timeout(50)
  let assert Ok(outcome) = execution.run(workflow, 0, config)
  let assert execution.Failed(execution.StepFailed(_, Boom), settlement) =
    outcome
  list.length(settlement.undo_failures) |> should.equal(3)
  effects(outcome)
  |> should.equal([
    #("hanging", execution.StepUndo, TimedOut),
    #("crashing", execution.StepUndo, Crashed),
  ])
}

// ---------------------------------------------------------------------------
// Cancelled
// ---------------------------------------------------------------------------

/// A cancellation that kills an attempt when the settle window closes names
/// it as interrupted; a completed, undone step is not an unknown effect.
pub fn cancel_interrupting_an_attempt_is_an_unknown_effect_test() {
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("first", fn(x: Int) { Ok(x) })
        |> saga.undo(fn(_i, _o) { Ok(Nil) }),
      )
      |> saga.perform(
        saga.step("blocked", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        }),
      )
    })

  let config = execution.config() |> execution.with_settle_timeout(0)
  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    execution.cancel(exec)
    let assert Ok(outcome) = execution.await(exec, 10_000)
    let assert execution.Cancelled(execution.CancelRequested, settlement) =
      outcome
    settlement.undone |> should.equal([at("first")])
    settlement.interrupted |> should.equal([at("blocked")])
    effects(outcome)
    |> should.equal([#("blocked", execution.StepAttempt(1), Interrupted)])
  })
}

/// A compensation decision still running when the settle window closes is
/// interrupted, and named as such.
pub fn cancel_interrupting_a_compensation_is_an_unknown_effect_test() {
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(_x: Int) -> Result(Int, DemoError) { Error(Boom) })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          probe.enter(gate)
          saga.Abort(Boom)
        }),
      )
    })

  let config = execution.config() |> execution.with_settle_timeout(0)
  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    execution.cancel(exec)
    let assert Ok(outcome) = execution.await(exec, 10_000)
    let assert execution.Cancelled(_, _) = outcome
    effects(outcome)
    |> should.equal([#("s", execution.StepCompensation(1), Interrupted)])
  })
}

// ---------------------------------------------------------------------------
// Unresolved
// ---------------------------------------------------------------------------

/// A crash the decider answers with `Hold` is unresolved, and the crashed
/// attempt is named as an unknown effect.
pub fn crash_held_is_an_unknown_effect_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(_x: Int) -> Result(Int, DemoError) {
          panic as "crashed"
        })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          saga.Hold(Boom)
        }),
      )
    })

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  let assert execution.Unresolved(_, Boom, settlement) = outcome
  effects(outcome)
  |> should.equal([#("s", execution.StepAttempt(1), Crashed)])
  settlement.unknown_effects |> should.equal(execution.unknown_effects(outcome))
}

/// A typed error the decider holds leaves nothing unknown.
pub fn typed_error_held_has_known_effects_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(_x: Int) -> Result(Int, DemoError) { Error(Boom) })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          saga.Hold(Boom)
        }),
      )
    })

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  let assert execution.Unresolved(_, Boom, _) = outcome
  execution.unknown_effects(outcome) |> should.equal([])
}
