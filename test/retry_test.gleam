import gleeunit/should
import saga
import saga/execution
import support/probe

pub type DemoError {
  Boom
  Declined
}

pub type DemoUndoError {
  UndoBoom
}

pub fn retry_until_success_test() {
  let counter = probe.new_counter()
  let workflow =
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
        |> saga.compensate(max_attempts: 5, with: fn(_failed) { saga.Retry }),
      )
    })

  let assert Ok(execution.Completed(output)) =
    execution.run(workflow, 0, execution.config())
  output |> should.equal(42)
  probe.total_entries(counter) |> should.equal(3)
}

pub fn retry_limit_triggers_rollback_test() {
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("always_fails", fn(_x: Int) { Error(Boom) })
        |> saga.compensate(max_attempts: 3, with: fn(_failed) { saga.Retry }),
      )
    })

  let assert Ok(execution.Failed(cause, _settlement)) =
    execution.run(workflow, 0, execution.config())

  case cause {
    execution.RetryLimitReached(step, _last) ->
      step.name |> should.equal("always_fails")
    _ -> panic as "expected RetryLimitReached"
  }
}

pub fn compensation_receives_attempt_numbers_test() {
  let counter = probe.new_counter()
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(_x: Int) { Error(Boom) })
        |> saga.compensate(max_attempts: 3, with: fn(failed) {
          let saga.FailedAttempt(failure: failure, ..) = failed
          probe.counter_enter(counter)
          let entries = probe.total_entries(counter)
          // 1-based attempt numbers: 1, 2, 3
          case failed.attempt == entries {
            True -> Nil
            False -> panic as "attempt number mismatch"
          }
          case failed.attempts_left == 3 - failed.attempt {
            True -> Nil
            False -> panic as "remaining mismatch"
          }
          case failure {
            saga.Returned(Boom) -> Nil
            _ -> panic as "expected Returned(Boom)"
          }
          saga.Retry
        }),
      )
    })

  let assert Ok(execution.Failed(_cause, _settlement)) =
    execution.run(workflow, 0, execution.config())
  probe.total_entries(counter) |> should.equal(3)
}

pub fn compensation_decisions_test() {
  // Abort: fails immediately with the given error.
  let abort_wf =
    saga.define("abort_wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(_x: Int) { Error(Boom) })
        |> saga.compensate(max_attempts: 3, with: fn(_failed) {
          saga.Abort(Declined)
        }),
      )
    })
  let assert Ok(execution.Failed(cause, _)) =
    execution.run(abort_wf, 0, execution.config())
  case cause {
    execution.StepFailed(_, Declined) -> Nil
    _ -> panic as "expected StepFailed(Declined)"
  }

  // No compensation attached at all: a single failure is terminal.
  let no_compensate_wf =
    saga.define("no_compensate_wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(_x: Int) { Error(Boom) }))
    })
  let assert Ok(execution.Failed(cause2, _)) =
    execution.run(no_compensate_wf, 0, execution.config())
  case cause2 {
    execution.StepFailed(_, Boom) -> Nil
    _ -> panic as "expected StepFailed(Boom)"
  }

  // RetryAfter: eventually succeeds after a delayed retry.
  let retry_counter = probe.new_counter()
  let retry_after_wf =
    saga.define("retry_after_wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(_x: Int) {
          probe.counter_enter(retry_counter)
          case probe.total_entries(retry_counter) < 2 {
            True -> Error(Boom)
            False -> Ok(Nil)
          }
        })
        |> saga.compensate(max_attempts: 3, with: fn(_failed) {
          saga.RetryAfter(1)
        }),
      )
    })
  let assert Ok(execution.Completed(Nil)) =
    execution.run(retry_after_wf, 0, execution.config())
}
