/// Test-support helpers for synchronizing with a running `saga.Workflow`,
/// built entirely on `saga/execution`'s public `progress` API. Saga
/// deliberately ships nothing here beyond this one polling helper: the rest
/// of what a test needs to coordinate with a blocked step (a gate the step
/// body enters, released by the test) is generic BEAM concurrency, not
/// anything Saga-specific — see README.md's "Testing workflows" section for
/// a trimmed recipe.
import saga/execution.{type Execution, type Progress}
import saga/internal/ffi

/// Why `wait_until` did not return a matching `Progress`.
///
/// `execution.progress` (unlike `execution.await`) has no single-owner
/// restriction — any process holding the `Execution` value may call it — so
/// there is no `NotOwner` case here to mirror `execution.AwaitError`'s.
pub type WaitError {
  /// `matching` never accepted a snapshot before `within` milliseconds
  /// elapsed overall.
  WaitTimedOut
  /// The run ended (the coordinator exited) before `matching` accepted a
  /// snapshot — `execution.progress`'s own `ExecutionEnded`.
  RunEnded
}

/// Polls `execution.progress(execution, ..)` until `matching` accepts a
/// snapshot, or `within` milliseconds elapse overall, whichever comes
/// first. Each poll is a real (short) message round-trip with the
/// coordinator — never a fixed `process.sleep` — so this returns as soon as
/// `matching` is satisfied instead of waiting out a guessed duration. The
/// deadline is tracked with the monotonic clock, so it is immune to wall-clock
/// adjustments during the wait.
///
/// **Polling can miss a state that passes through quickly.** `matching` sees
/// only whatever phase/step-state snapshot happens to be current at each
/// poll; a state entered and left again between two polls is never observed.
/// Write `matching` to accept the target state *or anything at or beyond
/// it*, not the target state alone. For example, waiting for a step to reach
/// `Compensating` should also accept `RetryScheduled` (the decision may
/// already have been made and scheduled by the time a poll lands) rather
/// than failing because the run raced past `Compensating` before this
/// helper's next poll — a predicate that only accepts `Compensating` itself
/// can time out on a perfectly healthy run.
pub fn wait_until(
  execution: Execution(o, e, u),
  matching matching: fn(Progress) -> Bool,
  within within: Int,
) -> Result(Progress, WaitError) {
  let deadline = ffi.monotonic_time() + within
  poll(execution, matching, deadline)
}

fn poll(
  execution: Execution(o, e, u),
  matching: fn(Progress) -> Bool,
  deadline: Int,
) -> Result(Progress, WaitError) {
  let remaining = deadline - ffi.monotonic_time()
  case remaining <= 0 {
    True -> Error(WaitTimedOut)
    False -> {
      let poll_timeout = case remaining < 50 {
        True -> remaining
        False -> 50
      }
      case execution.progress(execution, poll_timeout) {
        Ok(snapshot) ->
          case matching(snapshot) {
            True -> Ok(snapshot)
            False -> poll(execution, matching, deadline)
          }
        Error(execution.ExecutionEnded) -> Error(RunEnded)
        Error(execution.ProgressTimedOut) -> poll(execution, matching, deadline)
      }
    }
  }
}
