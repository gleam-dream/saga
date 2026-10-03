import gleam/time/duration
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

/// A slow sibling is still running when a fast sibling fails. Increment 1
/// has no settle-timeout/forced-kill machinery yet (that is increment 2),
/// so the natural behavior is: the coordinator stops admitting new work but
/// lets the in-flight sibling finish on its own, then undoes it like any
/// other completed step (reverse completion order).
pub fn failure_with_active_siblings_settles_test() {
  let slow_gate = probe.new_gate()
  let slow_undo_counter = probe.new_counter()

  let workflow =
    saga.define("siblings", fn(input) {
      let slow =
        input
        |> saga.perform(
          saga.step("slow", fn(x: Int) {
            probe.enter(slow_gate)
            Ok(x)
          })
          |> saga.undo(fn(_undo) {
            probe.counter_enter(slow_undo_counter)
            Ok(Nil)
          }),
        )
      let fast =
        input
        |> saga.perform(saga.step("fast_fail", fn(_x: Int) { Error(Boom) }))
      saga.both(slow, fast)
    })

  // `slow` and `fast_fail` must both be admitted at once for this test's
  // "the fast sibling fails while the slow one is still active" scenario
  // to happen at all — `max_concurrency` is set explicitly (rather than
  // relying on `config()`'s scheduler-count default) so this passes under
  // a single-scheduler `+S 1:1` run too.
  let config = execution.config() |> execution.with_max_concurrency(2)
  probe.with_run(workflow, 0, config, fn(exec) {
    // Let the slow sibling start, then release it once the run has already
    // failed via the fast sibling.
    let assert Ok(_slow_pid) = probe.wait_entered(slow_gate, 10_000)
    probe.open(slow_gate)

    let assert Ok(execution.Failed(cause, settlement)) =
      execution.await(exec, duration.seconds(10))
    case cause {
      execution.StepFailed(step, Boom) -> step.name |> should.equal("fast_fail")
      _ -> panic as "expected StepFailed(fast_fail, Boom)"
    }

    // The slow sibling completed and was undone (not orphaned).
    probe.total_entries(slow_undo_counter) |> should.equal(1)
    settlement.undone
    |> should.equal([saga.StepAddress(scope: [], name: "slow", occurrence: 1)])
  })
}
