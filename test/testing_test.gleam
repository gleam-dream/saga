import gleam/list
import gleeunit/should
import saga
import saga/execution
import saga/testing
import support/probe

/// `wait_until` returns the first `Progress` snapshot `matching` accepts,
/// well before `within` elapses.
pub fn wait_until_succeeds_when_predicate_is_met_test() {
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("blocked", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        }),
      )
    })

  probe.with_run(workflow, 0, execution.config(), fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    let assert Ok(progress) =
      testing.wait_until(
        exec,
        matching: fn(p) {
          list.any(p.steps, fn(sp) {
            sp.address.name == "blocked" && sp.state == execution.Attempting(1)
          })
        },
        within: 10_000,
      )
    progress.phase |> should.equal(execution.Running)
  })
}

/// A predicate that never matches reports `WaitTimedOut` once `within`
/// elapses, without ever hanging past it. Uses a step that blocks for the
/// whole wait (released only after `wait_until` returns) so the run is still
/// alive throughout — otherwise a run that finishes first would report
/// `RunEnded` instead, which is a different, correctly-distinguished case
/// covered by `wait_until_reports_run_ended_test` below.
pub fn wait_until_times_out_test() {
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("blocked", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        }),
      )
    })

  probe.with_run(workflow, 0, execution.config(), fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    let result =
      testing.wait_until(exec, matching: fn(_p) { False }, within: 200)
    result |> should.equal(Error(testing.WaitTimedOut))
  })
}

/// Once the run has ended, `wait_until` reports `RunEnded` instead of
/// waiting out the rest of `within`.
pub fn wait_until_reports_run_ended_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })

  let assert Ok(exec) = execution.start(workflow, 0, execution.config())
  let assert Ok(execution.Completed(0)) = execution.await(exec, 10_000)

  let result =
    testing.wait_until(exec, matching: fn(_p) { False }, within: 10_000)
  result |> should.equal(Error(testing.RunEnded))
}
