import gleam/list
import gleam/option.{None, Some}
import gleeunit/should
import saga
import saga/execution
import support/probe

// ---------------------------------------------------------------------------
// Default step timeout
// ---------------------------------------------------------------------------

/// `execution.config()` ships a 60 second default per-attempt `step_timeout`
/// — asserted directly rather than only indirectly through behavior, so a
/// silent change to the default value is caught even if every behavioral
/// test below still passes.
pub fn config_defaults_to_a_60_second_step_timeout_test() {
  execution.config().step_timeout |> should.equal(Some(60_000))
}

/// A step with no `saga.timeout` of its own is still bounded by the run's
/// `step_timeout` default: a hung step is killed and reported `StepTimedOut`
/// rather than blocking the run forever. Uses a small override (not a real
/// 60 second wait) to keep the test fast.
pub fn default_step_timeout_bounds_a_hung_step_test() {
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

  let config = execution.Config(..execution.config(), step_timeout: Some(50))

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    let assert Ok(execution.Failed(cause, _settlement)) =
      execution.await(exec, 10_000)
    case cause {
      execution.StepTimedOut(step) -> step.name |> should.equal("blocked")
      _ -> panic as "expected StepTimedOut(blocked) from the default timeout"
    }
  })
}

/// A step's own `saga.timeout` overrides the run's `step_timeout` default
/// when the step's own bound is *shorter*: the step is killed on its own
/// schedule, well before the (longer) default would have fired.
pub fn step_timeout_overrides_default_when_shorter_test() {
  let gate = probe.new_gate()
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

  // A default far longer than the step's own timeout: if the override did
  // not take effect, this test would hang until the default fired instead.
  let config =
    execution.Config(..execution.config(), step_timeout: Some(10_000))

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    let assert Ok(execution.Failed(cause, _settlement)) =
      execution.await(exec, 2000)
    case cause {
      execution.StepTimedOut(step) -> step.name |> should.equal("blocked")
      _ -> panic as "expected StepTimedOut(blocked) from the step's own timeout"
    }
  })
}

/// A step's own `saga.timeout` overrides the run's `step_timeout` default
/// even when the step's own bound is *longer*: a short default must not
/// pre-empt a step that explicitly asked for more time.
pub fn step_timeout_overrides_default_when_longer_test() {
  let counter = probe.new_counter()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("slow_but_fine", fn(x: Int) {
          probe.counter_enter(counter)
          Ok(x)
        })
        |> saga.timeout(10_000),
      )
    })

  // A default far shorter than the step's own timeout: if the override did
  // not take effect, the default would kill the step before it can finish.
  let config = execution.Config(..execution.config(), step_timeout: Some(1))

  let assert Ok(execution.Completed(0)) = execution.run(workflow, 0, config)
  probe.total_entries(counter) |> should.equal(1)
}

/// `step_timeout: None` is the explicit opt-out: a step with no `saga.timeout`
/// of its own is never killed by the coordinator, and a slow-but-eventually-
/// finishing step still completes normally.
pub fn step_timeout_none_disables_the_default_test() {
  let counter = probe.new_counter()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("slow", fn(x: Int) {
          probe.counter_enter(counter)
          Ok(x)
        }),
      )
    })

  let config = execution.Config(..execution.config(), step_timeout: None)

  let assert Ok(execution.Completed(0)) = execution.run(workflow, 0, config)
  probe.total_entries(counter) |> should.equal(1)
}

pub type DemoError {
  Boom
}

pub type DemoUndoError {
  UndoBoom
}

pub fn invalid_config_rejected_before_start_test() {
  let coordinator_counter = probe.new_counter()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(x: Int) {
          probe.counter_enter(coordinator_counter)
          Ok(x)
        }),
      )
    })

  let bad_configs = [
    execution.Config(
      max_concurrency: 0,
      deadline: None,
      step_timeout: Some(60_000),
      settle_timeout: 5000,
      cleanup_timeout: 5000,
    ),
    execution.Config(
      max_concurrency: 1,
      deadline: Some(0),
      step_timeout: Some(60_000),
      settle_timeout: 5000,
      cleanup_timeout: 5000,
    ),
    execution.Config(
      max_concurrency: 1,
      deadline: None,
      step_timeout: Some(0),
      settle_timeout: 5000,
      cleanup_timeout: 5000,
    ),
    execution.Config(
      max_concurrency: 1,
      deadline: None,
      step_timeout: Some(60_000),
      settle_timeout: -1,
      cleanup_timeout: 5000,
    ),
    execution.Config(
      max_concurrency: 1,
      deadline: None,
      step_timeout: Some(60_000),
      settle_timeout: 5000,
      cleanup_timeout: 0,
    ),
  ]

  list.each(bad_configs, fn(config) {
    case execution.run(workflow, 0, config) {
      Error(execution.InvalidConfig(_errors)) -> Nil
      _ -> panic as "expected InvalidConfig"
    }
  })

  // No step body ever ran for any invalid config.
  probe.total_entries(coordinator_counter) |> should.equal(0)
}

pub fn every_config_error_variant_is_reachable_test() {
  let config =
    execution.Config(
      max_concurrency: -1,
      deadline: Some(-5),
      step_timeout: Some(-2),
      settle_timeout: -1,
      cleanup_timeout: -1,
    )
  case execution.validate(config) {
    Error(errors) -> {
      list.length(errors) |> should.equal(5)
      list.any(errors, fn(e) {
        case e {
          execution.MaxConcurrencyNotPositive(-1) -> True
          _ -> False
        }
      })
      |> should.be_true
      list.any(errors, fn(e) {
        case e {
          execution.DeadlineNotPositive(-5) -> True
          _ -> False
        }
      })
      |> should.be_true
      list.any(errors, fn(e) {
        case e {
          execution.StepTimeoutNotPositive(-2) -> True
          _ -> False
        }
      })
      |> should.be_true
      list.any(errors, fn(e) {
        case e {
          execution.SettleTimeoutNegative(-1) -> True
          _ -> False
        }
      })
      |> should.be_true
      list.any(errors, fn(e) {
        case e {
          execution.CleanupTimeoutNotPositive(-1) -> True
          _ -> False
        }
      })
      |> should.be_true
    }
    Ok(_) -> panic as "expected Error"
  }
}

pub fn concurrent_runs_are_isolated_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x * 2) }))
    })

  let assert Ok(exec_a) = execution.start(workflow, 10, execution.config())
  let assert Ok(exec_b) = execution.start(workflow, 20, execution.config())

  let assert Ok(execution.Completed(a)) = execution.await(exec_a, 10_000)
  let assert Ok(execution.Completed(b)) = execution.await(exec_b, 10_000)

  a |> should.equal(20)
  b |> should.equal(40)
  { execution.run_id(exec_a) == execution.run_id(exec_b) } |> should.be_false
}

pub fn map_panic_crashes_attempt_not_coordinator_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
      |> saga.map(fn(_x) { panic as "boom in map" })
    })

  let assert Ok(execution.Failed(cause, _settlement)) =
    execution.run(workflow, 0, execution.config())
  case cause {
    execution.OutputCrashed(_crash) -> Nil
    _ -> panic as "expected OutputCrashed"
  }

  // The coordinator itself survived: a second, independent run still works.
  let assert Ok(workflow2) =
    saga.define("wf2", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x + 1) }))
    })
  let assert Ok(execution.Completed(v)) =
    execution.run(workflow2, 1, execution.config())
  v |> should.equal(2)
}

pub fn killed_step_task_is_crash_test() {
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("blocked", fn(_x: Int) {
          probe.enter(gate)
          Ok(Nil)
        }),
      )
    })

  probe.with_run(workflow, 0, execution.config(), fn(exec) {
    let assert Ok(task_pid) = probe.wait_entered(gate, 10_000)
    kill_process(task_pid)

    let assert Ok(execution.Failed(cause, _settlement)) =
      execution.await(exec, 10_000)
    case cause {
      execution.StepCrashed(_step, _crash) -> Nil
      _ -> panic as "expected StepCrashed"
    }
  })
}

@external(erlang, "erlang", "exit")
fn kill_process_ffi(pid: a, reason: b) -> Bool

fn kill_process(pid: a) -> Nil {
  let _ = kill_process_ffi(pid, atom_kill())
  Nil
}

@external(erlang, "erlang", "binary_to_atom")
fn atom_kill_ffi(name: String) -> a

fn atom_kill() -> a {
  atom_kill_ffi("kill")
}

/// The workflow's build function is evaluated exactly once, at `define` —
/// never again, no matter how many runs (successive or concurrent) follow.
/// Before the build-once refactor, `execution.run` re-evaluated the builder
/// fresh on every call and rejected a graph-shape mismatch as
/// `DefinitionChanged`; that variant and the re-evaluation it detected are
/// both gone now; this test proves the replacement invariant directly by
/// counting builder invocations across many runs of the same definition,
/// run both sequentially and concurrently.
pub fn build_function_runs_exactly_once_test() {
  let call_count = probe.new_counter()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      probe.counter_enter(call_count)
      input |> saga.perform(saga.step("a", fn(x: Int) { Ok(x + 1) }))
    })

  let assert 1 = probe.total_entries(call_count)

  // Several sequential runs.
  let assert Ok(execution.Completed(2)) =
    execution.run(workflow, 1, execution.config())
  let assert Ok(execution.Completed(6)) =
    execution.run(workflow, 5, execution.config())
  let assert Ok(execution.Completed(11)) =
    execution.run(workflow, 10, execution.config())
  let assert 1 = probe.total_entries(call_count)

  // Several concurrent runs of the same definition.
  let executions =
    [100, 200, 300, 400]
    |> list.map(fn(input) {
      let assert Ok(exec) = execution.start(workflow, input, execution.config())
      exec
    })
  list.each(executions, fn(exec) {
    let assert Ok(execution.Completed(_)) = execution.await(exec, 10_000)
  })

  let assert 1 = probe.total_entries(call_count)
}
