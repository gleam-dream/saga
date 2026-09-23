import gleam/list
import gleam/option.{None, Some}
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
      settle_timeout: 5000,
      cleanup_timeout: 5000,
    ),
    execution.Config(
      max_concurrency: 1,
      deadline: Some(0),
      settle_timeout: 5000,
      cleanup_timeout: 5000,
    ),
    execution.Config(
      max_concurrency: 1,
      deadline: None,
      settle_timeout: -1,
      cleanup_timeout: 5000,
    ),
    execution.Config(
      max_concurrency: 1,
      deadline: None,
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
      settle_timeout: -1,
      cleanup_timeout: -1,
    )
  case execution.validate(config) {
    Error(errors) -> {
      list.length(errors) |> should.equal(4)
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

  let assert Ok(execution.Completed(a)) = execution.await(exec_a, 2000)
  let assert Ok(execution.Completed(b)) = execution.await(exec_b, 2000)

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
    let assert Ok(task_pid) = probe.wait_entered(gate, 2000)
    kill_process(task_pid)

    let assert Ok(execution.Failed(cause, _settlement)) =
      execution.await(exec, 2000)
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

pub fn nondeterministic_builder_rejected_test() {
  let call_count = probe.new_counter()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      probe.counter_enter(call_count)
      case probe.total_entries(call_count) {
        1 ->
          // The define-time dry run: one step.
          input |> saga.perform(saga.step("a", fn(x: Int) { Ok(x) }))
        _ ->
          // Every later (real-run) evaluation: two steps. The shape no
          // longer matches what `define` recorded.
          input
          |> saga.perform(saga.step("a", fn(x: Int) { Ok(x) }))
          |> saga.perform(saga.step("b", fn(x: Int) { Ok(x) }))
      }
    })

  case execution.run(workflow, 0, execution.config()) {
    Ok(execution.Failed(execution.DefinitionChanged, _settlement)) -> Nil
    _other -> panic as "expected Failed(DefinitionChanged, _), got other outcome"
  }
}
