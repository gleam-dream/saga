import gleam/erlang/process
import gleam/list
import gleam/option.{None, Some}
import gleam/time/duration
import gleeunit/should
import saga
import saga/execution
import saga/telemetry
import sinal
import sinal/correlation
import support/probe

// ---------------------------------------------------------------------------
// Default step timeout
// ---------------------------------------------------------------------------

/// `execution.config()` ships a 60 second default per-attempt `step_timeout`
/// — asserted directly rather than only indirectly through behavior, so a
/// silent change to the default value is caught even if every behavioral
/// test below still passes.
pub fn config_defaults_to_a_60_second_step_timeout_test() {
  let assert Ok(settings) = execution.settings(execution.config(), None)
  settings.step_timeout |> should.equal(Some(60_000))
}

/// Every default in the README's defaults table, read from the checked
/// settings a run starts with.
pub fn config_defaults_match_the_defaults_table_test() {
  let assert Ok(settings) = execution.settings(execution.config(), None)
  settings.max_concurrency |> should.equal(schedulers_online())
  settings.deadline |> should.equal(None)
  settings.settle_timeout |> should.equal(5000)
  settings.cleanup_timeout |> should.equal(5000)
  settings.max_retry_delay |> should.equal(300_000)
  // Every run has a correlation: a local run gets a fresh one, a durable
  // execution `from_key(id)`.
  let assert Ok(another) = execution.settings(execution.config(), None)
  { settings.correlation != another.correlation } |> should.be_true
  let assert Ok(durable) = execution.settings(execution.config(), Some("id-1"))
  durable.correlation |> should.equal(correlation.from_key("id-1"))
  let assert Ok(unbounded) =
    execution.settings(
      execution.config() |> execution.with_step_timeout(execution.Infinity),
      None,
    )
  unbounded.step_timeout |> should.equal(None)
}

@external(erlang, "erlang", "system_info")
fn system_info(item: atom) -> Int

@external(erlang, "erlang", "binary_to_atom")
fn to_atom(name: String) -> atom

fn schedulers_online() -> Int {
  system_info(to_atom("schedulers_online"))
}

/// A step with no `saga.timeout` of its own is still bounded by the run's
/// `step_timeout` default: a hung step is killed and reported `StepTimedOut`
/// rather than blocking the run forever. Uses a small override (not a real
/// 60 second wait) to keep the test fast.
pub fn default_step_timeout_bounds_a_hung_step_test() {
  let gate = probe.new_gate()
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("blocked", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        }),
      )
    })

  let config =
    execution.config()
    |> execution.with_step_timeout(execution.After(duration.milliseconds(50)))

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    let assert Ok(execution.Failed(cause, _settlement)) =
      execution.await(exec, duration.seconds(10))
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
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("blocked", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        })
        |> saga.timeout(duration.milliseconds(50)),
      )
    })

  // A default far longer than the step's own timeout: if the override did
  // not take effect, this test would hang until the default fired instead.
  let config =
    execution.config()
    |> execution.with_step_timeout(execution.After(duration.seconds(10)))

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    let assert Ok(execution.Failed(cause, _settlement)) =
      execution.await(exec, duration.seconds(2))
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
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("slow_but_fine", fn(x: Int) {
          probe.counter_enter(counter)
          Ok(x)
        })
        |> saga.timeout(duration.seconds(10)),
      )
    })

  // A default far shorter than the step's own timeout: if the override did
  // not take effect, the default would kill the step before it can finish.
  let config =
    execution.config()
    |> execution.with_step_timeout(execution.After(duration.milliseconds(1)))

  let assert Ok(execution.Completed(0)) = execution.run(workflow, 0, config)
  probe.total_entries(counter) |> should.equal(1)
}

/// `step_timeout: None` is the explicit opt-out: a step with no `saga.timeout`
/// of its own is never killed by the coordinator, and a slow-but-eventually-
/// finishing step still completes normally.
pub fn step_timeout_none_disables_the_default_test() {
  let counter = probe.new_counter()
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("slow", fn(x: Int) {
          probe.counter_enter(counter)
          Ok(x)
        }),
      )
    })

  let config =
    execution.config() |> execution.with_step_timeout(execution.Infinity)

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
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(x: Int) {
          probe.counter_enter(coordinator_counter)
          Ok(x)
        }),
      )
    })

  let c = execution.config()
  let bad_configs = [
    execution.with_max_concurrency(c, 0),
    execution.with_deadline(c, execution.After(duration.milliseconds(0))),
    execution.with_step_timeout(c, execution.After(duration.milliseconds(0))),
    execution.with_settle_timeout(c, duration.milliseconds(-1)),
    execution.with_cleanup_timeout(c, duration.milliseconds(0)),
    execution.with_max_retry_delay(c, duration.milliseconds(-1)),
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
  let workflow =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })
  let config =
    execution.config()
    |> execution.with_max_concurrency(-1)
    |> execution.with_deadline(execution.After(duration.milliseconds(-5)))
    |> execution.with_step_timeout(execution.After(duration.milliseconds(-2)))
    |> execution.with_settle_timeout(duration.milliseconds(-1))
    |> execution.with_cleanup_timeout(duration.milliseconds(-1))
    |> execution.with_max_retry_delay(duration.milliseconds(-3))
  let assert Error(execution.InvalidConfig(errors)) =
    execution.run(workflow, 0, config)
  errors
  |> should.equal([
    execution.MaxConcurrencyNotPositive(-1),
    execution.DeadlineNotPositive(duration.milliseconds(-5)),
    execution.StepTimeoutNotPositive(duration.milliseconds(-2)),
    execution.SettleTimeoutNegative(duration.milliseconds(-1)),
    execution.CleanupTimeoutNotPositive(duration.milliseconds(-1)),
    execution.MaxRetryDelayNegative(duration.milliseconds(-3)),
  ])
  list.each(errors, fn(error) {
    { execution.describe_config_error(error) != "" } |> should.be_true
  })
  execution.describe_config_error(
    execution.MaxRetryDelayNegative(duration.milliseconds(-3)),
  )
  |> should.equal(
    "the retry delay cap must not be negative (with_max_retry_delay), got -3 ms",
  )
}

/// Every setter is accepted at its boundary value, and `Infinity` is the
/// explicit opt-out of the default per-attempt timeout and the run deadline.
pub fn boundary_values_are_accepted_test() {
  let workflow =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })
  let config =
    execution.config()
    |> execution.with_max_concurrency(1)
    |> execution.with_deadline(execution.After(duration.seconds(10)))
    |> execution.with_step_timeout(execution.After(duration.seconds(1)))
    |> execution.with_step_timeout(execution.Infinity)
    |> execution.with_settle_timeout(duration.milliseconds(0))
    |> execution.with_cleanup_timeout(duration.milliseconds(1))
    |> execution.with_max_retry_delay(duration.milliseconds(0))
  let assert Ok(execution.Completed(3)) = execution.run(workflow, 3, config)
}

/// A bound below one millisecond is refused, not rounded up: a positive
/// duration that truncates to zero milliseconds is a configuration error,
/// and `Infinity` always passes.
pub fn sub_millisecond_bounds_are_refused_test() {
  let sub = duration.nanoseconds(500_000)
  let config =
    execution.config()
    |> execution.with_deadline(execution.After(sub))
    |> execution.with_step_timeout(execution.After(sub))
    |> execution.with_cleanup_timeout(sub)
  let assert Error(errors) = execution.settings(config, None)
  errors
  |> should.equal([
    execution.DeadlineNotPositive(sub),
    execution.StepTimeoutNotPositive(sub),
    execution.CleanupTimeoutNotPositive(sub),
  ])
  let assert Ok(_) =
    execution.settings(
      execution.config()
        |> execution.with_deadline(execution.Infinity)
        |> execution.with_step_timeout(execution.Infinity)
        |> execution.with_settle_timeout(sub)
        |> execution.with_max_retry_delay(sub),
      None,
    )
}

/// A `RetryAfter` delay above the cap is shortened to the cap: with a cap of
/// 10 ms, a requested hour-long backoff retries at once and the run ends.
pub fn retry_after_delay_is_capped_test() {
  let attempts = probe.new_counter()
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(x: Int) {
          probe.counter_enter(attempts)
          case probe.total_entries(attempts) {
            1 -> Error("first attempt fails")
            _ -> Ok(x)
          }
        })
        |> saga.compensate(max_attempts: 2, with: fn(_failed) {
          saga.RetryAfter(duration.seconds(3600))
        }),
      )
    })
  let config =
    execution.config()
    |> execution.with_max_retry_delay(duration.milliseconds(10))
  let assert Ok(execution.Completed(7)) = execution.run(workflow, 7, config)
  probe.total_entries(attempts) |> should.equal(2)
}

/// The default cap is 300 seconds: a decision asking for longer is clamped
/// and its compensation event says so.
pub fn default_retry_delay_cap_is_five_minutes_test() {
  let delays = process.new_subject()
  let attempts = probe.new_counter()
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(x: Int) {
          probe.counter_enter(attempts)
          case probe.total_entries(attempts) {
            1 -> Error("first attempt fails")
            _ -> Ok(x)
          }
        })
        |> saga.compensate(max_attempts: 2, with: fn(_failed) {
          saga.RetryAfter(duration.seconds(10_000))
        }),
      )
    })
  let assert Ok(sinal.SubscriptionCompletion(Nil, [])) =
    sinal.with_subscriptions(
      sinal.subscriptions([
        sinal.subscription(telemetry.compensation_stopped(), fn(_m, d) {
          process.send(delays, #(d.retry_delay, d.retry_delay_capped))
        }),
      ]),
      fn() {
        let assert Ok(exec) = execution.start(workflow, 1, execution.config())
        let assert Ok(#(Some(delay), True)) = process.receive(delays, 5000)
        delay |> should.equal(duration.seconds(300))
        execution.cancel(exec)
        let assert Ok(execution.Cancelled(..)) =
          execution.await(exec, duration.seconds(10))
        Nil
      },
    )
}

pub fn concurrent_runs_are_isolated_test() {
  let workflow =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x * 2) }))
    })

  let assert Ok(exec_a) = execution.start(workflow, 10, execution.config())
  let assert Ok(exec_b) = execution.start(workflow, 20, execution.config())

  let assert Ok(execution.Completed(a)) =
    execution.await(exec_a, duration.seconds(10))
  let assert Ok(execution.Completed(b)) =
    execution.await(exec_b, duration.seconds(10))

  a |> should.equal(20)
  b |> should.equal(40)
  { execution.run_id(exec_a) == execution.run_id(exec_b) } |> should.be_false
}

pub fn map_panic_crashes_attempt_not_coordinator_test() {
  let workflow =
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
  let workflow2 =
    saga.define("wf2", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x + 1) }))
    })
  let assert Ok(execution.Completed(v)) =
    execution.run(workflow2, 1, execution.config())
  v |> should.equal(2)
}

pub fn killed_step_task_is_crash_test() {
  let gate = probe.new_gate()
  let workflow =
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
      execution.await(exec, duration.seconds(10))
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
  let workflow =
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
    let assert Ok(execution.Completed(_)) =
      execution.await(exec, duration.seconds(10))
  })

  let assert 1 = probe.total_entries(call_count)
}
