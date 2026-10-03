import gleam/erlang/process
import gleam/list
import gleam/string
import gleeunit/should
import saga
import saga/execution
import saga/internal/ffi
import support/probe

pub type DemoError {
  Boom
}

pub type DemoUndoError {
  UndoBoom
}

// ---------------------------------------------------------------------------
// Deadlines and timeouts
// ---------------------------------------------------------------------------

/// A blocked step that never releases, with a tiny deadline and no settle
/// window: the run fails with `DeadlineExceeded`, the blocked step is
/// reported `interrupted` (never undone), and any already-completed step is
/// still rolled back.
pub fn deadline_interrupts_run_test() {
  let gate = probe.new_gate()
  let undo_counter = probe.new_counter()

  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("a", fn(x: Int) { Ok(x) })
          |> saga.undo(fn(_i, _o) {
            probe.counter_enter(undo_counter)
            Ok(Nil)
          }),
        )
      a
      |> saga.perform(
        saga.step("blocked", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        }),
      )
    })

  let config =
    execution.config()
    |> execution.with_deadline(50)
    |> execution.with_settle_timeout(0)

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    let assert Ok(execution.Failed(cause, settlement)) =
      execution.await(exec, 10_000)
    cause |> should.equal(execution.DeadlineExceeded)
    settlement.interrupted
    |> list.map(fn(a) { a.name })
    |> should.equal(["blocked"])
    settlement.undone |> list.map(fn(a) { a.name }) |> should.equal(["a"])
    probe.total_entries(undo_counter) |> should.equal(1)
  })
}

/// The deadline fires while a step is waiting on its `RetryAfter` backoff:
/// the wait is interrupted immediately rather than left to elapse.
pub fn deadline_during_backoff_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(_x: Int) { Error(Boom) })
        |> saga.compensate(max_attempts: 5, with: fn(_i, _f, _a) {
          saga.RetryAfter(60_000)
        }),
      )
    })

  let config =
    execution.config()
    |> execution.with_deadline(50)
    |> execution.with_settle_timeout(0)

  let assert Ok(execution.Failed(cause, _settlement)) =
    execution.run(workflow, 0, config)
  cause |> should.equal(execution.DeadlineExceeded)
}

/// A step's own `timeout` fires while it is attempting: the task is killed,
/// its effect is unknown, and with no `compensate` attached the failure is
/// terminal `StepTimedOut`.
pub fn step_timeout_reports_timed_out_test() {
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

  probe.with_run(workflow, 0, execution.config(), fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    let assert Ok(execution.Failed(cause, _settlement)) =
      execution.await(exec, 10_000)
    case cause {
      execution.StepTimedOut(step) -> step.name |> should.equal("blocked")
      _ -> panic as "expected StepTimedOut(blocked)"
    }
  })
}

/// A timed-out attempt with `compensate` attached is offered a recovery
/// decision on `TimedOut`, and `Retry` starts a genuinely fresh attempt.
pub fn step_timeout_recovery_can_retry_test() {
  let gate = probe.new_gate()
  let counter = probe.new_counter()

  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(x: Int) {
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
        |> saga.compensate(max_attempts: 2, with: fn(_i, failure, _a) {
          case failure {
            saga.TimedOut -> saga.Retry
            _ -> panic as "expected TimedOut"
          }
        }),
      )
    })

  // The first attempt was killed mid-flight by its own `timeout` — its
  // effect is unknown, even though `Retry` let the run reach a normal
  // output — so the outcome must say so rather than reporting a plain
  // `Completed` that silently drops it.
  let assert Ok(execution.CompletedWithUnknownEffects(0, unknown_effects)) =
    execution.run(workflow, 0, execution.config())
  unknown_effects
  |> should.equal([
    execution.UnknownEffect(
      saga.StepAddress(scope: [], name: "flaky", occurrence: 1),
      execution.StepAttempt(1),
      execution.ActionTimedOut,
    ),
  ])
  probe.total_entries(counter) |> should.equal(2)
}

/// A late `AttemptDone` message arriving from an already-timed-out (and
/// killed) task is discarded via sequence correlation — it must not be
/// mistaken for a fresh attempt's result.
pub fn late_result_after_timeout_is_discarded_test() {
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("blocked", fn(x: Int) {
          probe.enter(gate)
          Ok(x)
        })
        |> saga.timeout(50)
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          saga.Abort(Boom)
        }),
      )
    })

  probe.with_run(workflow, 0, execution.config(), fn(exec) {
    let assert Ok(pid) = probe.wait_entered(gate, 10_000)
    let assert Ok(execution.Failed(cause, _settlement)) =
      execution.await(exec, 10_000)
    cause
    |> should.equal(execution.StepFailed(
      saga.StepAddress(scope: [], name: "blocked", occurrence: 1),
      Boom,
    ))
    // The gate task is still blocked (it was killed, so releasing the gate
    // is a no-op on a dead process) — this just proves no crash occurs and
    // the coordinator has already moved on.
    probe.open(gate)
    let _ = pid
    Nil
  })
}

/// An undo action's `cleanup_timeout` fires: it is killed, recorded as
/// `UndoTimedOut`, and rollback continues to the next journal entry.
pub fn undo_timeout_recorded_and_rollback_continues_test() {
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("a", fn(x: Int) { Ok(x) })
          |> saga.undo(fn(_i, _o) {
            probe.enter(gate)
            Ok(Nil)
          }),
        )
      a |> saga.perform(saga.step("b", fn(_x: Int) { Error(Boom) }))
    })

  let config = execution.config() |> execution.with_cleanup_timeout(50)

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    let assert Ok(execution.Failed(_cause, settlement)) =
      execution.await(exec, 10_000)
    case settlement.undo_failures {
      [execution.UndoTimedOut(step)] -> step.name |> should.equal("a")
      _ -> panic as "expected a single UndoTimedOut(a)"
    }
    settlement.undone |> should.equal([])
  })
}

/// A compensation decider's `cleanup_timeout` fires: it is killed, recorded
/// as `CompensationTimedOut`, and the step's own failure is still the
/// terminal cause.
pub fn compensation_timeout_recorded_test() {
  let gate = probe.new_gate()
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(_x: Int) { Error(Boom) })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          probe.enter(gate)
          saga.Abort(Boom)
        }),
      )
    })

  let config = execution.config() |> execution.with_cleanup_timeout(50)

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
    let assert Ok(execution.Failed(cause, settlement)) =
      execution.await(exec, 10_000)
    case cause {
      execution.StepTimedOut(step) -> step.name |> should.equal("s")
      _ -> panic as "expected StepTimedOut(s) from the killed compensation"
    }
    case settlement.compensation_failures {
      [execution.CompensationTimedOut(step)] -> step.name |> should.equal("s")
      _ -> panic as "expected a single CompensationTimedOut(s)"
    }
  })
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

/// Cancelling with two active siblings: the run moves to `Settling`;
/// releasing one sibling lets it complete and be undone; the other is
/// killed once the settle window closes and reported `interrupted`.
pub fn cancel_with_active_siblings_test() {
  let releasable_gate = probe.new_gate()
  let blocked_gate = probe.new_gate()
  let undo_counter = probe.new_counter()

  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      let releasable =
        input
        |> saga.perform(
          saga.step("releasable", fn(x: Int) {
            probe.enter(releasable_gate)
            Ok(x)
          })
          |> saga.undo(fn(_i, _o) {
            probe.counter_enter(undo_counter)
            Ok(Nil)
          }),
        )
      let blocked =
        input
        |> saga.perform(
          saga.step("blocked", fn(x: Int) {
            probe.enter(blocked_gate)
            Ok(x)
          }),
        )
      saga.both(releasable, blocked)
    })

  // Both siblings must be able to attempt concurrently for this test to
  // exercise "cancel while one is still active" at all — `max_concurrency`
  // is set explicitly here (rather than relying on `config()`'s
  // scheduler-count default) so the test passes under a single-scheduler
  // `+S 1:1` run too.
  let config =
    execution.config()
    |> execution.with_settle_timeout(100)
    |> execution.with_max_concurrency(2)

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_) = probe.wait_entered(releasable_gate, 10_000)
    let assert Ok(_) = probe.wait_entered(blocked_gate, 10_000)

    execution.cancel(exec)

    let assert Ok(progress) = execution.progress(exec, 10_000)
    progress.phase |> should.equal(execution.Settling)

    probe.open(releasable_gate)

    let assert Ok(execution.Cancelled(reason, settlement)) =
      execution.await(exec, 10_000)
    reason |> should.equal(execution.CancelRequested)
    settlement.undone
    |> list.map(fn(a) { a.name })
    |> should.equal(["releasable"])
    settlement.interrupted
    |> list.map(fn(a) { a.name })
    |> should.equal(["blocked"])
    probe.total_entries(undo_counter) |> should.equal(1)
  })
}

/// Cancelling a run that has already completed is a harmless no-op.
pub fn cancel_after_completion_is_noop_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })
  let assert Ok(exec) = execution.start(workflow, 0, execution.config())
  let assert Ok(execution.Completed(0)) = execution.await(exec, 10_000)
  execution.cancel(exec)
}

/// `cancel` may be called any number of times; it is idempotent.
pub fn cancel_is_idempotent_test() {
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
    execution.cancel(exec)
    execution.cancel(exec)
    execution.cancel(exec)
    probe.open(gate)
    let assert Ok(execution.Cancelled(execution.CancelRequested, _)) =
      execution.await(exec, 10_000)
    Nil
  })
}

/// If a step happens to complete before its `Cancel` message is processed
/// (mailbox order), that completion counts as completed and is undone on
/// rollback rather than reported `interrupted`.
pub fn completion_processed_before_cancel_is_undone_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(x: Int) { Ok(x) })
        |> saga.undo(fn(_i, _o) { Ok(Nil) }),
      )
    })

  let assert Ok(exec) = execution.start(workflow, 0, execution.config())
  // No gate: the step is expected to have completed by the time cancel is
  // requested, which is deterministic here because it does no blocking I/O.
  execution.cancel(exec)
  let assert Ok(outcome) = execution.await(exec, 10_000)
  case outcome {
    execution.Completed(0) -> Nil
    execution.Cancelled(_, settlement) ->
      settlement.undone
      |> list.map(fn(a) { a.name })
      |> should.equal(["s"])
    _ -> panic as "expected Completed or Cancelled with s undone"
  }
}

// ---------------------------------------------------------------------------
// Process death
// ---------------------------------------------------------------------------

/// The owning process exits: the coordinator treats this as a cancellation
/// (`OwnerExited`), settles, rolls back completed steps, and exits itself
/// (observed here via a monitor on `execution.pid`, from a third process).
///
/// The step is gated so it cannot complete until this test has confirmed
/// (via its own monitor on the owner process) that the owner has actually
/// exited first. Without this, the step's own trivial work has no forced
/// ordering against the owner's exit: nothing stops the coordinator from
/// admitting and completing "s" before it ever processes `OwnerDown`, in
/// which case the run legitimately completes with nothing to roll back and
/// `undo_counter` never reaches 1 — a race the build-once refactor's speedup
/// made easy to lose, not a behavior this test should depend on winning.
pub fn owner_exit_cancels_and_rolls_back_test() {
  let undo_counter = probe.new_counter()
  let coordinator_pid_subject = process.new_subject()
  let step_gate = probe.new_gate()

  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(x: Int) {
          probe.enter(step_gate)
          Ok(x)
        })
        |> saga.undo(fn(_i, _o) {
          probe.counter_enter(undo_counter)
          Ok(Nil)
        }),
      )
    })

  let owner_done = process.new_subject()
  let owner_pid =
    process.spawn(fn() {
      let assert Ok(exec) = execution.start(workflow, 0, execution.config())
      process.send(coordinator_pid_subject, execution.pid(exec))
      // Exit immediately without awaiting, simulating an owner crash.
      process.send(owner_done, Nil)
      Nil
    })

  let assert Ok(coordinator_pid) =
    process.receive(coordinator_pid_subject, 10_000)

  // Wait for the owner process to have genuinely exited before letting "s"
  // proceed, so `OwnerDown` is guaranteed to reach the coordinator ahead of
  // the step's own completion.
  let owner_monitor = process.monitor(owner_pid)
  let owner_down_selector =
    process.new_selector()
    |> process.select_specific_monitor(owner_monitor, fn(down) { down })
  let _owner_down = process.selector_receive_forever(owner_down_selector)
  let _ = process.receive(owner_done, 1000)

  let monitor = process.monitor(coordinator_pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })

  probe.open(step_gate)

  let _down = process.selector_receive_forever(selector)
  probe.await_total_entries(undo_counter, 1, 10_000)
}

/// If the coordinator itself is killed externally, `await` returns `Lost`,
/// and its spawned task processes die with it (no leaked processes).
pub fn coordinator_kill_terminates_tasks_test() {
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

  let assert Ok(exec) = execution.start(workflow, 0, execution.config())
  let assert Ok(task_pid) = probe.wait_entered(gate, 10_000)

  process.kill(execution.pid(exec))

  case execution.await(exec, 10_000) {
    Error(execution.Lost(_crash)) -> Nil
    _other -> panic as "expected Lost, got something else"
  }

  // The task the killed coordinator had spawned (linked) dies with it.
  wait_until_dead(task_pid, 10_000)
}

/// Waits for `pid` to exit via a monitor (no polling): if it is already
/// dead, `process.monitor` itself resolves immediately by delivering a
/// synthetic `Down`, so this never blocks on an already-finished process.
fn wait_until_dead(pid: process.Pid, timeout_ms: Int) -> Nil {
  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_down) { Nil })
  case process.selector_receive(selector, timeout_ms) {
    Ok(Nil) -> Nil
    Error(_) -> panic as "expected task process to die with its coordinator"
  }
}

/// Only the process that called `start` may `await` an `Execution`.
pub fn await_not_owner_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })
  let assert Ok(exec) = execution.start(workflow, 0, execution.config())

  let reply = process.new_subject()
  process.spawn(fn() { process.send(reply, execution.await(exec, 10_000)) })
  let assert Ok(Error(execution.NotOwner)) = process.receive(reply, 10_000)

  let assert Ok(execution.Completed(0)) = execution.await(exec, 10_000)
  Nil
}

/// A second `await` after the outcome has already been consumed returns
/// `AlreadyAwaited` rather than hanging or timing out.
pub fn await_twice_already_awaited_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })
  let assert Ok(exec) = execution.start(workflow, 0, execution.config())
  let assert Ok(execution.Completed(0)) = execution.await(exec, 10_000)
  let assert Error(execution.AlreadyAwaited) = execution.await(exec, 10_000)
  Nil
}

/// A second `await` reports `AlreadyAwaited` promptly — it must not wait
/// out the full timeout, since there is no process-dictionary flag left to
/// short-circuit it: it is derived entirely from a fresh monitor's
/// immediate `noproc` plus an empty mailbox.
pub fn await_twice_is_prompt_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })
  let assert Ok(exec) = execution.start(workflow, 0, execution.config())
  let assert Ok(execution.Completed(0)) = execution.await(exec, 10_000)
  let before = system_time_ms()
  let assert Error(execution.AlreadyAwaited) = execution.await(exec, 10_000)
  let elapsed = system_time_ms() - before
  // A generous bound, well under the 10s timeout: the point is that this
  // resolves from a fresh monitor's immediate `noproc` plus an empty
  // mailbox rather than idling out the timeout, not that it beats some
  // tight budget — under heavy scheduler load, this process's own wakeup
  // can be delayed too.
  { elapsed < 5000 } |> should.be_true
}

/// After a coordinator is killed (never awaited), `await` reports `Lost`. A
/// second `await` on the same `Execution` must not hang for the full
/// timeout either, and — since the original monitor's `Down` was already
/// consumed by the first call — settles as `AlreadyAwaited`.
pub fn await_after_lost_then_second_await_is_prompt_test() {
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

  let assert Ok(exec) = execution.start(workflow, 0, execution.config())
  let assert Ok(_pid) = probe.wait_entered(gate, 10_000)
  process.kill(execution.pid(exec))

  case execution.await(exec, 10_000) {
    Error(execution.Lost(_crash)) -> Nil
    _other -> panic as "expected Lost, got something else"
  }

  let before = system_time_ms()
  let assert Error(execution.AlreadyAwaited) = execution.await(exec, 10_000)
  let elapsed = system_time_ms() - before
  // See `await_twice_is_prompt_test`: a generous bound under the 10s
  // timeout, not a tight one — the property under test is "does not idle
  // out the timeout", which tolerates scheduler jitter under load.
  { elapsed < 5000 } |> should.be_true
}

/// If the coordinator dies *during* an in-flight `await` (rather than
/// before it was ever awaited), that same `await` call must observe the
/// original monitor's real `Down` and report `Lost` — never `AlreadyAwaited`
/// — and must leave nothing behind in the owner's mailbox. Repeated many
/// times to pressure the race between `await`'s two monitors (the original,
/// armed since `start`, and the fresh one `await` creates for this call):
/// both become eligible at once when the kill lands while `await` is
/// already blocked in `selector_receive`, and `await_signal`'s zero-timeout
/// check on `FreshDown` is what keeps that race from ever surfacing the
/// fresh monitor's uninformative `noproc` instead of the original's real
/// reason.
pub fn coordinator_killed_mid_await_reports_lost_test() {
  let iterations = list.repeat(Nil, 50)
  list.each(iterations, fn(_i) {
    probe.flush_mailbox()
    let mailbox_before = probe.mailbox_length()
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

    let assert Ok(exec) = execution.start(workflow, 0, execution.config())
    let assert Ok(_pid) = probe.wait_entered(gate, 10_000)

    // Kill from a fresh process, concurrently with the `await` call below,
    // so the coordinator's exit can land while `await` is already blocked
    // in `selector_receive` rather than strictly before it.
    process.spawn(fn() { process.kill(execution.pid(exec)) })

    case execution.await(exec, 10_000) {
      Error(execution.Lost(_crash)) -> Nil
      other ->
        panic as {
          "expected Lost, got "
          <> string.inspect(other)
          <> " (mailbox mid-await race)"
        }
    }

    probe.mailbox_length() |> should.equal(mailbox_before)
  })
}

/// `start`/`await`/`await` never grows the calling process's own process
/// dictionary — the "already awaited" tracking is derived statelessly from
/// a fresh monitor plus a zero-timeout mailbox check, not from a
/// process-dictionary flag that would otherwise accumulate one entry per
/// `Execution`.
pub fn await_does_not_grow_process_dictionary_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })

  let before = probe.dictionary_size()
  let assert Ok(exec) = execution.start(workflow, 0, execution.config())
  let assert Ok(execution.Completed(0)) = execution.await(exec, 10_000)
  let assert Error(execution.AlreadyAwaited) = execution.await(exec, 10_000)
  probe.dictionary_size() |> should.equal(before)
}

/// Repeated `run`s (start+await-to-completion in one call) never grow the
/// calling process's own process dictionary, nor leave anything behind in
/// its mailbox.
pub fn run_does_not_grow_process_dictionary_or_mailbox_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
    })

  probe.flush_mailbox()
  let dict_before = probe.dictionary_size()
  let mailbox_before = probe.mailbox_length()
  let assert Ok(execution.Completed(0)) =
    execution.run(workflow, 0, execution.config())
  let assert Ok(execution.Completed(0)) =
    execution.run(workflow, 0, execution.config())
  let assert Ok(execution.Completed(0)) =
    execution.run(workflow, 0, execution.config())
  probe.dictionary_size() |> should.equal(dict_before)
  probe.mailbox_length() |> should.equal(mailbox_before)
}

fn system_time_ms() -> Int {
  ffi.monotonic_time()
}

/// `await` may time out and be called again while the run continues, then
/// succeed once the run actually finishes.
pub fn await_timeout_then_success_test() {
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
    let assert Error(execution.AwaitTimedOut) = execution.await(exec, 50)
    probe.open(gate)
    let assert Ok(execution.Completed(0)) = execution.await(exec, 10_000)
    Nil
  })
}
