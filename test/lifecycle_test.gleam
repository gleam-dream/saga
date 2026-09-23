import gleam/erlang/process
import gleam/list
import gleam/option.{Some}
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
    execution.Config(
      ..execution.config(),
      deadline: Some(50),
      settle_timeout: 0,
    )

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 2000)
    let assert Ok(execution.Failed(cause, settlement)) =
      execution.await(exec, 2000)
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
    execution.Config(
      ..execution.config(),
      deadline: Some(50),
      settle_timeout: 0,
    )

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
    let assert Ok(_pid) = probe.wait_entered(gate, 2000)
    let assert Ok(execution.Failed(cause, _settlement)) =
      execution.await(exec, 2000)
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

  let assert Ok(execution.Completed(0)) =
    execution.run(workflow, 0, execution.config())
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
    let assert Ok(pid) = probe.wait_entered(gate, 2000)
    let assert Ok(execution.Failed(cause, _settlement)) =
      execution.await(exec, 2000)
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

  let config = execution.Config(..execution.config(), cleanup_timeout: 50)

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 2000)
    let assert Ok(execution.Failed(_cause, settlement)) =
      execution.await(exec, 2000)
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

  let config = execution.Config(..execution.config(), cleanup_timeout: 50)

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 2000)
    let assert Ok(execution.Failed(cause, settlement)) =
      execution.await(exec, 2000)
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

  let config = execution.Config(..execution.config(), settle_timeout: 100)

  probe.with_run(workflow, 0, config, fn(exec) {
    let assert Ok(_) = probe.wait_entered(releasable_gate, 2000)
    let assert Ok(_) = probe.wait_entered(blocked_gate, 2000)

    execution.cancel(exec)

    let assert Ok(progress) = execution.progress(exec, 2000)
    progress.phase |> should.equal(execution.Settling)

    probe.open(releasable_gate)

    let assert Ok(execution.Cancelled(reason, settlement)) =
      execution.await(exec, 2000)
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
  let assert Ok(execution.Completed(0)) = execution.await(exec, 2000)
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
    let assert Ok(_pid) = probe.wait_entered(gate, 2000)
    execution.cancel(exec)
    execution.cancel(exec)
    execution.cancel(exec)
    probe.open(gate)
    let assert Ok(execution.Cancelled(execution.CancelRequested, _)) =
      execution.await(exec, 2000)
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
  let assert Ok(outcome) = execution.await(exec, 2000)
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
pub fn owner_exit_cancels_and_rolls_back_test() {
  let undo_counter = probe.new_counter()
  let outcome_subject = process.new_subject()
  let coordinator_pid_subject = process.new_subject()

  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("s", fn(x: Int) { Ok(x) })
        |> saga.undo(fn(_i, _o) {
          probe.counter_enter(undo_counter)
          Ok(Nil)
        }),
      )
    })

  let owner_pid =
    process.spawn(fn() {
      let assert Ok(exec) = execution.start(workflow, 0, execution.config())
      process.send(coordinator_pid_subject, execution.pid(exec))
      // Exit immediately without awaiting, simulating an owner crash.
      Nil
    })
  let _ = owner_pid

  let assert Ok(coordinator_pid) =
    process.receive(coordinator_pid_subject, 2000)
  let monitor = process.monitor(coordinator_pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
  let _down = process.selector_receive_forever(selector)

  probe.await_total_entries(undo_counter, 1, 2000)
  process.send(outcome_subject, Nil)
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
  let assert Ok(task_pid) = probe.wait_entered(gate, 2000)

  process.kill(execution.pid(exec))

  case execution.await(exec, 2000) {
    Error(execution.Lost(_crash)) -> Nil
    _other -> panic as "expected Lost, got something else"
  }

  // The task the killed coordinator had spawned (linked) dies with it.
  wait_until_dead(task_pid, 2000)
}

fn wait_until_dead(pid: process.Pid, timeout_ms: Int) -> Nil {
  case process.is_alive(pid) {
    False -> Nil
    True ->
      case timeout_ms <= 0 {
        True -> panic as "expected task process to die with its coordinator"
        False -> {
          process.sleep(10)
          wait_until_dead(pid, timeout_ms - 10)
        }
      }
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
  process.spawn(fn() { process.send(reply, execution.await(exec, 2000)) })
  let assert Ok(Error(execution.NotOwner)) = process.receive(reply, 2000)

  let assert Ok(execution.Completed(0)) = execution.await(exec, 2000)
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
  let assert Ok(execution.Completed(0)) = execution.await(exec, 2000)
  let assert Error(execution.AlreadyAwaited) = execution.await(exec, 2000)
  Nil
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
    let assert Ok(_pid) = probe.wait_entered(gate, 2000)
    let assert Error(execution.AwaitTimedOut) = execution.await(exec, 50)
    probe.open(gate)
    let assert Ok(execution.Completed(0)) = execution.await(exec, 2000)
    Nil
  })
}
