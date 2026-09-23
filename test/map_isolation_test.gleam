/// Regression tests for independent-review findings on `saga.map`,
/// `saga.embed`/`saga.map_errors`, `saga.all`, and orphan step rejection —
/// ported from the reviewer's repro suite (REPRO1, REPRO2, REPRO7, REPRO8).
import gleam/erlang/process
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

// ---------------------------------------------------------------------------
// REPRO1: a panicking `map` between two steps must not crash the
// coordinator — it must be recovered like any other attempt crash, with the
// earlier step's undo running normally.
// ---------------------------------------------------------------------------

pub fn map_panic_between_steps_is_recovered_test() {
  let undone = process.new_subject()
  let assert Ok(wf) =
    saga.define("mp", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("a", fn(x: Int) { Ok(x) })
          |> saga.undo(fn(_, _) {
            process.send(undone, Nil)
            Ok(Nil)
          }),
        )
      a
      |> saga.map(fn(x) {
        case x > 0 {
          True -> panic as "map boom"
          False -> x
        }
      })
      |> saga.perform(saga.step("b", fn(x: Int) -> Result(Int, Nil) { Ok(x) }))
    })

  let assert Ok(execution.Failed(cause, settlement)) =
    execution.run(wf, 1, execution.config())

  case cause {
    execution.StepCrashed(step, _crash) -> step.name |> should.equal("b")
    _ ->
      panic as "expected StepCrashed(b, _): a panicking map is an attempt crash of its consumer"
  }
  settlement.undone |> list.map(fn(a) { a.name }) |> should.equal(["a"])
  process.receive(undone, 500) |> should.equal(Ok(Nil))

  // The coordinator itself survived: an independent run still works.
  let assert Ok(wf2) =
    saga.define("mp2", fn(input) {
      input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x + 1) }))
    })
  let assert Ok(execution.Completed(2)) =
    execution.run(wf2, 1, execution.config())
}

/// A slow `map` between two steps must not block cancellation: the map's
/// pure work runs inside the consuming step's own attempt task, so killing
/// that task (the settle sweep) interrupts the map along with it.
pub fn slow_map_does_not_block_cancel_test() {
  let gate = probe.new_gate()
  let assert Ok(wf) =
    saga.define("slow_map", fn(input) {
      input
      |> saga.perform(saga.step("a", fn(x: Int) { Ok(x) }))
      |> saga.map(fn(x) {
        probe.enter(gate)
        x
      })
      |> saga.perform(saga.step("b", fn(x: Int) -> Result(Int, Nil) { Ok(x) }))
    })

  let config = execution.Config(..execution.config(), settle_timeout: 100)
  probe.with_run(wf, 1, config, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(gate, 2000)
    // The map is blocked inside "b"'s attempt task. Cancellation must still
    // be observed promptly: settling begins immediately, and the settle
    // window (bounded) sweeps the blocked task rather than waiting forever.
    execution.cancel(exec)
    let assert Ok(execution.Cancelled(_reason, _settlement)) =
      execution.await(exec, 2000)
    Nil
  })
}

// ---------------------------------------------------------------------------
// REPRO2: embed(map_errors(inner)) must not deadlock — the inner workflow's
// dependency on the parent port must survive `map_errors`'s shadow
// rebuild.
// ---------------------------------------------------------------------------

pub fn embed_map_errors_runs_to_completion_test() {
  let assert Ok(inner) =
    saga.define("inner", fn(input) {
      input
      |> saga.perform(
        saga.step("inner", fn(x: Int) -> Result(Int, Int) { Ok(x + 1) }),
      )
    })
  let mapped =
    saga.map_errors(inner, error: fn(e) { e }, undo_error: fn(u) { u })
  let assert Ok(outer) =
    saga.define("outer", fn(input) {
      input
      |> saga.perform(
        saga.step("first", fn(x: Int) -> Result(Int, Int) { Ok(x) }),
      )
      |> saga.embed(mapped)
    })

  let assert Ok(execution.Completed(output)) =
    execution.run(outer, 1, execution.config())
  output |> should.equal(2)
}

/// `map_step_errors` at runtime: the translated error/undo-error actually
/// reach the outcome and the undo action, not just the static descriptor.
pub fn map_step_errors_runtime_test() {
  let undo_ran = process.new_subject()
  let inner_step =
    saga.step("inner", fn(x: Int) {
      case x {
        0 -> Error(Boom)
        _ -> Ok(x)
      }
    })
    |> saga.undo(fn(_i, _o) {
      process.send(undo_ran, Nil)
      Error(UndoBoom)
    })
  let mapped =
    saga.map_step_errors(
      inner_step,
      error: fn(_e: DemoError) { "mapped-error" },
      undo_error: fn(_u: DemoUndoError) { "mapped-undo-error" },
    )

  let assert Ok(workflow) =
    saga.define("mapped_wf", fn(input) {
      let a = input |> saga.perform(mapped)
      a |> saga.perform(saga.step("fails", fn(_x: Int) { Error("boom2") }))
    })

  let assert Ok(execution.Failed(_cause, settlement)) =
    execution.run(workflow, 1, execution.config())

  process.receive(undo_ran, 500) |> should.equal(Ok(Nil))
  let assert [failure] = settlement.undo_failures
  case failure {
    execution.UndoFailed(_step, "mapped-undo-error") -> Nil
    _ -> panic as "expected UndoFailed with the translated undo error"
  }
}

/// `map_errors` at runtime: a failing inner step's translated error reaches
/// the outer `Failed` cause.
pub fn map_errors_runtime_test() {
  let assert Ok(inner) =
    saga.define("inner", fn(input) {
      input
      |> saga.perform(
        saga.step("boom", fn(_x: Int) -> Result(Int, DemoError) { Error(Boom) }),
      )
    })
  let mapped =
    saga.map_errors(
      inner,
      error: fn(_e: DemoError) { "translated" },
      undo_error: fn(_u: DemoUndoError) { "translated-undo" },
    )
  let assert Ok(outer) =
    saga.define("outer", fn(input) { input |> saga.embed(mapped) })

  let assert Ok(execution.Failed(cause, _settlement)) =
    execution.run(outer, 1, execution.config())
  case cause {
    execution.StepFailed(_step, "translated") -> Nil
    _ -> panic as "expected StepFailed with the translated error"
  }
}

// ---------------------------------------------------------------------------
// REPRO7 / finding 4: an authored step whose output port is never consumed
// by the workflow's final output must be rejected at `define`, not silently
// dropped at run time.
// ---------------------------------------------------------------------------

pub fn orphan_step_rejected_test() {
  let result =
    saga.define("u", fn(input) {
      let _side =
        input
        |> saga.perform(
          saga.step("send_email", fn(x: Int) -> Result(Int, Nil) { Ok(x) }),
        )
      input
      |> saga.perform(
        saga.step("main", fn(x: Int) -> Result(Int, Nil) { Ok(x) }),
      )
    })

  case result {
    Error(errors) ->
      list.any(errors, fn(e) {
        case e {
          saga.OrphanStep(step) -> step.name == "send_email"
          _ -> False
        }
      })
      |> should.be_true
    Ok(_) -> panic as "expected definition to fail with OrphanStep"
  }
}

pub fn non_orphaned_shared_dependency_is_not_rejected_test() {
  // `order`'s port is consumed twice (by `fraud` and `inventory`), both of
  // which do reach the final output: this must not be flagged as an
  // orphan.
  let assert Ok(_workflow) =
    saga.define("diamond", fn(input) {
      let order =
        input |> saga.perform(saga.step("order", fn(x: Int) { Ok(x) }))
      let fraud =
        order |> saga.perform(saga.step("fraud", fn(x: Int) { Ok(x) }))
      let inventory =
        order |> saga.perform(saga.step("inventory", fn(x: Int) { Ok(x) }))
      saga.both(fraud, inventory)
    })
}

// ---------------------------------------------------------------------------
// Finding 12: `saga.all` takes a required first port, so there is no empty
// case to construct or reject at all — the type system rules it out at the
// call site (see `saga.all`'s doc comment).
// ---------------------------------------------------------------------------

pub fn all_single_port_runs_normally_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      let a = input |> saga.perform(saga.step("a", fn(x: Int) { Ok(x + 1) }))
      saga.all(a, [])
    })
  let assert Ok(execution.Completed([2])) =
    execution.run(workflow, 1, execution.config())
}
