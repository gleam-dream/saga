import gleam/list
import gleeunit/should
import saga
import saga/execution
import support/probe

pub type DemoError {
  Boom
}

pub type DemoUndoError {
  UndoBoom(step: String)
}

/// Sequential a -> b -> c, where c always fails. a and b are undoable.
/// Undo must happen in reverse completion order: c never completed (no
/// undo), then b, then a.
pub fn failure_undoes_completed_steps_test() {
  let assert Ok(workflow) =
    saga.define("chain", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("a", fn(x: Int) { Ok(x + 1) })
          |> saga.undo(fn(_i, _o) { Ok(Nil) }),
        )
      let b =
        a
        |> saga.perform(
          saga.step("b", fn(x: Int) { Ok(x + 1) })
          |> saga.undo(fn(_i, _o) { Ok(Nil) }),
        )
      b |> saga.perform(saga.step("c", fn(_x: Int) { Error(Boom) }))
    })

  let assert Ok(execution.Failed(cause, settlement)) =
    execution.run(workflow, 0, execution.config())

  case cause {
    execution.StepFailed(step, Boom) -> step.name |> should.equal("c")
    _ -> panic as "expected StepFailed(c, Boom)"
  }

  settlement.undone
  |> list.map(fn(a) { a.name })
  |> should.equal(["b", "a"])
}

pub fn multiple_undo_failures_retained_test() {
  let assert Ok(workflow) =
    saga.define("chain", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("a", fn(x: Int) { Ok(x + 1) })
          |> saga.undo(fn(_i, _o) { Error(UndoBoom("a")) }),
        )
      let b =
        a
        |> saga.perform(
          saga.step("b", fn(x: Int) { Ok(x + 1) })
          |> saga.undo(fn(_i, _o) { Error(UndoBoom("b")) }),
        )
      b |> saga.perform(saga.step("c", fn(_x: Int) { Error(Boom) }))
    })

  let assert Ok(execution.Failed(_cause, settlement)) =
    execution.run(workflow, 0, execution.config())

  list.length(settlement.undo_failures) |> should.equal(2)
  settlement.undone |> should.equal([])
}

pub fn compensation_vs_undo_test() {
  // The failed step gets compensate only; completed steps get undo only.
  // Each fires exactly once.
  let undo_counter = probe.new_counter()
  let compensate_counter = probe.new_counter()

  let assert Ok(workflow) =
    saga.define("chain", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("a", fn(x: Int) { Ok(x + 1) })
          |> saga.undo(fn(_i, _o) {
            probe.counter_enter(undo_counter)
            Ok(Nil)
          }),
        )
      a
      |> saga.perform(
        saga.step("b", fn(_x: Int) { Error(Boom) })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          probe.counter_enter(compensate_counter)
          saga.Abort(Boom)
        }),
      )
    })

  let assert Ok(execution.Failed(_cause, _settlement)) =
    execution.run(workflow, 0, execution.config())

  probe.total_entries(undo_counter) |> should.equal(1)
  probe.total_entries(compensate_counter) |> should.equal(1)
}

pub fn not_undoable_steps_are_reported_test() {
  let assert Ok(workflow) =
    saga.define("chain", fn(input) {
      let a = input |> saga.perform(saga.step("a", fn(x: Int) { Ok(x + 1) }))
      a |> saga.perform(saga.step("b", fn(_x: Int) { Error(Boom) }))
    })

  let assert Ok(execution.Failed(_cause, settlement)) =
    execution.run(workflow, 0, execution.config())

  settlement.not_undoable
  |> list.map(fn(a) { a.name })
  |> should.equal(["a"])
  settlement.undone |> should.equal([])
}

pub fn hold_leaves_completed_effects_unresolved_test() {
  let assert Ok(workflow) =
    saga.define("chain", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("a", fn(x: Int) { Ok(x + 1) })
          |> saga.undo(fn(_i, _o) { Ok(Nil) }),
        )
      a
      |> saga.perform(
        saga.step("b", fn(_x: Int) { Error(Boom) })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          saga.Hold(Boom)
        }),
      )
    })

  let assert Ok(execution.Unresolved(step, Boom, settlement)) =
    execution.run(workflow, 0, execution.config())

  step.name |> should.equal("b")
  settlement.undone |> should.equal([])
}

// Adapted from reactor/executor/step_runner_test.exs:252-307: undo
// receives the step's original input and its successful output. Saga has
// no undo-retry loop at all (deliberate difference, see PROVENANCE.md
// R10 and design §3.3): a failing undo is retained once, never retried.
pub fn undo_receives_input_and_output_test() {
  let assert Ok(workflow) =
    saga.define("chain", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("a", fn(x: Int) { Ok(x + 10) })
          |> saga.undo(fn(received_input, received_output) {
            case received_input == 5 && received_output == 15 {
              True -> Ok(Nil)
              False -> Error(UndoBoom("a: wrong args"))
            }
          }),
        )
      a |> saga.perform(saga.step("b", fn(_x: Int) { Error(Boom) }))
    })

  let assert Ok(execution.Failed(_cause, settlement)) =
    execution.run(workflow, 5, execution.config())

  // The undo closure asserted the exact input (5) and output (15) it
  // received; no UndoFailed means those assertions held.
  settlement.undo_failures |> should.equal([])
  settlement.undone |> list.map(fn(a) { a.name }) |> should.equal(["a"])
}

pub fn continue_replacement_undo_used_test() {
  let assert Ok(workflow) =
    saga.define("chain", fn(input) {
      input
      |> saga.perform(
        saga.step("a", fn(_x: Int) { Error(Boom) })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          saga.Continue(42, saga.UndoWith(fn() { Ok(Nil) }))
        }),
      )
    })

  let assert Ok(execution.Completed(output)) =
    execution.run(workflow, 0, execution.config())
  output |> should.equal(42)
}
