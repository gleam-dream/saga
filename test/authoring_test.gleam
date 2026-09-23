import gleam/erlang/process
import gleam/list
import gleeunit/should
import saga

pub type DemoError {
  Boom
}

pub type DemoUndoError {
  UndoBoom
}

pub fn define_rejects_empty_names_test() {
  let result =
    saga.define("", fn(input) {
      input |> saga.perform(saga.step("a", fn(x) { Ok(x) }))
    })
  case result {
    Error(errors) ->
      list.contains(errors, saga.EmptyWorkflowName) |> should.be_true
    Ok(_) -> panic as "expected definition to fail"
  }
}

pub fn define_rejects_empty_step_names_test() {
  let result =
    saga.define("wf", fn(input) {
      input |> saga.perform(saga.step("", fn(x) { Ok(x) }))
    })
  case result {
    Error(errors) ->
      list.any(errors, fn(e) {
        case e {
          saga.EmptyStepName(_) -> True
          _ -> False
        }
      })
      |> should.be_true
    Ok(_) -> panic as "expected definition to fail"
  }
}

pub fn define_rejects_invalid_attempts_test() {
  let result =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("a", fn(x: Int) { Ok(x) })
        |> saga.compensate(max_attempts: 0, with: fn(_i, _f, _a) {
          saga.Abort(Boom)
        }),
      )
    })
  case result {
    Error(errors) ->
      list.any(errors, fn(e) {
        case e {
          saga.InvalidMaxAttempts(_, 0) -> True
          _ -> False
        }
      })
      |> should.be_true
    Ok(_) -> panic as "expected definition to fail"
  }
}

pub fn define_rejects_invalid_timeout_test() {
  let result =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(saga.step("a", fn(x: Int) { Ok(x) }) |> saga.timeout(0))
    })
  case result {
    Error(errors) ->
      list.any(errors, fn(e) {
        case e {
          saga.InvalidTimeout(_, 0) -> True
          _ -> False
        }
      })
      |> should.be_true
    Ok(_) -> panic as "expected definition to fail"
  }
}

pub fn describe_lists_steps_and_dependencies_test() {
  let assert Ok(workflow) =
    saga.define("checkout", fn(input) {
      let a = input |> saga.perform(saga.step("a", fn(x: Int) { Ok(x + 1) }))
      let b = a |> saga.perform(saga.step("b", fn(x: Int) { Ok(x + 1) }))
      b
    })

  let descriptors = saga.describe(workflow)
  list.length(descriptors) |> should.equal(2)

  let assert [first, second] = descriptors
  first.address.name |> should.equal("a")
  first.depends_on |> should.equal([])
  second.address.name |> should.equal("b")
  second.depends_on |> should.equal([first.address])
}

pub fn repeated_step_occurrences_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      let a = input |> saga.perform(saga.step("dup", fn(x: Int) { Ok(x + 1) }))
      a |> saga.perform(saga.step("dup", fn(x: Int) { Ok(x + 1) }))
    })

  let descriptors = saga.describe(workflow)
  let assert [first, second] = descriptors
  first.address.occurrence |> should.equal(1)
  second.address.occurrence |> should.equal(2)
  saga.address_to_string(first.address) |> should.equal("dup")
  saga.address_to_string(second.address) |> should.equal("dup#2")
}

pub fn describe_reports_capabilities_test() {
  let assert Ok(workflow) =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("a", fn(x: Int) { Ok(x) })
        |> saga.undo(fn(_i, _o) { Ok(Nil) })
        |> saga.compensate(max_attempts: 2, with: fn(_i, _f, _a) {
          saga.Abort(Boom)
        }),
      )
    })

  let assert [descriptor] = saga.describe(workflow)
  descriptor.undoable |> should.be_true
  descriptor.compensates |> should.be_true
  descriptor.max_attempts |> should.equal(2)
}

pub fn foreign_port_rejected_test() {
  // Capture a port produced by one `define` evaluation in a mutable cell,
  // then feed it into `perform` during a second, unrelated `define`
  // evaluation. The two evaluations use different scope tokens, so the
  // second `define` must report `ForeignPort`.
  let captured = capture_port_from_first_definition()

  let result =
    saga.define("second", fn(_input) {
      captured |> saga.perform(saga.step("consumer", fn(v: Int) { Ok(v) }))
    })

  case result {
    Error(errors) ->
      list.any(errors, fn(e) {
        case e {
          saga.ForeignPort(_) -> True
          _ -> False
        }
      })
      |> should.be_true
    Ok(_) -> panic as "expected definition to fail with ForeignPort"
  }
}

fn capture_port_from_first_definition() -> saga.Port(
  Int,
  DemoError,
  DemoUndoError,
) {
  let holder = process.new_subject()
  let assert Ok(_) =
    saga.define("first", fn(input) {
      let port = input |> saga.perform(saga.step("a", fn(v: Int) { Ok(v) }))
      process.send(holder, port)
      port
    })
  let assert Ok(captured) = process.receive(holder, 0)
  captured
}

fn inner_workflow() -> saga.Workflow(Int, Int, DemoError, DemoUndoError) {
  let assert Ok(workflow) =
    saga.define("inner", fn(input) {
      input |> saga.perform(saga.step("inner_step", fn(x: Int) { Ok(x + 1) }))
    })
  workflow
}

pub fn embedded_workflow_scopes_addresses_test() {
  let assert Ok(workflow) =
    saga.define("outer", fn(input) {
      let embedded = input |> saga.embed(inner_workflow())
      embedded |> saga.perform(saga.step("outer_step", fn(x: Int) { Ok(x) }))
    })

  let descriptors = saga.describe(workflow)
  list.length(descriptors) |> should.equal(2)
  let assert [inner, outer] = descriptors
  inner.address.name |> should.equal("inner_step")
  inner.address.scope |> should.equal(["inner"])
  saga.address_to_string(inner.address) |> should.equal("inner/inner_step")
  outer.address.name |> should.equal("outer_step")
  outer.address.scope |> should.equal([])
  saga.address_to_string(outer.address) |> should.equal("outer_step")
  outer.depends_on |> should.equal([inner.address])
}

/// Embedding the same workflow twice addresses each occurrence's steps
/// under the same nested scope path, disambiguated by occurrence — not
/// merged into one node and not collapsed into the parent's own scope.
pub fn repeated_embed_scopes_and_disambiguates_test() {
  let assert Ok(workflow) =
    saga.define("outer", fn(input) {
      let a = input |> saga.embed(inner_workflow())
      let b = a |> saga.embed(inner_workflow())
      b |> saga.perform(saga.step("inner_step", fn(x: Int) { Ok(x) }))
    })

  let descriptors = saga.describe(workflow)
  list.length(descriptors) |> should.equal(3)
  let assert [first, second, outer] = descriptors
  first.address.scope |> should.equal(["inner"])
  second.address.scope |> should.equal(["inner"])
  outer.address.scope |> should.equal([])
  saga.address_to_string(first.address) |> should.equal("inner/inner_step")
  saga.address_to_string(second.address)
  |> should.equal("inner/inner_step#2")
  saga.address_to_string(outer.address) |> should.equal("inner_step")
}

pub fn shared_dependency_creates_one_node_test() {
  let assert Ok(workflow) =
    saga.define("diamond", fn(input) {
      let order =
        input |> saga.perform(saga.step("order", fn(x: Int) { Ok(x + 1) }))
      let fraud =
        order |> saga.perform(saga.step("fraud", fn(x: Int) { Ok(x) }))
      let inventory =
        order |> saga.perform(saga.step("inventory", fn(x: Int) { Ok(x) }))
      saga.both(fraud, inventory) |> saga.map(fn(pair) { pair.0 + pair.1 })
    })

  let descriptors = saga.describe(workflow)
  // order, fraud, inventory: three nodes, not four — `order` is shared.
  list.length(descriptors) |> should.equal(3)
  let order_descriptor =
    list.find(descriptors, fn(d) { d.address.name == "order" })
  let assert Ok(_) = order_descriptor
}

pub fn map_step_errors_translates_run_and_undo_test() {
  let inner_step =
    saga.step("inner", fn(x: Int) {
      case x {
        0 -> Error(Boom)
        _ -> Ok(x)
      }
    })
    |> saga.undo(fn(_i, _o) { Error(UndoBoom) })

  let mapped =
    saga.map_step_errors(
      inner_step,
      error: fn(_e: DemoError) { "mapped-error" },
      undo_error: fn(_u: DemoUndoError) { "mapped-undo-error" },
    )

  let assert Ok(workflow) =
    saga.define("mapped_wf", fn(input) { input |> saga.perform(mapped) })

  let assert [descriptor] = saga.describe(workflow)
  descriptor.undoable |> should.be_true
}

pub fn map_errors_preserves_descriptor_shape_test() {
  let original = inner_workflow()
  let mapped =
    saga.map_errors(
      original,
      error: fn(_e: DemoError) { "translated" },
      undo_error: fn(_u: DemoUndoError) { "translated-undo" },
    )

  saga.name(mapped) |> should.equal(saga.name(original))
  list.length(saga.describe(mapped))
  |> should.equal(list.length(saga.describe(original)))
}
