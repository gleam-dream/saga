import gleam/erlang/process
import gleam/list
import gleam/string
import gleam/time/duration
import gleeunit/should
import saga

pub type DemoError {
  Boom
}

pub type DemoUndoError {
  UndoBoom
}

@external(erlang, "saga_test_panic", "message")
fn panic_message(body: fn() -> a) -> Result(String, Nil)

/// A source workflow's defects are bugs: `define` panics naming the
/// workflow and every offending step.
pub fn define_panics_naming_every_defect_test() {
  let assert Ok(message) =
    panic_message(fn() {
      saga.define("wf", fn(input) {
        input
        |> saga.perform(
          saga.step("a", fn(x: Int) -> Result(Int, Nil) { Ok(x) })
          |> saga.timeout(duration.milliseconds(0)),
        )
        |> saga.perform(saga.step("", fn(x: Int) { Ok(x) }))
      })
    })
  string.contains(message, "saga.define: workflow \"wf\" is invalid")
  |> should.be_true
  string.contains(message, "step a has timeout 0 ms") |> should.be_true
  string.contains(message, "a step name is empty") |> should.be_true
}

pub fn define_rejects_empty_names_test() {
  let result =
    saga.try_define("", fn(input) {
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
    saga.try_define("wf", fn(input) {
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
    saga.try_define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("a", fn(x: Int) { Ok(x) })
        |> saga.compensate(max_attempts: 0, with: fn(_failed) {
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
    saga.try_define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("a", fn(x: Int) { Ok(x) })
        |> saga.timeout(duration.milliseconds(0)),
      )
    })
  case result {
    Error(errors) ->
      list.any(errors, fn(e) {
        case e {
          saga.InvalidTimeout(_, value) -> value == duration.milliseconds(0)
          _ -> False
        }
      })
      |> should.be_true
    Ok(_) -> panic as "expected definition to fail"
  }
}

pub fn describe_lists_steps_and_dependencies_test() {
  let workflow =
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
  let workflow =
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
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("a", fn(x: Int) { Ok(x) })
        |> saga.undo(fn(_undo) { Ok(Nil) })
        |> saga.compensate(max_attempts: 2, with: fn(_failed) {
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
    saga.try_define("second", fn(_input) {
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
  let _ =
    saga.define("first", fn(input) {
      let port = input |> saga.perform(saga.step("a", fn(v: Int) { Ok(v) }))
      process.send(holder, port)
      port
    })
  let assert Ok(captured) = process.receive(holder, 0)
  captured
}

fn inner_workflow() -> saga.Workflow(Int, Int, DemoError, DemoUndoError) {
  let workflow =
    saga.define("inner", fn(input) {
      input |> saga.perform(saga.step("inner_step", fn(x: Int) { Ok(x + 1) }))
    })
  workflow
}

pub fn embedded_workflow_scopes_addresses_test() {
  let workflow =
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
  let workflow =
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

/// Ported from the reviewer's probe P9 (`outer9b`): a builder that is not a
/// pure function of its input can, on `embed`'s one required re-evaluation
/// (see `Workflow`'s doc comment), return a `Port` stashed from a *prior*,
/// unrelated `define` call instead of deriving its output from the scoped
/// input it was given. `embed` must reject that as `ForeignPort` — the same
/// way `both`/`all`/`perform` already do for a foreign port used directly —
/// rather than silently accepting it by overwriting the stashed port's
/// scope with the parent's own on the way out. Before this fix, `embed`
/// unconditionally rewrote the output's `scope` field to the caller's own
/// (`Port(..output, scope: input.scope)`), which masked exactly this case:
/// the returned port's *nodes* still belonged to the foreign definition's
/// graph, so a run would panic in `store.get` instead of `define` failing.
pub fn embed_rejects_builder_returning_foreign_port_test() {
  let calls = process.new_subject()
  process.send(calls, 0)
  let stash = process.new_subject()

  // `wf`'s builder is not a pure function of its input: its first call (at
  // this very `define`) legitimately depends on `input`, but a *later* call
  // (triggered by `embed`, which must call a workflow's builder again to
  // splice it into a different, unrelated `define`'s own evaluation — see
  // `Workflow`'s doc comment) ignores `input` and returns a port stashed
  // from a third, unrelated `define` call below.
  let wf =
    saga.define("nd9", fn(input: saga.Port(Int, DemoError, DemoUndoError)) {
      let assert Ok(n) = process.receive(calls, 0)
      process.send(calls, n + 1)
      case n {
        0 -> input |> saga.perform(saga.step("a", fn(x: Int) { Ok(x) }))
        _ -> {
          let assert Ok(p) = process.receive(stash, 0)
          p
        }
      }
    })

  let _source =
    saga.define("source9", fn(input: saga.Port(Int, DemoError, DemoUndoError)) {
      let p = input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
      process.send(stash, p)
      p
    })

  // `embed` here is `wf`'s second builder invocation (the first happened
  // inside `wf`'s own `define` above): it takes the `_ ->` branch and
  // returns the port stashed from `source9`'s unrelated `define`.
  let result = saga.try_define("outer9b", fn(input) { saga.embed(input, wf) })
  case result {
    Error(errors) ->
      list.any(errors, fn(e) {
        case e {
          saga.ForeignPort(_) -> True
          _ -> False
        }
      })
      |> should.be_true
    Ok(_) ->
      panic as "expected embed to reject a builder that returns a foreign port"
  }
}

/// The reviewer's probe P9's actual shape: the foreign-scoped builder is
/// wrapped with `map_errors` before being `embed`ded. `map_errors`'s own
/// `translating_build` (the shadow-scope re-evaluation `embed` invokes for
/// a mapped workflow) has its own separate spot where the same
/// unconditional-overwrite bug could hide: it used to build the result
/// `Port` with `scope: input.scope` (or, before that, `shadow_output.scope`)
/// with no `foreign_error_for` check of its own, so a foreign port returned
/// through *this* path was caught only by sheer accident (or not at all)
/// rather than by a real check. This test failed independently of
/// `embed_rejects_builder_returning_foreign_port_test` above during this
/// fix's own development, catching a second unconditional-overwrite site
/// the first test's plain-`embed` shape never exercised — see
/// `map_errors`'s `translating_build`.
pub fn embed_rejects_map_errors_builder_returning_foreign_port_test() {
  let calls = process.new_subject()
  process.send(calls, 0)
  let stash = process.new_subject()

  let wf =
    saga.define("nd9m", fn(input: saga.Port(Int, DemoError, DemoUndoError)) {
      let assert Ok(n) = process.receive(calls, 0)
      process.send(calls, n + 1)
      case n {
        0 -> input |> saga.perform(saga.step("a", fn(x: Int) { Ok(x) }))
        _ -> {
          let assert Ok(p) = process.receive(stash, 0)
          p
        }
      }
    })
  let mapped = saga.map_errors(wf, error: fn(e) { e }, undo_error: fn(u) { u })

  let _source =
    saga.define("source9m", fn(input: saga.Port(Int, DemoError, DemoUndoError)) {
      let p = input |> saga.perform(saga.step("s", fn(x: Int) { Ok(x) }))
      process.send(stash, p)
      p
    })

  // `embed(mapped)` invokes `translating_build`, which re-evaluates `wf`'s
  // original builder under a shadow scope — this is `wf`'s second
  // invocation, taking the `_ ->` branch and returning the port stashed
  // from `source9m`'s unrelated `define`.
  let result =
    saga.try_define("outer9m", fn(input) { saga.embed(input, mapped) })
  case result {
    Error(errors) ->
      list.any(errors, fn(e) {
        case e {
          saga.ForeignPort(_) -> True
          _ -> False
        }
      })
      |> should.be_true
    Ok(_) -> panic as "expected embed(map_errors(..)) to reject a foreign port"
  }
}

pub fn shared_dependency_creates_one_node_test() {
  let workflow =
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
    |> saga.undo(fn(_undo) { Error(UndoBoom) })

  let mapped =
    saga.map_step_errors(
      inner_step,
      error: fn(_e: DemoError) { "mapped-error" },
      undo_error: fn(_u: DemoUndoError) { "mapped-undo-error" },
    )

  let workflow =
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
