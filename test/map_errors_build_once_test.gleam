/// Regression tests for an independent review finding: `map_errors`
/// re-ran the workflow's build function a second time (bypassing
/// `define`'s validation entirely) to compute its mapped graph, instead of
/// reusing the graph `define` already built and validated once. Ported
/// from the review's own probes (P3, P3b).
import gleam/erlang/process
import gleam/list
import gleeunit/should
import saga
import saga/execution
import support/probe

/// P3: a builder that returns a *different*, invalid shape on its second
/// evaluation (an empty step name, and an orphan step) than it did at
/// `define` time. Before the fix, `map_errors` invoked this builder a
/// second time with no validation at all, so `describe` and the actual run
/// could disagree about what the workflow even is, and an invalid shape
/// (which `define` would have rejected outright) could reach execution.
/// After the fix, the builder never runs again: `map_errors` reuses the
/// already-validated graph, so `describe(mapped)` always agrees with what
/// `execution.run` actually executes, and the counter proves the builder
/// ran exactly once (at `define`), never again for `map_errors`.
pub fn map_errors_reuses_validated_graph_test() {
  let calls = probe.new_counter()
  let workflow =
    saga.define("nd", fn(input: saga.Port(Int, Nil, Nil)) {
      probe.counter_enter(calls)
      case probe.total_entries(calls) {
        1 -> input |> saga.perform(saga.step("a", fn(x: Int) { Ok(x) }))
        _ -> {
          // An invalid shape `define` would reject: an orphan step and an
          // empty step name. Never reached if the builder truly runs once.
          let _orphan =
            input |> saga.perform(saga.step("orphan", fn(x: Int) { Ok(x) }))
          input |> saga.perform(saga.step("", fn(x: Int) { Ok(x + 1000) }))
        }
      }
    })

  let assert 1 = probe.total_entries(calls)

  let mapped =
    saga.map_errors(workflow, error: fn(e) { e }, undo_error: fn(u) { u })

  // The builder must not have run again just to compute the mapped graph.
  let assert 1 = probe.total_entries(calls)

  // `describe` must agree with what a real run actually executes: both
  // must reflect the *first* (validated) evaluation's shape, "a" alone.
  let described = saga.describe(mapped) |> list.map(fn(d) { d.address.name })
  described |> should.equal(["a"])

  let assert Ok(execution.Completed(1)) =
    execution.run(mapped, 1, execution.config())

  // Still exactly once: running the mapped workflow must not invoke the
  // original builder either.
  let assert 1 = probe.total_entries(calls)
}

/// P3b: a builder that, on a hypothetical second evaluation, would return a
/// `Port` value captured (stashed) from its *first* evaluation instead of
/// building fresh nodes. Before the fix, `map_errors`'s re-run of the
/// builder could receive such a stale port and read a node id through it
/// that the coordinator's fresh graph never populated, panicking `store.
/// get` and losing the run (`ExecutionLost`) instead of completing. After
/// the fix there is no second evaluation to receive a stale port at all.
pub fn map_errors_stale_port_does_not_crash_run_test() {
  let calls = probe.new_counter()
  let stashed = process.new_subject()
  let workflow =
    saga.define("nd2", fn(input: saga.Port(Int, Nil, Nil)) {
      probe.counter_enter(calls)
      case probe.total_entries(calls) {
        1 -> {
          let p = input |> saga.perform(saga.step("a", fn(x: Int) { Ok(x) }))
          process.send(stashed, p)
          p
        }
        _ -> {
          let assert Ok(p) = process.receive(stashed, 0)
          p
        }
      }
    })

  let mapped =
    saga.map_errors(workflow, error: fn(e) { e }, undo_error: fn(u) { u })

  let assert Ok(exec) = execution.start(mapped, 1, execution.config())
  let assert Ok(execution.Completed(1)) = execution.await(exec, 2000)
  let assert 1 = probe.total_entries(calls)
}

/// A builder is invoked exactly once per `define` (never again for any
/// number of runs or `map_errors` calls on that `Workflow`), and exactly
/// once *more* per `embed` call that splices it into a different, unrelated
/// workflow's own `define` (that composing `define`'s one evaluation is a
/// legitimate, separate, validated build — not a hidden re-run of the
/// original). Nothing else ever invokes a builder.
pub fn builder_runs_once_per_define_and_once_per_embed_test() {
  let inner_calls = probe.new_counter()
  let inner =
    saga.define("inner", fn(input: saga.Port(Int, Nil, Nil)) {
      probe.counter_enter(inner_calls)
      input |> saga.perform(saga.step("double", fn(x: Int) { Ok(x * 2) }))
    })
  let assert 1 = probe.total_entries(inner_calls)

  // map_errors: reuses inner's graph, no new builder invocation.
  let mapped =
    saga.map_errors(inner, error: fn(e) { e }, undo_error: fn(u) { u })
  let assert 1 = probe.total_entries(inner_calls)

  // Running the plain (unmapped) workflow several times: still no further
  // invocation (the build-once refactor's own invariant).
  let assert Ok(execution.Completed(2)) =
    execution.run(inner, 1, execution.config())
  let assert Ok(execution.Completed(4)) =
    execution.run(inner, 2, execution.config())
  let assert 1 = probe.total_entries(inner_calls)

  // Running the mapped workflow: also no further invocation.
  let assert Ok(execution.Completed(6)) =
    execution.run(mapped, 3, execution.config())
  let assert 1 = probe.total_entries(inner_calls)

  // embed(mapped) inside an outer define: exactly one more invocation of
  // the *original* inner builder (embed's translating `build`, validated
  // by the outer define's own one evaluation) -- never two, never zero.
  let outer_calls = probe.new_counter()
  let outer =
    saga.define("outer", fn(input: saga.Port(Int, Nil, Nil)) {
      probe.counter_enter(outer_calls)
      saga.embed(input, mapped)
    })
  let assert 1 = probe.total_entries(outer_calls)
  let assert 2 = probe.total_entries(inner_calls)

  // Running the outer workflow (any number of times) invokes neither
  // builder again.
  let assert Ok(execution.Completed(10)) =
    execution.run(outer, 5, execution.config())
  let assert Ok(execution.Completed(14)) =
    execution.run(outer, 7, execution.config())
  let assert 1 = probe.total_entries(outer_calls)
  let assert 2 = probe.total_entries(inner_calls)
}
