/// `saga/internal/store` holds the one unsafe coercion in the whole
/// package: a native identity cast from whatever a node `put`, back to the
/// type the reading `Port` expects. That coercion is sound by construction
/// (see the module's doc comment) because a node id and its element type
/// are bound together once, in the same `perform` call that both creates
/// the id and returns the typed `Port` that reads it back. This test
/// exercises that soundness directly: several *different* concrete types
/// (Int, String, a caller-owned record, a List, a Result) flow through
/// shared nodes, `embed`, `map_errors`, and `both`/`all` in one run, and
/// every value is checked to have arrived intact and at the right type —
/// if the coercion were unsound (e.g. two differently-typed ports ever
/// aliased the same store slot), this would surface as wrong data, not
/// just a crash.
import gleam/int
import gleam/list
import gleeunit/should
import saga
import saga/execution

pub type DemoError {
  Boom
}

pub type OtherError {
  OtherBoom
}

pub type DemoUndoError {
  UndoBoom
}

/// A caller-owned record type, unrelated to anything saga defines, to prove
/// the store carries application data through untouched.
pub type Ticket {
  Ticket(id: Int, label: String)
}

/// One workflow whose steps produce heterogeneous concrete types (Int,
/// String, Ticket, List(Int), Result(Int, String)) from a single shared
/// producer, recombined through `both`/`all`, plus an embedded inner
/// workflow and a `map_errors`-adapted step — all reading from and writing
/// to the same per-run `Store`.
pub fn heterogeneous_values_flow_correctly_test() {
  let assert Ok(inner) =
    saga.define("inner", fn(input: saga.Port(Int, DemoError, DemoUndoError)) {
      input
      |> saga.perform(saga.step("double", fn(x: Int) { Ok(x * 2) }))
    })

  let assert Ok(workflow) =
    saga.define("outer", fn(input: saga.Port(Int, DemoError, DemoUndoError)) {
      // Shared producer: one node, read by several differently-typed
      // consumers below (fan-out over one store slot).
      let produced =
        input
        |> saga.perform(saga.step("produce", fn(x: Int) { Ok(x) }))

      let as_string =
        produced
        |> saga.perform(
          saga.step("stringify", fn(x: Int) { Ok(int.to_string(x)) }),
        )

      let as_ticket =
        produced
        |> saga.perform(
          saga.step("ticketize", fn(x: Int) {
            Ok(Ticket(id: x, label: "ticket-" <> int.to_string(x)))
          }),
        )

      let as_list =
        produced
        |> saga.perform(
          saga.step("listify", fn(x: Int) { Ok([x, x + 1, x + 2]) }),
        )

      let as_result =
        produced
        |> saga.perform(saga.step("resultify", fn(x: Int) { Ok(Ok(x + 100)) }))

      // Composition: an embedded workflow sharing this run, and a
      // map_errors-adapted step, both reading the same shared producer.
      let embedded = saga.embed(produced, inner)

      let adapted_step =
        saga.step("adapt", fn(x: Int) { Ok(x - 1) })
        |> saga.map_step_errors(
          error: fn(e: OtherError) {
            let OtherBoom = e
            Boom
          },
          undo_error: fn(u: OtherError) {
            let OtherBoom = u
            UndoBoom
          },
        )
      let adapted = produced |> saga.perform(adapted_step)

      // Recombine everything heterogeneous into one output via
      // both/all/map, proving every store slot round-trips at its own
      // type even when read alongside differently-typed siblings.
      let strings_and_tickets =
        saga.both(as_string, as_ticket)
        |> saga.map(fn(pair) {
          let #(s, ticket) = pair
          #(s, ticket)
        })

      saga.both(
        strings_and_tickets,
        saga.all(as_list, [])
          |> saga.map(fn(lists) {
            let assert [only] = lists
            only
          }),
      )
      |> saga.map(fn(pair) { #(pair.0, pair.1) })
      |> saga.map(fn(pair) { #(pair.0, pair.1) })
      |> saga.both(as_result)
      |> saga.both(embedded)
      |> saga.both(adapted)
    })

  let assert Ok(execution.Completed(result)) =
    execution.run(workflow, 21, execution.config())

  let #(rest4, adapted_value) = result
  let #(rest3, embedded_value) = rest4
  let #(rest2, result_value) = rest3
  let #(#(string_value, ticket_value), list_value) = rest2

  string_value |> should.equal("21")
  ticket_value |> should.equal(Ticket(id: 21, label: "ticket-21"))
  list_value |> should.equal([21, 22, 23])
  result_value |> should.equal(Ok(121))
  embedded_value |> should.equal(42)
  adapted_value |> should.equal(20)
}

/// The same heterogeneous shapes, but across several runs of the same
/// `Workflow` value (built once), proving one run's `Store` never leaks
/// into another's despite sharing the identical node graph.
pub fn heterogeneous_values_isolated_across_runs_test() {
  let assert Ok(workflow) =
    saga.define("outer2", fn(input: saga.Port(Int, DemoError, DemoUndoError)) {
      let produced =
        input |> saga.perform(saga.step("produce", fn(x: Int) { Ok(x) }))
      let as_ticket =
        produced
        |> saga.perform(
          saga.step("ticketize", fn(x: Int) {
            Ok(Ticket(id: x, label: "t" <> int.to_string(x)))
          }),
        )
      let as_list =
        produced
        |> saga.perform(
          saga.step("listify", fn(x: Int) { Ok(list.repeat(x, 3)) }),
        )
      saga.both(as_ticket, as_list)
    })

  let assert Ok(execution.Completed(#(ticket1, list1))) =
    execution.run(workflow, 1, execution.config())
  let assert Ok(execution.Completed(#(ticket2, list2))) =
    execution.run(workflow, 2, execution.config())
  let assert Ok(execution.Completed(#(ticket3, list3))) =
    execution.run(workflow, 3, execution.config())

  ticket1 |> should.equal(Ticket(id: 1, label: "t1"))
  ticket2 |> should.equal(Ticket(id: 2, label: "t2"))
  ticket3 |> should.equal(Ticket(id: 3, label: "t3"))
  list1 |> should.equal([1, 1, 1])
  list2 |> should.equal([2, 2, 2])
  list3 |> should.equal([3, 3, 3])
}
