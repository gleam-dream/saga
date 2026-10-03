//// One application pool serves the application's own queries and saga's
//// storage at once: each run's step writes to an application table
//// through the same `pog.Connection` that saves the run's checkpoints.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/list
import gleam/string
import gleam/time/duration
import gleeunit/should
import pog
import saga
import saga/codec
import saga/durable
import saga/execution
import saga_postgres
import saga_postgres/support

pub fn one_pool_serves_the_application_and_saga_test() {
  // Two connections for five concurrent runs and their steps' queries.
  use connection <- support.using_pool(2)
  let schema = support.schema()
  let config = support.migrated(connection, schema, duration.seconds(5))
  let orders = "\"" <> schema <> "\".orders"
  let assert Ok(_) =
    pog.query("CREATE TABLE " <> orders <> " (id text PRIMARY KEY)")
    |> pog.execute(connection)
  let record =
    saga.effect("record", fn(order: String, _key) {
      case
        pog.query(
          "INSERT INTO " <> orders <> " (id) VALUES ($1) ON CONFLICT DO NOTHING",
        )
        |> pog.parameter(pog.text(order))
        |> pog.execute(connection)
      {
        Ok(_) -> Ok(order)
        Error(_) -> Error("insert failed")
      }
    })
    |> durable.recoverable(
      version: "1",
      input: codec.text(),
      output: codec.text(),
      resolve: fn(_, _) { durable.NotSent },
    )
  let workflow =
    saga.define("record-order", fn(order) { saga.perform(order, record) })
  let text = codec.text()
  let persistence =
    durable.new(
      workflow,
      input: text,
      output: text,
      error: text,
      undo_error: text,
    )
  let store = saga_postgres.storage(config)
  let results = process.new_subject()
  let ids = ["order-1", "order-2", "order-3", "order-4", "order-5"]
  list.each(ids, fn(order) {
    process.spawn(fn() {
      let assert Ok(run) =
        durable.start_or_reconnect(
          persistence,
          store,
          id: "record:" <> order,
          input: order,
        )
      process.send(results, #(
        order,
        durable.drive(run, timeout: duration.seconds(20)),
      ))
    })
  })
  list.map(ids, fn(_) {
    let assert Ok(result) = process.receive(results, 30_000)
    result
  })
  |> list.sort(fn(a, b) { string.compare(a.0, b.0) })
  |> list.map(fn(result) { result.1 })
  |> should.equal(list.map(ids, fn(order) { Ok(execution.Completed(order)) }))
  let assert Ok(counted) =
    pog.query("SELECT count(*) FROM " <> orders)
    |> pog.returning(decode.field(0, decode.int, decode.success))
    |> pog.execute(connection)
  counted.rows |> should.equal([5])
  durable.unfinished(store, limit: 100) |> should.equal(Ok([]))
  support.drop(connection, schema)
}
