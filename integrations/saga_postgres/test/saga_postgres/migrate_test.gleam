//// `migrate` creates the schema and brings it up to date once, however
//// often and from however many callers it runs; `with_schema` keeps
//// stores apart and refuses unsafe names; the shipped SQL file holds the
//// same statements.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/list
import gleam/string
import gleam/time/duration
import gleeunit/should
import pog
import saga/storage
import saga_postgres
import saga_postgres/internal/migrations
import saga_postgres/support

fn versions(connection: pog.Connection, schema: String) -> List(Int) {
  let assert Ok(returned) =
    pog.query(
      "SELECT version FROM \""
      <> schema
      <> "\".saga_schema_migrations ORDER BY version",
    )
    |> pog.returning(decode.field(0, decode.int, decode.success))
    |> pog.execute(connection)
  returned.rows
}

pub fn migrate_creates_the_schema_once_and_again_changes_nothing_test() {
  use connection <- support.using_pool(2)
  let schema = support.schema()
  let assert Ok(config) =
    saga_postgres.config(connection) |> saga_postgres.with_schema(schema)
  saga_postgres.migrate(config) |> should.equal(Ok(Nil))
  versions(connection, schema) |> should.equal([1])
  let store = saga_postgres.storage(config)
  let id = support.id()
  let assert Ok(_) = storage.do_create(store, id, <<"kept":utf8>>)
  saga_postgres.migrate(config) |> should.equal(Ok(Nil))
  versions(connection, schema) |> should.equal([1])
  let assert Ok(stored) = storage.do_load(store, id)
  storage.data(stored) |> should.equal(<<"kept":utf8>>)
  support.drop(connection, schema)
}

/// Nodes starting together all migrate: one applies the migrations, the
/// others wait for its lock and find nothing left to do.
pub fn concurrent_migrations_all_succeed_and_apply_once_test() {
  use connection <- support.using_pool(12)
  let schema = support.schema()
  let assert Ok(config) =
    saga_postgres.config(connection) |> saga_postgres.with_schema(schema)
  let results = process.new_subject()
  list.each(list.repeat(Nil, 10), fn(_) {
    process.spawn(fn() { process.send(results, saga_postgres.migrate(config)) })
  })
  list.map(list.repeat(Nil, 10), fn(_) {
    let assert Ok(result) = process.receive(results, 30_000)
    result
  })
  |> list.unique
  |> should.equal([Ok(Nil)])
  versions(connection, schema) |> should.equal([1])
  support.drop(connection, schema)
}

/// A database a newer version of this package migrated further is left
/// as it is.
pub fn migrate_leaves_a_newer_schema_alone_test() {
  use connection <- support.using_pool(2)
  let schema = support.schema()
  let config = support.migrated(connection, schema, duration.seconds(30))
  let assert Ok(_) =
    pog.query(
      "INSERT INTO \""
      <> schema
      <> "\".saga_schema_migrations (version) VALUES (99)",
    )
    |> pog.execute(connection)
  saga_postgres.migrate(config) |> should.equal(Ok(Nil))
  versions(connection, schema) |> should.equal([1, 99])
  support.drop(connection, schema)
}

pub fn migrate_reports_an_unreachable_database_test() {
  // The throwaway cluster, but a database it does not have.
  use connection <- support.using_pool_with(1, pog.database(_, "nowhere"))
  let config = saga_postgres.config(connection)
  let assert Error(saga_postgres.MigrationFailed(_)) =
    saga_postgres.migrate(config)
  let assert Error(storage.Unavailable(_)) =
    storage.do_load(saga_postgres.storage(config), support.id())
}

/// Two schemas in one database hold separate executions.
pub fn storages_in_separate_schemas_are_apart_test() {
  use connection <- support.using_pool(2)
  let #(a, b) = #(support.schema(), support.schema())
  let one =
    saga_postgres.storage(support.migrated(connection, a, duration.seconds(30)))
  let other =
    saga_postgres.storage(support.migrated(connection, b, duration.seconds(30)))
  let id = support.id()
  let assert Ok(_) = storage.do_create(one, id, <<"one":utf8>>)
  storage.do_load(other, id) |> should.equal(Error(storage.NotFound))
  let assert Ok(_) = storage.do_create(other, id, <<"other":utf8>>)
  let assert Ok(stored) = storage.do_load(one, id)
  storage.data(stored) |> should.equal(<<"one":utf8>>)
  support.drop(connection, a)
  support.drop(connection, b)
}

pub fn a_schema_name_is_a_plain_lowercase_identifier_test() {
  use connection <- support.using_pool(1)
  let config = saga_postgres.config(connection)
  let refused = fn(schema) {
    saga_postgres.with_schema(config, schema)
    |> should.equal(Error(saga_postgres.InvalidSchema(schema)))
  }
  refused("")
  refused("Sagas")
  refused("1sagas")
  refused("sagas\"; DROP TABLE x; --")
  refused("my-sagas")
  refused("sagas.public")
  refused(string.repeat("a", 64))
  let assert Ok(_) = saga_postgres.with_schema(config, "_sagas_2")
  let assert Ok(_) = saga_postgres.with_schema(config, string.repeat("a", 63))
}

/// Each shipped SQL file's up section is its migration's statements, in
/// order.
pub fn the_sql_files_hold_the_same_statements_test() {
  list.each(migrations.all(), fn(migration) {
    let assert Ok(text) =
      support.read_file("priv/migrations/" <> migration.file)
    let assert Ok(#(_, rest)) = string.split_once(text, "--- migration:up")
    let assert Ok(#(up, _)) = string.split_once(rest, "--- migration:down")
    up
    |> string.split(";\n")
    |> list.map(string.trim)
    |> list.filter(fn(statement) { statement != "" })
    |> should.equal(migration.statements)
  })
}

/// The shipped SQL files apply on their own, as another migration tool
/// would run them, and `migrate` then finds nothing to do.
pub fn the_sql_files_apply_without_migrate_test() {
  use connection <- support.using_pool(1)
  let schema = support.schema()
  let assert Ok(_) =
    pog.transaction(connection, fn(connection) {
      let run = fn(sql) { pog.query(sql) |> pog.execute(connection) }
      let assert Ok(_) = run("CREATE SCHEMA \"" <> schema <> "\"")
      let assert Ok(_) = run("SET LOCAL search_path TO \"" <> schema <> "\"")
      list.try_each(migrations.all(), fn(migration) {
        let assert Ok(text) =
          support.read_file("priv/migrations/" <> migration.file)
        let assert Ok(#(_, rest)) = string.split_once(text, "--- migration:up")
        let assert Ok(#(up, _)) = string.split_once(rest, "--- migration:down")
        up
        |> string.split(";\n")
        |> list.map(string.trim)
        |> list.filter(fn(statement) { statement != "" })
        |> list.try_each(run)
      })
    })
  let assert Ok(config) =
    saga_postgres.config(connection) |> saga_postgres.with_schema(schema)
  saga_postgres.migrate(config) |> should.equal(Ok(Nil))
  versions(connection, schema) |> should.equal([1])
  support.drop(connection, schema)
}
