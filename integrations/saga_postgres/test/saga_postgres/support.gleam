//// The throwaway cluster `scripts/test-postgres.sh` starts: its URL, a
//// connection pool per test, and a fresh schema per test.

import gleam/erlang/process
import gleam/int
import pog
import saga_postgres.{type Config}

@external(erlang, "saga_postgres_test_ffi", "getenv")
fn getenv(name: String) -> Result(String, Nil)

@external(erlang, "saga_postgres_test_ffi", "unique")
pub fn unique() -> Int

@external(erlang, "saga_postgres_test_ffi", "read_file")
pub fn read_file(path: String) -> Result(String, Nil)

@external(erlang, "saga_postgres_test_ffi", "with_process")
fn with_process(pid: process.Pid, work: fn() -> a) -> a

/// The throwaway cluster's URL. These tests run only through
/// `scripts/test-postgres.sh`, which sets it; without it they fail rather
/// than pass untested.
pub fn url() -> String {
  case getenv("SAGA_TEST_DATABASE_URL") {
    Ok(url) -> url
    Error(Nil) ->
      panic as "SAGA_TEST_DATABASE_URL is unset: run the PostgreSQL tests with scripts/test-postgres.sh"
  }
}

/// Runs `work` with a pool of `size` connections, stopped afterwards, also
/// when `work` fails. EUnit may reuse its process across tests, so links
/// alone do not bound a pool.
pub fn using_pool(size: Int, work: fn(pog.Connection) -> a) -> a {
  using_pool_with(size, fn(config) { config }, work)
}

/// `using_pool`, with the pool's configuration changed by `configure`.
pub fn using_pool_with(
  size: Int,
  configure: fn(pog.Config) -> pog.Config,
  work: fn(pog.Connection) -> a,
) -> a {
  let assert Ok(config) =
    pog.url_config(process.new_name("saga_postgres_test_pool"), url())
  let assert Ok(started) =
    config |> pog.pool_size(size) |> configure |> pog.start
  with_process(started.pid, fn() { work(started.data) })
}

/// A schema name no other test uses.
pub fn schema() -> String {
  "t" <> int.to_string(unique())
}

/// A configuration over `connection` in `schema` with `lease`, migrated.
pub fn migrated(
  connection: pog.Connection,
  schema: String,
  lease: Int,
) -> Config {
  let assert Ok(config) =
    saga_postgres.config(connection)
    |> saga_postgres.with_lease(lease)
    |> saga_postgres.with_schema(schema)
  let assert Ok(Nil) = saga_postgres.migrate(config)
  config
}

/// Drops `schema` and everything in it.
pub fn drop(connection: pog.Connection, schema: String) -> Nil {
  let assert Ok(_) =
    pog.query("DROP SCHEMA \"" <> schema <> "\" CASCADE")
    |> pog.execute(connection)
  Nil
}

/// A fresh execution id.
pub fn id() -> String {
  "execution-" <> int.to_string(unique())
}
