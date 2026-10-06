//// A PostgreSQL storage for `saga/durable` runs, for runners on one node or
//// several nodes that share one database: the `saga/storage` contract over
//// one table, `saga_executions`, and the migrations that create it.
////
//// ```gleam
//// import gleam/time/duration
//// import saga/durable
//// import saga_postgres
////
//// let config = saga_postgres.config(db) |> saga_postgres.with_lease(duration.seconds(30))
//// let assert Ok(Nil) = saga_postgres.migrate(config)
//// let storage = saga_postgres.storage(config)
//// let assert Ok(run) =
////   durable.start_or_reconnect(persistence, storage, id: "checkout-123", input: order)
//// let outcome = durable.drive(run, timeout: duration.seconds(30))
//// ```
////
//// The application owns the connection pool (`pog.start` or
//// `pog.supervised`) and passes its `pog.Connection`; the same pool serves
//// the application's own queries. Each execution is one row: its
//// checkpoint bytes, revision, ownership generation, cancellation flag,
//// phase, and claim (a random token and a lease expiry). Every write is one
//// conditional statement. Saga renews a live runner's lease every third
//// of its duration. Release clears ownership; expiry permits takeover.
//// An expired claim can still commit, renew or release until another claim
//// replaces it. Lease expiry uses the database's clock alone
//// (`clock_timestamp()`), so node clocks need not agree. Fencing saved
//// writes does not retract an already sent external effect.

import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import pog
import saga/storage.{type Storage}
import saga_postgres/internal/migrations
import saga_postgres/internal/store

/// Where and how the storage keeps executions: the application's
/// connection, the lease duration and the schema.
pub opaque type Config {
  Config(connection: pog.Connection, lease: Duration, schema: String)
}

/// A configuration over `connection`, with a 30 second lease, in the
/// schema `public`.
pub fn config(connection: pog.Connection) -> Config {
  Config(connection:, lease: duration.seconds(30), schema: "public")
}

/// Sets the lease (default 30 seconds), measured from the last accepted
/// claim, commit or renewal. Expiry permits takeover but does not revoke
/// an unreplaced claim. Saga renews a live runner's lease every third of
/// its duration. A lease below 100 milliseconds is raised to 100 milliseconds.
pub fn with_lease(config: Config, lease: Duration) -> Config {
  let milliseconds = int.max(100, duration.to_milliseconds(lease))
  Config(..config, lease: duration.milliseconds(milliseconds))
}

/// Why a schema name was refused (`with_schema`).
pub type SchemaError {
  /// A schema name here is 1 to 63 lowercase letters, digits and `_`, not
  /// starting with a digit.
  InvalidSchema(String)
}

/// Keeps the tables in `schema` (default `public`), which `migrate`
/// creates if missing. Several applications, or several tests, may share
/// one database in schemas of their own.
pub fn with_schema(
  config: Config,
  schema: String,
) -> Result(Config, SchemaError) {
  let allowed = fn(grapheme, first) {
    string.contains("abcdefghijklmnopqrstuvwxyz_", grapheme)
    || { !first && string.contains("0123456789", grapheme) }
  }
  let graphemes = string.to_graphemes(schema)
  case
    graphemes != []
    && list.length(graphemes) <= 63
    && list.index_fold(graphemes, True, fn(ok, grapheme, index) {
      ok && allowed(grapheme, index == 0)
    })
  {
    True -> Ok(Config(..config, schema:))
    False -> Error(InvalidSchema(schema))
  }
}

/// Why `migrate` failed.
pub type MigrateError {
  /// The database was unreachable or refused a statement (`reason`). The
  /// migration runs in one transaction, so it applied nothing, unless the
  /// failure was the commit's own reply; running `migrate` again is safe.
  MigrationFailed(reason: String)
}

/// Describes a migration error for logs.
pub fn describe_migrate_error(error: MigrateError) -> String {
  case error {
    MigrationFailed(reason) -> "saga_postgres migration failed: " <> reason
  }
}

/// Brings the schema up to date: creates it if missing, then applies, in
/// one transaction, each numbered migration it has not applied yet,
/// recording each in `saga_schema_migrations`. Idempotent, and safe to call
/// from several nodes at once: a transaction-scoped advisory lock per
/// schema serialises the callers, and a later one finds nothing to do.
/// Migrations only move forward; a database already migrated further by a
/// newer version of this package is left as it is.
///
/// The same migrations are in `priv/migrations/`, for an application that
/// applies its migrations with its own tool; they take the same lock.
pub fn migrate(config: Config) -> Result(Nil, MigrateError) {
  let schema = config.schema
  pog.transaction(config.connection, fn(connection) {
    let run = fn(sql) {
      pog.query(sql)
      |> pog.timeout(60_000)
      |> pog.execute(connection)
      |> result.replace(Nil)
    }
    // A later statement must see what an earlier locked migration
    // committed, whatever the database's default isolation.
    use Nil <- result.try(run("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
    use _ <- result.try(
      pog.query(
        "SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended($1, 0))) AS l",
      )
      |> pog.parameter(pog.text(migrations.lock_prefix <> schema))
      |> pog.timeout(60_000)
      |> pog.execute(connection),
    )
    use exists <- result.try(
      pog.query("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
      |> pog.parameter(pog.text(schema))
      |> pog.returning(decode.field(0, decode.bool, decode.success))
      |> pog.execute(connection)
      |> result.map(fn(returned) { returned.rows == [True] }),
    )
    // Creating only a missing schema needs no database privilege when an
    // administrator created it beforehand.
    use Nil <- result.try(case exists {
      True -> Ok(Nil)
      False -> run("CREATE SCHEMA " <> quoted(schema))
    })
    use Nil <- result.try(run("SET LOCAL search_path TO " <> quoted(schema)))
    use marked <- result.try(
      pog.query("SELECT to_regclass('saga_schema_migrations') IS NOT NULL")
      |> pog.returning(decode.field(0, decode.bool, decode.success))
      |> pog.execute(connection)
      |> result.map(fn(returned) { returned.rows == [True] }),
    )
    use applied <- result.try(case marked {
      False -> Ok(0)
      True ->
        pog.query(
          "SELECT coalesce(max(version), 0) FROM saga_schema_migrations",
        )
        |> pog.returning(decode.field(0, decode.int, decode.success))
        |> pog.execute(connection)
        |> result.map(fn(returned) {
          list.first(returned.rows) |> result.unwrap(0)
        })
    })
    migrations.all()
    |> list.filter(fn(migration) { migration.version > applied })
    |> list.try_each(fn(migration) { list.try_each(migration.statements, run) })
  })
  |> result.map_error(fn(error) {
    MigrationFailed(case error {
      pog.TransactionQueryError(error) | pog.TransactionRolledBack(error) ->
        store.describe(error)
    })
  })
}

/// The `saga/storage.Storage` of this configuration, over the table
/// `saga_executions` in its schema. Run `migrate` first. Saga owns the
/// heartbeat declared through `storage.with_renewal`, every third of the lease.
///
/// Each pool-backed query attempt has a 4.5-second budget and reports
/// `storage.TimedOut` on expiry. Retries and refusal diagnosis have separate
/// budgets; this is not a total operation deadline. Saga's default call wait
/// is 5 seconds. Transaction-scoped connections and migration use the driver's
/// connection/transaction timing. See
/// docs/adr/0004-separate-query-budgets-from-total-deadlines.md.
pub fn storage(config: Config) -> Storage {
  store.new(
    config.connection,
    quoted(config.schema) <> ".saga_executions",
    duration.to_milliseconds(config.lease),
  )
}

/// A schema name `with_schema` accepted, as an SQL identifier.
fn quoted(schema: String) -> String {
  "\"" <> schema <> "\""
}
