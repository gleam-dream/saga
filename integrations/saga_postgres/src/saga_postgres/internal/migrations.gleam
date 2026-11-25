//// The schema's forward-only migrations, numbered from 1. `migrate` runs
//// these statements; `priv/migrations/*.sql` holds the same statements for
//// an application that applies its migrations with its own tool
//// (`--- migration:up` ... `--- migration:down` ... `--- migration:end`,
//// cigogne's format), serialised by the same advisory lock.
//// `migrations_test` checks that the two agree statement for statement.
////
//// Each step's statements run with `search_path` set to the target schema
//// only, so they name no schema. The first statement of every step takes
//// the migration lock of that schema; the last records the step in
//// `saga_schema_migrations`.
////
//// Adding a step: append a `Migration` with the next version, add its SQL
//// file, and never change a released step.

pub type Migration {
  Migration(version: Int, file: String, statements: List(String))
}

/// The advisory lock key text of a schema's migrations, which the lock
/// statement below computes from `current_schema()`.
pub const lock_prefix = "saga-postgres-migrate:"

const lock =
  "SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('saga-postgres-migrate:' || current_schema(), 0))) AS l"

pub fn all() -> List(Migration) {
  [
    Migration(1, "20261002000000-saga_postgres_v1.sql", [
      lock,
      "CREATE TABLE saga_schema_migrations (version integer PRIMARY KEY, installed_at timestamptz NOT NULL DEFAULT clock_timestamp())",
      "CREATE TABLE saga_executions (id text PRIMARY KEY, revision bigint NOT NULL CHECK (revision >= 0), generation bigint NOT NULL CHECK (generation >= 0), cancelled boolean NOT NULL DEFAULT false, phase text NOT NULL DEFAULT 'pending' CHECK (phase IN ('pending', 'suspended', 'finished')), data bytea NOT NULL, owner_token text, lease_until timestamptz, created_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), CHECK ((owner_token IS NULL) = (lease_until IS NULL)))",
      "CREATE INDEX saga_executions_unfinished ON saga_executions (created_at, id) WHERE phase <> 'finished'",
      "INSERT INTO saga_schema_migrations (version) VALUES (1)",
    ]),
    // A checkpoint becomes a set of records, one row each, so that a commit
    // writes only the records that changed. Saved checkpoints of the earlier
    // single-record format are dropped: drain unfinished executions first.
    Migration(2, "20261125000000-saga_postgres_v2.sql", [
      lock,
      "ALTER TABLE saga_executions DROP COLUMN data",
      "CREATE TABLE saga_records (execution_id text NOT NULL REFERENCES saga_executions (id) ON DELETE CASCADE, key text NOT NULL, data bytea NOT NULL, PRIMARY KEY (execution_id, key))",
      "DROP INDEX saga_executions_unfinished",
      "CREATE INDEX saga_executions_listing ON saga_executions (phase, id COLLATE \"C\")",
      "INSERT INTO saga_schema_migrations (version) VALUES (2)",
    ]),
  ]
}
