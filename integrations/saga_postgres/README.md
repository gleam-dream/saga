# saga_postgres

A PostgreSQL storage for [saga](../../README.md)'s durable runs, for runners
on one node or several nodes that share one database. Each execution is one
row of the table `saga_executions`: its checkpoint bytes, revision,
ownership generation, cancellation flag, phase, and claim (a random token
and a lease expiry). The package implements the `saga/storage` contract and
passes saga's conformance suite (`saga/storage/conformance`).

It depends on `saga` and `pog` (4.1, over `pgo` 0.20). Saga itself does not
depend on pog. It is developed and tested against PostgreSQL 16.

## Usage

The application owns the connection pool and passes its `pog.Connection`.
Run `migrate` once the pool is up, then hand `storage(config)` to
`saga/durable`:

```gleam
import gleam/erlang/process
import pog
import saga/durable
import saga_postgres

let pool = process.new_name("db")
let assert Ok(pool_config) = pog.url_config(pool, database_url)
// Under a supervisor, add `pog.supervised(pool_config)` instead.
let assert Ok(_) = pog.start(pool_config |> pog.pool_size(10))
let db = pog.named_connection(pool)

let config = saga_postgres.config(db)
let assert Ok(Nil) = saga_postgres.migrate(config)
let storage = saga_postgres.storage(config)

// `persistence` comes from `durable.new`, as with any saga storage.
let assert Ok(run) =
  durable.start_or_reconnect(persistence, storage, id: "checkout:order-123", input: order)
let outcome = durable.drive(run, timeout: 30_000)
```

`durable.read`, `durable.cancel`, `durable.reconnect` and
`durable.unfinished` take the same storage. One storage value serves every
execution; build it once and share it.

## Defaults

| Setting           | Default                     | Change with                               |
| ----------------- | --------------------------- | ----------------------------------------- |
| Lease             | 30 000 ms (at least 100 ms) | `with_lease`                              |
| Renewal           | every lease / 3 (10 000 ms) | follows the lease                         |
| Query timeout     | 4 500 ms per statement      | fixed, below saga's call timeout          |
| Call timeout      | 5 000 ms                    | `storage.with_call_timeout` (saga)        |
| Schema            | `public`                    | `with_schema` (1-63 of `a-z`, `0-9`, `_`) |
| Owner-loss window | the lease                   | follows the lease                         |

A query slower than 4 500 ms fails with `storage.TimedOut` before saga gives
up on the call. Raising saga's call timeout does not raise the query
timeout.

## When a runner dies

A runner claims an execution before it runs it, and saga renews the claim
every lease / 3 while the runner lives, however long a step takes. When the
runner is killed or crashes while `drive`'s caller lives, `drive` releases
the claim at once and returns `RunnerLost`, so the next `drive` resumes
without waiting for the lease. A runner whose node dies stops renewing, and
nothing releases its claim. That claim ends when the lease expires, judged by the database's clock (`clock_timestamp()`), so the
nodes' clocks need not agree. Until then another `drive` of the execution
returns `StorageFailure(Busy)`, and `durable.unfinished` does not list it:
retry `Busy` no sooner than the lease, so that a job's snooze limit cannot
run out first.
After expiry, `durable.unfinished` lists it, and the next `drive` claims it
with a new generation and token and resumes from the last checkpoint. It
asks the resolver of each interrupted step what happened instead of running
the step again. The dead runner's claim is fenced: any commit with it fails
with `StaleOwner`.

A claim whose lease expired stays current until another claim replaces it,
so a runner that was only slow may still commit; the commit renews the
lease.

Saga does not wake runners. Call `durable.unfinished(storage, limit:)` from
a sweeper or a job, and `durable.reconnect` plus `durable.drive` for each id.

## One pool for the application, grind and saga

`saga_postgres` keeps only the application's `pog.Connection`, so the
application's own queries, grind, and saga's storage share one pool. Each
storage operation checks a connection out for one statement and returns it,
so a step may query the same pool.

`pog` and `pgo` are pinned to `>= 4.1.0 and < 4.2.0` and
`>= 0.20.0 and < 0.21.0`, not to the next major version. Grind pins these
exact minor ranges because its FFI matches pog's and pgo's private
connection shapes. An application that uses both packages resolves one
version of each, so these ranges must stay aligned with grind's
`gleam.toml`.

## Migrations

`migrate` creates the schema if missing and applies each numbered migration
not yet applied, in one transaction, under a transaction-scoped advisory
lock per schema. It records versions in `saga_schema_migrations`. It is
idempotent and safe to call from several nodes at once. Migrations only move
forward.

The same SQL is in `priv/migrations/` (`--- migration:up` /
`--- migration:down` sections) for an application that applies migrations
with its own tool. Run it with `search_path` set to the target schema; it
takes the same lock.

The statements are written for READ COMMITTED, PostgreSQL's default. A
write that fails to serialise under REPEATABLE READ or SERIALIZABLE is
retried a few times.

## Tests

The tests run against a throwaway PostgreSQL 16 cluster only. From the saga
dev shell (`nix develop`), which provides PostgreSQL:

```sh
cd integrations/saga_postgres
scripts/test-postgres.sh
```

The script runs `initdb` in a temporary directory, starts the server on
127.0.0.1 at a random free port with trust authentication and `fsync` off,
ignores every `PG*` variable, exports `SAGA_TEST_DATABASE_URL`, runs
`gleam test`, and removes the cluster on exit. Plain `gleam test` fails,
because `SAGA_TEST_DATABASE_URL` is unset.
