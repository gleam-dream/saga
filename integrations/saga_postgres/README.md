# saga_postgres

`saga_postgres` stores [Saga](../../README.md) durable executions in PostgreSQL.
It borrows the application's `pog.Connection` and implements `saga/storage`.

## Installation

The package is unreleased and uses local path dependencies. With an application,
Saga checkout, and Sinal checkout in sibling directories, add these dependencies
to the application for the example below:

```toml
[dependencies]
gleam_stdlib = ">= 0.70.0 and < 2.0.0"
gleam_time = ">= 1.11.0 and < 2.0.0"
pog = ">= 4.1.0 and < 4.2.0"
saga = { path = "../saga" }
saga_postgres = { path = "../saga/integrations/saga_postgres" }
```

The adapter targets Erlang and is developed and tested against PostgreSQL 16.
Its pog and pgo minor ranges must stay aligned with Grind when both share a pool;
the [dependency decision](docs/adr/0001-keep-postgres-as-a-borrowed-pool-adapter.md)
explains that constraint.

## Use the application pool

The application starts, supervises, and stops the pool with pog. Pass its pooled
connection to `prepare_storage` once at startup, then reuse the storage for every
execution in the selected schema. Neither function below owns a pool to stop.

```gleam
import gleam/result
import gleam/time/duration
import pog
import saga/durable
import saga/execution
import saga/storage
import saga_postgres

pub fn prepare_storage(
  db: pog.Connection,
) -> Result(storage.Storage, saga_postgres.MigrateError) {
  let config = saga_postgres.config(db)
  use Nil <- result.try(saga_postgres.migrate(config))
  Ok(saga_postgres.storage(config))
}

pub fn checkout(
  persistence: durable.Persistence(i, o, e, u),
  store: storage.Storage,
  order_id: String,
  order: i,
) -> Result(execution.Outcome(o, e, u), durable.Error) {
  use run <- result.try(durable.start_or_reconnect(
    persistence,
    store,
    id: "checkout:" <> order_id,
    input: order,
  ))
  durable.drive(run, timeout: duration.seconds(30))
}
```

`persistence` comes from `durable.new`. The application's workflow, root and step
codecs, compatible versions, and interrupted-effect resolvers are described in
[Saga's durable usage guide](../../DURABILITY.md). A successful `checkout` returns
the workflow's typed outcome; failure returns `durable.Error` without discarding
its recovery information. Use `durable.error_kind` to decide the next action:
Busy can require waiting for a lost owner's lease, while NeedsReconciliation
requires application evidence before an interrupted effect may run again.

The same storage supports `durable.read`, `durable.cancel`, `durable.reconnect`,
and `durable.unfinished`. Retain a pool connection handle for storage; a
transaction-scoped connection has the driver's transaction lifetime and is
unsuitable as a reusable store across runners.

## Configuration and operations

| Setting                   | Default                    | Change or ownership                                                                         |
| ------------------------- | -------------------------- | ------------------------------------------------------------------------------------------- |
| Schema                    | `public`                   | `with_schema`; 1–63 lowercase ASCII letters/digits/underscores, first character not a digit |
| Lease                     | 30 seconds, minimum 100 ms | `with_lease(Duration)`                                                                      |
| Renewal interval          | Effective lease / 3        | Declared by the adapter; Saga owns the heartbeat                                            |
| Pool query attempt budget | 4.5 seconds                | Fixed per pool-backed query attempt                                                         |
| Saga operation wait       | 5 seconds                  | `storage.with_call_timeout`; independent of query budget                                    |

Query retries and refusal diagnosis can issue more than one statement, so the
attempt budget is not a total operation deadline. Transaction-scoped connections
and migration use the driver's transaction/connection timing. The
[deadline decision](docs/adr/0004-separate-query-budgets-from-total-deadlines.md)
records this limit. A timed-out or lost reply does not prove a write was absent.

A successful release makes an execution available immediately. Without release,
takeover becomes possible when the database-clock lease expires. A successful
takeover replaces the generation/token and fences the former claim's writes.
An expired claim that has not been replaced can still commit, renew, or release.
Fencing cannot retract an already sent external effect; use stable effect
identities and Saga's recovery evidence.

Applications own scanning and wakeups. `durable.unfinished(storage, limit:)`
returns candidates without reserving them; reconnect and drive can still
encounter Busy. The adapter retains finished rows indefinitely and has no
pruning API. Application effects, job scheduling, and checkpoint commits have
separate transaction boundaries.

## Migrations

`migrate` applies forward schema migrations in a READ COMMITTED transaction under
the selected schema's advisory lock. Concurrent package callers serialize, and a
repeat skips applied versions.

[priv/migrations](priv/migrations) ships the equivalent SQL up sections. An
application migration tool selects unapplied versions under that lock and runs
them in a transaction with `search_path` set to the target schema. The raw up file
itself is not repeat-idempotent.

The packaged down section drops retained execution data. Package `migrate` never
runs it or converts Saga checkpoint bytes.

## Development

Use the parent Saga Nix shell. From this package, check formatting and compilation:

```sh
nix develop ../.. --command bash -c 'gleam format --check src test && gleam build --warnings-as-errors'
```

Run tests with the disposable PostgreSQL 16 harness:

```sh
nix develop ../.. --command scripts/test-postgres.sh
```

The script clears `PG*` settings and the prior test URL, creates its own loopback
cluster at a free port, and stops/removes the cluster on exit. Plain `gleam test`
fails without its script-provided `SAGA_TEST_DATABASE_URL`.

Tests cover Saga's public storage conformance, schema/SQL equivalence, fencing,
cancellation races, renewal, query timeout, runner loss/recovery, and shared
application-pool use. The harness disables media durability, so these tests do
not establish database failover, multi-VM partition behavior, or acknowledged-write
survival.

The [design](docs/design/design.typ), [rendered design](docs/design/design-layer.pdf),
[vocabulary](docs/design/CONTEXT.typ), [decisions](docs/adr), and
[coverage](docs/COVERAGE.md) describe the adapter and its unresolved extensions.
Saga owns the shared [storage protocol](../../docs/design/design.typ#storage-and-claim-lifetime).
[AGENTS.md](AGENTS.md) gives the nested design-document commands.
