# saga_postgres

`saga_postgres` stores [Saga](https://github.com/gleam-dream/saga) durable executions in PostgreSQL.
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

`persistence` comes from `durable.new`; the application's codecs, versions and
interrupted-effect resolvers are described in the main Saga README. A successful
`checkout` returns the workflow's typed outcome; failure returns `durable.Error`
with its recovery information, and `durable.error_kind` selects the next action.
The same storage supports `durable.read`, `durable.cancel`, `durable.reconnect`
and `durable.unfinished`. Retain a pool connection handle for storage; a
transaction-scoped connection is unsuitable as a reusable store across runners.

## Configuration

| Setting                   | Default                    | Change or ownership                                                                         |
| ------------------------- | -------------------------- | ------------------------------------------------------------------------------------------- |
| Schema                    | `public`                   | `with_schema`; 1–63 lowercase ASCII letters/digits/underscores, first character not a digit |
| Lease                     | 30 seconds, minimum 100 ms | `with_lease(Duration)`                                                                      |
| Renewal interval          | Effective lease / 3        | Declared by the adapter; Saga owns the heartbeat                                            |
| Pool query attempt budget | 4.5 seconds                | Fixed per pool-backed query attempt                                                         |
| Saga operation wait       | 5 seconds                  | `storage.with_call_timeout`; independent of query budget                                    |

Application effects, job scheduling and checkpoint commits have separate
transaction boundaries, and the adapter keeps finished rows indefinitely. Lease,
fencing, query-budget and recovery semantics are in the [design](docs/design/design.typ).

## Migrations

`migrate` applies forward schema migrations in a READ COMMITTED transaction under
the selected schema's advisory lock; a repeat skips applied versions.
[priv/migrations](priv/migrations) ships the equivalent SQL up sections for an
application migration tool, which runs them in a transaction with `search_path` set
to the target schema. The raw up file is not repeat-idempotent. The packaged down
section drops retained execution data, and package `migrate` never runs it.

## Development

Use the parent Saga Nix shell; [AGENTS.md](AGENTS.md) lists the commands. The
disposable PostgreSQL 16 harness is `scripts/test-postgres.sh`, and plain
`gleam test` fails without its script-provided database URL. The harness disables
media durability, so its tests establish no database failover or power-loss
guarantee.
