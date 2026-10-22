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

The application starts, supervises, and stops the pool with pog. Migrate once
at startup, then reuse `saga_postgres.storage(config)` for every execution in the
selected schema. The adapter never owns a pool to stop.

This integration test demonstrates a stored execution and reconnection to its
saved outcome. Its helpers supply a disposable pool and schema: `support.migrated`
configures and migrates the adapter, `one_step` defines a recoverable workflow,
and `start` calls `durable.start_or_reconnect` with input `"x"`.

<!-- prettier-ignore-start -->

<!-- snips: test/saga_postgres/durable_test.gleam#durable-run -->
```gleam
pub fn a_durable_run_finishes_on_postgres_test() {
  use connection <- support.using_pool(4)
  let schema = support.schema()
  let store =
    support.migrated(connection, schema, duration.milliseconds(lease))
    |> saga_postgres.storage
  let persistence =
    one_step(fn(value) { Ok(value <> "!") }, process.new_subject())
  let id = support.id()
  let run = start(persistence, store, id)
  durable.read(run) |> should.equal(Ok(durable.Pending))
  // Saved and not driven: it waits for a runner.
  listed(store, id) |> should.be_true
  durable.drive(run, timeout: duration.seconds(10))
  |> should.equal(Ok(execution.Completed("x!")))
  durable.read(run)
  |> should.equal(Ok(durable.Finished(execution.Completed("x!"))))
  listed(store, id) |> should.be_false
  // Reconnecting returns the saved outcome without running the step again.
  let again = start(persistence, store, id)
  durable.drive(again, timeout: duration.seconds(10))
  |> should.equal(Ok(execution.Completed("x!")))
  support.drop(connection, schema)
}

```

<!-- prettier-ignore-end -->

Application persistence comes from `durable.new`; the application's codecs,
versions and interrupted-effect resolvers are described in the main Saga README.
`durable.drive` returns the workflow's typed outcome or `durable.Error` with its
recovery information; `durable.error_kind` selects the next action.
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
fencing, query-budget and recovery semantics are in the [design](docs/design/design-layer.pdf).

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
