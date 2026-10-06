# saga

Saga runs typed workflows of dependent steps in Gleam. It supports retries,
undo of completed steps, cancellation and recovery from saved checkpoints.

## Installation

Saga is unpublished and targets Erlang with Gleam 1.18 or later. Add a local
checkout to your application's `gleam.toml`:

```toml
[dependencies]
saga = { path = "../saga" }
```

Keep a [Sinal checkout](https://github.com/gleam-dream/sinal) beside Saga:
Saga currently depends on `../sinal` for its execution events. The application
owns any external clients and storage it supplies.

## Run a checkout workflow

Build the workflow once, then run it with each order. This complete module takes
application service callbacks; the `main` function supplies fake services so the
example runs without a provider.

```gleam
import saga
import saga/execution

pub type CheckoutError {
  OutOfStock
  Declined
}

pub fn checkout(
  reserve: fn(String) -> Result(String, CheckoutError),
  release: fn(String) -> Result(Nil, CheckoutError),
  charge: fn(String, String) -> Result(String, CheckoutError),
) {
  let reservation =
    saga.step("reserve_inventory", reserve)
    |> saga.undo(fn(undo) { release(undo.output) })
  let payment =
    saga.effect("charge_payment", fn(hold, key) {
      charge(hold, saga.idempotency_key(key))
    })
  saga.define("checkout", fn(order) {
    order |> saga.perform(reservation) |> saga.perform(payment)
  })
}

pub fn main() {
  let workflow =
    checkout(
      fn(order) { Ok("hold-" <> order) },
      fn(_hold) { Ok(Nil) },
      fn(hold, key) { Ok("receipt-" <> hold <> ":" <> key) },
    )
  let assert Ok(execution.Completed(_receipt)) =
    execution.run(workflow, "o-1", execution.config())
}
```

A known charge failure runs `release` for the completed reservation. The callback
error type belongs to the application. `execution.run` returns a typed terminal
report that retains failure, cleanup and unknown-effect evidence; inspect that
report before using [outcome classification](docs/USAGE.md#act-on-an-executions-outcome).

A payment error that could mean the charge succeeded needs an explicit
uncertainty policy. The [checkout guide](docs/USAGE.md#define-a-checkout-workflow)
shows `unknown_when`, compensation and held reconciliation. Idempotency keys
stay stable across attempts of one step; local runs receive a fresh run identity.

## Configure and recover

Local execution needs no codecs or storage. The process that starts it owns its
lifetime. When using `start`, cancel and consume the terminal report with `await`
before dropping an unfinished handle.

Defaults allow one attempt per step, use the online scheduler count for run
concurrency, and set a 60-second attempt timeout plus five-second settlement and
cleanup windows. A run deadline is opt-in. [Configuration and defaults](docs/USAGE.md#configuration)
explain overrides and timing limits.

The [usage guide](docs/USAGE.md) covers configuration, correlation, telemetry,
test synchronization and reporting across owner exit. The [order consumer](examples/order_consumer/src/order_consumer/workflows.gleam)
shows shared dependencies and application-owned records and error types. [Durable operations](DURABILITY.md)
cover codecs, resolvers, saved identities, timing margins and recovery. Storage
adapters include memory, file and the separate [PostgreSQL package](integrations/saga_postgres).

## Benchmarks and design

The [benchmark guide](bench/README.md) includes retained measurements and a
reproduction command. These measure trivial workflow construction and scheduling,
with their historical revision and environment limits stated beside the results.

Read the [design PDF](docs/design/design-layer.pdf) for architecture and ownership,
and [ADRs](docs/adr) for decision rationale. The [design source](docs/design/design.typ),
[vocabulary](docs/design/CONTEXT.typ) and [coverage map](docs/COVERAGE.md) are also available.

Reactor 1.0.6 is a scoped behavioral reference. [Provenance](PROVENANCE.md)
records the oracle revision, test coverage and deliberate behavior differences.

## Development

Run from this repository:

```sh
nix develop
gleam format --check src test
gleam build --warnings-as-errors
gleam test
(cd examples/order_consumer && gleam test && gleam run)
```

The [agent instructions](AGENTS.md#gates) list compiler-negative, restart,
benchmark and adapter checks for changes affecting those contracts.
