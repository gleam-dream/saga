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

Run the shared registry from this repository:

```sh
nix develop --command python3 -B scripts/check.py fast
nix develop --command python3 -B scripts/check.py full
nix develop .#oracle --command python3 -B scripts/check.py oracle
nix develop --command python3 -B scripts/check.py benchmark
```

| Profile     | Obligations and evidence                                                                                                                                                          |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fast`      | Formatting, Ruff correctness lint, ShellCheck, actionlint, gate regressions and root strict build/tests.                                                                          |
| `ci`        | All four packages; external order consumer tests and executable; strict benchmark build; disposable PostgreSQL 16; positive/negative compiler fixtures; actual VM restart probes. |
| `design`    | Both native layers' render freshness, vocabulary, links and integrity.                                                                                                            |
| `full`      | All `ci` and `design` obligations.                                                                                                                                                |
| `oracle`    | Seven real Reactor 1.0.6 scenarios, captured upstream output and normalized Saga comparison, including retained deliberate differences; full root tests.                          |
| `benchmark` | Existing chain/fan/wide harness with correctness assertions and observational timings; no latency threshold.                                                                      |

Every package build treats Gleam warnings as errors and separately compiles
authored Erlang in `src` and `test` with `erlc -Werror` and dependency includes.
Generated dependency code and retained oracle/fixture bytes are excluded from
authored script formatting/lint. Ruff checks syntax, imports and undefined names;
it is not a Python type checker. Gate commands check the tree without repairing it.

The ordinary push, pull request and manual workflow requires both `ci` and
`design` results. A separate workflow runs the oracle on relevant source changes,
weekly and manually; observational benchmarks run weekly and manually. Each profile retains per-check logs,
nonempty result entries, dependency revisions and environment/lock metadata in
`.artifacts/PROFILE`. Failures stop the profile and retain their evidence.

Hosted verification checks out immutable revisions from `sibling-revisions.txt`.
Sinal and JSON Blueprint use ordinary checkout with the default GitHub Actions
token to read their public repositories. Checkout never persists credentials.
Fork pull requests run the same required checks.

The oracle proves only its seven scoped scenarios, not general Reactor or
Temporal parity. The database harness disables fsync and synchronous_commit;
passing runner/VM recovery tests do not prove database power-loss durability.
The [benchmark guide](bench/README.md) gives the retained measurement method and
limits. The [agent instructions](AGENTS.md#gates) describe the retained consumers
and restart probes.
