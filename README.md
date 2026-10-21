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

Gleam fetches the pinned [Sinal source](https://github.com/gleam-dream/sinal)
for execution events; no sibling checkout is required. The application
owns any external clients and storage it supplies.

## Run a checkout workflow

Build the workflow once, then run it with each order. The tested checkout
example uses fake services and includes a policy for uncertain payment outcomes.
Call `checkout()` to obtain the workflow; the consumer tests run it against
successful and declined orders.

<!-- prettier-ignore-start -->

<!-- snips: examples/order_consumer/src/order_consumer/usage/checkout.gleam#checkout -->
```gleam
import gleam/time/duration
import saga
import saga/execution
import saga/telemetry
import sinal
import sinal/correlation

pub type CheckoutError {
  OutOfStock
  Declined
  MaybeCharged
}

pub fn reserve(order: String) -> Result(String, CheckoutError) {
  Ok("hold-" <> order)
}

pub fn release(_hold: String) -> Result(Nil, CheckoutError) {
  Ok(Nil)
}

fn charge(
  reservation: String,
  idempotency_key key: String,
) -> Result(String, CheckoutError) {
  case reservation {
    "hold-declined" -> Error(Declined)
    _ -> Ok("receipt-" <> key)
  }
}

/// Two dependent steps: a reservation that can be undone and a payment whose
/// outcome may be unknown, with a compensation policy.
pub fn checkout() {
  let reserve =
    saga.step("reserve_inventory", reserve)
    |> saga.undo(fn(undo) { release(undo.output) })
  let charge =
    saga.effect("charge_payment", fn(reservation, key) {
      // `saga.idempotency_key(key)` is the same for every attempt of this step.
      charge(reservation, idempotency_key: saga.idempotency_key(key))
    })
    |> saga.unknown_when(fn(error) { error == MaybeCharged })
    |> saga.compensate(max_attempts: 3, with: fn(failed) {
      case failed.failure {
        saga.Returned(MaybeCharged) ->
          saga.RetryAfter(duration.milliseconds(500))
        saga.Returned(error) -> saga.Abort(error)
        saga.Crashed(_) | saga.TimedOut -> saga.Hold(MaybeCharged)
      }
    })
  saga.define("checkout", fn(order) {
    order |> saga.perform(reserve) |> saga.perform(charge)
  })
}

```

<!-- prettier-ignore-end -->

A known charge failure runs `release` for the completed reservation. The callback
error type belongs to the application. `execution.run` returns a typed terminal
report that retains failure, cleanup and unknown-effect evidence; `saga/outcome`
classifies it, and `outcome.summary` renders it without application payloads.

A payment error that could mean the charge succeeded needs an explicit
uncertainty policy: `saga.unknown_when` marks the error, and `saga.compensate`
or `saga.on_unknown` chooses retry, rollback or a held run that keeps completed
steps for reconciliation. Idempotency keys stay stable across attempts of one
step.

## Configure a run

Local execution needs no codecs or storage. The process that starts it owns its
lifetime. Defaults allow one attempt per step, use the online scheduler count for
run concurrency, and set a 60-second attempt timeout plus five-second settlement
and cleanup windows. A run deadline is opt-in; every bound is a `Duration`.

<!-- prettier-ignore-start -->

<!-- snips: examples/order_consumer/src/order_consumer/usage/checkout.gleam#configure-run -->
```gleam
/// Configure concurrency, deadline and correlation, then start, watch and
/// await a run instead of blocking in `run`.
pub fn run_configuration() -> Nil {
  let workflow = checkout()
  let config =
    execution.config()
    |> execution.with_max_concurrency(4)
    |> execution.with_deadline(execution.After(duration.seconds(30)))
    |> execution.with_correlation(correlation.unique())
  let assert Ok(exec) = execution.start(workflow, "o-2", config)
  let _ = execution.progress(exec, timeout: duration.seconds(1))
  let assert Ok(execution.Completed(_)) =
    execution.await(exec, timeout: duration.seconds(10))
  Nil
}

```

<!-- prettier-ignore-end -->

Cancel and consume the terminal report with `await` before dropping an
unfinished handle. `execution.start_reporting` delivers the report to another
`Subject`, and `saga/reporting.run_owned` preserves it across the exit of a
short-lived invocation worker.

## Persist progress

A durable run saves a checkpoint at every step boundary and survives the loss of
its runner, caller or VM. Attach versioned codecs and interrupted-effect
resolvers to the same workflow, then admit and drive one stable execution id.
The tested `persistence()` helper in
[the durable example](examples/order_consumer/src/order_consumer/usage/durable_run.gleam)
constructs that definition; `action_for` classifies operational errors:

<!-- prettier-ignore-start -->

<!-- snips: examples/order_consumer/src/order_consumer/usage/durable_run.gleam#durable-run -->
```gleam
pub fn run() -> Nil {
  let assert Ok(store) = memory.start()
  let assert Ok(run) =
    durable.start_or_reconnect(
      persistence(),
      memory.storage(store),
      id: "checkout:o-1",
      input: "o-1",
    )
  let handled = case durable.drive(run, timeout: duration.seconds(30)) {
    Ok(execution.Completed(_)) -> "completed"
    Ok(_) -> "other outcome"
    Error(error) -> action_for(error)
  }
  let assert "completed" = handled
  memory.stop(store)
  Nil
}
```

<!-- prettier-ignore-end -->

Storage adapters are `saga/storage/memory`, `saga/storage/file` and the separate
[PostgreSQL package](integrations/saga_postgres). A local SQL write can commit
before Saga saves its checkpoint, even when both use the same database; the
[financial recovery consumer](https://github.com/gleam-dream/oversight) shows how
an application posting record resolves that interruption without repeating the
posting callback.

## Observe and test

`saga/telemetry` defines six Sinal events that carry the run's correlation, and
every step reads the same correlation from its `EffectKey`. `saga/testing`
provides `wait_until` for synchronizing a test with a run in progress.

The [order consumer](examples/order_consumer) compiles and runs every example in
this README, and the operating topics beyond it, with fake application services:
[checkout and configuration](examples/order_consumer/src/order_consumer/usage/checkout.gleam),
[unknown effects](examples/order_consumer/src/order_consumer/usage/unknown_effect.gleam),
[step correlation](examples/order_consumer/src/order_consumer/usage/correlation.gleam),
[durable runs](examples/order_consumer/src/order_consumer/usage/durable_run.gleam),
[stable ids, reconnecting and discovering unfinished runs](examples/order_consumer/src/order_consumer/usage/operations.gleam),
and [recovering a committed local effect](examples/order_consumer/src/order_consumer/usage/committed_effect.gleam).
The full gate runs them, so an API change that breaks an example fails the build.

## Design and development

The [design source](docs/design/design.typ) holds the architecture and its two
contexts, [execution](docs/design/execution/design.typ) and
[durability](docs/design/durability/design.typ). The [context map](docs/CONTEXT-MAP.md),
[ADRs](docs/adr) and [coverage map](docs/COVERAGE.md) hold vocabulary, rationale and
the part-to-design mapping. Two retained references back the behavior claims:
[docs/ORACLE.md](docs/ORACLE.md) pins Reactor 1.0.6 as the behavioral comparison
and lists every deliberate difference, and [docs/BENCHMARKS.md](docs/BENCHMARKS.md)
records the run-cost method and results.

Run the shared tools and application tests; [AGENTS.md](AGENTS.md) lists focused commands:

```sh
nix flake check
nix run .#design-gate-check -- docs/design . --nested-project integrations/saga_postgres
nix develop --command bash scripts/test.sh
```
