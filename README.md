# saga

A strongly-typed saga/DAG orchestrator for Gleam: typed dependency graphs
instead of dynamic step maps.

**Status:** first local-execution release, not yet published to Hex. Local,
in-memory execution only — no durable journals, no persistence, no
distributed coordination. See [CAPABILITIES.md](CAPABILITIES.md) for the
full implemented/deferred/excluded inventory against
[saga-design.md](https://github.com/gleam-dream/oversight/blob/master/saga-design.md)
in [gleam-dream/oversight](https://github.com/gleam-dream/oversight), and
[CHANGELOG.md](CHANGELOG.md) for what changed release to release.

Behavioral reference: Reactor 1.0.6 (Elixir). Saga is a typed reimagining,
not a port: dependencies are typed `Port` values checked by the compiler,
not names resolved at runtime.

## The common path

```gleam
import saga
import saga/execution

pub type CheckoutError {
  OutOfStock
}

pub fn build_checkout(order_id: String) {
  saga.define("checkout", fn(input) {
    let reserved =
      input
      |> saga.perform(
        saga.step("reserve_inventory", fn(id: String) {
          case has_stock(id) {
            True -> Ok(id)
            False -> Error(OutOfStock)
          }
        })
        |> saga.undo(fn(_id, _reserved) { release_inventory(order_id) }),
      )
    reserved
    |> saga.perform(saga.step("charge_payment", fn(id) { charge(id) }))
  })
}

pub fn run_checkout(order_id: String) {
  let assert Ok(workflow) = build_checkout(order_id)
  execution.run(workflow, order_id, execution.config())
}
```

`define` validates the workflow once (names, attempt budgets, timeouts, and
that every `Port` used belongs to this build); `execution.run` blocks until
the run finishes and returns `Completed`, `Failed(cause, settlement)`,
`Cancelled(reason, settlement)`, or `Unresolved(step, evidence, settlement)`.

## The advanced path

Configure concurrency, a run deadline, and cleanup bounds; start without
blocking; inspect progress; cancel:

```gleam
import gleam/option.{Some}
import saga/execution

let config =
  execution.Config(
    max_concurrency: 4,
    deadline: Some(30_000),
    settle_timeout: 5000,
    cleanup_timeout: 5000,
  )

let assert Ok(exec) = execution.start(workflow, order_id, config)
let assert Ok(progress) = execution.progress(exec, 1000)
execution.cancel(exec)
let assert Ok(outcome) = execution.await(exec, 10_000)
```

Adapt a workflow's error and undo-error types to your own application
vocabulary with `saga.map_errors` (whole workflow) or
`saga.map_step_errors` (one step) — saga never requires you to adopt a
saga-owned error type.

A full external consumer package, exercising both paths plus compensation
and caller-owned types from outside the package (public imports only), is
under [`examples/order_consumer`](examples/order_consumer):

```sh
cd examples/order_consumer
gleam run    # prints a readable trace of four scenarios
gleam test   # asserts the same scenarios
```

## Semantics you should know before relying on this

- **Cancellation never reverses an unknown effect.** A step whose attempt
  or compensation is killed — by its own `timeout`, or because the settle
  window closed before it finished — is reported `interrupted`, not
  undone. Only steps known to have completed are rolled back. The same
  rule applies to a run that times out on its `deadline`.
  `Settlement.not_undoable` and `Settlement.interrupted` exist precisely
  so a caller can see the difference between "reversed" and "unknown."
- **Undo runs in reverse completion order**, not forward order (a
  deliberate difference from the Reactor 1.0.6 oracle; see
  `CAPABILITIES.md`).
- **Resource bounds.** Worst-case run time is bounded by
  `deadline + settle_timeout + (undone entries + compensations) *
cleanup_timeout`: once a run stops admitting new work, in-flight
  attempts and compensations get up to `settle_timeout` to finish on
  their own before being killed, and each individual compensation
  decision or undo action is bounded by `cleanup_timeout`.
- **The workflow builder must be pure and deterministic.** `define`
  evaluates it once to validate and compute static descriptors; every run
  evaluates it again, fresh. If a real run's graph shape differs from what
  `define` recorded, the run fails immediately with `DefinitionChanged`
  before any step is admitted — nothing partially executes against a
  builder that cannot be trusted to reproduce its own shape.
- **`saga.map` is not memoized.** It re-runs in every task that consumes
  the resulting port. Use a `saga.step` for expensive or effectful
  transforms.

## Development

```sh
nix develop
gleam format --check src test
gleam build --warnings-as-errors
gleam test
(cd examples/order_consumer && gleam test && gleam run)
scripts/check_negative.sh
nix fmt
nix flake check
```
