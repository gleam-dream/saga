# saga

A strongly-typed saga/DAG orchestrator for Gleam: typed dependency graphs
instead of dynamic step maps.

**Status:** unpublished. One typed `saga.Workflow` runs in memory with
`saga/execution`, or with saved checkpoints with `saga/durable` over a
storage adapter: memory, file, or PostgreSQL through the separate
[`saga_postgres`](integrations/saga_postgres) package. Local runs need no
codecs or storage. See [DURABILITY.md](DURABILITY.md) for recovery rules and
the adapter contract, and [docs/migration-wave-3.md](docs/migration-wave-3.md)
for the changes from the previous API.

Behavioral reference: Reactor 1.0.6 (Elixir). Saga is a typed reimagining,
not a port: dependencies are typed `Port` values checked by the compiler,
not names resolved at runtime.

## The common path

Define the workflow once, at startup, and run it for each order; the order
is the run's input, not part of the definition.

```gleam
import saga
import saga/execution

pub type CheckoutError {
  OutOfStock
  Declined
  MaybeCharged
}

pub fn checkout() {
  let reserve =
    saga.step("reserve_inventory", reserve)
    |> saga.undo(fn(undo) { release(undo.output) })
  let charge =
    saga.effect("charge_payment", fn(reservation, key) {
      // `key.idempotency` is the same for every attempt of this step.
      charge(reservation, idempotency_key: key.idempotency)
    })
    |> saga.unknown_when(fn(error) { error == MaybeCharged })
    |> saga.compensate(max_attempts: 3, with: fn(failed) {
      case failed.failure {
        saga.Returned(MaybeCharged) -> saga.RetryAfter(500)
        saga.Returned(error) -> saga.Abort(error)
        saga.Crashed(_) | saga.TimedOut -> saga.Hold(MaybeCharged)
      }
    })
  saga.define("checkout", fn(order) {
    order |> saga.perform(reserve) |> saga.perform(charge)
  })
}

pub fn run_checkout(workflow, order) {
  case execution.run(workflow, order, execution.config()) {
    Ok(execution.Completed(receipt)) -> Ok(receipt)
    Ok(outcome) -> Error(execution.unknown_effects(outcome))
    Error(_run_error) -> Error([])
  }
}
```

`define` validates the workflow once (names, attempt budgets, timeouts, and
that every `Port` belongs to this build) and returns the `Workflow`. A
defect is a bug in the source, so `define` panics with a message that names
the workflow and every offending step. When names, budgets or timeouts come
from runtime data, `saga.try_define` returns every `DefinitionError`
instead; `saga.describe_definition_error` renders one.
`execution.run` blocks until the run ends and returns `Completed`,
`CompletedWithUnknownEffects`, `Failed(cause, settlement)`,
`Cancelled(reason, settlement)` or `Unresolved(step, evidence, settlement)`.
`execution.kind(outcome)` classifies an outcome, and
`execution.unknown_effects(outcome)` names every action whose effect is
unknown: a crashed or timed-out attempt, and a returned error that
`unknown_when` marks, such as a payment that may have been charged.
`execution.describe_cause(cause, error: describe)` renders a cause for logs,
with the step's error rendered by `describe`.

An error that `unknown_when` marks, with no `compensate` decision to settle
it, holds the run for reconciliation: the run ends `Unresolved(step, error,
settlement)`, nothing is undone, and `settlement.held` lists the completed
steps left in place. An uncertain payment therefore never releases the stock
reserved before it. This also applies when the decider asked for a retry
that the attempt budget no longer allows, as in `checkout` above after three
`MaybeCharged` answers. A decider's explicit `Abort` still rolls back, and
`saga.on_unknown(saga.RollBack)` makes the step fail and roll back instead
of holding:

```gleam
saga.effect("charge_payment", charge_with_key)
|> saga.unknown_when(fn(error) { error == MaybeCharged })
|> saga.on_unknown(saga.RollBack)
```

Step callbacks receive one record each: `saga.undo` gets
`UndoRequest(input, output, key)`, `saga.compensate` gets
`FailedAttempt(input, failure, attempt, attempts_left, key)`, and
`saga.effect` gets an `EffectKey(idempotency, attempt, attempt_key)`. Read
them by label.

## Configuration

`execution.config()` holds safe defaults; the `with_*` setters change one
bound each. `run`, `start`, `start_reporting` and `durable.drive` check the
configuration and return `InvalidConfig` with every violation.

```gleam
import saga/execution
import sinal/correlation

let config =
  execution.config()
  |> execution.with_max_concurrency(4)
  |> execution.with_deadline(30_000)
  |> execution.with_correlation(correlation.unique())

let assert Ok(exec) = execution.start(workflow, order, config)
let assert Ok(progress) = execution.progress(exec, timeout: 1000)
execution.cancel(exec)
let assert Ok(outcome) = execution.await(exec, timeout: 10_000)
```

A step's own `saga.timeout(..)` always overrides the per-attempt default, in
either direction; `execution.without_step_timeout` is the explicit opt-out
for steps that set none.

To learn the outcome somewhere other than the starting process, start with
`execution.start_reporting(workflow, input, config, to: subject)`. The run
sends its outcome, once, to that `Subject`. The starting process still owns
the run: its exit cancels the run, which settles and rolls back, and the
`Cancelled(OwnerExited, settlement)` outcome still reaches the subject.

Adapt a workflow's error and undo-error types to the application's own
vocabulary with `saga.map_errors` (whole workflow) or `saga.map_step_errors`
(one step); saga never requires a saga-owned error type.

## Defaults

Every wait, retry and saved value is bounded by default.

| Operation                                              | Default                                    | Change it with                                                                  |
| ------------------------------------------------------ | ------------------------------------------ | ------------------------------------------------------------------------------- |
| Concurrent attempts and compensation decisions per run | schedulers online                          | `execution.with_max_concurrency`                                                |
| Run deadline                                           | none; the run is bounded by the rows below | `execution.with_deadline`                                                       |
| Each attempt of a step without its own timeout         | 60 000 ms                                  | `execution.with_step_timeout`, `execution.without_step_timeout`, `saga.timeout` |
| Settle window after a run stops admitting work         | 5 000 ms                                   | `execution.with_settle_timeout`                                                 |
| Each compensation decision and each undo               | 5 000 ms                                   | `execution.with_cleanup_timeout`                                                |
| Attempts per step                                      | 1                                          | `saga.compensate(max_attempts:)`                                                |
| `RetryAfter` delay                                     | capped at 300 000 ms                       | `execution.with_max_retry_delay`                                                |
| `execution.run`                                        | until the run ends (finite, see below)     | use `start` and `await`                                                         |
| `await`, `progress`, `testing.wait_until`              | the caller's timeout (required)            | `timeout:`, `within:`                                                           |
| Coordinator startup handshake                          | 5 000 ms                                   | none                                                                            |
| `durable.drive`                                        | the caller's timeout (required)            | `drive(run, timeout:)`                                                          |
| `durable.drive` caller exits                           | runner stops, checkpoint kept              | none                                                                            |
| Each storage call                                      | 5 000 ms, then `storage.TimedOut`          | `storage.with_call_timeout`                                                     |
| Memory adapter call                                    | 5 000 ms                                   | none                                                                            |
| File adapter mutation lock                             | 5 000 ms, then `storage.Busy`              | none                                                                            |
| Checkpoint size                                        | 16 MiB (16 777 216 bytes)                  | `durable.with_max_checkpoint_bytes`                                             |
| PostgreSQL claim lease                                 | 30 000 ms, renewed every 10 000 ms         | `saga_postgres.with_lease`                                                      |
| Conformance owner-loss window                          | declared by the adapter                    | `conformance.run(owner_loss_within:)`                                           |
| Sinal handlers                                         | synchronous in the coordinator             | route `["saga"]` to a `sinal/forwarder`                                         |

Without a deadline a run is still finite: each step takes at most
`max_attempts * (attempt timeout + cleanup_timeout + max_retry_delay)`, and
once the run stops admitting work it ends within `settle_timeout +
(completed steps + compensations) * cleanup_timeout`. Saga does not default
a run deadline, because a deadline would cut healthy long workflows.

## Durable runs

A durable run saves a checkpoint at every step boundary, so it survives the
loss of its runner, its caller or its VM. Every step declares how to save its
values and how to establish the effect of an attempt that was interrupted.

```gleam
import gleam/dynamic/decode
import gleam/json
import saga
import saga/codec
import saga/durable
import saga/storage/memory

let order = codec.json("order-1", fn(id) { Ok(json.string(id)) }, decode.string)
let text = codec.text()
let charge =
  saga.effect("charge", fn(order, key) { charge(order, key.idempotency) })
  |> saga.undo(fn(undo) { refund(undo.output, undo.key.idempotency) })
  |> durable.recoverable(version: "1", input: order, output: text, resolve: fn(order, key) {
    case lookup_charge(order, key.idempotency) {
      Ok(Charged(receipt)) -> durable.Completed(receipt)
      Ok(NoCharge) -> durable.NotSent
      Error(_) -> durable.MaybeSent
    }
  })
  |> durable.resolve_undo(lookup_refund)
let workflow = saga.define("checkout", saga.perform(_, charge))
let persistence =
  durable.new(workflow, input: order, output: text, error: text, undo_error: text)

let assert Ok(store) = memory.start()
let assert Ok(run) =
  durable.start_or_reconnect(persistence, memory.storage(store), id: "checkout:o-1", input: "o-1")
case durable.drive(run, timeout: 30_000) {
  Ok(outcome) -> handle(outcome)
  Error(error) ->
    case durable.error_kind(error) {
      durable.Busy | durable.Transient -> retry_later()
      durable.NeedsReconciliation -> alert(durable.describe_error(error))
      durable.Incompatible | durable.Defect -> fail(durable.describe_error(error))
    }
}
```

- One `Storage` serves every execution of a store; the execution id given
  to `start_or_reconnect` addresses one. `start_or_reconnect` is idempotent
  for the same id and input; `durable.reconnect` attaches by id alone.
- `durable.new` checks that every step is recoverable and panics, naming
  each offending step and codec, when one is not: that is a bug in the
  source. The workflow version is `"1"`; change it with
  `durable.with_version` whenever the workflow's behavior changes, so a saved
  execution of the old behavior is refused with `IncompatibleDefinition`.
- `drive` waits at most `timeout`. On timeout, when the calling process
  exits, and when the runner is killed or crashes (`RunnerLost`), the runner
  stops: in-flight attempts are killed, the claim is released at once, and
  the next `drive` resumes from the last checkpoint, asking each interrupted
  attempt's resolver what happened. Only the loss of the runner's VM leaves
  the claim to the storage's own expiry (saga_postgres's lease, 30 s by
  default). Only `durable.cancel` cancels.
- `Busy` means another runner holds the claim. Retry it no sooner than the
  storage's owner-loss window, such as saga_postgres's lease: a grind job
  that drives a saga snoozes `Busy` for at least the lease, so that its
  snooze limit cannot end the job while a lost runner's lease runs out.
- Waking a runner after a restart is the application's job, for example a
  grind job per execution. `durable.unfinished(storage, limit:)` lists the
  executions that wait for one.
- Adapters: `saga/storage/memory` (in VM, supervised with
  `memory.supervised(name)`), `saga/storage/file` (one directory, one VM at
  a time) and the [`saga_postgres`](integrations/saga_postgres) package,
  which takes the application's own `pog.Connection` so one pool serves the
  application, grind and saga. `saga/storage/conformance` checks any
  adapter.

## Telemetry

`saga/telemetry` defines six Sinal events: run start and stop, step start
and stop, compensation decisions and undo outcomes. Every event's metadata
carries `workflow`, `run` (this VM's id for one run), `execution` (the
durable id, or `None`) and `correlation` (from `execution.with_correlation`
or `durable.with_correlation`).

```gleam
import saga/telemetry
import sinal

let attachment =
  sinal.observe(telemetry.run_stopped(), fn(_measurements, metadata) {
    log(metadata.correlation, metadata.execution, metadata.outcome)
  })
```

A compensation event reports the delay a `RetryAfter` decision was
scheduled with and whether the cap shortened it.

## Testing workflows

A test that needs to synchronize with a run in progress — waiting for a
step to reach a particular state before releasing it, cancelling it, or
asserting on it — should poll `execution.progress` rather than sleep a
guessed duration. `saga/testing` ships exactly one helper for this:

```gleam
import saga/testing

let assert Ok(progress) =
  testing.wait_until(
    exec,
    matching: fn(p) { p.phase == execution.Settling },
    within: 10_000,
  )
```

`wait_until` polls `execution.progress` at a short internal interval (never
a fixed `process.sleep`) until `matching` accepts a snapshot or `within`
milliseconds elapse overall, using the monotonic clock for the deadline.
`Error(testing.WaitTimedOut)` means the predicate never matched in time;
`Error(testing.RunEnded)` means the run's coordinator process was already
gone (`execution.progress`'s own `ExecutionEnded`).

**Polling can miss a state that passes through quickly.** `matching` only
ever sees whatever snapshot happens to be current at each poll; a state
entered and left again between two polls is never observed. Write
`matching` to accept the target state _or anything at or beyond it_ — for
example, a predicate waiting for a step to reach `Compensating` should also
accept `RetryScheduled` (the recovery decision may already have landed and
scheduled a backoff by the time a poll arrives), or it can time out on a
perfectly healthy run that simply raced past `Compensating` first.

Saga deliberately does not ship a gate a step body can block on — that is
generic BEAM concurrency, not anything Saga-specific. The recipe below (a
small broker process, trimmed from `examples/order_consumer`'s own
`test/support/gate.gleam`) is enough for most workflow tests: a step calls
`enter` to block and announce arrival, the test calls `wait_entered` to
synchronize on that arrival, then `open` to release exactly one blocked
`enter` call (queuing the release if none is waiting yet):

```gleam
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list

pub opaque type Gate {
  Gate(broker: Subject(GateMessage), arrived: Subject(Pid))
}

type GateMessage {
  Register(reply: Subject(Nil))
  Release
}

pub fn new_gate() -> Gate {
  let ready = process.new_subject()
  process.spawn(fn() {
    let broker = process.new_subject()
    process.send(ready, broker)
    gate_loop(broker, [])
  })
  let assert Ok(broker) = process.receive(ready, 1000)
  Gate(broker: broker, arrived: process.new_subject())
}

fn gate_loop(broker: Subject(GateMessage), waiters: List(Subject(Nil))) -> Nil {
  case process.receive_forever(broker) {
    Register(reply) -> gate_loop(broker, [reply, ..waiters])
    Release ->
      case list.reverse(waiters) {
        [] -> gate_loop(broker, [])
        [oldest, ..rest] -> {
          process.send(oldest, Nil)
          gate_loop(broker, list.reverse(rest))
        }
      }
  }
}

// A `Subject` may only be received on by the process that created it, and a
// step's body runs inside a fresh task process on every attempt — so `enter`
// registers the calling process's own reply subject with the broker rather
// than the test process handing out one it owns.
pub fn enter(gate: Gate) -> Nil {
  process.send(gate.arrived, process.self())
  let my_reply = process.new_subject()
  process.send(gate.broker, Register(my_reply))
  process.receive_forever(my_reply)
}

pub fn open(gate: Gate) -> Nil {
  process.send(gate.broker, Release)
}

pub fn wait_entered(gate: Gate, timeout_ms: Int) -> Result(Pid, Nil) {
  process.receive(gate.arrived, timeout_ms)
}
```

See `examples/order_consumer/test/support/gate.gleam` for the full version
(handles a `Release` that arrives before anyone is waiting yet) and
`examples/order_consumer/test/order_consumer_test.gleam`'s cancellation
scenario for it in use alongside `execution.progress`.

## Semantics you should know before relying on this

- **Cancellation never reverses an unknown effect.** A step whose attempt
  or compensation is killed (by its own timeout, or because the settle
  window closed) is reported `interrupted`, not undone. Only steps known to
  have completed are rolled back. `Settlement.not_undoable` and
  `Settlement.interrupted` show the difference between "reversed" and
  "unknown".
- **An unknown effect holds the run unless the step opts into rollback.**
  A returned error that `saga.unknown_when` marks ends the run `Unresolved`
  when no `compensate` decision settles it; `saga.on_unknown(saga.RollBack)`
  fails the run and undoes the completed steps instead. Crashes and timeouts
  keep failing the run and rolling back.
- **Every outcome says which effects are unknown.** Each action of a run (a
  step attempt, a compensation decision, an undo) ends with a known result
  or with an unknown effect: it crashed, it was killed at its time bound or
  when the settle window closed, or it returned an error that
  `saga.unknown_when` marks. `execution.unknown_effects(outcome)` lists every
  action of the second kind as `UnknownEffect(step, action, ending)` and is
  `[]` exactly when every effect is known. An action is recorded when it
  ends, so a later decision cannot hide it: a crashed or "maybe sent"
  attempt retried to success makes the run `CompletedWithUnknownEffects`.
- **A `StepFailed` cause may follow a crash.** A `compensate` decider is
  asked about crashed and timed-out attempts too, and its `Abort(error)` is
  reported as `StepFailed(step, error)`. Return an error that says the
  attempt crashed when the caller must tell the two apart from the cause.
- **A refused retry is distinguished from an exhausted one.** A retry
  decided after the run began settling for another reason is
  `Cause.RetrySuperseded`, not `RetryLimitReached`.
- **Undo runs in reverse completion order**, not forward order (a
  deliberate difference from Reactor 1.0.6; see `CAPABILITIES.md`), and an
  undo runs at most once.
- **The workflow builder runs once, at `define` time.** No run evaluates the
  builder again. Per-run values live in a store keyed by node id and
  isolated per run, so concurrent runs of one `Workflow` never see each
  other's data.
- **A step whose output never reaches the workflow's output is rejected**
  at `define` time as `OrphanStep(step)`, instead of silently never running.
- **`saga.map` is not memoized.** It re-runs in every task that consumes
  the resulting port. Use a `saga.step` for expensive or effectful work.
- **`saga.all` takes a required first port**, so there is no empty case:
  split a `List(Port(..))` with a `case` and handle `[]` yourself.
- **The settle window is set per run, not per cancel.** Settling ends as
  soon as nothing is in flight; `with_settle_timeout(0)` rolls back at once
  and reports in-flight steps `interrupted`.
- **Await once more after `cancel`.** `await` drains its monitor and the
  outcome only on the call that consumes them; an `Execution` dropped after
  a timed-out `await` can leave a message in the owner's mailbox.
- **A reported outcome survives the owner; a killed coordinator does not.**
  `start_reporting`'s subject receives at most one message, after rollback.
  Monitor `execution.pid(exec)` to detect a killed coordinator: its `Down`
  always arrives after its outcome.
- **A local `EffectKey` belongs to one run.** Its `idempotency` derives from
  the run's id, which `execution.run` creates anew each time. A durable
  execution's keys derive from its id and survive restarts.

## Design decisions

**The scheduler keeps a central per-run value store** instead of the
per-node typed cells that saga-design.md's "Typed DAG construction" section
asks for. `saga/internal/store` holds one `Dict(Int, Native)` per run, keyed
by node id, where `Native` is an opaque, type-erased carrier that is only
ever cast back to the type it was stored as. The cast is sound by
construction: a node id and its element type are bound together once, in
the `perform` call that allocates the id and returns the typed, opaque
`Port` whose `fetch` reads it back, and no other code can build a `Port` for
that id. With that invariant, a dependency read is a map lookup and
admission is a min-heap operation, so a run's scheduling cost is linear in
its steps; `bench/RESULTS.md` has the measurements.

## Development

```sh
nix develop
gleam format --check src test
gleam build --warnings-as-errors
gleam test
(cd examples/order_consumer && gleam test && gleam run)
scripts/check_negative.sh
scripts/check_durable_restart.sh
(cd integrations/saga_postgres && scripts/test-postgres.sh)
nix fmt
nix flake check
```

`examples/order_consumer` is the external acceptance consumer: it imports
only saga's public modules.
