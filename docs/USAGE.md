# Using Saga

These examples cover configuration, uncertainty, persistence, observation and
reporting. Start with the [README](../README.md) for installation and an ordinary
local run. The separate [order consumer](../examples/order_consumer) compiles
and exercises the examples with fake application services.

## Define a checkout workflow

Define the workflow once, at startup, and run it for each order; the order
is the run's input, not part of the definition.

```gleam
import gleam/time/duration
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
`saga.effect` gets an opaque `EffectKey`. Read the records by label and the
key with its accessors: `saga.idempotency_key(key)`,
`saga.attempt_number(key)`, `saga.attempt_key(key)` and
`saga.correlation_of(key)`.

**A step reads its run's correlation** from the `EffectKey` that `effect`,
`undo` and `compensate` already receive, so a step's own clients join the
run's events without threading the value by hand:

```gleam
saga.effect("refund", fn(refund, key) {
  let shop = shop.with_correlation(shop, saga.correlation_of(key))
  shop.refund(shop, refund, idempotency: saga.idempotency_key(key))
})
```

`saga.correlation_of(key)` is the correlation set with
`execution.with_correlation` or `durable.with_correlation`, the same value
that the run's `saga/telemetry` events carry. A durable execution that sets
none carries `correlation.from_key(id)` of its execution id, and a local run
that sets none gets a fresh `correlation.unique()` at start. Every run has a
correlation, so `saga.correlation_of(key)` returns a `Correlation`, not an
`Option`. `saga.step` hands its function the input only: use `effect` for a step that needs the context.
The durable resolvers (`recoverable`, `resolve_undo`, `resolve_compensation`)
receive the same key.

**A durable execution keeps one correlation.** The first drive saves the
correlation it uses (the handle's, or `from_key(id)`) with the checkpoint,
and every later drive, on any handle, reads it back: a `with_correlation`
call after the first drive is ignored, and an execution saved by an earlier
release reads as `from_key(id)`. Set `durable.with_correlation` before the
first `drive`.

## Configuration

`execution.config()` sets the defaults below; the `with_*` setters change one
bound each. `run`, `start`, `start_reporting` and `durable.drive` check the
configuration and return `InvalidConfig` with every violation.

```gleam
import gleam/time/duration
import saga/execution
import sinal/correlation

let config =
  execution.config()
  |> execution.with_max_concurrency(4)
  |> execution.with_deadline(execution.After(duration.seconds(30)))
  |> execution.with_correlation(correlation.unique())

let assert Ok(exec) = execution.start(workflow, order, config)
let assert Ok(progress) = execution.progress(exec, timeout: duration.seconds(1))
execution.cancel(exec)
let assert Ok(outcome) = execution.await(exec, timeout: duration.seconds(10))
```

Every timeout, deadline, interval and lease in saga is a `gleam/time/duration`
`Duration`; no public function takes milliseconds as an `Int`. A bound that
may be lifted takes an `execution.Timeout`, `After(duration)` or `Infinity`,
and unbounded is always the explicit `Infinity`. A step's own
`saga.timeout(step, duration)` always overrides the per-attempt default, in
either direction; `execution.with_step_timeout(execution.Infinity)` is the
explicit opt-out for steps that set none.

To learn the outcome somewhere other than the starting process, start with
`execution.start_reporting(workflow, input, config, to: subject)`. The run
sends its outcome, once, to that `Subject`. The starting process still owns
the run: its exit cancels the run, which settles and rolls back, and the
`Cancelled(OwnerExited, settlement)` outcome still reaches the subject.

Adapt a workflow's error and undo-error types to the application's own
vocabulary with `saga.map_errors` (whole workflow) or `saga.map_step_errors`
(one step); saga never requires a saga-owned error type.

## Defaults

Attempt, settlement, cleanup, retry-delay and checkpoint limits have defaults.
The run deadline is opt-in; observation and drive waits take caller bounds.

| Operation                                              | Default                                          | Change it with                                                        |
| ------------------------------------------------------ | ------------------------------------------------ | --------------------------------------------------------------------- |
| Concurrent attempts and compensation decisions per run | schedulers online                                | `execution.with_max_concurrency`                                      |
| Run deadline                                           | `Infinity`; the run is bounded by the rows below | `execution.with_deadline`                                             |
| Each attempt of a step without its own timeout         | 60 seconds                                       | `execution.with_step_timeout` (`After` or `Infinity`), `saga.timeout` |
| Settle window after a run stops admitting work         | 5 seconds                                        | `execution.with_settle_timeout`                                       |
| Each compensation decision and each undo               | 5 seconds                                        | `execution.with_cleanup_timeout`                                      |
| Attempts per step                                      | 1                                                | `saga.compensate(max_attempts:)`                                      |
| `RetryAfter` delay                                     | capped at 5 minutes                              | `execution.with_max_retry_delay`                                      |
| `execution.run`                                        | until an outcome or coordinator loss             | use `start` and `await`                                               |
| `await`, `progress`, `testing.wait_until`              | the caller's `Duration` (required)               | `timeout:`, `within:`                                                 |
| Coordinator startup handshake                          | 5 seconds                                        | none                                                                  |
| `durable.drive`                                        | the caller's `Duration` (required)               | `drive(run, timeout:)`                                                |
| `durable.drive` caller exits                           | runner stops, checkpoint kept                    | none                                                                  |
| Each storage call                                      | 5 seconds, then `storage.TimedOut`               | `storage.with_call_timeout`                                           |
| Memory adapter call                                    | 5 seconds                                        | none                                                                  |
| File adapter mutation lock                             | 5 seconds, then `storage.Busy`                   | none                                                                  |
| Checkpoint size                                        | 16 MiB (16 777 216 bytes)                        | `durable.with_max_checkpoint_bytes`                                   |
| PostgreSQL claim lease                                 | 30 seconds, renewed every 10 seconds             | `saga_postgres.with_lease`                                            |
| Conformance owner-loss window                          | declared by the adapter                          | `conformance.run(owner_loss_within:)`                                 |
| Correlation of a run, its events and its steps         | local: `unique()`; durable: `from_key(id)`       | `execution.with_correlation`, `durable.with_correlation`              |
| Sinal handlers                                         | synchronous in the coordinator                   | route `["saga"]` to a `sinal/forwarder`                               |

Finite attempt timeouts and retry budgets bound the configured action waits.
Settlement and sequential undo add their own waits after admission stops.
Synchronous observers can extend wall time, and an explicit `Infinity` lifts
the bound it configures. See the [timing model](design/design.typ#time-and-capacity)
for the limits of those bounds.

## Durable runs

A durable run saves a checkpoint at every step boundary, so it survives the
loss of its runner, its caller or its VM. Every step declares how to save its
values and how to establish the effect of an attempt that was interrupted.

```gleam
import gleam/dynamic/decode
import gleam/json
import gleam/time/duration
import saga
import saga/codec
import saga/durable
import saga/storage/memory

let order = codec.json("order-1", fn(id) { Ok(json.string(id)) }, decode.string)
let text = codec.text()
let charge =
  saga.effect("charge", fn(order, key) { charge(order, saga.idempotency_key(key)) })
  |> saga.undo(fn(undo) { refund(undo.output, saga.idempotency_key(undo.key)) })
  |> durable.recoverable(version: "1", input: order, output: text, resolve: fn(order, key) {
    case lookup_charge(order, saga.idempotency_key(key)) {
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
case durable.drive(run, timeout: duration.seconds(30)) {
  Ok(outcome) -> handle(outcome)
  Error(error) ->
    case durable.error_kind(error) {
      durable.Busy | durable.Transient -> retry_later()
      durable.NeedsReconciliation -> alert(durable.describe_error(error))
      durable.Incompatible | durable.Defect -> fail(durable.describe_error(error))
    }
}
memory.stop(store)
```

- One `Storage` serves every execution of a store; the execution id given
  to `start_or_reconnect` addresses one. `start_or_reconnect` is idempotent
  for the same id and input; `durable.reconnect` attaches by id alone.
- `durable.new` checks that every step is recoverable and panics, naming
  each offending step and codec, when one is not: that is a bug in the
  source. The workflow version is `"1"`; change it with
  `durable.with_version` whenever the workflow's behavior changes, so a saved
  execution of the old behavior is refused with `IncompatibleDefinition`.
- `drive` uses `timeout` as its execution budget, then drains the runner and
  releases its claim before returning. Return can therefore exceed `timeout`;
  reserve the [documented timing margins](../DURABILITY.md#choose-timing-margins).
  On timeout, when the calling process
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
  a time) and the [`saga_postgres`](../integrations/saga_postgres) package,
  which takes the application's own `pog.Connection` so one pool serves the
  application, grind and saga. `saga/storage/conformance` checks any
  adapter.

## Telemetry

`saga/telemetry` defines six Sinal events: run start and stop, step start
and stop, compensation decisions and undo outcomes. Every event's metadata
carries `workflow`, `run` (this VM's id for one run), `execution` (the
durable id, or `None`) and `correlation` (from `execution.with_correlation`
or `durable.with_correlation`; a durable execution without one carries
`correlation.from_key(id)`, and a local run a fresh `correlation.unique()`;
a durable execution reports the value its first drive saved). It is a
`Correlation`, never absent. The step and undo callbacks read
the same value from their `EffectKey`.

```gleam
import saga/telemetry
import sinal

let attachment =
  sinal.observe(telemetry.run_stopped(), fn(_measurements, metadata) {
    log(metadata.correlation, metadata.execution, metadata.outcome)
  })
```

Keep the attachment while observing the run, then detach it with
`sinal.detach(attachment)`.

A compensation event reports the delay a `RetryAfter` decision was
scheduled with and whether the cap shortened it.

## Testing workflows

A test that needs to synchronize with a run in progress — waiting for a
step to reach a particular state before releasing it, cancelling it, or
asserting on it — should poll `execution.progress` rather than sleep a
guessed duration. `saga/testing` ships exactly one helper for this:

```gleam
import gleam/time/duration
import saga/testing

let assert Ok(progress) =
  testing.wait_until(
    exec,
    matching: fn(p) { p.phase == execution.Settling },
    within: duration.seconds(10),
  )
```

`wait_until` polls `execution.progress` at a short internal interval (never
a fixed `process.sleep`) until `matching` accepts a snapshot or `within`
elapses overall, using the monotonic clock for the deadline.
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
synchronize on that arrival, then `open` releases one waiting
`enter` call. Use the full consumer gate when a release may arrive first; that
version queues the release:

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

See [the test gate](../examples/order_consumer/test/support/gate.gleam) for the full version
(handles a `Release` that arrives before anyone is waiting yet) and
[the order consumer tests](../examples/order_consumer/test/order_consumer_test.gleam)' cancellation
scenario for it in use alongside `execution.progress`.

## Execution semantics

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
  deliberate difference from Reactor 1.0.6; see [the uncertainty decision](adr/0003-uncertainty-and-rollback-authority.md)), and an
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
  soon as nothing is in flight; `with_settle_timeout(duration.seconds(0))` rolls back at once
  and reports in-flight steps `interrupted`.
- **Await once more after `cancel`.** `await` drains its monitor and the
  outcome only on the call that consumes them; an `Execution` dropped after
  a timed-out `await` can leave a message in the owner's mailbox.
- **A reported outcome survives the owner; a killed coordinator does not.**
  `start_reporting`'s subject receives at most one message, after rollback.
  Monitor `execution.pid(exec)` to detect a killed coordinator: its `Down`
  always arrives after its outcome.
- **A local `EffectKey` belongs to one run.** Its `idempotency_key` derives from
  the run's id, which `execution.run` creates anew each time. A durable
  execution's keys derive from its id and survive restarts.

## Act on an execution's outcome

`saga/outcome` classifies what the execution proves: `Completed`, `Compensated`
(all effects known and none left in place), or `Unresolved`. A completed value
with unknown effects is unresolved. `held_steps` retains native step addresses;
`summary` names actions and steps without application errors, outputs or crash
reasons. `classify(report, explain)` returns the native output or a typed
`Failure`; use `failure_kind` and `describe_failure`. Only the held error is
rendered by the application's `explain` in uncertain evidence.

```gleam
import saga/outcome

case outcome.classify(report, describe_payment_error) {
  Ok(receipt) -> accept(receipt)
  Error(failure) -> record_failure(outcome.failure_kind(failure),
    outcome.describe_failure(failure))
}
```

A short-lived invocation worker uses `saga/reporting.run_owned(workflow, input,
config, on_stopped, rollback_within)` when compensation must be reported after
that worker dies.

- The synchronous result and `on_stopped` callback receive
  `Result(execution.Outcome(output, error, undo_error), reporting.Error)`.
  `Ok(report)` means a report was obtained, including a failed or unresolved
  workflow. The report retains application-native business and undo errors.
- The application calls `outcome.classify(report, explain)` or
  `outcome.summary(report)` when it needs a classification or a safe summary.
  Summaries include every settlement category and unknown action without
  exposing application payloads.
- An operational error has no execution report. Use `reporting.error_kind`,
  `effect_status` and `describe_error`; use `run_error`, `exit_reason` and
  `invalid_rollback_within` for optional typed causes. `NotStarted` proves
  prelaunch rejection; `Unknown` cannot exclude workflow effects.
- Receiver readiness waits at most five seconds. Receiver exit or readiness
  timeout returns a typed error with `NotStarted`, stops the receiver and
  discards its startup replies without consuming unrelated caller messages.
- The notification runs in a guarded worker bounded by `rollback_within`.
  After returning a report the receiver watches its owner until that owner
  exits; normal exit sends no second notification. If the owner dies before
  confirming a coordinator and no report arrives within the bound, delivery
  remains unconfirmed.
- Use this boundary inside one invocation worker. Ordinary callers use
  `execution.run` or `start_reporting`; a long-lived server loop would keep
  receivers watching the server after each invocation.

The compiled [fabric recipe](../../fabric/consumers/saga_tool/src/saga_tool.gleam)
uses both ports through public imports, without a saga dependency on fabric.
