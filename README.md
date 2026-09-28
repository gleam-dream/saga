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

Configure concurrency, a run deadline, and cleanup bounds by updating the
default config's record fields rather than constructing one from scratch;
start without blocking; inspect progress; cancel:

```gleam
import gleam/option.{Some}
import saga/execution

let config =
  execution.Config(..execution.config(), max_concurrency: 4, deadline: Some(5000))

let assert Ok(exec) = execution.start(workflow, order_id, config)
let assert Ok(progress) = execution.progress(exec, 1000)
execution.cancel(exec)
let assert Ok(outcome) = execution.await(exec, 10_000)
```

`execution.config()` defaults to one attempt/compensation task per
scheduler core (`max_concurrency`), no run `deadline`, a 60 second default
per-attempt `step_timeout`, a 5 second `settle_timeout`, and a 5 second
`cleanup_timeout` — the record-update (`..execution.config()`) is the
advanced-config path; it changes only the fields you name and keeps the
library's defaults for the rest, so a new `Config` field added later does
not silently reset every existing caller's untouched settings back to a
stale literal.

To learn the outcome somewhere other than the starting process, start with
`execution.start_reporting`. The run sends its outcome, once, to the
`Subject` you pass: a subject of another process, which learns the outcome
even after the starting process exits, or a subject of your own, which you
can add to a `Selector` next to your other messages:

```gleam
import gleam/erlang/process

let report = process.new_subject()
let assert Ok(exec) =
  execution.start_reporting(workflow, order_id, config, to: report)
let selector =
  process.new_selector()
  |> process.select_map(report, RunEnded)
  |> process.select_map(other_messages, Other)
```

The starting process still owns the run: its exit cancels the run, which
settles and rolls back, and the `Cancelled(OwnerExited, settlement)` outcome
still reaches `report`. `execution.await` on a reporting run returns
`Error(NotOwner)`. A coordinator killed from outside sends nothing; monitor
`execution.pid(exec)` to detect that, since its `Down` always arrives after
its outcome.

A step's own `saga.timeout(..)` always overrides `step_timeout`, in either
direction (shorter or longer than the default). Opt out of the default
entirely — for a step that may legitimately run unbounded, with only its own
`saga.timeout` or the run's `deadline` (if any) to bound it — with
`step_timeout: None`:

```gleam
let config = execution.Config(..execution.config(), step_timeout: None)
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
- **Every attempt is bounded by a default `step_timeout`; only the run
  `deadline` is unbounded by default.** `execution.config()`'s
  `step_timeout` defaults to `Some(60_000)` (60 seconds): a step with no
  `saga.timeout` of its own still gets this default, so a step body that
  never returns — a genuine hang, not a crash — is killed and reported
  `StepTimedOut` rather than blocking `execution.run`/`execution.await`
  forever. A step's own `saga.timeout(..)` always overrides the default, in
  either direction; `step_timeout: None` opts out of the default entirely
  for every step that does not set its own. `deadline` stays `None` by
  default regardless: `step_timeout` already bounds each attempt and
  `saga.compensate`'s `max_attempts` already bounds how many attempts a step
  can accumulate, so every step already has a finite worst case without a
  run-wide deadline; `deadline` is instead a coarser, opt-in ceiling on the
  whole run, cutting across still-healthy steps too, for callers who
  specifically want that.
- **The workflow builder runs once, at `define` time.** `define` evaluates
  it to validate the workflow and to build its node graph; no run ever
  evaluates the builder again (see `saga.Workflow`'s doc comment for
  precisely the two situations — `define` and `embed` — a builder is ever
  invoked at all). The builder no longer needs to be pure or reproducible
  run to run — it runs exactly once, period — though it should still be a
  straightforward description of the workflow's shape, since whatever
  graph it produces at `define` time is what every future run replays.
  Per-run values live in a store keyed by node id, isolated per run, so
  concurrent or successive runs of the same `Workflow` never see each
  other's data despite sharing the same built graph (see "Design
  decisions" below for the trade this makes).
  **What is linear in the number of steps, per run:** a dependency value
  read (a single map lookup, not a scan proportional to how many steps
  have completed); admitting a ready step (a single min-heap operation,
  not a scan over every step); the reverse-dependency index and
  `saga.all`'s combined-list construction (each built once, in one linear
  pass, not by repeated appends). Total scheduling work for a run is
  therefore linear overall, not quadratic — see `bench/RESULTS.md` (roughly
  14x-49x faster median run time at 2000 steps, depending on shape, versus
  per-run builder re-evaluation with per-node mailbox reads and full-graph
  admission scans, with growth per doubling dropping from ~4x to ~2x).
  **What stays O(N) per call, deliberately, because it is not a per-step
  hot path:** `execution.progress` (a caller-driven snapshot, not
  triggered by run progress itself); the settle-window sweep and the
  waiting-node skip on a run's first terminal trigger (each happens at
  most once per run); and the search for which node a task pid belongs to
  on an abnormal (non-`ffi.rescue`d) task exit (bounded by concurrently
  in-flight tasks, not by total step count, and only reached on an
  externally-killed task, not a normal completion).
- **A step whose output port is never consumed is rejected at `define`
  time**, as `DefinitionError.OrphanStep(step)`, instead of silently never
  running: every step created via `perform`/`embed` must have its output
  port threaded (directly or through `map`/`both`/`all`) into the
  workflow's final returned port.
- **Every outcome says which effects are unknown.** Each action of a run
  — a step attempt, a compensation decision, an undo — ends with a known
  result (it returned `Ok` or a typed error) or with an unknown effect (it
  crashed or its process exited, it was killed at its time bound, or it was
  killed when the settle window closed). `execution.unknown_effects(outcome)`
  lists every action of the second kind as an
  `UnknownEffect(step, action, ending)`, for every outcome kind, and is
  `[]` exactly when every action returned:
  ```gleam
  case execution.unknown_effects(outcome) {
    // Every effect is known: done, undone, or left in place by a result.
    [] -> Definite
    // Each names a step, `StepAttempt(n)`, `StepCompensation(n)` or
    // `StepUndo`, and `ActionCrashed(_)`, `ActionTimedOut` or
    // `ActionInterrupted`.
    unknown -> Uncertain(unknown)
  }
  ```
  An action is recorded when it ends, so a later decision cannot hide it: a
  crashed attempt retried to success, continued, aborted or held is still
  named. An effect a result left in place (an undo that returned an error,
  a step with no undo, a held step) is known, and is reported by the
  settlement instead.
- **A `Completed` outcome does not always mean every effect is known.**
  `execution.Outcome.CompletedWithUnknownEffects(output, unknown_effects)`
  is `Completed`'s counterpart for a run that reached its output although
  a step attempt crashed (or its process exited) or was killed by its own
  `timeout`, and the step's `compensate` decider chose
  `Retry`/`RetryAfter`/`Continue`. That attempt's own effect is still
  unknown and was never journaled or undone — only the _replacement_
  attempt is known. `unknown_effects` is never empty on this variant, and a
  plain `Completed` means every action returned. `saga/observation`'s
  `run_stopped` event reports this case as `OutcomeCompleted` (the same
  `OutcomeKind` as a plain `Completed`), with its `interrupted` measurement
  populated from `unknown_effects`'s length instead — check that field, not
  the outcome kind, to tell the two apart from telemetry alone.
- **A `StepFailed` cause may follow a crash.** A `compensate` decider is
  asked about crashed and timed-out attempts too, and its `Abort(error)` is
  reported as `StepFailed(step, error)`, the same cause as an aborted typed
  error, whether as the run's primary cause or as a sibling failure. The
  crashed attempt is in `unknown_effects`. A decider that aborts after a
  crash should return an error that says so if the caller must tell the
  two apart from the cause alone.
- **A refused retry is distinguished from an exhausted one.** A
  `Retry`/`RetryAfter` compensation decision that arrives after the run has
  already begun settling for a different, unrelated trigger cannot be
  honored (it would race the settle window); it is recorded as
  `Cause.RetrySuperseded(step, last)`, kept distinct from
  `RetryLimitReached` (which means the step's own attempt budget was
  actually exhausted) so a `case` over `Cause` cannot conflate "never got
  the chance to retry" with "ran out of retries."
- **`saga.map` is not memoized.** It re-runs in every task that consumes
  the resulting port. Use a `saga.step` for expensive or effectful
  transforms.
- **`saga.all` takes a required first port.**
  `saga.all(first: Port(a, e, u), rest: List(Port(a, e, u))) -> Port(List(a), e, u)`
  combines `first` and `rest` (in that order) into one port producing
  their values as a list. There is no empty-list case to construct or
  reject: a caller with zero ports has no `Port` to pass as `first` and
  cannot call `all` at all, which the type system enforces at the call
  site. To combine an existing `List(Port(..))` of unknown length, split
  it yourself first with a `case`, handling the empty list explicitly
  rather than asserting it away:
  ```gleam
  case ports {
    [first, ..rest] -> Ok(saga.all(first, rest))
    [] -> Error(NoPortsToCombine)
  }
  ```
- **A killed attempt's own effect can outlive the run that killed it.**
  `Settlement.interrupted`/`not_undoable` name exactly which steps' effects
  are unknown, but an `Execution` you stop awaiting — an `await` that timed
  out, followed by `cancel`, with no further `await` — can still leave a
  monitor `Down` or an outcome message sitting in your own mailbox once the
  run finally settles: `await` only demonitors/drains on the call that
  actually consumes a signal. Always `await` again (even with a short
  timeout) after `cancel`, so the run's eventual `Cancelled` outcome is
  consumed and nothing is left behind in your mailbox.
- **A reported outcome survives the owner; a killed coordinator does not.**
  `start_reporting`'s subject receives at most one message per run, sent
  when the run ends, after any rollback. It receives exactly one unless the
  coordinator itself is killed, or, for a `process.named_subject`, no
  process holds the name at that moment (the outcome is then dropped). The
  owner's exit is a cancellation, not a loss: the run settles, rolls back,
  and reports `Cancelled(OwnerExited, settlement)`, whose settlement names
  every failed, timed-out, or interrupted compensation.
- **The settle window is set per run, not per cancel.** `settle_timeout`
  is fixed when the run starts; an owner's exit cancels with no call to
  carry another value. Settling ends as soon as nothing is in flight, so
  the window only delays rollback while a step is still running.
  `settle_timeout: 0` rolls back at once and reports in-flight steps
  `interrupted`; a longer window lets them finish, so they are known and
  undone.
- **A repeated `await` cannot always tell `AlreadyAwaited` apart from a
  previously-reported `Lost`.** `execution.await`/`AwaitError` are
  deliberately stateless on the caller's side (no process-dictionary
  bookkeeping survives between calls), so a _second_ `await` on an
  `Execution` whose coordinator already exited reports `AlreadyAwaited`
  whether the first `await` consumed a normal outcome or already reported
  `Lost`. If you need to know which one actually happened, keep the first
  `await`'s own result — do not rely on a second call to re-derive it.

## Design decisions

**The scheduler keeps a central per-run value store, departing from
saga-design.md's original stance (see "Typed DAG construction" /
saga-design.md:197-215, which reads "The scheduler must not use a central
native-value structure like `Dict(NodeId, Dynamic)`").**

Local execution (this repo's scope, before durable execution) originally
gave every node its own single-value mailbox cell, allocated fresh each
time the workflow's builder ran. That kept the scheduler's own state
free of any central heterogeneous map — each node's result lived only in
that node's own typed `Subject`. It also meant every run re-evaluated the
builder from scratch, and every dependency read was a selective receive
whose cost grew with how many prior messages already sat in that mailbox:
O(N) per read, O(N^2) total per run for a workflow whose reads scale with
N.
`bench/RESULTS.md` measured this directly — before this change, a 2000-step
chain's median run time was ~392ms and grew roughly 4x every time the step
count doubled.

The fix builds a workflow's graph exactly once, at `define`, and moves
per-run values into `saga/internal/store` — one `Dict(Int, Native)` keyed
by node id, `Native` being that module's own opaque, type-erased carrier
(never `gleam/dynamic.Dynamic`, never inspected or decoded — only ever
cast back to the exact type it was stored as). This is, structurally,
exactly the central `Dict(NodeId, Dynamic)` shape saga-design.md ruled
out. The trade was made deliberately: `store.get`'s one native identity
coercion is sound _by construction_, not by convention or caller
discipline — see `saga/internal/store`'s own doc comment for the full
argument, summarized here:

> A node id and its element type are bound together exactly once, in the
> same `perform` call that both allocates the id and returns the typed,
> opaque `Port` whose `fetch` closure reads that id back. No other code
> path can construct a `Port` for one node id typed differently than the
> `perform` call that created it, so no caller can ever read a node's
> value at the wrong type.

With that invariant holding, centralizing per-run storage turned an O(N)
mailbox read into an O(1) map lookup, and a follow-on fix
(`saga/internal/min_heap`, replacing a full node-order scan in the
admission loop) turned an O(N) admission decision into an O(log N) one.
Together these took the same 2000-step chain from ~392ms median to ~8ms
(roughly 49x), and flattened the growth curve from ~4x per doubling
(quadratic) to ~2x per doubling (linear) — see `bench/RESULTS.md` for the
full before/after tables. The scheduler's _authoring_-time API is
unaffected: no caller-facing type ever becomes `Dynamic`, no step looks up
a dependency by name, and the one unsafe cast is confined to a single
internal module with a stated soundness invariant, not spread through the
scheduler or exposed to callers.

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
