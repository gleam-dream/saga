# Optional persistence

Saga has one authoring model, `saga.Workflow`, and one concurrent execution
engine. `saga/execution` runs that workflow in memory. `saga/durable` adds
checked persistence, recovery, and persistent result reads to the same graph.
Local use requires no codecs, storage, database, or job system.

## Ownership

| Component                     | Responsibility                                                                                                    |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Saga workflow and runner      | Dependencies, bounded admission, choices, attempts, retries, cancellation, outcomes, compensation, and undo order |
| Storage adapter               | Saved bytes, atomic creation and conditional updates, claims and their expiry, cancellation intent, listing       |
| Delivery integration (caller) | Waking a runner, scheduling jobs, capacity, and delivery recovery                                                 |

Persistence is an execution capability. `durable.new` checks an existing
workflow and attaches root codecs and a compatibility stamp built from the
workflow version (`"1"`, changed with `durable.with_version`), the graph and
every codec version; it does not construct a second graph.

## Example

```gleam
import gleam/dynamic/decode
import gleam/json
import saga
import saga/codec
import saga/durable
import saga/storage/memory

let order = codec.json("order-1", fn(id) { Ok(json.string(id)) }, decode.string)
let text = codec.text()
let action =
  saga.effect("charge", fn(order, key) { charge(order, key.idempotency) })
  |> saga.undo(fn(undo) { refund(undo.input, undo.output, undo.key.idempotency) })
  |> durable.recoverable(version: "1", input: order, output: text, resolve: lookup_charge)
  |> durable.resolve_undo(lookup_refund)
let workflow = saga.define("checkout", saga.perform(_, action))

// The same workflow remains usable with execution.run(workflow, input, config).
let persistence =
  durable.new(workflow, input: order, output: text, error: text, undo_error: text)
let assert Ok(store) = memory.start()
let assert Ok(run) =
  durable.start_or_reconnect(persistence, memory.storage(store), id: "checkout:123", input: "order-123")
let outcome = durable.drive(run, timeout: 30_000)
let saved = durable.read(run)
memory.stop(store)
```

`file.open(directory)` and `saga_postgres.storage(config)` supply persistent
adapters through the same `Storage` contract. A third-party package can
implement `Storage` with `storage.new`, without changing workflow authoring
or depending on runner internals.

## Authoring and eligibility

- `saga.choose(input, name, decision, when_true, when_false)` constructs both
  typed branch graphs once. Both branches have the same output/error types.
  The decision becomes a scheduled, saved Bool value. Only the selected branch
  executes, including nested choices. Shared dependencies still execute once.
- `durable.recoverable(step, version:, input:, output:, resolve:)` adds
  persistence capability to a step. Every step must have it: `durable.new`
  panics on a graph that cannot be persisted, naming every step without
  `recoverable`, every compensating step without `restore_undo`, and every
  empty step or codec version, because each is a bug in the source. Choice
  decisions supply their own codec.
- `saga.effect` gives each attempt an `EffectKey`. Its `idempotency` is the
  same for every attempt of the step in one execution and survives restarts;
  its `attempt_key` is unique per attempt. Send `idempotency` downstream as
  the provider's idempotency key. A resolver receives the same key the
  interrupted attempt had. An undo has its own key.
- `saga.undo` keeps undo reconstruction automatically. Every persistent
  compensating step declares `durable.restore_undo`, including an explicit
  `saga.NoUndo` factory when it never returns undo; `durable.new` panics on a
  missing declaration before any effect. Checkpointing rejects an actual
  `Continue` undo when the factory returns `NoUndo`. The factory must be pure;
  it may run during checkpoint validation and restoration.
- `durable.resolve_undo` and `durable.resolve_compensation` keep their
  callbacks whatever the order of the modifiers. `saga.map_step_errors` keeps
  the codecs and maps every resolver's answers forward. A `Failed` answer from
  a `recoverable` added after `map_step_errors` needs a `compensate` decider
  in the mapped vocabulary; without one the execution suspends with
  `InvalidCheckpoint(DeciderMissingAfterMapping(step))`.
- Codecs catch exceptions and check that encoded data decodes back.
  Determinism and semantic round trips remain the codec author's
  responsibility. Callback changes require version changes, even when value
  types stay the same.

The compatibility stamp covers workflow identity and version, ordered nodes,
scoped addresses, dependency and choice wiring, step versions, codecs,
attempt budgets, timeouts, and declared recovery capabilities. Restoration
checks the stamp before invoking application decoders; a different stamp is
`IncompatibleDefinition`.

## State and dispatch

The coordinator represents checkpoint state as values: step progress, saved
inputs/outputs, retry times, deadline, failures, selected decisions, rollback
intent, ordered undo journal, unknown effects and settlement. Runtime
closures, PIDs, monitors, and timers are reconstructed from the checked
workflow and are never saved.

Workers wait for permission at persistence boundaries. Saga saves admission,
then saves the checked input before releasing the actual effect. An accepted
outcome and newly ready work share one checkpoint before further dispatch.
Storage or codec failure stops workers and leaves the last committed state
available for recovery. Local runs use the same transition functions without
storage or encoding. Each commit rewrites the complete snapshot, bounded by
`durable.with_max_checkpoint_bytes` (16 MiB by default); a larger checkpoint
suspends the execution with `CheckpointTooLarge`.

The recovery rules are:

1. Saved successful outputs are decoded and reused without executing the step.
2. A task interrupted before its input admission did not receive effect
   permission and may prepare again.
3. An admitted effect with no saved result uses its resolver.
   `durable.Completed` and `durable.Failed` restore the observed result.
   `NotSent` authorizes a retry under the original key; `MaybeSent` suspends
   progress with `RecoveryRequired`. During cancellation, known absence never
   starts another effect.
4. An interrupted undo uses `durable.resolve_undo`: `Completed(Nil)`,
   `Failed(error)`, `NotSent` (replay the saved undo) or `MaybeSent`
   (suspend). Without a resolver, an interrupted undo suspends.
5. An interrupted compensation decision uses `durable.resolve_compensation`
   with its saved input and the failed attempt's key, under the original
   attempt budget. `Some(decision)` applies that decision through the normal
   runner transitions; `None` keeps the execution suspended. Saga never
   repeats the original decider on restart.
6. A returned error that `saga.unknown_when` marks is recorded in the saved
   unknown effects when it ends, and the decision about it is saved like any
   other. After a restart, a decision that was in flight goes to the
   compensation resolver (rule 5), and an attempt whose result was not yet
   saved goes to its effect resolver (rule 3); a `Failed` answer that the
   classifier marks is recorded as unknown again, and then follows the step's
   `saga.on_unknown`: by default the execution finishes `Unresolved` with
   that error as evidence and undoes nothing; with `RollBack` it fails and
   rolls back. A journaled error is never replayed as if it were known.

Resolvers must establish absence or use downstream idempotency. Storage alone
cannot promise exactly-once external effects. Reverse completion-order undo,
retry budgets and backoff, branch selection, and rollback intent survive
restart. A resumed concurrency limit cannot be smaller than the saved number
of in-flight attempts and compensation decisions
(`ConcurrencyBelowInFlight`). Compensation requires an admitted input; a crash
while preparing input suspends persistent compensation before its callback
can run. Saved deadlines and backoff use wall-clock timestamps; clocks must
suit the deployment's timing requirements.

## Driving

`durable.drive(run, timeout:)` runs the execution in a runner process and
waits at most `timeout` milliseconds.

- The runner claims the execution, restores the checkpoint and runs. The
  outcome is returned once it is saved; a finished execution returns its
  saved outcome at once.
- On timeout, `drive` stops the runner and returns `DriveTimedOut`. When the
  process that called `drive` exits, the runner stops by itself. When the
  runner is killed or crashes while the caller lives, `drive` returns
  `RunnerLost`. In every case in-flight attempts are killed, the claim is
  released at once (by `drive` itself when the runner could not), and the
  last checkpoint stays: the next `drive` resumes and asks each interrupted
  attempt's resolver what happened. This is never cancellation. Only when
  the runner's VM is lost does the claim wait for the storage to notice, as
  a lease that expires.
- Every storage call from the runner is bounded by the storage's call
  timeout (5 s by default, `storage.with_call_timeout`); a slower call stops
  the runner with `StorageFailure(TimedOut)`.
- A storage declared `with_renewal` has its claim renewed from a heartbeat
  linked to the runner. A renewal that finds the claim taken over stops the
  runner with `StorageFailure(StaleOwner)`.
- Concurrent `drive`s of one execution contend through the storage: all but
  one return `StorageFailure(Busy)`. Retry `Busy` no sooner than the
  storage's owner-loss window (saga_postgres's lease, 30 s by default), so
  that a delivery system's retry or snooze limit outlasts a lost VM's lease.

`durable.error_kind` classifies every error into `Busy`, `Transient`,
`NeedsReconciliation`, `Incompatible` or `Defect`, so a job handler can map
an error to snooze, retry, operator attention or failure without matching
the growing `Error` union.

Saga does not wake runners. `durable.unfinished(storage, limit:)` lists the
executions that are pending or suspended and that no live runner owns, and
`durable.reconnect(persistence, storage, id:)` attaches to one by id. Who
calls them (a grind job, an application sweeper, an operator) is the
application's choice until the ecosystem settles durability ownership.

## Storage contract

One `Storage` value serves a whole store: every operation names the
execution it acts on, so one database pool backs every execution of an
application. `storage.new` takes seven atomic operations:

- `create(id, bytes)` succeeds once and returns revision zero, phase
  `Pending`.
- `load(id)` returns revision, ownership generation, cancellation flag, and
  bytes.
- `claim(id)` excludes competing live owners, advances the generation, and
  returns an opaque `Claim` (id, generation, adapter token).
- `commit(claim, Commit(expected_revision:, observed_cancelled:, phase:,
data:))` succeeds only for the current claim and the matching revision
  and cancellation observation, increments the revision, and keeps
  ownership. Failures take precedence `StaleOwner`, then
  `CancellationChanged`, then `Conflict`.
- `release(claim)` releases only the current claim.
- `cancel(id)` idempotently records cancellation without overwriting
  progress.
- `unfinished(limit)` lists unowned executions whose phase is `Pending` or
  `Suspended`.

**Ownership is the claim value.** Any process holding the current claim may
commit or release; a claim rebuilt with the right generation but another
token is refused. An adapter therefore needs no registry of claiming
processes. How an adapter notices that an owner is gone is its own choice,
within the window it declares to the conformance suite: the memory adapter
watches the claiming process, the file adapter checks that the claiming
process still lives in this VM, and `saga_postgres` uses a lease.

**Lease renewal.** A lease-based adapter declares `storage.with_renewal`.
Commit-only refresh is not enough: the gap between two commits is the
longest action in flight, which a step may extend up to its attempt timeout
(or without bound with `without_step_timeout`) and a `RetryAfter` backoff
up to five minutes. A lease shorter than that gap would expire under a live
runner and let a second runner start the same step concurrently. With
renewal, a lease expires only when its runner is gone, so the lease can stay
short and the owner-loss window small, and fencing by generation and token
still guards every write.

A distributed adapter must fence stale writers across its supported
deployment scope; implementing this as unguarded load/store does not satisfy
the contract. External effects still need reconciliation when a runner loses
ownership.

The memory adapter is a gleam_otp actor (`memory.start`, or
`memory.supervised(name)` found by `memory.named(name)`); it survives runner
and caller loss but not VM shutdown, and loses its executions on restart. The
file adapter keeps one file per execution in a directory, with synced
temporary files and atomic replacement. It supports fresh-VM recovery after
shutdown, but requires one VM at a time to use the directory. Concurrent
VMs, path aliases, and power-loss directory durability are outside its
contract. `saga_postgres` shares executions across VMs on the application's
pool.

## Adapter conformance

An adapter package runs the public suite without depending on saga's tests
or a particular test framework:

```gleam
import saga/storage/conformance

let result =
  conformance.run(
    fn() {
      let resource = create_test_store()
      Ok(conformance.fixture(adapter.storage(resource), cleanup: fn() {
        drop_test_store(resource)
      }))
    },
    timeout: 5000,
    owner_loss_within: lease + 500,
  )
```

Each scenario receives a fresh fixture and fresh execution ids. The factory
and cleanup run in the caller; adapter operations run in separate workers.
`timeout` bounds each scenario's storage work, and scenarios that wait for a
lost owner get `owner_loss_within` on top.

The suite checks atomic creation, unchanged data after refused writes,
exclusive claims, claims as values, revision and generation checks,
cancellation races, release, that a live owner keeps its claim past the
owner-loss window, that a lost owner's claim ends within it, the
`unfinished` listing, and that a `durable.drive` whose runner is killed
frees the execution at once for the next drive, which resumes from the
checkpoint. The memory, file and PostgreSQL adapters run this same
suite. A passing result establishes these protocol checks from one VM;
adapter authors must separately test distributed fencing and media
durability claims.

## Results and cancellation

`start_or_reconnect` is idempotent for the same id, definition, and encoded
input. A different input is `InputMismatch`. `read` returns `Pending`,
`Suspended`, or `Finished(execution.Outcome)` without a live runner. `drive`
returns that same outcome after it has been saved.

Failures are typed: `StorageFailure(storage.Error)`,
`CodecFailure(boundary, codec.CodecError)`, `InvalidCheckpoint(problem)`,
`CheckpointTooLarge(bytes, limit)` and
`RecoveryRequired(Required(step, action, key))`. A saved suspension keeps its
reason for later reads. If saving the suspension fails with a different
error, `SuspensionNotSaved(cause, recording)` returns both; an identical
repeated error is returned once. The last committed checkpoint remains the
recovery authority.

`cancel` records explicit intent; caller or runner death is not cancellation.
A running runner observes cancellation at its next checkpoint, so
cancellation latency can include the current action's timeout. A
cancellation recorded before the next admission or terminal commit wins.
Interrupted admitted actions are reconciled before rollback. Cancellation
does not imply that an external effect has been reversed.

## Grind and Fabric integration

An integration keeps a stable execution id before scheduling a job.
Redelivery reconnects by that id, reads any saved outcome, then calls
`drive` with the job's own time budget as `timeout`. `error_kind` maps the
result: `Busy` and `Transient` snooze the job, `NeedsReconciliation` makes it
uncertain, and the rest fail it. Delivery acknowledgment is not the
authority for saga completion; the saved outcome is.

Grind supplies delivery, worker capacity, and wakeups; saga owns progress
and compensation. An integration needing atomic coordination between state
changes and external job scheduling must supply that transaction or outbox
contract. Fabric can keep the same id for an independently owned child
workflow; parent process loss is never translated into saga cancellation.

## Evidence

The public tests exercise canonical local/persistent authoring, concurrent
shared dependencies, two interrupted concurrent attempts, saved outputs,
branch recovery, retry/Continue behavior, interrupted undo and compensation, cancellation races,
codec and commit refusal before effects, duplicate starts, ownership conflicts,
compatibility rejection, and persisted result reads. The external consumer uses
only public modules. Compiler fixtures check branch output compatibility.

`scripts/check_durable_restart.sh` kills an entire Erlang VM after two concurrent
fake effects record receipts. A fresh VM reconstructs the workflow, reuses the
saved shared output, reconciles both stable keys, and reads the saved result.

A second VM-kill probe interrupts two concurrent compensation callbacks after
they save receipts. The fresh VM resolves both original keys and attempt
budgets without repeating either callback. Public tests also cover mapped
resolver results, every recovery decision, a second restart before Continue
undo, configuration order, early undo eligibility, typed failures, failed
suspension recording, and adapter conformance. Durable tests also cover a
bounded `drive` (timeout, caller exit and a killed runner stop the runner
and release its claim, the last through the conformance suite for every
adapter), lease renewal detecting a takeover, slow storage calls, the
checkpoint size limit, the `unfinished` listing, one storage serving many
executions, and "maybe sent" errors recorded as unknown across a restart,
holding the execution `Unresolved` by default and rolling back with
`saga.on_unknown(saga.RollBack)`.
