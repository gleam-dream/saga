# Optional persistence

Saga has one authoring model, `saga.Workflow`, and one concurrent execution
engine. `saga/execution` runs that workflow in memory. `saga/durable` adds
checked persistence, recovery, and persistent result reads to the same graph.
Local use requires no codecs, storage, database, or job system.

## Ownership

| Component                     | Responsibility                                                                                                    |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Saga workflow and runner      | Dependencies, bounded admission, choices, attempts, retries, cancellation, outcomes, compensation, and undo order |
| Storage adapter               | Saved bytes, atomic creation and conditional updates, execution ownership, and cancellation intent                |
| Optional delivery integration | Waking a runner, scheduling jobs, capacity, and delivery recovery                                                 |

Persistence is an execution capability. `durable.prepare` checks an existing
workflow and attaches root codecs and a compatibility stamp; it does not
construct a second graph. This is the initial unpublished persistence API and
checkpoint format.

## Example

```gleam
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage/memory

let text = codec.text()
let action =
  saga.effect("charge", fn(order, key) { charge(order, key) })
  |> saga.undo_effect(fn(order, receipt, key) { refund(order, receipt, key) })
  |> saga.recoverable("1", text, text, fn(order, key) {
    lookup_charge(order, key)
  })
  |> saga.reconcile_undo(fn(order, receipt, key) {
    lookup_refund(order, receipt, key)
  })
let assert Ok(workflow) = saga.define("checkout", fn(input) {
  saga.perform(input, action)
})

// The same workflow remains usable with execution.run(workflow, input, config).
let assert Ok(persistence) =
  durable.prepare(workflow, "1", text, text, text, text)
let memory = memory.new()
let storage = memory.storage(memory)
let assert Ok(reference) =
  durable.start_or_reconnect(storage, "checkout-123", persistence, "order-123")
let outcome = durable.drive(storage, reference, persistence, execution.config())
let saved = durable.read(storage, reference, persistence)
memory.close(memory)
```

`file.open(canonical_absolute_path)` supplies the reference persistent adapter
through the same `Storage` contract. The caller creates its parent directory.
A third-party package can implement `Storage`, including through ETS or a
database, without changing workflow authoring or depending on runner internals.

## Authoring and eligibility

- `saga.choose(input, name, decision, when_true, when_false)` constructs both
  typed branch graphs once. Both branches have the same output/error types.
  The decision becomes a scheduled, saved Bool value. Only the selected branch
  executes, including nested choices. Shared dependencies still execute once.
- `saga.recoverable(step, version, input_codec, output_codec, resolve)` adds
  persistence capability to a step. Every step must have this capability before
  `durable.prepare` accepts the graph. Choice decisions supply their own codec.
- `saga.effect` and `saga.undo_effect` expose stable keys. Retries receive distinct
  attempt keys; reconciliation and authorized replay retain the original key.
  Undo has its own stable key. Ordinary `saga.step` and `saga.undo` also work
  when the integration can reconcile their effects without a key argument.
- `saga.undo` and `saga.undo_effect` retain undo reconstruction automatically.
  Every persistent compensating step must declare `saga.restore_undo`, including
  an explicit `NoUndo` factory when it never returns undo. `durable.prepare`
  rejects a missing declaration before effects. Checkpointing rejects an actual
  `Continue` undo when the factory returns `NoUndo`. The factory must be pure;
  it may run during checkpoint validation and restoration. A saved `NoUndo`
  never acquires an undo merely because the step has a default factory.
- `saga.reconcile_undo` and `saga.reconcile_compensation` retain their callbacks
  regardless of whether `recoverable` comes before or after them. Reattaching
  codecs preserves both callbacks. Step error mapping translates resolver
  results; apply `recoverable` after mapping to attach codecs in that vocabulary.
- Apply step error mapping before adding persistence codecs. Reconciliation of
  a returned failure needs a compensation decider in that error vocabulary;
  configure compensation after mapping when such recovery is needed. Whole
  workflow error mapping retains the node's bound value codecs.
- Codecs catch exceptions and check that encoded data decodes. Determinism and
  semantic round trips remain the codec author's responsibility. Pure maps and
  definition factories must remain pure. Callback changes require version
  changes, even when value types stay the same.

The compatibility stamp covers workflow identity/version, ordered nodes,
scoped addresses, dependency and choice wiring, step versions, codecs,
attempt budgets, timeouts, and declared recovery capabilities. Restoration
checks the stamp before invoking application decoders. Exact compatibility is
required; callers retain the matching definition factory for live executions.

## State and dispatch

The coordinator represents checkpoint state as values: step progress, saved
inputs/outputs, retry times, deadline, failures, selected decisions, rollback
intent, ordered undo journal, and settlement. Runtime closures, PIDs, monitors,
and timers are reconstructed from the checked workflow and are never saved.

Workers wait for permission at persistence boundaries. Saga saves admission,
then saves the checked input before releasing the actual effect. An accepted
outcome and newly ready work share one checkpoint before further dispatch.
Storage or codec failure stops workers and leaves the last committed state
available for recovery. Local runs use the same transition functions without
storage or encoding. Persistent checkpoints currently rewrite the complete
snapshot; no incremental journal or high-volume performance claim is made.

The recovery rules are:

1. Saved successful outputs are decoded and reused without executing the step.
2. A task interrupted before its input admission did not receive effect
   permission and may prepare again.
3. An admitted effect with no saved result uses its resolver. `EffectCompleted`
   and `EffectFailed` restore the observed result. `EffectAbsent` authorizes a
   retry under the original key; `EffectUnknown` suspends progress. During
   cancellation, known absence never starts another effect.
4. Interrupted undo uses `UndoCompleted`, `UndoFailed`, `UndoStillApplied`, or
   `UndoUnknown`. Only `UndoStillApplied` authorizes replay of the saved undo.
5. An interrupted compensation decision uses `reconcile_compensation` with its
   saved input, original attempt budget, and stable compensation key.
   `CompensationResolved(decision)` applies that decision through the normal
   runner transitions. `CompensationUnknown` keeps the execution suspended.
   Saga never repeats the original compensation callback on restart.

Resolvers must establish absence or use downstream idempotency. Storage alone
cannot promise exactly-once external effects. Reverse completion-order undo,
retry budgets and backoff, branch selection, and rollback intent survive
restart. A resumed concurrency limit cannot be smaller than the saved number
of in-flight attempts and compensation decisions. Compensation requires an
admitted input; a crash while preparing input suspends persistent compensation
before its callback can run. Saved deadlines/backoff use wall-clock timestamps;
clocks must be suitable for the deployment's timing requirements.

## Compensation recovery

Use `compensate_with_key` when the compensation callback performs an external
action. Save the decision under its key in the external system. The resolver
must be safe to call repeatedly and reconstruct the complete decision, including
any `Continue` output and undo capability.

```gleam
let action =
  saga.step("reserve", reserve)
  |> saga.compensate_with_key(3, fn(input, failure, attempt, key) {
    decide_and_record(input, failure, attempt, key)
  })
  |> saga.reconcile_compensation(fn(input, attempt, key) {
    lookup_decision(input, attempt, key)
  })
  |> saga.restore_undo(fn(input, output, key) {
    saga.UndoWith(fn() { release_reservation(input, output, key) })
  })
  |> saga.recoverable("1", input_codec, output_codec, lookup_reservation)
```

A resolver returns any ordinary recovery decision (`Retry`, `RetryAfter`,
`Continue`, `Abort`, `AbortAfterCleanupFailure`, `Hold`) inside
`CompensationResolved`. Retry still consumes the original budget. Cancellation
supersedes retry; a resolved Continue joins settlement and rollback. Hold
retains its lack of rollback authority. If no decision can be established,
return `CompensationUnknown` and leave the execution suspended.

## Storage contract

A `Storage` value addresses one execution and supplies these atomic operations:

- `create(bytes)` succeeds once and returns revision zero.
- `load()` returns revision, ownership generation, cancellation flag, and bytes.
- `claim()` excludes competing live runners and advances the ownership generation.
- `commit(generation, expected_revision, observed_cancelled, bytes)` succeeds
  only for the current owner and matching revision/cancellation observation,
  increments the revision, and preserves ownership.
- `release(generation)` releases only the current owner's claim.
- `cancel()` idempotently records cancellation without overwriting progress.

An adapter must reject stale owners and release ownership after worker loss.
A distributed adapter must fence stale writers across its supported deployment
scope; implementing this as unguarded load/store does not satisfy the contract.
External effects still need reconciliation when a worker loses ownership.

The memory adapter uses a dedicated process and survives execution-worker
loss, but not VM shutdown. Call `memory.close` when finished. The file adapter
uses VM-local ownership, revision/generation checks, synced temporary files,
and atomic replacement. It supports fresh-VM recovery after shutdown, but
requires one VM at a time to access a canonical path. Concurrent VMs, path
aliases, and power-loss directory durability are outside its contract.

## Adapter conformance

An adapter package can run the public suite without depending on Saga's tests
or a particular test framework:

```gleam
import saga/storage/conformance

let result = conformance.run(fn() {
  let resource = create_fresh_test_execution()
  Ok(conformance.Fixture(
    adapter.storage(resource),
    fn() { delete_test_execution(resource) },
  ))
}, 5000)
```

Each scenario receives a fresh execution. The factory and cleanup run in the
caller; adapter operations run in separate workers. The timeout bounds each
scenario. Cleanup runs after the scenario worker stops, including on failure.
Factories and cleanup must return promptly and must not raise.

The suite checks atomic creation, unchanged data after refused writes,
exclusive ownership, revision and generation checks, cancellation races,
release, and ownership recovery after process death. The memory and file
adapters run this same suite. A passing result establishes these protocol
checks within one VM; adapter authors must separately test their deployment's
distributed fencing and media durability claims.

## Results and cancellation

`start_or_reconnect` is idempotent for the same reference, definition, and
encoded input. A mismatch is refused. `read` returns `Pending`, `Suspended`,
or `Finished(execution.Outcome)` without relying on a live worker or completion
notification. `drive` returns that same outcome after it has been saved.

Operational failures retain typed categories: `StorageError(storage.Error)`,
`CodecFailure(reason)`, `InvalidCheckpoint(reason)`, and
`RecoveryRequired(reconciliation.Required(step, action, key))`. A saved suspension
retains the category for later reads. If saving the suspension fails with a
different error, `SuspensionNotSaved(cause, recording)` returns both causes; an
identical repeated error is returned once. The last committed checkpoint remains
the recovery authority. Checkpoint format 1 stores these typed reasons. Its
version starts at 1 for the first release; development snapshots are disposable.

`cancel` records explicit intent; caller or worker death is not cancellation.
A running worker observes cancellation at its next checkpoint, so cancellation
latency can include the current action's timeout. A cancellation recorded before
the next admission or terminal commit wins. Interrupted admitted actions are
reconciled before rollback. Cancellation does not imply an external effect
has been reversed.

## Optional Grind and Fabric integration

An integration retains a stable Saga execution reference before scheduling a
job. Redelivery uses that reference, reads any saved outcome, then calls
`drive` if work remains. `Busy` means another runner owns the execution.
Delivery acknowledgment is not the authority for Saga completion.

Grind can supply delivery, worker capacity, and wakeups. Saga owns progress and
compensation. An integration needing atomic coordination between state changes
and external job scheduling must supply that transaction/outbox contract.
Fabric can retain the same reference for an independently owned child workflow;
parent process loss must not be translated into Saga cancellation. Neither
integration is implemented here.

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
suspension recording, and adapter conformance.
