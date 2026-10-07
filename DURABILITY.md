# Operating durable executions

- The [native design](docs/design/design.typ#durable-execution-model) owns the workflow, checkpoint, claim, recovery, and cancellation contracts. The [rendered design](docs/design/design-layer.pdf) presents the full model; [ADRs](docs/adr) retain rationale.
- Use `saga/execution` for a local owner-bound run and `saga/durable` when saved progress must outlive a driver. The same Workflow supports both paths.

## Prepare the application

- Define the Workflow once. Attach versioned root/step codecs, an attempt resolver for every step, and undo/compensation reconstruction and resolvers where required.
- Keep codec encodings deterministic and semantically round-trippable. Change the workflow, step, or codec version when its meaning changes; incompatibility is checked before application decoders run.
- Create or obtain one Storage for the application. Start/stop a memory store under application supervision, reserve one canonical file directory for one VM, or use saga_postgres with the application's existing pool and migration lifecycle.

## Start or reconnect

```gleam
let persistence =
  durable.new(workflow, input: order_codec, output: receipt_codec,
    error: pay_error_codec, undo_error: undo_error_codec)
let assert Ok(run) =
  durable.start_or_reconnect(persistence, storage, id: "checkout:" <> order_id,
    input: order)
let run = durable.with_correlation(run, application_correlation)
let result = durable.drive(run, timeout: duration.seconds(30))
```

- Keep the id stable before job admission or external delivery. Reuse of id requires the same definition and encoded input; another input is `InputMismatch`.
- Set correlation before the first drive. The first drive saves it; later handles use the saved value even when configured differently.
- Inspect the full typed Outcome. A returned report may describe failure, cancellation, or unresolved effects; preserving it permits audit and reconciliation before application projection.

## Choose timing margins

- `drive(timeout:)` stops execution waiting at its budget and then performs synchronous runner drain and possible bounded claim release before returning. It can return after the requested Duration.
- Drain uses five-second receive windows; claim release is a separate Storage operation bounded by its call timeout. The [timing model](docs/design/design.typ#time-and-capacity) and [open ruling](docs/adr/0009-drive-budget-and-example-gaps.md) state the exact limits.
- Reserve shutdown/release margin when a delivery worker has its own deadline. Stopping drive alone preserves resumable state; call `durable.cancel` when durable cancellation is intended.
- Keep storage call timeout, lease, renewal interval, effect timeout, and delivery retry/snooze policy compatible. A lost VM can leave Busy until the adapter detects owner loss; retry windows must outlast that deployment policy.

## Handle failure and recovery

| `durable.error_kind` | Application action                                                                                |
| -------------------- | ------------------------------------------------------------------------------------------------- |
| Busy                 | Retry after the adapter's owner-loss window under the application's delivery policy.              |
| Transient            | Retain the stable id and retry deliberately; inspect the saved state before assuming work absent. |
| NeedsReconciliation  | Establish downstream effect evidence or request operator attention.                               |
| Incompatible         | Supply the matching definition/input or use an explicitly approved migration.                     |
| Defect               | Correct configuration, codec, definition, checkpoint, or adapter behavior.                        |

- Preserve typed `StorageFailure`, `CodecFailure`, `InvalidCheckpoint`, `CheckpointTooLarge`, and `RecoveryRequired` detail. `SuspensionNotSaved` retains both causes when recording failure differs from the original cause.
- After process/VM interruption, rebuild a compatible definition and drive the stable id. Saved successes are reused; admitted unfinished effects consult their original-key resolver.
- `NotSent` requires proof of absence; `MaybeSent` preserves suspension. Do not replay a remote effect because the old driver disappeared or a delivery system retried.
- Explicit cancel records intent. Reconcile interrupted admitted effects before rollback; cancellation does not prove reversal, and an already completed last step may be undone when cancellation wins before terminal commit.

## Recover a committed local database effect

- Sharing PostgreSQL or a pool does not make an application's SQL transaction atomic with Saga's subsequent checkpoint. The [adapter's commit boundaries](integrations/saga_postgres/docs/design/design.typ#effect-timing-and-recovery) keep those writes separate.
- Retain a stable business operation ID and an authoritative posting record in the application transaction. Make the effect idempotent and discoverable under that identity. Do not use a missing checkpoint as evidence that SQL rolled back.
- The [financial recovery consumer](https://github.com/gleam-dream/oversight/tree/master/apps/financial_recovery) exercises this sequence through public APIs, disposable PostgreSQL, a scripted provider and controlled runner termination:

  ```text
  Posting transaction commits
    → runner dies before Saga checkpoints completion
    → restarted resolver reads the application's posting record
    → Completed restores the recorded result
    → posting callback is not repeated
  ```

- Reconnect the same durable execution with a compatible definition. Its attempt resolver returns `Completed` only when the application record establishes the result. A failed lookup or inconclusive absence remains `MaybeSent`; it does not authorize another effect.
- The consumer also demonstrates Grind redelivering the coordinator. That delivery resumes Saga recovery; it does not itself authorize resubmitting a payment. The application owns financial evidence, reservation and posting independently of both libraries.

## Discover unfinished executions

```gleam
let ids = durable.unfinished(storage, limit: 100)
// Reconnect with the compatible persistence and drive each id under app policy.
```

- Saga supplies listing and reconnect, not a sweeper or delivery scheduler. The application owns capacity, duplicate wakeups, operator policy, and any transaction/outbox that coordinates a saved transition with job admission.
- `durable.read(run)` obtains Pending, Suspended(reason), or Finished(report) without a live runner. A saved Outcome is the completion authority; notifications and telemetry are observations.

## Select storage

| Adapter       | Application obligation                                      | Failure scope                                                                                      |
| ------------- | ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| memory        | Start/supervise/stop store; use named handle if restartable | Survives runner/caller loss while store lives; store/VM restart loses records.                     |
| file          | Create canonical absolute directory; use one VM at a time   | Fresh-VM recovery; no concurrent-VM, path-alias, or power-loss directory guarantee.                |
| saga_postgres | Own pool, schema/migrations, lease/renewal and query limits | Database-backed cross-VM protocol; see [adapter operations](integrations/saga_postgres/README.md). |

## Verify an adapter

- Run `saga/storage/conformance.run` with a fresh fixture per scenario, cleanup, operation timeout, and declared owner-loss allowance. The [verification contract](docs/design/design.typ#observation-and-verification-ports) states what it checks.
- Conformance from one VM does not prove distributed stale-writer fencing or media durability. Test those deployment claims separately.
- Preserve `scripts/check_durable_restart.sh`: it kills a VM during concurrent effect and compensation work, then recovers from fresh definitions and saved receipts.
