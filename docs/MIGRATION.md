# Migrating from Saga 0.2 to 0.3

Saga 0.3.0 is a breaking release. The matching PostgreSQL adapter is `saga_postgres` 0.2.0, Git tag `saga_postgres-v0.2.0`.

> **Drain every unfinished durable execution before you upgrade.**
> Saga 0.3 does not read checkpoints saved by Saga 0.2 or earlier, in any storage adapter.
> PostgreSQL migration 2 of `saga_postgres` deletes them.
> An execution that is pending or suspended when you upgrade cannot be resumed afterwards.
> [Drain unfinished executions](#drain-unfinished-executions-before-you-upgrade) explains the steps.

Change the dependencies in `gleam.toml`:

```toml
[dependencies]
saga = ">= 0.3.0 and < 0.4.0"
sinal = ">= 0.2.0 and < 0.3.0"
# Only with PostgreSQL storage:
saga_postgres = { git = "https://github.com/gleam-dream/saga.git", ref = "saga_postgres-v0.2.0", path = "integrations/saga_postgres" }
```

Saga 0.3 depends on Sinal 0.2. An application that attaches Sinal handlers to Saga's telemetry also follows the [Sinal 0.2 migration guide](https://github.com/gleam-dream/sinal/blob/main/docs/MIGRATION.md): a handler is now detached when the process that attached it exits.

Then work through the sections below in order. The first one affects most applications.

| Change                                                                                            | Who must act                                                           |
| ------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| [Drain unfinished executions before you upgrade](#drain-unfinished-executions-before-you-upgrade) | Every application that uses `saga/durable`                             |
| [A crash or timeout holds the run](#a-crash-or-a-timeout-holds-the-run)                           | Every application with a step that has no `compensate` decider         |
| [`Unresolved` carries the held failure](#unresolved-carries-the-held-failure)                     | Code that matches `execution.Unresolved`                               |
| [`Hold` takes no argument](#hold-takes-no-argument)                                               | Code with a `compensate` decider that holds                            |
| [`durable.list` replaces `durable.unfinished`](#durablelist-replaces-durableunfinished)           | Recovery loops that find unfinished executions                         |
| [PostgreSQL: run migration 2](#postgresql-run-migration-2)                                        | Applications that use `saga_postgres`                                  |
| [File storage uses a directory per execution](#file-storage-uses-a-directory-per-execution)       | Applications that use `saga/storage/file`                              |
| [The storage port saves records](#the-storage-port-saves-records)                                 | Authors of a storage adapter                                           |
| [Other behaviour changes](#other-behaviour-changes)                                               | Code that compares idempotency keys or reads `reporting.effect_status` |

## Drain unfinished executions before you upgrade

**What changed.** A durable checkpoint is now a set of named records: the identity and run input are written once, and the run header and one record per step are written when they change. Saga 0.3 reads only this format (format 3). It does not read the single-record checkpoints that Saga 0.2 and earlier saved, and the code that upgraded format 1 is removed.

**Why.** Every commit used to rewrite every earlier step's input and output, so the bytes committed grew with the square of the step count: about 93 MB for a 200-step chain with 1 KB values. With records, the same chain commits about 0.7 MB.

**What to do.** While the application still runs Saga 0.2:

1. Stop starting new durable executions, for example by pausing the jobs or message consumers that call `durable.start_or_reconnect`.
2. List the unfinished executions with Saga 0.2's `durable.unfinished(storage, limit:)`. Reconnect to each with `durable.reconnect` and call `durable.drive` until it returns a terminal outcome.
3. A suspended execution waits until its unknown effect is established. Resolve the effect from authoritative evidence so the drive can finish, or `durable.cancel` the execution and record the effect's state in your own system before you upgrade.
4. Repeat step 2 until `durable.unfinished` returns an empty list.
5. Deploy Saga 0.3, then start new executions again.

Finished executions need no action. Their saved outcomes are not readable after the upgrade either, so read any outcome you still need with Saga 0.2's `durable.read` before you upgrade.

## A crash or a timeout holds the run

**What changed.** A step's outcome is unknown when its attempt crashes, times out, or returns an error that `unknown_when` classified, and when its `compensate` decider crashes or times out. One rule applies to every unknown outcome that no decision settles: the step's `on_unknown` policy. The default, `saga.Reconcile`, ends the run `execution.Unresolved` and undoes nothing. In 0.2, a step with no decider that crashed or timed out failed the run with `StepCrashed` or `StepTimedOut` and undid every earlier step.

The same rule applies when a decider crashes or times out, and when the retry budget runs out after a crash or a timeout. An explicit decision still applies: `Abort` rolls back and `Hold` holds.

**Why.** A crash or a timeout is the most common way an effect's outcome becomes unknown. A payment provider that answers after the step timeout may have taken the charge; rolling back then released the reservation for stock the customer paid for.

**What to do.** Decide for each step without a decider whether an unknown outcome must be reconciled or rolled back. To keep the 0.2 behaviour for a step, add `saga.on_unknown(saga.RollBack)`:

```gleam
// 0.2: a crash or timeout of this step rolled the run back.
let notify = saga.step("notify", send_notification)

// 0.3: keep the rollback explicitly.
let notify =
  saga.step("notify", send_notification)
  |> saga.on_unknown(saga.RollBack)
```

Expect `execution.Unresolved` where 0.2 returned `execution.Failed(execution.StepCrashed(..), _)` or `execution.Failed(execution.StepTimedOut(..), _)`, and route it to the same reconciliation as other held runs. `StepCrashed` and `StepTimedOut` remain the failure causes of a `RollBack` step.

## `Unresolved` carries the held failure

**What changed.** `execution.Unresolved(step, evidence: e, settlement)` is now `execution.Unresolved(step, failure: saga.AttemptFailure(e), settlement)`. `saga.AttemptFailure(e)` is `Returned(error)`, `Crashed(crash)` or `TimedOut`.

**Why.** A crash or a timeout carries no application error value, and the run can now hold on one.

**What to do.** Match `saga.Returned(error)` where code matched the error, and handle the two new cases:

```gleam
// 0.2
execution.Unresolved(step, error, _) -> reconcile(step, error)

// 0.3
execution.Unresolved(step, saga.Returned(error), _) -> reconcile(step, error)
execution.Unresolved(step, saga.Crashed(_), _)
| execution.Unresolved(step, saga.TimedOut, _) -> reconcile_unknown(step)
```

`outcome.summary` renders a held crash or timeout by its kind only ("a crash left the step's outcome unknown", "a timeout left the step's outcome unknown"), never the crash reason. The crash or timeout can be the attempt's or its decider's.

## `Hold` takes no argument

**What changed.** The decider result `saga.Hold(evidence)` is now `saga.Hold`.

**Why.** The outcome now reports the failure that was actually held, so a second value from the decider is not needed.

**What to do.** Drop the argument. The `Unresolved` outcome carries the decided attempt's failure.

```gleam
// 0.2
saga.Returned(MaybeCharged) -> saga.Hold(MaybeCharged)

// 0.3
saga.Returned(MaybeCharged) -> saga.Hold
saga.Crashed(_) | saga.TimedOut -> saga.Hold
```

## `durable.list` replaces `durable.unfinished`

**What changed.** `durable.unfinished(storage, limit:)` is removed. `durable.list(storage, phase:, after:, limit:)` lists the unowned executions of one phase (`storage.Pending`, `storage.Suspended` or `storage.Finished`) in ascending id order, after the cursor id `after`.

**Why.** `unfinished` mixed pending and suspended executions, oldest first, with no way to read past the first page. Executions that stay suspended until an operator acts could fill every page, so a recovery loop never reached newer pending executions.

**What to do.** List pending and suspended executions separately, and page with the last id of each page. A page shorter than `limit` is the last one.

```gleam
// 0.2
let assert Ok(ids) = durable.unfinished(storage, limit: 100)

// 0.3: every pending id, read 100 per page.
fn all_pending(
  storage: storage.Storage,
  after: Option(String),
  found: List(String),
) -> Result(List(String), durable.Error) {
  use page <- result.try(durable.list(
    storage,
    phase: storage.Pending,
    after: after,
    limit: 100,
  ))
  let found = list.append(found, page)
  case list.length(page) == 100, list.last(page) {
    True, Ok(last) -> all_pending(storage, Some(last), found)
    _, _ -> Ok(found)
  }
}
```

The [operations example](https://github.com/gleam-dream/saga/blob/main/examples/order_consumer/src/order_consumer/usage/operations.gleam) compiles and runs this loop. List `storage.Suspended` in a separate loop that hands each execution to reconciliation.

## PostgreSQL: run migration 2

**What changed.** `saga_postgres` 0.2.0 stores each checkpoint record in its own row of a new table, `saga_records`. Migration 2 adds that table, drops the `saga_executions.data` column, and replaces the listing index. Dropping the column deletes every checkpoint saved by `saga_postgres` 0.1.

**What to do.**

1. [Drain unfinished executions](#drain-unfinished-executions-before-you-upgrade) while the application still runs Saga 0.2 and `saga_postgres` 0.1.
2. Change the `saga_postgres` Git tag to `saga_postgres-v0.2.0` and the `saga` range to `>= 0.3.0 and < 0.4.0`.
3. Run `saga_postgres.migrate` at startup as before. If an application migration tool applies the SQL in `priv/migrations` instead, add migration 2 to it.

The `pog` and `pgo` ranges are unchanged (`pog >= 4.1.0 and < 4.2.0`, `pgo >= 0.20.0 and < 0.21.0`). `saga_postgres.storage(config)` and the configuration functions keep their signatures.

## File storage uses a directory per execution

**What changed.** `saga/storage/file` now saves an execution as a subdirectory, `<hex id>.saga/`, with one file per record and a head file that indexes them. In 0.2 an execution was one file. Files saved by 0.2 are not read.

**What to do.** [Drain unfinished executions](#drain-unfinished-executions-before-you-upgrade), then remove the files saved by 0.2 from the storage directory. `file.open(directory)` is unchanged.

## The storage port saves records

This section applies only to a custom `saga/storage` adapter. The memory, file and PostgreSQL adapters already implement the new port.

**What changed.**

- `create` takes `Dict(String, BitArray)` and returns `Result(Nil, Error)`.
- `commit` takes `Commit(expected_revision:, observed_cancelled:, phase:, records:)` and returns `Result(Nil, Error)`. A commit replaces the records it names and keeps the others.
- `storage.data(stored)` is replaced by `storage.records(stored)`, and `storage.stored(.., data:)` by `storage.stored(.., records:)`.
- The `unfinished: fn(Int)` operation is replaced by `list: fn(Listing)`, with `Listing(phase:, after:, limit:)`. `storage.do_unfinished` is replaced by `storage.do_list`.

Fencing, the refusal order (`StaleOwner`, then `CancellationChanged`, then `Conflict`), renewal and the call timeout are unchanged. A refused commit writes no record.

**What to do.** Store each record under its key. Upsert the records a commit names in the same atomic write as the revision. Return every record from `load` and `claim`. Filter `list` by phase, skip executions a live runner owns, and return ids after the cursor in id byte order. Then run `saga/storage/conformance` against the adapter.

## Other behaviour changes

These need no code change in most applications.

- **Local idempotency keys are random per run.** A local run (`execution.run`, `execution.start`) draws its key scope from 128 random bits when it starts, instead of a per-VM counter that repeated after every restart and on every node. Every attempt of the same step inside one run still shares one key. A key is about 37 bytes and stays opaque. Submitting the same business operation again is a new run with new keys; use `saga/durable` with a stable execution id when a repeat must deduplicate. Durable keys are unchanged.
- **A lost startup handshake starts no step.** When `execution.start` or `execution.start_reporting` returns `ExecutionLost` because the coordinator did not answer, the coordinator is stopped before the call returns and no step has started. `reporting.effect_status` now returns `NotStarted` for that error instead of `Unknown`. `ExecutionLost` from `execution.run` can still mean a coordinator was lost after it started.
- **A settling run does not start a proven-absent attempt.** When a durable run resumes while it settles after a failure or a deadline, an interrupted attempt with no saved input, or one its resolver proves `NotSent`, is no longer started; it settles as absent and the run rolls back.
- **Telemetry handlers follow Sinal 0.2.** A Sinal handler attached to Saga's events is detached when the attaching process exits. Attach it from a process that outlives the runs it observes, such as a supervised one.
