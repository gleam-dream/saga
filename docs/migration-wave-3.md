# Migrating to the wave 3 saga API

This guide lists every removed or changed public item of `saga` in the wave 3
release redesign, with its replacement and a before and after snippet, grouped
by module. The last section indexes the saga symbols each known dependent uses
and how heavily each breaks.

Pinned siblings: sinal and json_blueprint at their wave 3 heads. New
dependencies: `gleam_json >= 3.0.0 and < 4.0.0`, `gleam_otp >= 1.0.0 and <
2.0.0`.

## `saga`

### `undo` takes an `UndoRequest`

The undo callback receives one record, `UndoRequest(input, output, key)`,
instead of two positional arguments. `key` is the undo's own `EffectKey`.

```gleam
// before
saga.undo(fn(order, hold) { release(hold) })
// after
saga.undo(fn(undo) { release(undo.output) })
// after, when the input is needed too
saga.undo(fn(undo) {
  let saga.UndoRequest(input: order, output: hold, ..) = undo
  release(order, hold)
})
```

### `undo_effect` is removed

`saga.undo` now passes the key; use `undo.key`.

```gleam
// before
saga.undo_effect(fn(order, receipt, key) { refund(order, receipt, key) })
// after
saga.undo(fn(undo) { refund(undo.input, undo.output, undo.key.idempotency) })
```

### `compensate` takes a `FailedAttempt`

The decider receives `FailedAttempt(input, failure, attempt, attempts_left,
key)`. `attempt` is the failed attempt's 1-based number (was
`Attempt.number`), `attempts_left` the remaining budget (was
`Attempt.remaining`), and `key` the failed attempt's `EffectKey`.

```gleam
// before
saga.compensate(max_attempts: 3, with: fn(order, failure, attempt) {
  case failure, attempt.number {
    saga.Returned(Declined), _ -> saga.Abort(Declined)
    _, n if n < 3 -> saga.RetryAfter(100)
    _, _ -> saga.Hold(Unknown)
  }
})
// after
saga.compensate(max_attempts: 3, with: fn(failed) {
  case failed.failure, failed.attempts_left {
    saga.Returned(Declined), _ -> saga.Abort(Declined)
    _, left if left > 0 -> saga.RetryAfter(100)
    _, _ -> saga.Hold(Unknown)
  }
})
```

### `compensate_with_key` is removed

`compensate` passes the key. The old compensation key string
(`...:compensation:<n>`) is gone; record a decision's external effect under
`failed.key.attempt_key`, which the compensation resolver receives too.

```gleam
// before
saga.compensate_with_key(3, fn(input, failure, attempt, key) {
  decide_and_record(input, failure, attempt, key)
})
// after
saga.compensate(max_attempts: 3, with: fn(failed) {
  decide_and_record(failed.input, failed.failure, failed.attempt, failed.key.attempt_key)
})
```

### `Attempt` is removed

Its fields are `FailedAttempt.attempt` and `FailedAttempt.attempts_left`.

### `effect` receives an `EffectKey`

The run function's second argument is
`EffectKey(idempotency, attempt, attempt_key)` instead of a per-attempt
`String`. `idempotency` is the same for every attempt of the step (send it as
a provider's idempotency key); `attempt_key` is the old per-attempt key's
replacement. Local runs derive the key from the run id; durable runs from the
execution id, so it survives restarts.

```gleam
// before
saga.effect("charge", fn(order, key) { charge(order, idempotency_key: key) })
// after
saga.effect("charge", fn(order, key) { charge(order, idempotency_key: key.idempotency) })
```

An application that derived its own stable key (checkout's `pay-<order>`,
CHK-8) can use `key.idempotency` instead.

### New: `unknown_when`

```gleam
saga.effect("charge", charge)
|> saga.unknown_when(fn(error) { error == MaybeCharged })
```

A returned error the classifier marks is named in `execution.unknown_effects`
with the new ending `execution.ActionReturnedUnknown`. A test or report that
expected a plain `Completed` after a retried "maybe sent" error now sees
`CompletedWithUnknownEffects` (CHK-3). Since the follow-up fixes, such an
error with no decision to settle it ends the run `Unresolved` instead of
rolling back; see "Follow-up fixes".

### Persistence modifiers moved to `saga/durable`

| Before                                                                                        | After                                                                                 |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `saga.recoverable(step, version, input, output, resolve)`                                     | `durable.recoverable(step, version:, input:, output:, resolve:)`                      |
| `saga.restore_undo(step, fn(i, o, key) -> Undo(u))`                                           | `durable.restore_undo(step, fn(UndoRequest(i, o)) -> Undo(u))`                        |
| `saga.reconcile_undo(step, fn(i, o, key) -> UndoStatus(u))`                                   | `durable.resolve_undo(step, fn(UndoRequest(i, o)) -> durable.Evidence(Nil, u))`       |
| `saga.reconcile_compensation(step, fn(i, Attempt, key) -> CompensationStatus)`                | `durable.resolve_compensation(step, fn(i, EffectKey) -> Option(Recovery))`            |
| `saga.EffectStatus`: `EffectCompleted(o)`, `EffectFailed(e)`, `EffectAbsent`, `EffectUnknown` | `durable.Evidence`: `Completed(o)`, `Failed(e)`, `NotSent`, `MaybeSent`               |
| `saga.UndoStatus`: `UndoCompleted`, `UndoFailed(u)`, `UndoStillApplied`, `UndoUnknown`        | `durable.Completed(Nil)`, `durable.Failed(u)`, `durable.NotSent`, `durable.MaybeSent` |
| `saga.CompensationStatus`: `CompensationResolved(r)`, `CompensationUnknown`                   | `Some(r)`, `None`                                                                     |

The resolver's key argument is an `EffectKey` instead of a `String`; the
compensation resolver's `Attempt` argument is `key.attempt`.

```gleam
// before
saga.step("reserve", reserve)
|> saga.recoverable("1", order_codec, reserved_codec, fn(order, _key) {
  case db.find(order) {
    Ok(db.Absent) -> saga.EffectAbsent
    Ok(_) -> saga.EffectCompleted(Reserved(order))
    Error(_) -> saga.EffectUnknown
  }
})
|> saga.restore_undo(fn(order, charged, _key) { saga.UndoWith(fn() { void(order, charged) }) })
|> saga.reconcile_compensation(fn(order, _attempt, _key) {
  case lookup(order) {
    Ok(found) -> saga.CompensationResolved(saga.Retry)
    Error(_) -> saga.CompensationUnknown
  }
})
// after
saga.step("reserve", reserve)
|> durable.recoverable(version: "1", input: order_codec, output: reserved_codec, resolve: fn(order, _key) {
  case db.find(order) {
    Ok(db.Absent) -> durable.NotSent
    Ok(_) -> durable.Completed(Reserved(order))
    Error(_) -> durable.MaybeSent
  }
})
|> durable.restore_undo(fn(undo) { saga.UndoWith(fn() { void(undo.input, undo.output) }) })
|> durable.resolve_compensation(fn(order, _key) {
  case lookup(order) {
    Ok(_) -> Some(saga.Retry)
    Error(_) -> None
  }
})
```

`map_step_errors` after `recoverable` keeps the codecs (the probe in the
release review failed with `InvalidDefinition("step requires persistence
codecs")`); no reordering is needed any more.

### `CrashOrTimeout` is removed

It appeared in no public signature.

### New

`EffectKey`, `UndoRequest`, `FailedAttempt`, `unknown_when`,
`describe_definition_error`.

## `saga/execution`

### `Config` is opaque

Construction and record update no longer compile. Use the setters.

| Record field             | Setter                                                        |
| ------------------------ | ------------------------------------------------------------- |
| `max_concurrency: n`     | `execution.with_max_concurrency(config, n)`                   |
| `deadline: Some(ms)`     | `execution.with_deadline(config, ms)`                         |
| `deadline: None`         | the default; omit                                             |
| `step_timeout: Some(ms)` | `execution.with_step_timeout(config, ms)`                     |
| `step_timeout: None`     | `execution.without_step_timeout(config)`                      |
| `settle_timeout: ms`     | `execution.with_settle_timeout(config, ms)`                   |
| `cleanup_timeout: ms`    | `execution.with_cleanup_timeout(config, ms)`                  |
| (new)                    | `execution.with_max_retry_delay(config, ms)`, default 300 000 |
| (new)                    | `execution.with_correlation(config, correlation)`             |

```gleam
// before
execution.Config(..execution.config(), max_concurrency: 4, deadline: Some(5000))
// after
execution.config()
|> execution.with_max_concurrency(4)
|> execution.with_deadline(5000)
```

A field read (`config.step_timeout`) has no replacement; keep the value you
passed to the setter.

### `validate` is removed

`run`, `start`, `start_reporting` and `durable.drive` check the configuration
and return `InvalidConfig(errors)` (or `durable.InvalidConfig`) with every
violation. A caller that validated early, such as `fabric_saga.tool`, lets the
first run report it instead.

```gleam
// before
use config <- result.map(execution.validate(config))
// after: no early check; execution.start_reporting returns
// Error(execution.InvalidConfig(errors)) on the first run.
```

`ConfigError` gains `MaxRetryDelayNegative(value)`;
`describe_config_error` renders each.

### `RetryAfter` delays are capped

A `RetryAfter(ms)` above the cap (300 000 ms by default) is shortened to the
cap. Raise it with `with_max_retry_delay`.

### `Crash` and `StepAddress` aliases are removed

```gleam
// before
fn show(step: execution.StepAddress, crash: execution.Crash)
// after
fn show(step: saga.StepAddress, crash: saga.Crash)
```

### `UnknownEnding` gains `ActionReturnedUnknown`

An exhaustive `case` on `UnknownEnding` needs one more arm.

```gleam
case effect.ending {
  execution.ActionCrashed(_) -> " crashed"
  execution.ActionTimedOut -> " timed out"
  execution.ActionInterrupted -> " was interrupted"
  execution.ActionReturnedUnknown -> " may have happened"
}
```

### New

`kind(outcome) -> telemetry.OutcomeKind`, `describe_cause(cause)` (it takes
`error:` since the follow-up fixes),
`describe_config_error(error)`, and the setters above.

## `saga/telemetry` (was `saga/observation`)

Rename the import. Every metadata record gains `execution:
Option(String)` (the durable id) and `correlation: Option(Correlation)`, after
`run`; read fields by label.

```gleam
// before
import saga/observation
sinal.observe(observation.run_stopped(), fn(_m, d) { log(d.workflow, d.outcome) })
// after
import saga/telemetry
sinal.observe(telemetry.run_stopped(), fn(_m, d) { log(d.correlation, d.workflow, d.outcome) })
```

A positional constructor or pattern (`RunMetadata(workflow, run)`) breaks;
use labels or `..`.

| Change                 | Detail                                                                                                                                                    |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `OutcomeKind`          | gains `OutcomeCompletedWithUnknownEffects`; `run_stop` reports it instead of `OutcomeCompleted` for such runs, encoded `"completed_with_unknown_effects"` |
| `AttemptKind`          | gains `AttemptUnknown` (an attempt that ended with an `unknown_when` error and no decider)                                                                |
| `CompensationMetadata` | gains `retry_delay: Option(Int)` and `retry_delay_capped: Bool`                                                                                           |
| new                    | `outcome_kind_name(kind)`                                                                                                                                 |

Attributing durable events by `process.self()` in a handler, through a hook in
the storage's `claim` (checkout CHK-5), is no longer needed: match on
`metadata.execution` or `metadata.correlation`.

## `saga/durable`

### `prepare` is `new`, with labels

```gleam
// before
durable.prepare(workflow, "1", input_codec, output_codec, error_codec, undo_codec)
// after
durable.new(workflow, version: "1", input: input_codec, output: output_codec, error: error_codec, undo_error: undo_codec)
```

The follow-up fixes make `new` total and move `version:` to
`durable.with_version`; see "Follow-up fixes".

### One `Run` handle; storage addressed by id

`Reference` and `reference_id` are removed. `start_or_reconnect` takes the
persistence first and returns a `Run`; `drive`, `read` and `cancel` take only
the run. The run configuration moves to `with_config`; `drive` takes a
required `timeout:`.

```gleam
// before
let storage = saga_store.storage(store, id)
let assert Ok(reference) = durable.start_or_reconnect(storage, id, persistence, order)
let outcome = durable.drive(storage, reference, persistence, config)
let status = durable.read(storage, reference, persistence)
durable.cancel(storage, reference, persistence)
// after
let persistence = durable.with_config(persistence, config)
let assert Ok(run) = durable.start_or_reconnect(persistence, storage, id: id, input: order)
let outcome = durable.drive(run, timeout: 30_000)
let status = durable.read(run)
durable.cancel(run)
```

The `Storage` value now serves every execution: build it once per store, not
once per execution. `durable.reconnect(persistence, storage, id:)` attaches
without the input; `durable.id(run)` returns the id;
`durable.with_correlation(run, correlation)` tags its events.

### `drive` is bounded and stops with its caller

`drive(run, timeout:)` returns `Error(DriveTimedOut)` after `timeout`
milliseconds, and the runner stops when the calling process exits; in both
cases the runner's attempts are killed and its claim released, and the next
`drive` resumes. A grind job that drives a saga passes its own budget as
`timeout` and no longer leaves a runner behind when the job is killed
(CHK-6). `InvalidTimeout(ms)` reports a timeout below 1.

### Errors

| Before                                                                         | After                                                                                                                                                                                                        |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `StorageError(storage.Error)`                                                  | `StorageFailure(error: storage.Error)`                                                                                                                                                                       |
| `InvalidDefinition(String)`                                                    | `NotPersistable(problems: List(PersistenceProblem))`: `EmptyWorkflowVersion`, `EmptyCodecVersion(Boundary)`, `MissingRecoverable(step)`, `EmptyStepVersion(step)`, `MissingRestoreUndo(step)`                |
| `ReferenceMismatch`                                                            | removed (ids address executions); a checkpoint saved under another id is `InvalidCheckpoint(ForeignExecution(saved))`                                                                                        |
| `CodecFailure(String)`                                                         | `CodecFailure(boundary: Boundary, error: codec.CodecError)`                                                                                                                                                  |
| `InvalidCheckpoint(String)`                                                    | `InvalidCheckpoint(problem: CheckpointProblem)`: `Malformed`, `ForeignExecution`, `GraphMismatch`, `ConcurrencyBelowInFlight`, `UndoNotRestorable`, `CompensationInputMissing`, `DeciderMissingAfterMapping` |
| `RecoveryRequired(reconciliation.Required(step: String, action, key: String))` | `RecoveryRequired(required: durable.Required(step: saga.StepAddress, action: execution.Action, key: saga.EffectKey))`                                                                                        |
| (new)                                                                          | `CheckpointTooLarge(bytes, limit)`, `InvalidTimeout(ms)`, `DriveTimedOut`                                                                                                                                    |
| unchanged                                                                      | `IncompatibleDefinition`, `InputMismatch`, `SuspensionNotSaved(cause, recording)`, `InvalidConfig(errors)`, `RunnerLost`                                                                                     |

`reconciliation.Action`'s `Activity`, `Compensation` and `Undo` are
`execution.StepAttempt(n)`, `execution.StepCompensation(n)` and
`execution.StepUndo`.

Branch on `error_kind` instead of matching variants, and log with
`describe_error`:

```gleam
// before (checkout jobs.gleam)
case driven {
  Error(durable.RunnerLost) -> worker.WorkerSnoozed(redeliver, "runner lost")
  Error(durable.StorageError(storage.Busy)) -> worker.WorkerSnoozed(redeliver, "busy")
  Error(error) -> worker.WorkerUncertain(string.inspect(error))
  Ok(outcome) -> ...
}
// after
case driven {
  Ok(outcome) -> ...
  Error(error) ->
    case durable.error_kind(error) {
      durable.Busy | durable.Transient -> worker.WorkerSnoozed(redeliver, durable.describe_error(error))
      durable.NeedsReconciliation -> worker.WorkerUncertain(durable.describe_error(error))
      durable.Incompatible | durable.Defect -> worker.WorkerUncertain(durable.describe_error(error))
    }
}
```

### New

`Run`, `new`, `with_config`, `with_max_checkpoint_bytes` (16 MiB default),
`reconnect`, `with_correlation`, `id`, `unfinished(storage, limit:)`,
`error_kind`, `describe_error`, `Evidence`, `recoverable`, `restore_undo`,
`resolve_undo`, `resolve_compensation`, and the types `ErrorKind`,
`PersistenceProblem`, `Boundary`, `CheckpointProblem`, `Required`.

## `saga/codec`

### `new` keeps its shape; errors are typed

`new(version, encode, decode)` still takes `String` conversions with `String`
messages. Failures surface as `codec.CodecError` inside
`durable.CodecFailure`.

### New: `json`

Replace a hand-written gleam_json bridge with `codec.json`. The encoder may
fail, so a JSON Blueprint codec bridges with `result.map_error`.

```gleam
// before (checkout codecs.gleam)
pub fn to_saga(c: JsonCodec(a)) -> codec.Codec(a) {
  codec.new(c.version, fn(value) { Ok(json.to_string(c.encode(value))) }, fn(text) {
    json.parse(text, c.decoder) |> result.map_error(fn(e) { string.inspect(e) })
  })
}
// after
pub fn to_saga(c: JsonCodec(a)) -> codec.Codec(a) {
  codec.json(c.version, fn(value) { Ok(c.encode(value)) }, c.decoder)
}

// before (research_agent wire.gleam)
saga_codec.new(version, fn(value) {
  codec.encode_json(c, value) |> result.map_error(codec.describe_encode_error)
}, fn(text) {
  codec.decode_json(c, text) |> result.map_error(codec.describe_decode_error)
})
// after
saga_codec.json(version, fn(value) {
  codec.to_json(c, value) |> result.map_error(codec.describe_encode_error)
}, codec.decoder(c))
```

### Removed from the public surface

`codec.version`, `codec.encode` and `codec.decode` (saga's own machinery).

### New

`json`, `int`, `CodecError`, `describe_error`.

## `saga/storage`

The contract is opaque and id-addressed. An adapter builds a `Storage` with
labelled operations, returns `Stored` and `Claim` values it builds with
`storage.stored` and `storage.claim`, and needs no process tracking.

| Before                                                                                                | After                                                                                                                                 |
| ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `pub type Storage { Storage(create:, load:, claim:, commit:, release:, cancel:) }`, one per execution | `storage.new(create:, load:, claim:, commit:, release:, cancel:, unfinished:)`, one per store                                         |
| `create: fn(BitArray)`                                                                                | `create: fn(id, BitArray) -> Result(Stored, Error)`                                                                                   |
| `load: fn()`                                                                                          | `load: fn(id) -> Result(Stored, Error)`                                                                                               |
| `claim: fn()` returning a `Record`, owned by the calling process                                      | `claim: fn(id) -> Result(#(Claim, Stored), Error)`                                                                                    |
| `commit: fn(generation, revision, cancelled, bytes)`                                                  | `commit: fn(Claim, Commit(expected_revision:, observed_cancelled:, phase:, data:))`                                                   |
| `release: fn(generation)`                                                                             | `release: fn(Claim)`                                                                                                                  |
| `cancel: fn()`                                                                                        | `cancel: fn(id)`                                                                                                                      |
| (new)                                                                                                 | `unfinished: fn(limit) -> Result(List(String), Error)`                                                                                |
| `Record(revision, generation, cancelled, data)`                                                       | opaque `Stored`: `storage.stored(revision:, generation:, cancelled:, data:)`, read with `revision`, `generation`, `cancelled`, `data` |
| `Io(String)`                                                                                          | `Unavailable(detail: String)`, and `TimedOut`                                                                                         |
| (new)                                                                                                 | opaque `Claim`: `storage.claim(id:, generation:, token:)`, `claim_id`, `claim_generation`, `claim_token`                              |
| (new)                                                                                                 | `Phase`: `Pending`, `Suspended`, `Finished`                                                                                           |
| (new)                                                                                                 | `with_renewal(storage, every:, renew:)`, `with_call_timeout(storage, ms)`, `describe_error`                                           |

```gleam
// before (an app adapter: one Storage per execution, with an owner registry)
pub fn storage(store: SagaStore, id: String) -> Storage {
  storage.Storage(
    create: fn(data) { create(store, id, data) },
    claim: fn() { actor.call(store.owners, 10_000, Claim(id, process.self(), _)) },
    commit: fn(generation, revision, cancelled, data) { ... },
    ...
  )
}
// after: use saga_postgres, or build one storage for the whole pool
pub fn storage(store: SagaStore) -> Storage {
  storage.new(
    create: fn(id, data) { create(store, id, data) },
    load: fn(id) { load(store, id) },
    claim: fn(id) { claim_row(store, id) },
    commit: fn(claim, commit) { commit_row(store, claim, commit) },
    release: fn(claim) { release_row(store, claim) },
    cancel: fn(id) { cancel(store, id) },
    unfinished: fn(limit) { unfinished(store, limit) },
  )
  |> storage.with_renewal(every: store.lease_ms / 3, renew: fn(claim) { renew(store, claim) })
}
```

Ownership is the claim value: commit and release check the claim's
generation and token, not the calling process. A lease-based adapter declares
`with_renewal`; saga renews from a heartbeat while the runner lives.

## `saga/storage/memory`

| Before                                                    | After                                                                                   |
| --------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `memory.new() -> Memory`, one execution, unlinked process | `memory.start() -> Result(Memory, actor.StartError)`, every execution, linked actor     |
| (none)                                                    | `memory.supervised(name) -> ChildSpecification(Memory)`, `memory.named(name) -> Memory` |
| `memory.close(memory)`                                    | `memory.stop(memory)`                                                                   |
| `memory.storage(memory)` per execution                    | `memory.storage(memory)` for the whole store                                            |

```gleam
// before
let memory = memory.new()
let storage = memory.storage(memory)
memory.close(memory)
// after
let assert Ok(store) = memory.start()
let storage = memory.storage(store)
memory.stop(store)
```

## `saga/storage/file`

`file.open(path)` for one execution is `file.open(directory)` for every
execution in that directory (one file per id). The caller creates the
directory.

## `saga/storage/conformance`

```gleam
// before
conformance.run(fn() {
  Ok(conformance.Fixture(adapter.storage(resource), fn() { cleanup(resource) }))
}, 5000)
// after
conformance.run(
  fn() { Ok(conformance.fixture(adapter.storage(pool), cleanup: fn() { cleanup(pool) })) },
  timeout: 5000,
  owner_loss_within: lease + 500,
)
```

`Fixture` is opaque; build it with `fixture`. The suite uses fresh ids per
scenario, so one fixture may serve several executions. It no longer requires
process ownership ("token alone grants no ownership"): it requires that a
claim is a value, that a rebuilt claim with another token is refused, that a
live owner keeps its claim past `owner_loss_within`, and that a lost owner's
claim ends within it. A lease adapter declares its lease plus a margin
instead of meeting a fixed 500 ms window.

## `saga/reconciliation` (removed)

| Before                                                       | After                                                                                     |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------- |
| `reconciliation.Required(step: String, action, key: String)` | `durable.Required(step: saga.StepAddress, action: execution.Action, key: saga.EffectKey)` |
| `reconciliation.Activity`                                    | `execution.StepAttempt(n)`                                                                |
| `reconciliation.Compensation`                                | `execution.StepCompensation(n)`                                                           |
| `reconciliation.Undo`                                        | `execution.StepUndo`                                                                      |

## `saga_postgres` (new package)

`integrations/saga_postgres` stores executions in PostgreSQL on the
application's own `pog.Connection`, with a lease that saga renews, and ships
its migration. An application with its own pog adapter (checkout's 326-line
`saga_store`, research_agent's 262-line one) can delete it:

```gleam
let config = saga_postgres.config(db)
let assert Ok(Nil) = saga_postgres.migrate(config)
let storage = saga_postgres.storage(config)
```

Its pog and pgo ranges match grind's pins (`pog >= 4.1.0 and < 4.2.0`, `pgo

> = 0.20.0 and < 0.21.0`). See its README for defaults.

## Dependents

Symbols each dependent uses, from a grep of its `src` and `test` at the start
of wave 3, and what breaks.

### fabric/integrations/fabric_saga (light)

- `saga`: `Abort`, `Crash`, `Crashed`, `ErrorClass`, `Retry`, `RetryAfter`,
  `Returned`, `StepAddress`, `TimedOut`, `Workflow`, `address_to_string`,
  `both`, `compensate`, `define`, `map`, `perform`, `step`, `undo`
- `saga/execution`: `Action`, `ActionCrashed`, `ActionInterrupted`,
  `ActionTimedOut`, `CancelRequested`, `Cancelled`, `Cause`, `CleanupFailed`,
  `CompensationCrashed`, `CompensationTimedOut`, `Completed`,
  `CompletedWithUnknownEffects`, `Config`, `ConfigError`, `DeadlineExceeded`,
  `ExecutionLost`, `Failed`, `InvalidConfig`, `MaxConcurrencyNotPositive`,
  `Outcome`, `OutputCrashed`, `OwnerExited`, `RetryLimitReached`,
  `RetrySuperseded`, `Settlement`, `StepAttempt`, `StepCompensation`,
  `StepCrashed`, `StepFailed`, `StepTimedOut`, `StepUndo`, `UndoCrashed`,
  `UndoFailed`, `UndoTimedOut`, `UnknownEffect`, `UnknownEnding`,
  `Unresolved`, `pid`, `start_reporting`, `unknown_effects`, `validate`
- `saga/observation`: `AttemptFailed`, `step_stopped`

Breaks: `execution.validate` in `fabric_saga.tool` (src/fabric_saga.gleam:56);
the exhaustive `UnknownEnding` match in `internal/verdict.gleam`
(`describe_effect`) needs `ActionReturnedUnknown`; tests use
`execution.Config(..)` records and positional `undo`/`compensate` callbacks;
`saga/observation` becomes `saga/telemetry`.

### fabric/consumers/app (trivial)

- `saga`: `Workflow`, `both`, `define`, `perform`, `step`, `undo`
- `saga/execution`: `config`

Breaks: one `saga.undo(fn(_, _) { .. })` callback.

### fabric/experiments/workflow_composition (light, experiment)

- `saga`: `Abort`, `Continue`, `Crashed`, `DefinitionError`, `Hold`, `NoUndo`,
  `Returned`, `Step`, `StepAddress`, `TimedOut`, `Workflow`,
  `address_to_string`, `all`, `both`, `compensate`, `define`, `describe`,
  `perform`, `step`, `undo`
- `saga/execution`: `AwaitTimedOut`, `CancelRequested`, `Cancelled`,
  `Completed`, `CompletedWithUnknownEffects`, `Config`, `Execution`, `Failed`,
  `Outcome`, `StepFailed`, `Unresolved`, `await`, `cancel`, `run`, `start`

Breaks: `execution.Config(..)` records; `undo`/`compensate` callback shapes.

### oversight/apps/checkout (heavy; deletes glue)

- `saga`: `Abort`, `AbortAfterCleanupFailure`, `CompensationResolved`,
  `CompensationUnknown`, `Continue`, `Crashed`, `EffectAbsent`,
  `EffectCompleted`, `EffectUnknown`, `Hold`, `NoUndo`, `Retry`, `Returned`,
  `TimedOut`, `UndoWith`, `address_to_string`, `compensate`, `define`,
  `perform`, `reconcile_compensation`, `recoverable`, `restore_undo`, `step`,
  `Step`, `Workflow`, `undo`
- `saga/execution`: `Cancelled`, `Cause`, `Completed`,
  `CompletedWithUnknownEffects`, `Config`, `Failed`, `Outcome`,
  `RetryLimitReached`, `RetrySuperseded`, `StepAttempt`, `StepCrashed`,
  `StepFailed`, `UnknownEffect`, `Unresolved`, `await`, `run`, `run_id`,
  `start`, `unknown_effects`
- `saga/durable`: `Persistence`, `RunnerLost`, `StorageError`, `drive`,
  `prepare`, `start_or_reconnect`
- `saga/codec`: `Codec`, `new`
- `saga/storage`: `AlreadyExists`, `Busy`, `CancellationChanged`, `Conflict`,
  `Error`, `Io`, `NotFound`, `Record`, `StaleOwner`, `Storage`
- `saga/storage/conformance`: `Fixture`, `run`
- `saga/observation`: `compensation_stopped`, `run_started`, `run_stopped`,
  `step_started`, `step_stopped`, `undo_stopped`

Breaks and removals: `checkout/saga_store.gleam` (326 lines) is replaced by
`saga_postgres` on the app pool, and its `on_claim` hook and the
`Emitter(Pid)` attribution go (use `metadata.execution` /
`metadata.correlation`, set with `durable.with_correlation`); `jobs.gleam`
moves to the `Run` handle, `drive(run, timeout:)` and `error_kind`;
`codecs.to_saga` becomes `codec.json`; `workflow.gleam` moves `recoverable`,
`restore_undo` and `reconcile_compensation` to `durable`, rewrites the
`undo`/`compensate` callbacks, can drop the derived `pay-<order>` keys for
`key.idempotency`, and marks the payment's `MayHaveBeenSent` failure with
`unknown_when`; `app.gleam` replaces the `execution.Config(..)` record;
`unknown_payment_outcome_is_invisible_to_saga_test` must now expect
`CompletedWithUnknownEffects` and a non-empty `unknown_effects`.

### oversight/apps/support_desk (light)

- `saga`: `Abort`, `Crashed`, `Hold`, `Returned`, `TimedOut`, `Workflow`,
  `both`, `compensate`, `define`, `map`, `perform`, `step`, `undo`
- `saga/execution`: `Config`, `Failed`, `StepFailed`, `Unresolved`, `run`,
  `unknown_effects`
- `saga/observation` (as `saga_obs`): `compensation_stopped`, `run_started`,
  `run_stopped`, `step_started`, `step_stopped`, `undo_stopped`

Breaks: `refund.gleam`'s `undo` and `compensate` callbacks and its
`execution.Config(..)` record; `saga/observation` becomes `saga/telemetry`. The
refund's `PaymentOutcomeUnknown` error can use `unknown_when` so SD-1's
"refund the provider took" appears in `unknown_effects`.

### oversight/apps/research_agent (heavy; deletes glue)

- `saga`: `EffectAbsent`, `EffectCompleted`, `EffectUnknown`, `UndoWith`,
  `Workflow`, `both`, `define`, `effect`, `perform`, `recoverable`,
  `restore_undo`, `step`, `undo`
- `saga/execution`: `Completed`, `Config`, `Failed`, `Outcome`
- `saga/durable`: `Error`, `drive`, `prepare`, `start_or_reconnect`
- `saga/codec`: `Codec`, `new`, `text`
- `saga/storage`: `AlreadyExists`, `Busy`, `CancellationChanged`, `Conflict`,
  `Error`, `Io`, `NotFound`, `Record`, `StaleOwner`, `Storage`
- `saga/storage/conformance`: `Fixture`, `run`
- `saga/observation`: `RunMetadata`, `RunStopMetadata`, `StepStopMetadata`,
  `UndoMetadata`, `run_started`, `run_stopped`, `step_stopped`, `undo_stopped`

Breaks and removals: `research_agent/saga_store.gleam` (262 lines plus the
pid FFI) is replaced by `saga_postgres`; `publish.gleam` moves to
`durable.new`, the `Run` handle and `drive(run, timeout:)`, its
`saga.effect` key is an `EffectKey` (use `key.idempotency` for the
publisher's idempotency header), and `recoverable`/`restore_undo` move to
`durable`; `wire.saga` becomes `codec.json(version, .., codec.decoder(c))`;
`execution.Config(..)` becomes setters; telemetry handlers that pattern-match
the metadata records positionally need labels, and can attribute saga events
by `metadata.execution`.

## Follow-up fixes

Four changes after the wave 3 re-runs of checkout, support_desk and
research_agent. Each one is breaking; the fifth item is a fix with no API
change.

### An unknown effect holds the run unless the step opts into rollback

A returned error that `unknown_when` marks, with no `compensate` decision to
settle it, used to end the run `Failed` and undo the completed steps. It now
ends the run `Unresolved(step, error, settlement)` and undoes nothing, as a
`Hold(error)` decision does: `settlement.held` lists the steps left in place.
The same rule applies when the decider asked for a retry that the attempt
budget no longer allows after an unknown attempt (the cause was
`RetryLimitReached`), and to a resolver's `Failed` answer after a durable
restart. A decider's explicit `Abort` still rolls back; crashes and timeouts
are unchanged.

```gleam
// before: a `Hold` decider kept the reservation of an uncertain payment
saga.step("pay", pay)
|> saga.unknown_when(is_maybe_sent)
|> saga.compensate(max_attempts: 1, with: fn(failed) {
  case failed.failure {
    saga.Returned(error) -> saga.Hold(error)
    _ -> saga.Abort(Interrupted)
  }
})
// after: holding is the default
saga.step("pay", pay)
|> saga.unknown_when(is_maybe_sent)
```

To keep the previous rollback, opt in with `on_unknown`:

```gleam
// before: Failed(StepFailed(step, MaybeSent), settlement) and the earlier steps undone
saga.step("pay", pay) |> saga.unknown_when(is_maybe_sent)
// after: the same outcome
saga.step("pay", pay)
|> saga.unknown_when(is_maybe_sent)
|> saga.on_unknown(saga.RollBack)
```

A `case` on the outcome that matched `Failed(StepFailed(_, MaybeSent), _)`
for such a step matches `Unresolved(_, MaybeSent, _)` instead.

### `saga.define` returns the `Workflow`

A definition defect is a bug in the source (decision 4), so `define` panics
with a message that names the workflow and every offending step.
`saga.try_define` keeps the `Result` for workflows built from runtime data.

```gleam
// before
let assert Ok(workflow) = saga.define("checkout", build)
// after
let workflow = saga.define("checkout", build)

// before: names from runtime data
case saga.define(config.name, build) {
  Ok(workflow) -> Ok(workflow)
  Error(errors) -> Error(list.map(errors, saga.describe_definition_error))
}
// after
case saga.try_define(config.name, build) {
  Ok(workflow) -> Ok(workflow)
  Error(errors) -> Error(list.map(errors, saga.describe_definition_error))
}
```

### `durable.new` is total and defaults the version

`new` drops `version:` and starts at workflow version `"1"`;
`durable.with_version` sets another and panics on an empty one. A workflow
that cannot be persisted (a step without `recoverable`, a compensating step
without `restore_undo`, an empty step or codec version) panics with a
message naming every step and codec. `NotPersistable` and
`PersistenceProblem` are removed.

```gleam
// before
let assert Ok(persistence) =
  durable.new(workflow, version: "1", input: order, output: text, error: text, undo_error: text)
// after
let persistence =
  durable.new(workflow, input: order, output: text, error: text, undo_error: text)

// before
let assert Ok(persistence) =
  durable.new(workflow, version: "2", input: order, output: text, error: text, undo_error: text)
// after
let persistence =
  durable.new(workflow, input: order, output: text, error: text, undo_error: text)
  |> durable.with_version("2")
```

A version of `"1"` keeps the stamp of executions saved before this change.

### `execution.describe_cause` renders the step's error

`describe_cause` takes a describer for the step's error type and includes
its text; a retry-limit or superseded-retry cause also describes the last
attempt.

```gleam
// before: "step publish returned an error"
case cause {
  execution.StepFailed(_, PublishError(_, detail)) -> "publish failed: " <> detail
  _ -> execution.describe_cause(cause)
}
// after: "step publish returned an error: HTTP 500"
execution.describe_cause(cause, error: describe_publish_error)
```

### A killed runner releases its claim

No API change. When the runner process is killed or crashes while `drive`'s
caller lives, `drive` releases the claim before it returns `RunnerLost`, so
the next `drive` resumes at once from the checkpoint instead of returning
`StorageFailure(Busy)` until saga_postgres's 30 s lease expires. A grind job
that snoozed `Busy` for the lease only to cover this case may snooze for
less, but should still snooze `Busy` for at least the storage's owner-loss
window: the lease remains the fallback when the runner's node is lost.
`saga/storage/conformance` now also drives a durable run, kills its runner,
and requires the next drive to claim and resume at once; a third-party
adapter passes it without change unless its `release` depends on the
calling process.

```gleam
// before: the killed runner's claim blocked the next drive for the lease
let assert Error(durable.RunnerLost) = lost
let assert Error(durable.StorageFailure(storage.Busy)) = durable.drive(run, timeout: 10_000)
// after
let assert Error(durable.RunnerLost) = lost
let assert Ok(outcome) = durable.drive(run, timeout: 10_000)
```

### Dependents

- **fabric/integrations/fabric_saga**: `src` builds unchanged. Its tests
  replace `let assert Ok(workflow) = saga.define(..)` with `let workflow =
saga.define(..)` (8 sites); with that change all 39 tests pass. A refund
  marked with `unknown_when` now ends `Unresolved`, which the tool already
  reports as uncertain.
- **fabric/consumers/app**, **fabric/experiments/workflow_composition**:
  `saga.define` (1 and 3 sites).
- **oversight/apps/checkout**: `saga.define` (2 sites), `durable.new` drops
  `version: "1"`, and `execution.describe_cause` (1 site) takes `error:`.
  Its payment and shipment deciders decide every unknown error explicitly,
  so the outcomes do not change. The `Busy` snooze can stay at the lease.
- **oversight/apps/support_desk**: `saga.define` (1 site). The refund step
  has no decider, so an unknown refund now ends `Unresolved` instead of
  `Failed`; a workflow whose uncertain step follows a payment needs no
  `Hold` decider any more.
- **oversight/apps/research_agent**: `saga.define` (1 site), `durable.new`
  drops `version:`, and `publish.describe` can call
  `execution.describe_cause(cause, error: ..)` instead of matching
  `StepFailed` for the detail.
