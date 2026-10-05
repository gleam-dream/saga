# Changelog

All notable changes to `saga` are recorded here. This package is
pre-release (not yet published to Hex); entries track the implementation
increments toward the first local-execution release.

## Unreleased

### Round 9

- Return a definite no-effects failure when the reporting receiver exits or
  misses its five-second startup deadline. Startup cleanup stops the receiver,
  removes its monitor and suppresses late replies; it no longer asserts readiness.

- Add `saga/outcome`: complete/compensated/unresolved classification, held-step
  evidence, safe summaries, and typed failures with stable accessors.
- Add `saga/reporting.run_owned`: a bounded, independent receiver reports
  compensation after a short-lived owner stops. Move the former bridge's
  verdict behavior into saga; preserve its 24 tests and add ownership checks.
- Existing execution and durable records, and the Postgres adapter, are unchanged.

### Step correlation (wave 5)

Every change below has a before and after in
[docs/migration-wave-5.md](docs/migration-wave-5.md).

#### Added

- **A step reads its run's correlation.** `saga.EffectKey` has a new field,
  `correlation_of(key)`: the value set with
  `execution.with_correlation` or `durable.with_correlation`, which is also
  the `correlation` of the run's `saga/telemetry` events. `saga.effect`,
  `saga.undo` (`undo.key`), `saga.compensate` (`failed.key`) and the durable
  resolvers (`recoverable`, `resolve_undo`, `resolve_compensation`) already
  receive an `EffectKey`, so a step's own HTTP client can be correlated
  without threading the value by hand (support_desk, research_agent,
  checkout). An undo restored from a checkpoint and a resolver that runs
  after a restart carry the correlation of the handle that drives then.
  `durable.Required.key`, in `RecoveryRequired`, carries it too.
  `saga.step` still hands its function the input only; use `saga.effect` for
  a step that needs the context.
- **`EffectKey` accessors:** `saga.idempotency_key(key)`,
  `saga.attempt_number(key)`, `saga.attempt_key(key)` and
  `saga.correlation_of(key)`.
- **A durable execution keeps one correlation.** The first drive saves the
  correlation it uses (the handle's, or `from_key(id)`) with the checkpoint
  before it dispatches anything, and every later drive reads it back, so the
  events and steps of all drives agree. A `durable.with_correlation` after
  the first drive is ignored. The checkpoint format is now 2; a format 1
  record, saved before the correlation was recorded, still reads and means
  `from_key(id)`, and the next commit saves it in format 2. The first drive
  of an execution makes one extra commit.

#### Changed

- **Every execution has a correlation.** A durable execution without
  `durable.with_correlation` carries `correlation.from_key(id)` of its
  execution id, in its events and in its steps. A local run without
  `execution.with_correlation` gets a fresh `correlation.unique()` at start,
  used for its events and its steps, so no run is uncorrelated.

#### Breaking

- **Correlation is never optional.** `saga.correlation_of(key)` returns a
  `Correlation`, and the `correlation` field of every `saga/telemetry`
  metadata record changes from `Option(Correlation)` to `Correlation`:
  `RunMetadata`, `RunStopMetadata`, `StepMetadata`, `StepStopMetadata`,
  `CompensationMetadata` and `UndoMetadata`. The event's wire encoding is
  unchanged (the same `correlation` key), so an Erlang or Elixir handler is
  unaffected, and a handler that reads with `sinal/correlation.field()` still
  decodes it as `Some`. A `None` arm on these values no longer compiles.
- **`saga.EffectKey` is opaque.** Callers only receive it, and a record
  field broke positional code when `correlation` was added. Read it with
  `saga.idempotency_key`, `saga.attempt_number`, `saga.attempt_key` and
  `saga.correlation_of`: `key.idempotency` becomes
  `saga.idempotency_key(key)`, `key.attempt` `saga.attempt_number(key)`,
  `key.attempt_key` `saga.attempt_key(key)`. `EffectKey(..)` can no longer be
  built or matched outside saga. `UndoRequest`, `FailedAttempt`,
  `durable.Required` and the other records saga hands to callers stay public
  records, read by label; their docs say so.

### Durations (wave 4)

Every breaking change below has a before and after in
[docs/migration-wave-4.md](docs/migration-wave-4.md).

#### Breaking

- **Every timeout, deadline, interval and lease is a `gleam/time/duration`
  `Duration`.** No public signature or setter takes milliseconds as an
  `Int` any more, matching grind and http_gun. Defaults and behavior are
  unchanged. A bound below 1 millisecond keeps the refusal it had (a
  `ConfigError`, a `DefinitionError`, `durable.InvalidTimeout` or
  `conformance.InvalidTimeout`); a sub-millisecond `Duration` truncates to
  0 ms before the check, so it is refused.
  - `saga.timeout(step, limit)`, `saga.RetryAfter(delay)`,
    `saga.InvalidTimeout(step, value)` and `StepDescriptor.timeout` carry a
    `Duration`.
  - `execution.with_settle_timeout`, `with_cleanup_timeout` and
    `with_max_retry_delay` take a `Duration`; `execution.await` and
    `execution.progress` take `timeout: Duration`; the `ConfigError`
    variants carry the offending `Duration`.
  - `durable.drive(run, timeout: Duration)` and
    `durable.InvalidTimeout(timeout)`.
  - `storage.with_call_timeout(storage, timeout)` and
    `storage.with_renewal(every: Duration, ..)`; `conformance.run` takes
    `timeout:` and `owner_loss_within:` as `Duration`s;
    `testing.wait_until(within: Duration)`.
  - `telemetry.CompensationMetadata.retry_delay` is `Option(Duration)`. The
    event's map still carries whole milliseconds, and the numeric
    measurements (`duration`, `system_time`) are unchanged.
  - `saga_postgres.with_lease(config, lease)` takes a `Duration`.
- **Unbounded is an explicit `Infinity`.** New `execution.Timeout`
  (`After(Duration)` or `Infinity`), shaped like grind's and http_gun's.
  `execution.with_deadline` and `execution.with_step_timeout` take it, and
  `execution.without_step_timeout` is removed in favor of
  `with_step_timeout(execution.Infinity)`. The deadline's default is
  `Infinity`, as it was `None`.
- New dependency: `gleam_time >= 1.11.0 and < 2.0.0`, in saga,
  `saga_postgres` and `examples/order_consumer`.

The memory adapter's 5 second call bound, the file adapter's 5 second
mutation lock wait, the coordinator's 5 second start handshake and
`saga_postgres`'s 4.5 second query bound have no setter and no public
signature, so they stay fixed internal constants.

### Follow-up fixes (wave 3)

Every breaking change below has a before and after in the "Follow-up fixes"
section of [docs/migration-wave-3.md](docs/migration-wave-3.md).

#### Breaking

- **An unknown effect holds the run by default.** A returned error that
  `saga.unknown_when` marks, with no `compensate` decision to settle it,
  ended the run `Failed` and undid the completed steps, so an uncertain
  payment released the stock reserved before it (support_desk). The run now
  ends `Unresolved(step, error, settlement)`, undoes nothing and lists the
  completed steps in `settlement.held`, as a `Hold(error)` decision does.
  The same applies when the decider asked for a retry the attempt budget no
  longer allows after an unknown attempt, and to a resolver's `Failed`
  answer after a durable restart. A decider's explicit `Abort` still rolls
  back. New: `saga.on_unknown(step, policy)` with `saga.OnUnknown`:
  `Reconcile` (default) or `RollBack`, which restores the previous
  behavior. Crashes and timeouts are unchanged.
- **`saga.define` returns the `Workflow`** and panics on a definition
  defect, with a message naming the workflow and every offending step
  (decision 4). `saga.try_define` keeps the `Result(Workflow,
List(DefinitionError))` for workflows built from runtime data.
- **`durable.new` is total and defaults the version.** It takes
  `(workflow, input:, output:, error:, undo_error:)`, starts at workflow
  version `"1"`, and panics, naming every step and codec, on a workflow
  that cannot be persisted. `durable.with_version(persistence, version)`
  changes the version and panics on an empty one. `durable.NotPersistable`
  and `durable.PersistenceProblem` are removed.
- **`execution.describe_cause(cause, error: describe)`** renders the step's
  error with `describe`: `"step publish returned an error: HTTP 500"`
  (research_agent). A retry-limit cause also describes its last attempt.

#### Fixed

- **A killed runner no longer keeps its claim.** When the runner process
  was killed or crashed while `drive`'s caller lived, `drive` returned
  `RunnerLost` without releasing the claim, so every later `drive` was
  `Busy` for the whole lease (30 s on saga_postgres) and a grind job could
  reach its snooze limit first (checkout). `drive` now releases the claim
  as soon as the runner exits abnormally, keeping the checkpoint; a runner
  that raises exits abnormally too, so its heartbeat and in-flight attempts
  stop. Lease expiry remains the fallback when the runner's node is lost.
  `saga/storage/conformance` checks it for every adapter: a drive whose
  runner is killed must leave the execution claimable at once.
- The README and DURABILITY.md say to retry `Busy` no sooner than the
  storage's owner-loss window, such as saga_postgres's lease.

### Release API redesign (wave 3)

Every breaking change below has a before and after in
[docs/migration-wave-3.md](docs/migration-wave-3.md).

#### Breaking

- **`execution.Config` is opaque.** Build it with `execution.config()` and
  `with_max_concurrency`, `with_deadline`, `with_step_timeout`,
  `without_step_timeout`, `with_settle_timeout`, `with_cleanup_timeout`,
  `with_max_retry_delay` and `with_correlation`. A record literal or record
  update no longer compiles, so a new setting never breaks callers again.
  `execution.validate` is gone: `run`, `start`, `start_reporting` and
  `durable.drive` check the configuration and return `InvalidConfig` with
  every violation (new: `MaxRetryDelayNegative`).
- **`RetryAfter` delays are capped**, 300 000 ms by default
  (`with_max_retry_delay`). A longer delay is shortened, and the
  compensation event reports `retry_delay` and `retry_delay_capped`.
- **Callbacks take one labelled record.** `saga.undo` receives
  `UndoRequest(input, output, key)` and `saga.compensate` receives
  `FailedAttempt(input, failure, attempt, attempts_left, key)`. `saga.effect`
  receives an `EffectKey(idempotency, attempt, attempt_key)` whose
  `idempotency` is the same for every attempt of a step, so it can be sent
  downstream as an idempotency key (CHK-8). `saga.undo_effect`,
  `saga.compensate_with_key` and `saga.Attempt` are removed.
- **Persistence modifiers moved to `saga/durable`**, with labels:
  `durable.recoverable(step, version:, input:, output:, resolve:)`,
  `durable.restore_undo`, `durable.resolve_undo` (was `reconcile_undo`) and
  `durable.resolve_compensation` (was `reconcile_compensation`). Effect and
  undo resolvers answer `durable.Evidence`: `Completed`, `Failed`, `NotSent`
  (authorizes a replay) or `MaybeSent` (suspends). A compensation resolver
  receives the input and the failed attempt's `EffectKey` and returns
  `Option(Recovery)`. `saga.EffectStatus`, `UndoStatus` and
  `CompensationStatus` are removed. `saga.map_step_errors` keeps the codecs
  and maps resolver answers forward instead of dropping them.
- **One durable `Run` handle, one storage per store.** `durable.new(workflow,
input:, output:, error:, undo_error:)` replaces `prepare` (its `version:`
  moved to `durable.with_version` in the follow-up fixes).
  `durable.start_or_reconnect(persistence, storage, id:, input:)` returns a
  `Run`, which `drive`, `read`, `cancel`, `id` and `with_correlation` take;
  `durable.reconnect` attaches by id. Storage operations take the execution
  id, so one `Storage` serves a whole database pool. `Reference`,
  `reference_id` and `ReferenceMismatch` are removed.
- **`durable.drive(run, timeout:)` is bounded.** On timeout, and when its
  caller exits, the runner stops: in-flight attempts are killed, the claim
  is released and the last checkpoint stays (CHK-6). This is never
  cancellation; the next `drive` resumes.
- **Typed durable errors.** `StorageError` is `StorageFailure`;
  `InvalidDefinition(String)` is removed (`durable.new` panics instead; see
  the follow-up fixes);
  `CodecFailure` carries a `Boundary` and a `codec.CodecError`;
  `InvalidCheckpoint` carries a `CheckpointProblem`; `RecoveryRequired`
  carries `durable.Required(step: StepAddress, action: execution.Action,
key: EffectKey)`. New: `CheckpointTooLarge`, `InvalidTimeout`,
  `DriveTimedOut`. `durable.error_kind` and `durable.describe_error`
  classify and describe every error. `saga/reconciliation` is removed.
- **The storage contract is opaque.** Adapters build a `Storage` with
  `storage.new(create:, load:, claim:, commit:, release:, cancel:,
unfinished:)`. `claim` returns an opaque `Claim` that `commit` and
  `release` take, so ownership is a value and a database adapter needs no
  registry of claiming processes (CHK-4, RA-6). `commit` takes a labelled
  `Commit` with the execution's phase. `Record` is replaced by the opaque
  `Stored`; `Io(String)` by `Unavailable(detail)` and `TimedOut`.
- **`saga/codec`** encoders and decoders return a typed `CodecError`.
  `codec.encode`, `decode` and `version` leave the public surface.
- **`saga/observation` is renamed `saga/telemetry`.** Every metadata record
  gains `execution` (the durable id) and `correlation`, and `OutcomeKind`
  gains `OutcomeCompletedWithUnknownEffects`, which `run_stop` reports
  instead of `OutcomeCompleted` for such runs.
- **Memory and file adapters.** `memory.start`, `memory.supervised(name)`,
  `memory.named(name)` and `memory.stop` replace `new` and `close`; the
  store is a gleam_otp actor holding every execution, with 5 second calls.
  `file.open` takes a directory and keeps one file per execution.
- **Conformance.** `conformance.run(fresh, timeout:, owner_loss_within:)`
  takes the adapter's owner-loss window, and fixtures are built with
  `conformance.fixture(storage, cleanup:)`.
- `execution.Crash` and `execution.StepAddress` aliases, `saga.CrashOrTimeout`
  and `execution.validate` are removed.

#### Added

- `saga.unknown_when(step, classify)`: a returned error the classifier
  marks, such as a payment that may have been charged, is named in
  `unknown_effects` with `ActionReturnedUnknown`, so a retried success
  becomes `CompletedWithUnknownEffects` (CHK-3, SD-1). The decider still
  receives the typed error, and the record survives a durable restart.
- `codec.json(version, encode, decoder)` over `gleam/json` with a fallible
  encoder, `codec.int()` and `codec.describe_error`.
- `execution.kind`, `execution.describe_cause`,
  `execution.describe_config_error` and `saga.describe_definition_error`.
- `durable.unfinished(storage, limit:)` lists executions that wait for a
  driver; `storage.with_renewal` declares lease renewal, run by a heartbeat
  linked to the runner; `storage.with_call_timeout` bounds each storage call
  (5 000 ms by default); `durable.with_max_checkpoint_bytes` bounds each
  checkpoint (16 MiB by default); `durable.with_config` and
  `durable.with_correlation`.
- The `saga_postgres` package (`integrations/saga_postgres`): a PostgreSQL
  storage on the application's own `pog.Connection`, with its schema
  migration, passing the storage conformance suite.
- Dependencies: `gleam_json >= 3.0.0 and < 4.0.0`, `gleam_otp >= 1.0.0 and
< 2.0.0`.

### Earlier pre-release changes

#### Added

- **Breaking: the outcome names every action whose effect is unknown.**
  A consumer that must tell a caller whether a run's effects are known
  (Fabric reports a workflow to an AI model as definite or uncertain)
  could not decide it from the `Outcome`: a crashed attempt whose
  `compensate` decider then chose `Abort(e)` was reported as an ordinary
  `StepFailed`, a sibling in the settle window likewise, and a crashed
  attempt retried to success left no trace at all. Only timed-out attempts
  were folded into `interrupted` or `CompletedWithUnknownEffects`.
  `execution.unknown_effects(outcome) -> List(UnknownEffect)` now lists,
  for every outcome kind, each step attempt, compensation decision and undo
  that ended with an unknown effect, in the order they ended, and is `[]`
  exactly when every action returned `Ok` or a typed error. An
  `UnknownEffect(step, action, ending)` names the step, the action
  (`StepAttempt(n)`, `StepCompensation(n)` for the decision about attempt
  `n`, or `StepUndo`), and how it ended (`ActionCrashed(crash)` for a raise
  or process exit, `ActionTimedOut` for a kill at the step's timeout or
  `cleanup_timeout`, `ActionInterrupted` for a kill when the settle window
  closed). The record is taken when the action ends, so no later decision
  (retry, `Continue`, `Abort`, `Hold`) hides it. `Cause.StepFailed`'s and
  `saga.compensate`'s docs now say that an `Abort` after a crash is
  reported as `StepFailed`. Two public shapes change:
  - `Settlement` gains `unknown_effects: List(UnknownEffect)`. A
    `Settlement(..)` literal must add `unknown_effects: []` (or the
    expected list); record updates (`Settlement(..s, …)`) and field
    access are unaffected. `interrupted` keeps its meaning (attempts killed
    at their timeout, attempts and decisions killed by the settle window).
  - `CompletedWithUnknownEffects.unknown_effects` changes from
    `List(StepAddress)` to `List(UnknownEffect)`, and the variant is now
    also returned when an attempt crashed or its process exited and its
    decider retried or continued (previously a plain `Completed`). A caller
    that used the addresses maps them:
    `list.map(unknown_effects, fn(effect) { effect.step })`. A plain
    `Completed` now proves every action of the run returned.
    `saga/observation`'s `run_stopped` `interrupted` measurement for such a
    run counts these crashed attempts too, since it is the list's length.

- **`execution.start_reporting(workflow, input, config, to: report)`
  delivers a run's outcome to a caller-supplied `Subject`.** A consumer
  that ran a workflow in a task it could lose (Fabric runs one as a tool,
  and cancelling Fabric kills the task) could not learn how compensation
  ended: `await` is owner-only, and the outcome went to the dead owner.
  `report` now receives one `Outcome` when the run ends, after rollback,
  whether or not the starting process is alive; a subject of the caller's
  own can join its `Selector`, removing the helper owner process that an
  owner-only `await` required. The starting process still owns the run: its
  exit cancels the run with `OwnerExited`, and that cancellation's
  settlement is what `report` receives. The report is sent at most once,
  and exactly once unless the coordinator is killed or a named subject has
  no process behind it; a receiver detects a lost run with a monitor on
  `execution.pid`, whose `Down` always follows the outcome. `await` on such
  a run returns `NotOwner`. `start` and `await` are unchanged. The settle
  window stays per run (`Config.settle_timeout`): the owner-exit
  cancellation has no call to carry a per-cancel value, and settling already
  ends as soon as nothing is in flight.

- **`execution.Config` gains `step_timeout: Option(Int)`, defaulting to
  `Some(60_000)` (60 seconds) in `execution.config()`.** Previously a step
  with no `saga.timeout` of its own could hang forever and block
  `execution.run`/`execution.await` indefinitely, with no default anywhere
  in the library to prevent it. Every attempt is now bounded by
  `step_timeout` unless the step declares its own `saga.timeout`, which
  always overrides the default (shorter or longer). `step_timeout: None` is
  the explicit opt-out, restoring the previous no-default-timeout behavior
  for every step that does not set its own. `validate` rejects a non-positive
  `step_timeout` as the new `ConfigError.StepTimeoutNotPositive(value)`,
  before any process starts, consistent with the existing `deadline`
  validation. The run `deadline` default stays `None`: `step_timeout` and
  `saga.compensate`'s `max_attempts` already bound every attempt and its
  total retry budget, so a run-wide deadline remains an opt-in, coarser
  ceiling rather than something the library defaults on every caller's
  behalf. A timed-out attempt's semantics are unchanged: its effect is
  unknown, never journaled or undone, and reported exactly as before
  (`StepTimedOut`, `interrupted`, `CompletedWithUnknownEffects`).
- **New public module `saga/testing`, with one helper:
  `wait_until(execution, matching:, within:)`.** Polls `execution.progress`
  at a short internal interval (never a fixed `process.sleep`) until a
  caller-supplied predicate accepts a `Progress` snapshot, or `within`
  milliseconds elapse overall, using the monotonic clock for the deadline.
  Returns `Error(testing.WaitTimedOut)` or `Error(testing.RunEnded)`
  (mirroring `execution.ProgressError`'s `ExecutionEnded`) on the two ways it
  can fail to observe a match. Saga's own test suite now dogfoods this
  helper (`probe.wait_until_progress` is removed in favor of it) instead of
  keeping a private, near-identical copy. See README.md's "Testing
  workflows" section for the helper plus a trimmed step-blocking gate
  recipe — Saga does not ship a gate itself, since blocking a step body on a
  test-controlled release is generic BEAM concurrency, not anything
  Saga-specific.

#### Changed

- **Built on the wave 2 Sinal API.** `saga/observation`'s descriptors use
  Sinal's record builder and `fields.enum`, and the coordinator calls
  `sinal.emit` directly, so an application that routes the `saga` prefix to
  a `sinal/forwarder` moves saga's handlers off the coordinator. The event
  names, keys, and Gleam types are unchanged; a kind field now also decodes
  from an atom with the same name.
- **Module docs render.** All 11 public modules wrote their module doc as
  `///` before the imports, which `gleam docs` attaches to the first
  definition, so no module page had an introduction. Each now starts with a
  `////` module doc that states its responsibility, when to use it, and how
  it relates to the other modules; `saga`, `saga/execution` and
  `saga/durable` include an example. The rewrite drops statements that no
  longer matched behavior: `saga/durable` said storage is supplied by
  integrations (saga ships memory and file adapters), `saga/execution`
  stated a worst-case run time that holds only with a `deadline`, and the
  `saga` module doc and `define`'s doc narrated earlier designs. Every
  `saga/codec` function and the undocumented `saga/durable`, `storage`,
  `reconciliation` and `storage/memory` definitions now have docs, and
  `AttemptFailure`'s doc no longer marks timeouts as a future increment. A
  test (`module_docs_test`) fails if a public module lacks a `////` doc.
- **README common path defines the workflow once.** The example built a
  new workflow for every order id; it now defines `checkout()` once and
  passes the order id as the run's input, and the outcome list includes
  `CompletedWithUnknownEffects`. No API change.
- **`gleam_stdlib` range widened to `>= 0.70.0 and < 2.0.0`** in saga,
  `examples/order_consumer` and `bench`, so an application can combine
  saga with packages that need `gleam_stdlib` 1.x. Each manifest now
  resolves `gleam_stdlib` 1.0.5; no source change was needed.

- **Performance: the workflow build function now runs exactly once, at
  `define`, never again per run.** Previously every `execution.run`/`start`
  re-evaluated the builder fresh and checked its shape against the
  define-time descriptors; per-run values also lived in per-node mailbox
  cells read via selective receive, which cost O(N) per read and up to
  O(N^2) per run for a workflow whose reads scale with N. The graph is now
  built once and shared, unchanged, across every run of a `Workflow`;
  per-run values live in a run-scoped store (`saga/internal/store`) keyed
  by node id, with exactly one unsafe (but sound-by-construction) coercion
  in the whole package, isolated to that module. See `bench/RESULTS.md` and
  the "Design decisions" section of README.md for before/after
  measurements and the trade this makes: the store change alone measured
  1.42x-1.68x faster median run time at N=2000 (not "roughly 4x" — that
  figure was the _growth-ratio_ ceiling that still remained, not the
  speedup); combined with the admission fix below, the two together
  measured 14x-49x faster median run time at N=2000, depending on shape,
  with build invocations dropping from one per run to one per `Workflow`.
- **Performance: `coordinator.admit` no longer scans every node on every
  admission decision.** A min-heap of ready node ids
  (`saga/internal/min_heap`) is pushed to exactly when a node becomes
  admittable and popped by `admit`, in the same ascending (builder-call)
  order a full scan always yielded — verified unchanged against the
  Reactor differential oracle and a new direct regression test. This
  closed the remaining growth-ratio gap the store change alone left open:
  median run time now grows ~2x per doubling (linear) instead of ~4x
  (quadratic).
- Relaxed the workflow builder's purity/determinism requirement
  accordingly: it is evaluated exactly once by `define`, and only ever
  evaluated again — once — by `embed`, when composing it into a different,
  unrelated workflow's own `define` evaluation. `map_errors` does **not**
  evaluate the builder again: it reuses the already-built, already-
  validated graph directly (see `saga.Workflow`'s and `saga.map_errors`'s
  own doc comments for exactly which two situations ever invoke a
  builder).
- **Performance: `define` time no longer grows superlinearly for
  wide-dependency workflows.** `Port` used to carry its own transitive
  `nodes: Dict(Int, Node(e, u))`, merged with `dict.merge` on every
  `both`/`map` combinator call (`saga.gleam`'s `merge_ports`); a workflow
  whose steps each read a sliding window of several prior outputs (like
  `bench`'s "wide" shape) merged an ever-larger dict on every such call,
  for an O(N^2) `define` cost. Profiling (`:eprof` on the wide shape's
  `define` at N=2000) attributed 35% of total time to `maps:merge/2` alone.
  Every node is now appended once (O(1)) to its scope's own registry
  instead (`saga/internal/cell.Registry`, already used for orphan-step
  tracking); `define` assembles the node table exactly once, at the end,
  by walking the registered nodes' own `deps` edges from the workflow's
  output (`reachable_nodes`/`walk_reachable`) — one O(V+E) graph walk
  instead of one O(N) merge per combinator call. See `bench/RESULTS.md`'s
  "Define time" section: the wide shape's `define`-time growth per
  doubling dropped from ~3.0x-3.24x (superlinear) to ~1.9x-2.8x (linear,
  matching chain's and fan's own define-time growth, both already linear
  and unaffected by this change). Run time, admission, and validation
  order are unchanged.

#### Removed

- `execution.Cause.DefinitionChanged` and the definition-shape check it
  reported: with the build function evaluated exactly once, there is no
  later re-evaluation whose shape could ever diverge from what `define`
  recorded, so the variant became permanently unreachable. Removed rather
  than kept for compatibility, since this package is pre-release and an
  unreachable public variant in an exhaustive `case` is misleading rather
  than future-proofing (the same precedent as this changelog's earlier
  removal of `execution.Phase`'s unreachable `Finishing` variant). Any
  exhaustive `case` over `execution.Cause` must drop its `DefinitionChanged`
  arm.

#### Added

- External acceptance package at `examples/order_consumer`: a separate
  Gleam package, depending on saga only through its public modules
  (`saga`, `saga/execution`, `saga/observation`), exercising a shared-data
  - parallel-work checkout, a payment failure with compensation and a
    retained undo failure, advanced configuration with cancellation via
    `start`/`progress`/`cancel`/`await`, and caller-owned error types via
    `map_errors`. Runnable with `gleam run` (a readable trace) and
    `gleam test`.
- Compiler-negative fixtures under `fixtures/negative/` plus
  `scripts/check_negative.sh`, proving from the consumer's own point of
  view that: mixing two steps' error types without `map_step_errors`
  fails to compile, a `Continue` recovery's replacement output must match
  the step's declared output type, `embed` rejects a mismatched input
  type, and `Port`/`Execution` cannot be constructed outside their owning
  modules.
- `CAPABILITIES.md`: the full implemented/deferred/excluded capability
  inventory against the design's retained scope.
- Orphan / unreachable-output rejection at `define` time: a step whose
  output is never consumed by the workflow's declared output is now
  reported as `DefinitionError.OrphanStep`, naming the step, instead of
  silently never running.
- Scoped embed addresses: `saga.embed` now pushes a nested scope (the
  embedded workflow's own name) for every step it introduces, so repeated
  `embed` calls of the same workflow (or a name collision with an outer
  step) are addressed distinctly (e.g. `inner/charge`, `inner/charge#2`)
  instead of colliding at the root scope.
- `execution.Outcome.CompletedWithUnknownEffects(output, unknown_effects)`:
  a new outcome variant for the case a plain `Completed` cannot honestly
  report — a step whose attempt was killed by its own `timeout` but whose
  recovery decider chose `Retry`/`RetryAfter`/`Continue` anyway. The killed
  attempt's own effect is still unknown and was never journaled or undone,
  even though the run otherwise completed; existing exhaustive `case`
  expressions over `Outcome` must add this arm.
- `execution.Cause.RetrySuperseded(step, last)`: a retry decision refused
  only because the run had already begun settling for a different trigger
  is now reported distinctly from `RetryLimitReached` (which means the
  step's own attempt budget was exhausted).
- `saga/observation`'s `step_stopped` now reports a settle-sweep kill as
  `AttemptInterrupted`, and every `step_stopped`/`compensation_stopped`/
  `undo_stopped` event's `duration` is a real elapsed measurement instead
  of a hard-coded `0`.

#### Changed

- Removed the unreachable `Finishing` variant from `saga/execution.Phase`
  (and its internal mirror in `saga/internal/coordinator.Phase`). The
  coordinator reports its outcome and exits in the same step that
  finishes rollback, so `progress` never observed a window in which
  `Finishing` could be returned; keeping an unreachable public variant in
  an exhaustive `case` was misleading rather than future-proofing.
- `execution.start`'s error type changed from `List(ConfigError)` to
  `RunError`: a coordinator that fails to complete its startup handshake
  within 5 seconds is now reported as `ExecutionLost` instead of a `let
assert` panic.
- `execution.progress` on a run that has already ended now returns
  `Error(ExecutionEnded)` promptly instead of always timing out.
- **Breaking:** `saga.all`'s signature changed from
  `all(ports: List(Port(a, e, u))) -> Port(List(a), e, u)` to
  `all(first: Port(a, e, u), rest: List(Port(a, e, u))) -> Port(List(a), e, u)`.
  A caller with zero ports now simply has no `first` to pass, so there is
  no empty case left to construct, panic on, or report as a definition
  error — `DefinitionError.EmptyAll` and its placeholder port (which
  allocated a real, unused registry as a side effect on every empty call)
  are both removed. Existing call sites change from `saga.all(ports)` to
  `saga.all(a, [b, c])` for a literal list, or, for a `List(Port(..))` of
  unknown length, a `case` that handles the empty list explicitly instead
  of asserting it away:
  ```gleam
  case ports {
    [first, ..rest] -> Ok(saga.all(first, rest))
    [] -> Error(NoPortsToCombine)
  }
  ```

#### Fixed

- `embed` did not check its re-invoked builder's returned port for a
  foreign scope before restoring the parent's own scope on it
  (`Port(..output, scope: input.scope)`, unconditionally). A builder that
  is not a pure function of its input (see the previous `map_errors`
  finding on this same page) could return a `Port` stashed from a
  different, unrelated `define` call; `embed` accepted it silently instead
  of reporting `DefinitionError.ForeignPort` — the same check `both`/`all`/
  `perform` already apply to a foreign port used directly — so `define`
  succeeded on a graph it never actually validated, and running it later
  panicked in `store.get` (reported to the caller as `Lost`) instead of
  failing at `define` time. `embed` now calls the same `foreign_error_for`
  check used elsewhere before restoring scope. Ported the reviewer's probe
  P9 as a regression test (`embed_rejects_builder_returning_foreign_port_test`
  in `test/authoring_test.gleam`).
- `map_errors`'s own `embed`-time builder retyping (`translating_build`) had
  the identical unconditional-overwrite gap at its own, separate
  scope-restoring site — found while porting the reviewer's probe P9 in
  full (P9's actual shape wraps the foreign-scoped builder with
  `map_errors` before `embed`ding it, not a plain `embed`, so the fix
  above alone did not close it). Fixed the same way, as part of the
  `define`-time node-table rework below (which already had to rebuild
  `translating_build`'s own registry handling). New regression test:
  `embed_rejects_map_errors_builder_returning_foreign_port_test` in
  `test/authoring_test.gleam`.
- `map_errors` re-ran a workflow's build function a second time (with no
  validation at all) to compute its own graph, instead of reusing the
  already-built, already-validated graph `define` produced. Found by
  independent review: a builder that was not a pure function of its input
  could, under this second evaluation, produce a graph shape `define`
  never checked (`describe(mapped)` could then disagree with what
  `execution.run` actually executed) or hand back a `Port` stashed from a
  different evaluation, panicking `store.get` and losing the run instead
  of completing it. `map_errors` now reuses `workflow`'s own `nodes`/
  `order`/`root_input_id`/`fetch_output` directly, translating only the
  node closures (`node.map_errors`); its `build` field is retained solely
  so a _later_ `embed` of the mapped workflow has a validated builder to
  call, per the invariant `saga.Workflow`'s doc comment now states
  precisely: a builder is invoked in exactly two situations (once by its
  own `define`, once more per `embed` into another workflow), never any
  other way.
- A panicking or slow `saga.map` placed between two steps ran unprotected
  in the coordinator process itself, able to crash the whole run's
  scheduler (losing rollback) or block cancellation/deadline handling for
  every run. Both the value read and any pending `map` transform are now
  deferred into the consuming step's own attempt task, under the same
  `rescue` as the step body, so a panicking or slow `map` is an ordinary
  attempt crash/duration.
- `embed(map_errors(inner))` deadlocked: `map_errors`'s shadow input port
  dropped the real input's dependency set and accumulated definition
  errors. Both are now carried through.
- A `RetryAfter` backoff firing (and an immediate `Retry` decision) could
  push the number of concurrently-attempting steps above
  `Config.max_concurrency`, since a fired retry started unconditionally
  rather than through the normal admission gate. Fired retries now join
  the ready queue and are admitted like any other node; a node entering
  backoff also nudges admission so a freed concurrency slot is not left
  idle until the backoff fires.
- A monitor `Down` message could leak into the caller's mailbox after
  `run`/`await` (and, separately, into `define`'s caller's mailbox from
  an internal orphan-tracking registry) — both are now demonitored/drained
  before returning.
- A step body's native `throw` was reported as `ErrorClass` instead of
  `ThrowClass`; every rescued-crash site now passes through the real
  crash class. An `Abnormal` exit reason's payload is now included in the
  formatted crash reason instead of being replaced with the bare word
  "abnormal".
- `lifecycle_test.gleam`'s coordinator-kill test no longer polls
  `process.is_alive` in a sleep loop; it waits on a monitor instead.
- `execution.await`'s "already awaited" tracking grew the calling process's
  own process dictionary by one entry per `Execution`, forever (a boolean
  flag keyed by a fresh integer, never removed). `await` is now stateless
  on the owner's side: a repeated `await` is detected from a fresh,
  per-call monitor's immediate `noproc` `Down` instead, and the original
  monitor set up in `start` still catches a coordinator that dies
  abnormally before ever being awaited (reported `Lost`). A second `await`
  after an earlier `Lost` is now also reported promptly, as
  `AlreadyAwaited` rather than idling out the full timeout — see
  `AwaitError`'s doc comment for why the two cases are not distinguished.
- A step killed by its own `timeout` when a `compensate` decider was
  attached never emitted its own `step_stopped(AttemptTimedOut)`
  observation at all — only the decider's later resolution emitted a
  `step_stopped`, under its own (much shorter) duration and outcome kind.
  The killed attempt's `step_stopped(AttemptTimedOut)` is now always
  emitted, with its own real elapsed duration (previously overwritten with
  a fresh, near-zero timestamp before that duration was read).
- `execution.await`'s `FreshDown` branch (the run was already lost by the
  time this call's fresh monitor was set up) always reported
  `AlreadyAwaited`, even when the coordinator died _during_ this very
  `await` and the original monitor's real `Down` was racing the fresh
  one's synthetic `noproc` in the mailbox — losing the real crash reason to
  a generic "already awaited" in that window. `await` now checks the
  original monitor with a zero-timeout selective receive first and reports
  `Lost` with its real reason when found, and demonitors the original (with
  `[flush]`) on every `FreshDown` outcome rather than only some. `run`'s
  now-truly-unreachable `AlreadyAwaited` case is mapped to
  `Error(ExecutionLost(_))` instead of a panic.
- `scheduling_test.gleam`'s `retry_after_backoff_honors_max_concurrency_test`
  waited for step `a` to reach `Compensating` specifically before releasing
  a gated sibling — but `a`'s decider body is a synchronous, allocation-free
  `case`, so under heavy scheduler contention the coordinator can process
  its `AttemptDone` and the decider's `RecoveryDone` back-to-back in one
  scheduling slice, skipping the `Compensating` snapshot entirely before
  the test process ever gets to poll in between. Fixed by waiting for
  either `Compensating` or the already-settled `RetryScheduled`, both of
  which confirm what the test actually needs (the attempt slot was freed
  through admission). Several other timing-sensitive tests used 2-3 second
  wait budgets for lower-bound "poll until true" conditions
  (progress-polling, gate entry, run completion), tight enough to
  spuriously fail under heavy machine load; widened to 10-30 seconds where
  waiting longer only costs wall-clock time. Two "must be prompt" assertions
  in `lifecycle_test.gleam` compared elapsed time against an overly tight
  upper bound; widened to a generous multiple of the property actually
  under test (not idling out a much longer timeout).

## Increment 2

### Added

- Run deadlines, per-step timeouts, a cancellation-settle window, and a
  per-action cleanup timeout.
- `execution.cancel`, idempotent past the point settling has begun.
- Owner-exit detection and `execution.pid` for external monitoring.
- `saga/observation`: Sinal event descriptors for run/step/compensation/
  undo lifecycle.

## Increment 1

### Added

- Typed workflow authoring (`saga.step`, `saga.undo`, `saga.compensate`,
  `saga.timeout`, `saga.perform`, `saga.both`, `saga.all`, `saga.map`,
  `saga.embed`, `saga.define`) with full-collection definition validation.
- Bounded local concurrency scheduling, retry/compensation, reverse-order
  rollback, and settlement reporting.
- `saga/execution`: config and validation, `run`/`start`/`await`/`cancel`/
  `progress`.
