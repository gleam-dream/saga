# Changelog

All notable changes to `saga` are recorded here. This package is
pre-release (not yet published to Hex); entries track the implementation
increments toward the first local-execution release.

## Unreleased

### Added

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
- `saga.DefinitionError.EmptyAll`: `saga.all([])` no longer panics: an
  empty list is reported as an ordinary definition error instead.
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

### Changed

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

### Fixed

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
