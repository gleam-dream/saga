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

### Changed

- Removed the unreachable `Finishing` variant from `saga/execution.Phase`
  (and its internal mirror in `saga/internal/coordinator.Phase`). The
  coordinator reports its outcome and exits in the same step that
  finishes rollback, so `progress` never observed a window in which
  `Finishing` could be returned; keeping an unreachable public variant in
  an exhaustive `case` was misleading rather than future-proofing.

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
