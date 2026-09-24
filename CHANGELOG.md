# Changelog

All notable changes to `saga` are recorded here. This package is
pre-release (not yet published to Hex); entries track the implementation
increments toward the first local-execution release.

## Unreleased

### Changed

- **Performance: the workflow build function now runs exactly once, at
  `define`, never again per run.** Previously every `execution.run`/`start`
  re-evaluated the builder fresh and checked its shape against the
  define-time descriptors; per-run values also lived in per-node mailbox
  cells read via selective receive, which cost O(N) per read and up to
  O(N^2) per run for a workflow whose reads scale with N. The graph is now
  built once and shared, unchanged, across every run of a `Workflow`;
  per-run values live in a run-scoped store (`saga/internal/store`) keyed
  by node id, with exactly one unsafe (but sound-by-construction) coercion
  in the whole package, isolated to that module. See `bench/RESULTS.md`
  for before/after measurements (roughly 4x median run-time reduction at
  N=2000 across the benchmarked shapes, with build invocations dropping
  from one per run to one per `Workflow`).
- Relaxed the workflow builder's purity/determinism requirement
  accordingly: it is evaluated exactly once, so nothing about running a
  `Workflow` depends on calling the builder again and getting the same
  answer (composing it into another workflow via `embed`/`map_errors`
  still evaluates it again, at that _other_ workflow's own one-time
  `define`-time graph construction — never at run time).

### Removed

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
