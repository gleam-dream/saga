# Changelog

All notable changes to `saga` are recorded here. This package is
pre-release (not yet published to Hex); entries track the implementation
increments toward the first local-execution release.

## Unreleased

### Added

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
