# Capabilities

This inventory tracks saga's first local-execution release against the full
scope recorded in
[`oversight/saga-design.md`](https://github.com/gleam-dream/oversight/blob/master/saga-design.md).
Every capability below carries a status:

- **Delivered** — implemented and covered by tests in this release.
- **Deferred** — in the design's retained scope, not yet implemented. The
  local model is built to stay compatible with these being added later
  (definitions are pure descriptions, journals are closure lists, and
  addresses are stable and deterministic), but no code for them ships here.
- **Excluded** — ruled out for this design, not merely postponed.

## Delivered

- Typed workflow authoring: `saga.step`, `saga.undo`, `saga.compensate`,
  `saga.timeout`, `saga.perform`, `saga.both`, `saga.all`, `saga.map`,
  `saga.embed`, `saga.define` with full-collection validation of names,
  attempt budgets, timeouts, and foreign-port usage.
- Typed dependency ports in place of Reactor's dynamic named argument maps:
  a `Port(a, e, u)` is the only way to wire a step's input, so the scheduler
  never touches a heterogeneous value map and sharing a `Port` value shares
  one scheduled node.
- Bounded local concurrency scheduling (`Config.max_concurrency`), with a
  scheduler that admits work up to the bound and no further.
- Retry and recovery vocabulary: `Retry`, `RetryAfter`, `Continue` (a
  replacement output with its own undo), `Abort`,
  `AbortAfterCleanupFailure`, `Hold` (no rollback authority).
- Reverse-order undo of completed steps on failure or cancellation — a
  deliberate difference from Reactor 1.0.6, which undoes in forward order
  (see the design's conflict log).
- Settlement reporting: `undone`, `undo_failures` (all retained, not just
  the first), `not_undoable`, `held`, `interrupted`, `compensation_failures`,
  `sibling_failures`.
- Run deadlines, per-step timeouts, a settle window for in-flight siblings
  after a terminal trigger, and a cleanup timeout bounding each individual
  compensation decision or undo action.
- Cancellation via `execution.cancel`, idempotent and a no-op once settling
  or later has begun; active siblings are given `settle_timeout` before
  being killed and reported `interrupted`.
- Process lifecycle: `execution.start`/`await`/`run`, owner-exit detection,
  `execution.pid` for external monitoring, `execution.progress` for a
  read-only phase/step-state snapshot.
- Sinal-based observations (`saga/observation`): run start/stop, step
  start/stop, compensation decisions, and undo outcomes, with typed
  measurements and metadata.
- Caller-owned error and undo-error types throughout, via
  `saga.map_step_errors` (single step) and `saga.map_errors` (whole
  workflow) — saga never requires an application to adopt a saga-owned
  error type.
- Definition-shape validation at run start (`DefinitionChanged`): a
  workflow builder that produces a different graph shape on a real run
  than it did at `define` time is rejected before any work is admitted,
  enforcing the pure-builder requirement.
- Sequential composition (`saga.embed`) of one workflow into another's
  port graph, sharing the same run and journal.

## Deferred

- Durable execution: PostgreSQL journals, persistent checkpoints,
  freeze/thaw, snapshot codecs, restore, and migration.
- Grind integration (`saga_grind` optional runner), durable per-activity
  admission, and outbox.
- Durable approvals, approval signals, and command dedup/revision checks.
- Process-loss resume and Reactor-style in-memory halt/resume.
- Persistence eligibility of local definitions (closure serialization is
  Excluded, not deferred — see below).
- Independent children, child admission, attachment, and compensation
  authority.
- Post-success undo of a completed run (Reactor's `undo/2`) and local
  compensation authority beyond a single run's own rollback.
- Value-dependent `and_then`, bounded homogeneous `traverse`/
  `traverse_parallel`, and closed choice execution.
- Runtime-authored schema graphs: registry, publication, unification, and
  Blueprint runtime contracts.
- Visual editors, graph visualization export (Reactor's mermaid export),
  and historical review forks.
- Fabric/LLM execution nodes, bounded agent loops as nodes, and suspending
  agents.
- Concurrency budgets shared across runs or nested runs (Reactor
  concurrency pools) — `Config.max_concurrency` bounds one run only.
- Multiple-writer reducers and Fabric pure-graph validation extras
  (duplicate writers). Cycles and missing producers are already
  unrepresentable by construction.
- Undo retry policy (Reactor retries undo up to 5 times) — a local undo
  failure is recorded once in `settlement.undo_failures`, not retried.
- Supervisor-tree integration for coordinators.
- Stable persisted identities and versions for steps (the design
  laboratory's `StepId`/`Version`/`DefinitionId`); required only at a
  future durable boundary, not for local, non-persisted execution.

## Excluded

- Closure serialization: a `Step`/`Workflow` closure is never serialized.
  This is a design decision, not a gap to fill later.

## Deliberate behavioral differences from the Reactor oracle

Recorded here because they affect what a caller can rely on, not because
they are missing capabilities:

- **Undo order.** Reactor 1.0.6 undoes completed steps in forward
  (completion) order. Saga undoes in reverse completion order, matching
  the design's requirement.
- **Active siblings at failure.** Reactor returns while siblings continue
  running and orphans their effects. Saga settles siblings within
  `settle_timeout` and reports anything not known to have finished as
  `interrupted`, never claiming it as undone.
