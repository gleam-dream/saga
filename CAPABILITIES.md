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
- Sequential composition (`saga.embed`) of one workflow into another's
  port graph, sharing the same run and journal.
- Foreign-port rejection: `define` rejects a `Port` used from outside the
  build it belongs to (`DefinitionError.ForeignPort`).
- Scoped step addresses as representation: `StepAddress(scope, name,
occurrence)` distinguishes repeated occurrences of the same step name at
  one scope. `saga.embed` pushes a fresh nested scope path (the embedded
  workflow's own name) for every step it introduces, so two `embed` calls
  of the same workflow — or an embedded step whose name collides with an
  outer step — are addressed distinctly (e.g. `inner/charge`,
  `inner/charge#2`) rather than colliding at the root scope.
- Orphan / unreachable-output rejection: `define` tracks every node
  `perform` creates during one builder evaluation (including ones later
  discarded rather than threaded into the returned port) and rejects any
  that never reach the workflow's own output, as `DefinitionError.OrphanStep`
  — a step that would otherwise silently never run (e.g. a side-effecting
  step whose port is built but never consumed) is caught at `define` time.

## Deferred

- **Step labels and metadata beyond name.** `StepDescriptor` exposes
  `address`, `depends_on`, `undoable`, `compensates`, `max_attempts`, and
  `timeout`, but there is no free-text label or arbitrary metadata
  attachment point separate from a step's own `name`. The design's
  "Diagnostic labels do not transport values or replace typed port
  connections" language (saga-design.md, "Typed DAG construction")
  anticipates a label surface distinct from the step name used for
  addressing; that surface does not exist yet.
- **Predefined generic runtime nodes** (`Identity<T>`, `Delay<T>`,
  `Choose<T>`, `Collect<T>`, `Constant<T>`, `Validate<T>`, `Map<A, B>`,
  `LLM<Input, Output>` from saga-design.md's "Parametric node
  definitions"). None of these has an executable adapter; the design
  itself notes "No executable runtime-node adapter exists for this
  operation" for `Identity`, `Choose`, and the LLM node specifically.
  Blocked on the runtime-authored schema graph surface below.
- **Exact structural compatibility for runtime-authored connections.**
  The design's conservative first cut ("exact structural equality plus
  explicit transformation nodes," saga-design.md "Schema compatibility")
  has no implementation; it presupposes the runtime-authored schema graph
  surface, which is itself deferred below. Width subtyping and broader
  assignability are explicitly out of scope until the conservative form
  exists.
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
- Draft/Published/Retired graph states, immutable executable graphs once
  published, and registry versions (saga-design.md "Graph publication");
  publication's ten validation checks (decode configurations, resolve
  definitions/versions, instantiate type parameters, unify ports, derive
  output schemas, detect cycles, validate constants/defaults, check
  required inputs, validate graph-level input/output schemas, produce an
  immutable `PublishedGraph`) have no implementation to test against.
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
- Arbitrary heterogeneous runtime steps and named native-result lookup
  (saga-design.md, "Where exact Reactor parity becomes impossible" and the
  capability-disposition table's final row): loading an arbitrary step
  type from configuration, referencing a result by name instead of a
  typed `Port`, serializing an arbitrary workflow closure, storing
  heterogeneous results centrally, and returning arbitrary new
  heterogeneous steps from a step (Reactor's
  `{:ok, value, dynamically_created_steps}`) are original, intentional
  exclusions of this design, not backlog. The schema-typed runtime graph
  surface (deferred above) is a separate, narrower mechanism for
  runtime-introduced types and is not a route back to unrestricted
  heterogeneity.

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
