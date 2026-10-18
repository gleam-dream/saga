#import ".render/designlib.typ": *

#let title = [Saga]
#let accent = "slate"
#let index_only = true
#let body = [
#section(title: "Foundation", lead: "Saga owns typed dependency execution and the evidence needed to settle effects.", body: [
  #no-goal(title: "A job-delivery or agent-policy runtime")[Delivery, queue capacity, agent policy, protocol transport, and application retry policy belong to their respective owners.]
  #no-goal(title: "Serialize closures or expose arbitrary native lookup")[Executable closures are reconstructed from deployed definitions. Native dependency values cannot be obtained through runtime result names or caller-visible type erasure.]
  #principle(title: "Separate evidence from authority")[An observed result, cancellation request, notification, or telemetry event cannot establish authority it does not carry. Compensation authority and absence of external effects require explicit contracts.]
  #principle(title: "Keep package boundaries at distinct lifecycles")[Saga owns effect progress and settlement. Storage owns persistent ownership, while applications compose delivery and agent lifetimes through small public ports.]
])

#section(title: "System at a glance", lead: "One workflow model is interpreted by the local coordinator or by a checkpointed drive.", visual: diagram(
  altitude: "L1", viewpoint: "runtime", title: "Ownership around Saga",
  nodes: (
    (id: "app", label: "Application", sub: "values, effects, policy", kind: "external-system", tint: "slate"),
    (id: "saga", label: "Saga", sub: "definition, scheduling, settlement", kind: "component", tint: "violet"),
    (id: "store", label: "Storage adapter", sub: "atomic claims and saved bytes", kind: "external-system", tint: "slate"),
    (id: "delivery", label: "Delivery owner", sub: "wakeups and job capacity", kind: "external-system", tint: "slate"),
    (id: "sinal", label: "Sinal", sub: "observation transport", kind: "external-system", tint: "slate"),
  ),
  edges: (
    (from: "app", to: "saga", relation: "call", label: "define, run, drive, reconcile"),
    (from: "saga", to: "app", relation: "dataflow", label: "full native outcome"),
    (from: "saga", to: "store", relation: "call", label: "conditional checkpoint protocol"),
    (from: "delivery", to: "saga", relation: "call", label: "drive stable execution id"),
    (from: "saga", to: "sinal", relation: "dataflow", label: "facts after transitions"),
  ), caption: [Storage implements a port; it does not choose workflow progress. Delivery acknowledgments and Sinal events never replace the saved outcome.],
), body: [
  #points(
    [The application supplies native input/output/error/undo-error records and callbacks. The #ctx("execution") context retains them through #term("term-port") connections and #term("term-outcome") reports.],
    [A #term("term-local-run") belongs to its starting process; owner exit requests cancellation. A #term("term-durable-execution") belongs to its saved record; driver exit stops driving without recording cancellation.],
    [The package requires Sinal and standard Gleam/OTP/time/JSON libraries. PostgreSQL, Blueprint, Grind, Fabric, Relay, and provider clients compose through optional adapters or application code.],
  )
  #answers(title: "Saga", responsibility: [Execute typed dependencies and preserve effect evidence.], interface: [Workflow authoring; local execution; optional durable capability; storage, outcome, reporting, observation, and testing ports.], interactions: [Calls application effects and recovery callbacks; interprets storage conditional writes; emits observations through Sinal.], invariants: [One native type relation per Port; bounded per-run admission; uncertainty and settlement survive projection boundaries.], failure: [Separate definition/configuration rejection, workflow outcomes, durable operational failures, and lost report delivery.])
])


#section(title: "Context boundaries", lead: "Execution defines what a workflow does; durability defines how its progress survives.", body: [
  #md-table(3, (
    [*Context*], [*Owns*], [*Depends on*],
    [#ctx("execution")], [Workflow definition, typed ports, scheduling, retry, compensation, undo, outcome, settlement, local ownership, telemetry, and the intended composition and runtime-graph extensions.], [Application callbacks and Sinal.],
    [#ctx("durability")], [Saved execution identity, checkpoints, compatibility, recovery resolvers, claims, the storage port, reference adapters, and delivery obligations.], [The Workflow, EffectKey, and Outcome values of execution.],
  ))
  #points(
    [Durability is the customer of execution: it attaches codecs, versions, and resolvers to the same Workflow and never defines a second graph. The same Workflow is interpreted by the local coordinator or by a checkpointed drive.],
    [Execution owns every term about a step action and its settlement. Durability owns every term about saved state and its ownership. Neither context redefines the other's terms; each cites the owning slug.],
    [The PostgreSQL storage adapter is a nested project with its own layer under `integrations/saga_postgres`. It implements the durability storage port and owns SQL, schema, and database-clock leases.],
  )
])

#section(title: "Verification gates", lead: "One registry of profiles proves each obligation by the evidence that can establish it.", body: [
  #md-table(3, (
    [*Evidence*], [*Guarantee checked*], [*Boundary*],
    [examples/order_consumer], [Common and advanced native use, mapped errors, failures and undo, durable recovery, Blueprint bridge, and full reporting.], [Separate package using public modules only; each documented usage topic is one compiled module that asserts what it shows (#adr(14)).],
    [fixtures/negative with positive controls], [Opaque Port, EffectKey, and Execution; wrong error, choice, and Continue types.], [Actual compiler failures with expected diagnostics.],
    [Scheduling, lifecycle, rollback, retry, unknown-effect, and reporting tests], [Admission, timers, owner loss, reverse undo, evidence, and mailbox cleanup.], [Public behavioral and process probes.],
    [scripts/check_durable_restart.sh], [Concurrent effect and compensation receipts, and stable-key reconciliation.], [Actual VM kill and fresh definition in another VM.],
    [Reactor oracle], [Seven scoped scenarios; reverse undo and sibling settlement are deliberate differences.], [Reactor 1.0.6; not full Reactor API parity. Pin, mapping and limits: #lnk("../ORACLE.md")[oracle reference].],
    [Benchmark harness], [Build-once, store, and readiness cost, with correctness checks.], [Measured observation; no latency guarantee. Method and results: #lnk("../BENCHMARKS.md")[benchmark reference].],
  ))
  #md-table(2, (
    [*Profile*], [*Obligations and evidence*],
    [fast], [Formatting, Ruff correctness lint, ShellCheck, actionlint, gate regressions, and the root strict build and tests.],
    [ci], [All four packages; external order consumer tests and executable; strict benchmark build; disposable PostgreSQL 16; positive and negative compiler fixtures; actual VM restart probes.],
    [design], [Render freshness, vocabulary, links, and integrity of both native layers.],
    [full], [Every ci and design obligation.],
    [oracle], [Seven real Reactor 1.0.6 scenarios, captured upstream output, and normalized Saga comparison including the checked deliberate differences; full root tests.],
    [benchmark], [The chain, fan, and wide harness with correctness assertions and observational timings; no latency threshold.],
  ))
  #points(
    [Every package build treats Gleam warnings as errors and compiles authored Erlang in `src` and `test` with `erlc -Werror` and dependency includes. Generated dependency code and retained oracle or fixture bytes are excluded from authored-script formatting and lint.],
    [Gate commands check the tree without repairing it. A profile stops at its first failure and retains per-check logs, nonempty result entries, dependency revisions, and environment metadata under `.artifacts/PROFILE`.],
    [Hosted verification checks out immutable sibling revisions from `sibling-revisions.txt` without persisting credentials. Fork pull requests run the same required checks.],
    [A push, pull request, or manual run requires the ci and design profiles. The oracle runs on relevant source changes, weekly, and manually; the benchmark runs weekly and manually.],
    [The oracle proves only its seven scoped scenarios, not general Reactor or Temporal parity. The database harness disables fsync and synchronous commit, so passing runner and VM recovery tests establish no database power-loss durability. The decision is #adr(13).],
  )
])

#section(title: "End-to-end walkthrough", lead: "An order reserves inventory and charges payment while preserving uncertainty and cancellation authority.", visual: diagram(
  altitude: "L3", viewpoint: "dataflow", title: "One typed order workflow",
  nodes: (
    (id: "input", label: "Order", kind: "value-object", tint: "slate"),
    (id: "load", label: "Load order", kind: "component", tint: "violet"),
    (id: "inventory", label: "Reserve inventory", sub: "undo releases", kind: "component", tint: "violet"),
    (id: "payment", label: "Charge payment", sub: "stable key + refund undo", kind: "component", tint: "violet"),
    (id: "join", label: "Checkout", sub: "native output", kind: "value-object", tint: "slate"),
  ), edges: (
    (from: "input", to: "load", relation: "dataflow", label: "typed input"),
    (from: "load", to: "inventory", relation: "dataflow", label: "shared loaded order"),
    (from: "load", to: "payment", relation: "dataflow", label: "shared loaded order"),
    (from: "inventory", to: "join", relation: "dataflow", label: "Reservation"),
    (from: "payment", to: "join", relation: "dataflow", label: "Receipt"),
  ), caption: [The external order consumer supplies native Order, Reservation, Receipt, PayError, and UndoError. Load runs once while independent branches run within the selected capacity.],
), body: [
  #points(
    [Define the graph once at application startup. Pure configuration sets concurrency and distinct attempt/deadline/settle/cleanup bounds before one local run starts.],
    [A clean charge and reservation produce native Checkout. A known payment decline fails with the typed business error; the inventory undo succeeds or contributes its located native UndoError.],
    [A MaybeCharged error marked by unknown_when can request a keyed retry. Even if retry succeeds, the earlier unknown attempt remains visible in CompletedWithUnknownEffects. Without a settling decision, default Hold retains inventory for reconciliation.],
    [For persistence, attach versioned root/step codecs and effect/undo/compensation resolvers to the same graph. start_or_reconnect stores one id/input before drive; a repeated compatible request reuses that execution.],
    [If the runner dies after the provider receipt but before checkpoint outcome, the next compatible drive invokes the resolver with the original key. Completed restores the receipt; NotSent proves replay permission; MaybeSent suspends.],
    [Explicit durable cancellation records intent, reconciles admitted work, and undoes only known completions. Driver/job death alone preserves resumable state. A survivor reads the saved full report or receives local owned-report settlement.],
    [The application retains the typed Outcome for audit/recovery and chooses outcome.classify for public behavior. Safe summary keeps settlement facts without exposing payloads; reporting operational error stays separate from obtained workflow failure.],
  )
])

]
