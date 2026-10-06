#import ".render/designlib.typ": *

#let title = [Saga]
#let accent = "violet"
#let body = [
#section(title: "Foundation", lead: "Saga owns typed dependency execution and the evidence needed to settle effects.", body: [
  #goal(title: "Execute native typed workflows with complete evidence")[One definition supports shared dependencies, concurrency, retry, compensation, and undo while retaining caller-owned values and failure types.]
  #goal(title: "Recover saved execution without guessing effect absence")[Optional persistence retains progress and stable identities across runner or VM loss. Recovery uses explicit application evidence before repeating admitted work.]
  #no-goal(title: "A job-delivery or agent-policy runtime")[Delivery, queue capacity, agent policy, protocol transport, and application retry policy belong to their respective owners.]
  #no-goal(title: "Serialize closures or expose arbitrary native lookup")[Executable closures are reconstructed from deployed definitions. Native dependency values cannot be obtained through runtime result names or caller-visible type erasure.]
  #invariant(title: "A native dependency preserves its producer type", enforcement: "mechanism")[An opaque Port binds one produced value type to every consumer. Only construction of that Port can establish its node identity and typed read capability.]
  #invariant(title: "An uncertain action remains recorded after recovery", enforcement: "mechanism")[A later retry, replacement, abort, or hold does not erase an earlier unknown effect. Completion with such evidence has a distinct outcome.]
  #invariant(title: "Only known completions enter the undo journal", enforcement: "mechanism")[An interrupted attempt is never reported as undone. Successful undo entries run in reverse actual completion order and retain every failure.]
  #invariant(title: "Durable effect permission follows a committed state", enforcement: "mechanism")[Admission and checked input are saved before a worker receives permission for the external action. A refused commit stops further dispatch.]
  #invariant(title: "A current claim guards every checkpoint write", enforcement: "mechanism")[The adapter checks claim identity, cancellation observation, and revision atomically. A released or replaced claim cannot write progress.]
  #principle(title: "Separate evidence from authority")[An observed result, cancellation request, notification, or telemetry event cannot establish authority it does not carry. Compensation authority and absence of external effects require explicit contracts.]
  #principle(title: "Preserve local capability without persistence ceremony")[Native local execution requires no codecs or storage. Persistence attaches to the same definition and checks its additional obligations.]
  #principle(title: "Keep package boundaries at distinct lifecycles")[Saga owns effect progress and settlement. Storage owns persistent ownership, while applications compose delivery and agent lifetimes through small public ports.]
])

#pending-ledger(
  pending-entry(title: "Value-dependent composition and bounded traversal", kind: "build", adr: [#adr(8)])[
    Add typed value-dependent composition and homogeneous traversal with ordering, graph-size, cancellation, and nested-capacity contracts. Preserve arbitrary local typed fragments; their exact admission and durable eligibility need a ruling before an API is accepted.
  ],
  pending-entry(title: "Approval and historical fork authority", kind: "ruling", adr: [#adr(8)])[
    Retain durable approval waits, authenticated revision-checked deduplicated signals, and historical review forks. The owner must decide how a changed answer treats prior effects and which continuation owner suspends a Fabric agent.
  ],
  pending-entry(title: "Independent children and completed-run compensation", kind: "build", adr: [#adr(8)])[
    Implement typed admission, attachment, observation, command reconciliation, journal retention, and exclusive compensation authority. Scripted laboratory records establish intended distinctions but do not establish atomic admission, authorization, races, or recovery.
  ],
  pending-entry(title: "Runtime graph publication and execution", kind: "build", adr: [#adr(8)])[
    Implement finite-contract registered graphs, parametric node instantiation, exact connection compatibility, publication checks, and execution. Generic node families, native adapters, LLM nodes, visualization, and persistence remain required within this surface.
  ],
  pending-entry(title: "Extended graph validation and introspection", kind: "build", adr: [#adr(8)])[
    Add declared associative multiple-writer reducers, separate diagnostic labels and metadata, and graph visualization export. Preserve pure authoring without OTP startup; static construction already excludes cycles and missing producers.
  ],
  pending-entry(title: "Shared capacity and explicit lifecycle extensions", kind: "ruling", adr: [#adr(8)])[
    Decide shared or partitioned budgets across runs and children, coordinator supervision, explicit in-memory halt/resume, and undo retry policy. Current concurrency is per run and each undo is attempted once.
  ],
  pending-entry(title: "Checkpoint migration and delivery transaction", kind: "ruling", adr: [#adr(4)])[
    Define explicit definition/checkpoint migration and any optional Saga/Grind runner or outbox. An application currently owns wakeups, transaction boundaries, and redelivery; no generic durable runtime is inferred from similar claims or durations.
  ],
  pending-entry(title: "A total drive return deadline needs an owner decision", kind: "ruling", adr: [#adr(9)])[
    The drive execution budget is followed by synchronous runner drain and possible bounded claim release. A stronger total-return promise requires an explicit cleanup owner and takeover contract.
  ],
  pending-entry(title: "The common example must retain typed failures", kind: "ruling", adr: [#adr(9)])[
    The README and execution module example reduce all noncompletion to unknown-effects lists and admission failure to an empty list. Correcting the executable example and its consumer assertion requires a coordinated example change; the library already returns full typed reports.
  ],
)

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
    [The application supplies native input/output/error/undo-error records and callbacks. Saga retains them through #term("term-port") connections and #term("term-outcome") reports.],
    [A #term("term-local-run") belongs to its starting process; owner exit requests cancellation. A #term("term-durable-execution") belongs to its saved record; driver exit stops driving without recording cancellation.],
    [The package requires Sinal and standard Gleam/OTP/time/JSON libraries. PostgreSQL, Blueprint, Grind, Fabric, Relay, and provider clients compose through optional adapters or application code.],
  )
  #answers(title: "Saga", responsibility: [Execute typed dependencies and preserve effect evidence.], interface: [Workflow authoring; local execution; optional durable capability; storage, outcome, reporting, observation, and testing ports.], interactions: [Calls application effects and recovery callbacks; interprets storage conditional writes; emits observations through Sinal.], invariants: [One native type relation per Port; bounded per-run admission; uncertainty and settlement survive projection boundaries.], failure: [Separate definition/configuration rejection, workflow outcomes, durable operational failures, and lost report delivery.])
])

#section(title: "Workflow model", lead: "Definitions, execution identity, and observed action results are separate planes.", visual: diagram(
  altitude: "L2", viewpoint: "domain-model", title: "The model and its cardinalities",
  nodes: (
    (id: "workflow", label: "Workflow", sub: "immutable definition", kind: "aggregate", tint: "violet"),
    (id: "step", label: "Step occurrence", sub: "scoped address", kind: "entity", tint: "violet"),
    (id: "port", label: "Port", sub: "native dependency capability", kind: "value-object", tint: "violet"),
    (id: "execution", label: "Execution", sub: "one input and lifecycle", kind: "aggregate", tint: "violet"),
    (id: "action", label: "Action evidence", sub: "attempt / decision / undo", kind: "value-object", tint: "violet"),
    (id: "checkpoint", label: "Checkpoint", sub: "durable recovery authority", kind: "value-object", tint: "violet"),
  ), edges: (
    (from: "workflow", to: "step", relation: "dependency", label: "one → zero or more"),
    (from: "step", to: "port", relation: "dataflow", label: "producer → typed consumers"),
    (from: "workflow", to: "execution", relation: "dependency", label: "one → many executions"),
    (from: "execution", to: "action", relation: "dependency", label: "one → zero or more"),
    (from: "execution", to: "checkpoint", relation: "dependency", label: "durable only; one latest authority"),
  ), caption: [A step definition can be used in multiple occurrences. Runtime node numbers support scheduling but do not become stable persistence identities.],
), body: [
  #entity(title: "Workflow", description: [The validated reusable native dependency definition.], kind: "aggregate", owner: "Saga authoring", lifecycle: "immutable", domain: "workflow", tint: "violet")[
    #attribute(name: "Native contract", type: "Input × output × business-error × undo-error", provenance: "authored")[Caller types remain generic throughout construction and execution.]
    #attribute(name: "Definition name", type: "Diagnostic workflow name", provenance: "authored")[Names the definition; it does not identify one execution.]
    #attribute(name: "Step occurrences and dependencies", type: "Validated acyclic typed dependency structure", provenance: "derived")[Derived once when the builder returns its output Port.]
    #relates(cardinality: "1 : 0..n")[Defines step occurrences and supports separate executions.]
  ]
  #entity(title: "Step and Port", description: [The operation contract and its construction-owned native dependency capability.], kind: "value-object", owner: "Saga authoring", lifecycle: "immutable", domain: "workflow", tint: "violet")[
    #attribute(name: "Step policy", type: "Attempt × recovery × undo × timing × persistence capability", provenance: "authored")[A Step can be configured without running its callback. Persistence is optional.]
    #attribute(name: "Port membership", type: "Construction identity and typed fetch", provenance: "derived")[A Port can only be read at the type paired with its producer. Foreign membership is rejected.]
    #relates(cardinality: "1 : 0..n")[One produced Port may feed several consumers, sharing one scheduled producer.]
  ]
  #entity(title: "Effect key", description: [The opaque context of one attempt, compensation decision, or undo.], kind: "value-object", owner: "Saga execution", lifecycle: "immutable", domain: "workflow", tint: "violet")[
    #attribute(name: "Idempotency identity", type: "Execution × step address × action family", provenance: "derived")[Stable across attempts of the same step and across durable restart. An undo has its own identity.]
    #attribute(name: "Attempt identity", type: "Action identity × positive attempt number", provenance: "derived")[Distinguishes attempt evidence; its text is opaque and must never be parsed.]
    #attribute(name: "Correlation", type: "Sinal Correlation", provenance: "derived")[Chosen at local start or saved at the first durable drive.]
  ]
  #md-table(3, (
    [*Value or sum*], [*Meaning*], [*Refinement*],
    [StepAddress], [Scope, name, occurrence], [Occurrence is positive; scope records embed/choice structure.],
    [AttemptFailure], [Returned(error), Crashed(crash), TimedOut], [Closed; preserves native error independently from effect status.],
    [Undo], [NoUndo or application action], [NoUndo is absence, never a successful no-op.],
    [Timeout], [After(Duration) or Infinity], [Positive millisecond resolution where required; explicit Infinity lifts the bound.],
    [Recovery], [Retry, RetryAfter, Continue, Abort, AbortAfterCleanupFailure, Hold], [Closed; replacement includes its own undo capability.],
    [Outcome], [Completed, CompletedWithUnknownEffects, Failed, Cancelled, Unresolved], [Evidence-bearing alternatives have different payloads.],
  ))
])

#section(title: "Typed authoring", lead: "Construction establishes dependencies once; execution reuses the validated graph.", body: [
  #components(
    component(name: "Definition builder", tint: "violet", mission: "Bind typed dependencies and validate every constructed step.", answers: answers-data(
      responsibility: [Evaluate the builder once and collect every constructed occurrence.],
      interface: [`define` panics for source defects; `try_define` returns all DefinitionErrors; `name` and `describe` expose read-only metadata.],
      interactions: [Mints construction membership and Ports; collects reachable steps from the output; checks unused constructed steps.],
      invariants: [Empty names, invalid budgets/timeouts, foreign Ports, and orphan steps are rejected together. Construction invokes no step effects.],
      failure: [Builder raises become a located definition failure; `define` reports the workflow and offending steps. Runtime-derived settings use `try_define`.],
    )),
    component(name: "Typed composition", tint: "violet", mission: "Compose values while retaining one run and undo journal.", answers: answers-data(
      responsibility: [Connect `perform`, `both`, nonempty `all`, `map`, `embed`, and closed `choose`.],
      interface: [Opaque Port and Workflow values; caller-owned error adaptation through `map_step_errors` and `map_errors`.],
      interactions: [Each embed introduces a distinct scope; each choose builds both branches and schedules a decision. Shared dependencies remain shared.],
      invariants: [Closed branches have the same output/error types; only the selected branch executes. `map_errors` reuses the graph and never reevaluates the builder.],
      failure: [Compiler rejects mismatched outputs/errors; runtime definition validation rejects foreign/orphan construction. Output transformation crashes become OutputCrashed.],
    )),
  )
  #subsection(title: "Native transformations and diagnostics")[
    #points(
      [`map` transforms a Port's fetched value; it is not a scheduled memoized step. It may execute in each consumer task; expensive or effectful work uses a Step.],
      [`all` requires a first Port and accepts further Ports of the same native type. The caller handles an empty collection explicitly.],
      [StepDescriptor contains address, dependency addresses, undo capability, compensation capability, attempt limit, and optional step timeout. It exposes neither callbacks nor executable control over child internals.],
      [Step names are diagnostic and structural identity, not native value lookup. Reassociation or different scopes can change durable identity even when the final business value remains equivalent.],
    )
  ]
  #behavior(title: "A shared producer runs once", area: "Authoring", level: "boundary")[
    #given[Several consumers depend on the same Port in one execution.]
    #when[The workflow executes.]
    #then[The producer succeeds at most once under its retry policy, and every consumer reads that committed output.]
  ]
  #behavior(title: "Unselected branch effects do not execute", area: "Closed choice", level: "boundary")[
    #given[The workflow contains a closed choice.]
    #when[The choice decision selects one branch.]
    #then[Only that branch's effects execute; shared prerequisites remain shared.]
    #then[Durable recovery preserves the selection before further branch effects.]
  ]
  #points([Authoring and scope semantics are owned here; the representation decision is #adr(1). The build-once storage trade is #adr(2).])
])

#section(title: "Coordinator internals", lead: "Run state owns values, readiness, workers, and the completion journal together.", visual: diagram(
  altitude: "L4", viewpoint: "dataflow", title: "A coordinator advances one run",
  nodes: (
    (id: "graph", label: "Validated graph", kind: "value-object", tint: "violet"),
    (id: "ready", label: "Ready heap", sub: "builder order", kind: "component", tint: "violet"),
    (id: "admit", label: "Admission", sub: "available run slots", kind: "component", tint: "violet"),
    (id: "worker", label: "Attempt workers", sub: "bounded, correlated", kind: "component", tint: "violet"),
    (id: "values", label: "Run-scoped store", kind: "component", tint: "violet"),
    (id: "journal", label: "Undo journal", sub: "latest completion first", kind: "value-object", tint: "violet"),
  ), edges: (
    (from: "graph", to: "ready", relation: "dataflow", label: "dependency counts"),
    (from: "ready", to: "admit", relation: "dataflow", label: "next ready occurrence"),
    (from: "admit", to: "worker", relation: "call", label: "dispatch permitted work"),
    (from: "worker", to: "values", relation: "dataflow", label: "accepted native output"),
    (from: "worker", to: "journal", relation: "dataflow", label: "known completion + undo"),
    (from: "values", to: "ready", relation: "dataflow", label: "newly ready dependents"),
  ), caption: [A durable commit gates dispatch across these transitions. The coordinator receives uniform action events while typed node closures own native values.],
), body: [
  #answers(title: "Coordinator", responsibility: [Serialize transitions of one run and enforce readiness, capacity, timers, settlement, and rollback.], interface: [Uniform completion/control messages; read-only progress; one terminal delivery function.], interactions: [Interprets immutable node closures with a fresh store, monitors owner/workers, and optionally commits checkpoint values.], invariants: [Only ready work enters a slot; attempts and compensation decisions share max_concurrency; retries reenter admission instead of bypassing capacity.], failure: [A terminal trigger stops admission and initiates settlement. Durable storage/refusal or driver loss stops dispatch under the last checkpoint authority.])
  #subsection(title: "Value storage and soundness")[
    #answers(title: "Run-scoped store", responsibility: [Retain completed native outputs and typed action records for one run.], interface: [Internal opaque Store; node-bound put/fetch operations.], interactions: [The Port's typed fetch and producer are created together; the coordinator threads a fresh store through run state.], invariants: [No other run shares the store. Only the producer-bound native type reads a node's value; type erasure is confined to this module.], failure: [Reading a value before dependency commit is an internal defect and panics. Opaque Port construction and compiler-negative fixtures exclude foreign typed reads.])
    #points([The implementation uses an opaque native carrier and an identity cast, never caller-visible Dynamic. Readiness and one binding of node identity to native type make that cast sound; #adr(2) records the choice.])
  ]
  #subsection(title: "Readiness and worker lifetime")[
    #answers(title: "Readiness index", responsibility: [Admit ready nodes without rescanning the whole graph.], interface: [Dependency counts, dependents, ordered ready min-heap, active count, completed count.], interactions: [A committed success lowers dependent counts; retry readiness returns to the heap.], invariants: [No dependent fetch precedes committed output. Builder order breaks readiness ties, while actual completion order defines undo order.], failure: [Waiting nodes become skipped after admission closes; stale timer/worker messages cannot advance another action sequence.])
    #answers(title: "Action workers", responsibility: [Isolate attempt, compensation, and undo execution from coordinator transitions.], interface: [Correlated action sequence, result, monitor, timeout, and durable permission channel.], interactions: [Attempts prepare typed input; persistence checks that input before effect permission; workers report returned result or typed crash evidence.], invariants: [A stale completion cannot settle a newer action. A worker killed before permission cannot start its effect; a killed admitted action remains uncertain.], failure: [Raises and exits become Crash; time limits kill workers and retain unknown-effect evidence. Detached durable runners stop their workers without inventing cancellation.])
  ]
])

#section(title: "Execution lifecycle", lead: "Admission, sibling settlement, and reverse undo are distinct phases.", body: [
  #state-type(id: "execution-phase", title: "Execution phase", variants: (
    (id: "running", description: [Admits ready work.]),
    (id: "settling", description: [Waits for active siblings without admitting new work.]),
    (id: "rolling-back", description: [Runs known undo entries sequentially.]),
    (id: "finished", description: [Retains one terminal outcome; coordinator exits.]),
  ))
  #entity(id: "execution", title: "Execution", description: [One input interpretation of a Workflow and its effect evidence.], kind: "aggregate", owner: "Saga coordinator", lifecycle: "stateful", domain: "workflow", tint: "violet")[
    #attribute(name: "Run identity", type: "Local fresh run id or durable execution id", provenance: "derived")[Local id is fresh per start; the durable id is caller-chosen within one store.]
    #attribute(id: "phase", name: "Phase", type: "Execution phase", provenance: "derived", state-type: "execution-phase", state-machine: "execution-lifecycle")[Derived from accepted transitions; public progress exposes the three active phases only.]
    #attribute(name: "Primary trigger", type: "Failure, cancellation, or held evidence", provenance: "derived")[The original stopping cause remains separate from subsequently observed sibling and cleanup failures.]
    #attribute(name: "Action evidence", type: "Known results, journal, and unknown actions", provenance: "observed")[Accepted callback results record observations; recovery decisions supply explicit transition authority.]
    #relates(cardinality: "1 : 1")[Interprets one Workflow definition.]
    #relates(cardinality: "1 : 0..n")[Owns attempts, compensation decisions, and journal entries.]
  ]
  #state-machine(id: "execution-lifecycle", subject: "execution", state-field: "phase", state-type: "execution-phase", title: "The execution phases", initial: "running", accepting: ("finished",), states: ("running", "settling", "rolling-back", "finished"), transitions: (
    ("running", "finished", "output reached"),
    ("running", "settling", "failure / cancel / deadline / hold"),
    ("settling", "finished", "held uncertainty; preserve journal"),
    ("settling", "rolling-back", "siblings drained; rollback permitted"),
    ("rolling-back", "finished", "journal exhausted; retain all failures"),
  ), caption: [Cancellation after settlement has begun is idempotent and does not replace the stopping trigger. A completed last step can still be undone when cancellation wins before terminal completion.])
  #md-table(4, (
    [*From*], [*Event and guard*], [*Next state*], [*Evidence or refusal*],
    [Running], [Known output reached before a stopping trigger], [Finished], [Completed, or CompletedWithUnknownEffects.],
    [Running], [Failure, explicit cancel, local owner exit, deadline, or Hold], [Settling], [Skip waiting steps; preserve primary trigger.],
    [Settling], [Active work returns before settle bound], [Settling], [Accept known completions and retain later failures.],
    [Settling], [Settle bound expires], [RollingBack or Finished], [Kill remaining work and record interruption; Hold forbids rollback.],
    [RollingBack], [An undo returns, fails, crashes, or times out], [RollingBack], [Continue remaining entries; retain every located result.],
    [Settling / RollingBack / Finished], [Repeated cancellation], [Unchanged], [No new admission and no replacement of the original trigger.],
  ))
])

#section(title: "Step action lifecycle", lead: "A retry creates another attempt while preserving prior action evidence.", body: [
  #state-type(id: "step-progress", title: "Step progress", variants: ("waiting", "attempting", "compensating", "retry-scheduled", "succeeded", "failed-step", "interrupted", "undoing", "undone", "undo-failed-step", "skipped"))
  #entity(id: "step-occurrence", title: "Step occurrence", description: [A structurally addressed operation within one Execution.], kind: "entity", owner: "Saga coordinator", lifecycle: "stateful", domain: "workflow", tint: "violet")[
    #attribute(name: "Address", type: "Step address", provenance: "derived")[Resolved from construction scope, name, and occurrence.]
    #attribute(id: "state", name: "State", type: "Step progress", provenance: "derived", state-type: "step-progress", state-machine: "step-lifecycle")[Attempt numbers and correlated action sequences are associated data.]
    #attribute(name: "Attempts remaining", type: "Finite positive budget including the first attempt", provenance: "derived")[A compensation decision still runs after the last allowed attempt because cleanup may be needed.]
  ]
  #state-machine(id: "step-lifecycle", subject: "step-occurrence", state-field: "state", state-type: "step-progress", title: "Step progress and rollback", flow: "top-to-bottom", initial: "waiting", accepting: ("succeeded", "failed-step", "interrupted", "undone", "undo-failed-step", "skipped"), states: ("waiting", "attempting", "compensating", "retry-scheduled", "succeeded", "failed-step", "interrupted", "undoing", "undone", "undo-failed-step", "skipped"), transitions: (
    ("waiting", "attempting", "ready + slot"),
    ("waiting", "skipped", "closed / excluded"),
    ("attempting", "succeeded", "known success"),
    ("attempting", "compensating", "failure + decider"),
    ("attempting", "failed-step", "no decider"),
    ("attempting", "interrupted", "settle expires"),
    ("compensating", "retry-scheduled", "Retry permitted"),
    ("retry-scheduled", "attempting", "due + slot"),
    ("retry-scheduled", "skipped", "closed"),
    ("compensating", "succeeded", "Continue"),
    ("compensating", "failed-step", "stop / Hold"),
    ("compensating", "interrupted", "settle expires"),
    ("succeeded", "undoing", "rollback + undo"),
    ("undoing", "undone", "undo Ok"),
    ("undoing", "undo-failed-step", "undo fails"),
  ), caption: [Succeeded remains terminal when no rollback is needed or no undo exists. FailedStep includes the stopped node of a held run; it does not imply that the effect is known.])
  #points([Public Progress reports phase and every StepProgress without native values or closures. It is a read-only observation, never a control or recovery token.])
])

#section(title: "Compensation and uncertainty", lead: "Failed-attempt decisions and completed-step undo grant different permissions.", body: [
  #answers(title: "Recovery policy", responsibility: [Choose how a failed attempt affects its own progress and prior completed work.], interface: [FailedAttempt(input, failure, attempt, attempts_left, key) → Recovery(output, error, undo_error).], interactions: [Receives returned native errors and native crash/timeout evidence; may perform cleanup under its own bound.], invariants: [Retry does not erase uncertainty; Continue supplies the replacement output's undo; Hold grants neither retry nor rollback authority.], failure: [A raising or timed-out decider retains unknown compensation evidence; AbortAfterCleanupFailure retains its cleanup error separately.])
  #md-table(3, (
    [*Decision*], [*Permitted transition*], [*Required retained evidence*],
    [Retry / RetryAfter], [Another attempt only within budget and open admission; delay capped], [Earlier unknown action stays recorded; retry-superseded differs from exhausted budget.],
    [Continue(output, undo)], [Accept replacement output and its undo capability], [Replacement completion is known; original uncertain attempt remains unknown.],
    [Abort(error)], [Fail and permit reverse undo of known completions], [Native application cause plus earlier unknown effects.],
    [AbortAfterCleanupFailure(error, undo_error)], [Same rollback permission], [Separate compensation cleanup failure.],
    [Hold(evidence)], [End Unresolved after siblings settle; do not undo], [Native held evidence and every completed step left in place.],
  ))
  #points(
    [`unknown_when` marks returned business errors whose external effect may have happened. Without a deciding compensation, default `on_unknown(Reconcile)` preserves completed reservations; explicit `RollBack` permits their undo.],
    [Exhausted retry of a marked uncertain error follows the same Hold/RollBack policy. An explicit Abort permits rollback even after a marked error.],
    [Crashes and timeouts without a decider fail and roll back known completions while retaining the crashed/timed-out action as unknown. A decider's Abort may produce StepFailed after a crash; the cause alone cannot establish known effects.],
    [Applications own downstream idempotency and reconciliation evidence. Passing an idempotency key alone never proves provider deduplication or exactly-once effects.],
  )
  #behavior(title: "An uncertain retry cannot become clean completion", area: "Unknown effects", level: "boundary")[
    #given[An attempt ended with unknown effect evidence.]
    #when[A later attempt or replacement reaches the workflow output.]
    #then[The outcome retains the output and the earlier unknown effect in CompletedWithUnknownEffects.]
  ]
  #behavior(title: "Held uncertainty preserves prior completions", area: "Compensation", level: "boundary")[
    #given[A step returns uncertain evidence without rollback permission.]
    #when[The run settles.]
    #then[The outcome is Unresolved and completed steps remain held.]
    #then[No earlier completion is claimed as reversed.]
  ]
  #points([The uncertainty and rollback decisions are recorded in #adr(3).])
])

#section(title: "Outcome and settlement", lead: "One full typed report is the authority for application and operational evidence.", body: [
  #md-table(3, (
    [*Outcome*], [*Payload*], [*Meaning*],
    [Completed], [Native output], [Output reached with no unknown actions.],
    [CompletedWithUnknownEffects], [Native output + nonempty unknown effects], [Output reached; earlier effects still require reconciliation.],
    [Failed], [Cause(error) + Settlement(error, undo_error)], [Failure stops admission and settlement records rollback evidence.],
    [Cancelled], [CancelRequested or OwnerExited + Settlement], [Cancellation intent stopped admission; it does not assert complete reversal.],
    [Unresolved], [Step address + native evidence + Settlement], [Held uncertainty stopped progress without rollback authority.],
  ))
  #entity(title: "Settlement", description: [The immutable evidence accompanying a stopped Outcome.], kind: "value-object", owner: "Saga execution", lifecycle: "immutable", domain: "workflow", tint: "violet")[
    #attribute(name: "Reversed and retained completions", type: "Undone × not-undoable × held step addresses", provenance: "observed")[Known undo success is distinct from a completed effect lacking undo or held without permission.]
    #attribute(name: "Failed cleanup", type: "All located undo and compensation failures", provenance: "observed")[Native undo errors, crashes, and timeouts remain distinct; one failure never stops the remaining journal.]
    #attribute(name: "Concurrent stopping evidence", type: "Interrupted steps × sibling causes", provenance: "observed")[The primary trigger remains separate from failures arriving during settlement.]
    #attribute(name: "Unknown effects", type: "Step × action × ending", provenance: "observed")[Attempt and compensation number remain visible; identical addresses do not erase distinct actions.]
  ]
  #answers(title: "Outcome projection", responsibility: [Classify full reports conservatively and provide payload-safe summaries.], interface: [`outcome.classify(report, explain)`, `kind`, `held_steps`, `failure_kind`, `describe_failure`, and `summary`.], interactions: [A single private evidence projection supplies classification and rendering; the caller retains the original Outcome.], invariants: [A definite stopped result requires every effect known and none left in place. Summary excludes output, business/undo error payloads, and crash reasons.], failure: [Completion with uncertainty, Hold, retained effects, failed undo, or unknown actions classify unresolved. Neither definite classification nor reporting NotStarted chooses retry policy.])
  #md-table(3, (
    [*Projection*], [*Permitted result*], [*Disclosure*],
    [Clean Completed], [Native output], [No payload-safe summary promise applies to the returned output itself.],
    [Fully reversed typed failure or cancellation], [Definitely(message)], [Caller explain renders selected business error.],
    [CompletedWithUnknownEffects or retained/unknown effects], [Unknown(evidence)], [Complete safe settlement summary; Unresolved adds explicit held-error explanation.],
    [summary], [Outcome/cause kinds and located evidence], [Never invokes explain; distinct actions/attempts and all settlement categories remain.],
  ))
  #points([A safe summary keeps addresses visible. Typed cause accessors and caller-provided renderers may expose private application data; the application owns disclosure. #adr(7) records why reporting retains full reports before projection.])
])

#section(title: "Time and capacity", lead: "A duration bounds its named operation; it is not an end-to-end return promise.", body: [
  #md-table(3, (
    [*Bound*], [*Default*], [*Ownership and limit*],
    [Concurrent attempts + compensation decisions], [Online scheduler count], [Per run; at least one; retries need slots.],
    [Run deadline], [Infinity], [After(Duration) is opt-in; triggers settlement and rollback.],
    [Attempt timeout], [60 seconds], [Step-specific timeout overrides default in either direction; Infinity opts out for undeclared steps.],
    [Sibling settlement], [5 seconds], [Can be zero; no new admission; active siblings may finish.],
    [Each compensation / undo], [5 seconds], [Positive; undo runs sequentially and is not retried.],
    [RetryAfter cap], [5 minutes], [Nonnegative; requested larger delay clamps and is observed.],
    [Storage operation], [5 seconds], [with_call_timeout; minimum one millisecond; slow call suspends/stops runner.],
    [Checkpoint bytes], [16 MiB], [Whole saved snapshot; oversize returns CheckpointTooLarge.],
    [Receiver readiness], [5 seconds], [Proven prelaunch failure returns typed reporting error.],
    [Reporting rollback / notification], [Caller Duration], [1 through 2^32−1 milliseconds; each callback has this bound.],
    [Drive execution], [Required caller Duration], [Positive; after expiry drain/release work precedes return.],
  ))
  #formula(id: "action-budget", title: "Conservative sequential action budget", notation: (
    ([$A_s$], [allowed attempts for step s]), ([$T_s$], [effective finite attempt timeout]),
    ([$C$], [cleanup timeout]), ([$R$], [retry-delay cap]), ([$S$], [settle timeout]),
    ([$J$], [known journal entries requiring undo]),
  ), caption: [The sum overcounts parallel work and the final retry delay. It bounds the configured action waits, not arbitrary synchronous observer or extension latency.])[
    $ B = sum_s A_s T_s + sum_s A_s C + sum_s A_s R + S + J C $
  ]
  #points(
    [A finite deadline substitutes its execution portion for the summed action portion, then settlement and cleanup still occur. Infinity, caller callbacks without an applicable timer, or synchronous Sinal handlers can defeat a literal wall-clock bound.],
    [Local progress/await bounds limit the caller's observation wait; a timeout does not cancel the run. The caller explicitly cancels and consumes the terminal report before dropping an ordinary Execution handle.],
    [The current local start path contains an internal readiness wait and a separate public control-handshake wait, each five seconds. The module's single five-second wording is narrower than these sequential waits; no total-start bound is asserted here.],
    [A drive begins its execution-budget clock after spawning and monitoring the runner. Its drain receives wait up to five seconds each and reset after a claim/result message; the normal finite protocol has at most one claim and one result. An abnormal exit with a known claim then performs one separately bounded release call.],
    [Thus drive may return after its requested Duration. Drain scheduling and release timing are explicit margin obligations when an application nests drive under a job worker deadline; a stronger total deadline remains a pending ruling.],
    [Persistent deadlines/backoff use wall-clock timestamps across restart; current run timers use monotonic timing. The deployment owns wall-clock suitability and cannot infer cross-node agreement from local Duration types.],
  )
  #behavior(title: "Cancellation settles known concurrent work", area: "Local lifetime", level: "boundary")[
    #given[A local run has active siblings.]
    #when[Explicit cancellation or owner exit stops admission.]
    #then[Active siblings may finish within the configured settle window.]
    #then[Remaining actions are interrupted and unknown; only known completed effects enter rollback.]
  ]
])

#section(title: "Local ownership and reporting", lead: "The starting process owns execution, while outcome delivery may belong to a survivor.", body: [
  #answers(title: "Local execution port", responsibility: [Start one owner-bound coordinator and expose its outcome, cancellation, and progress.], interface: [`run`, `start`, `await`, `start_reporting`, `cancel`, `progress`, `pid`, and `run_id`.], interactions: [Monitors starting process; ordinary handle delivers to that owner, reporting handle delivers once to its Subject.], invariants: [Only the ordinary starter may await. Reporting handles cannot await; cancel/progress work from any process.], failure: [InvalidConfig starts no work; ExecutionLost cannot prove no effect started. Await distinguishes timeout, NotOwner, consumed outcome, and typed Lost evidence.])
  #points(
    [A successful ordinary await consumes the outcome and drains its monitor. A second await may first time out while the coordinator is exiting; AlreadyAwaited is guaranteed only after exit.],
    [A dropped handle after a timed-out await can leave its terminal outcome or monitor message in the owner mailbox. Cancellation requires a subsequent await to consume completion.],
    [`start_reporting` allocates no outcome monitor/mail in the starting process. Its Subject receives at most one terminal report after settlement; coordinator death without a report remains distinguishable through monitoring.],
  )
  #subsection(title: "One invocation receiver")[
    #answers(title: "Owned reporting", responsibility: [Preserve a full typed report across abnormal exit of one invocation owner.], interface: [`reporting.run_owned(workflow, input, config, on_stopped, rollback_within)` returns Result(Outcome, reporting.Error); callback receives the same result.], interactions: [Starts an independent receiver and waits for readiness before execution; receiver monitors invocation and coordinator; a guarded worker invokes on_stopped.], invariants: [Receiver lives until invocation exit even after synchronous return. Normal owner exit produces no second notification; abnormal exit can deliver settlement.], failure: [Startup exit/timeout, admission, receiver loss, and coordinator loss retain typed operational causes; callback crash/timeout leaves notification unconfirmed.])
    #points(
      [Use this boundary within a per-invocation worker. Repeated use from a long-lived actor retains one receiver per invocation until that actor exits.],
      [rollback_within bounds owner-loss waiting when no coordinator was reported and bounds the notification worker. Once a known coordinator exists, its configured settlement/cleanup govern completion; rollback_within is not a global execution deadline.],
      [Readiness failure closes the startup reply channel, removes monitors, and preserves unrelated mail before any workflow launch. Normal result return and admission failure remove the caller's receiver monitor.],
      [NotStarted is proven only for rejected rollback/configuration or prelaunch receiver failure. Lost execution handshake, receiver loss, and coordinator loss are Unknown; typed run_error/exit_reason accessors retain causes and safe describe_error excludes crash payloads.],
    )
  ]
])

#section(title: "Durable execution model", lead: "A saved execution outlives a drive; a fresh compatible definition supplies executable behavior.", body: [
  #state-type(id: "saved-status", title: "Saved status", variants: ("pending", "suspended", "finished"))
  #entity(id: "saved-execution", title: "Durable execution", description: [The storage-addressed aggregate that retains input, progress, compatibility, and outcome.], kind: "aggregate", owner: "Saga durable protocol", lifecycle: "stateful", domain: "workflow", tint: "violet")[
    #attribute(name: "Execution identifier", type: "Caller-chosen id within Storage", provenance: "authored")[A stable application key normally includes workflow identity. It is independent from local runner and correlation identity.]
    #attribute(name: "Compatibility and input", type: "Computed stamp × checked encoded native input", provenance: "derived")[Reconnect requires compatible definition and exactly the same encoded input.]
    #attribute(id: "status", name: "Status", type: "Saved status", provenance: "derived", state-type: "saved-status", state-machine: "saved-lifecycle")[One latest committed authority; a suspended reason retains typed failure details.]
    #attribute(name: "Cancellation intent", type: "Monotonic requested flag", provenance: "authored")[Independent from status and claim; a terminal commit must observe its current value.]
    #attribute(name: "Checkpoint revision", type: "Increasing Revision", provenance: "derived")[Changes only on accepted checkpoint commits, not cancellation alone.]
    #attribute(name: "Persisted correlation", type: "Absent before first drive, then one Sinal Correlation", provenance: "derived")[The first drive chooses and commits it; every later handle uses the saved value.]
    #relates(cardinality: "1 : 0..1")[Has one current Claim; lack of a runner does not make it cancelled.]
  ]
  #state-machine(id: "saved-lifecycle", subject: "saved-execution", state-field: "status", state-type: "saved-status", title: "The saved execution lifecycle", initial: "pending", accepting: ("finished",), states: ("pending", "suspended", "finished"), transitions: (
    ("pending", "pending", "commit progress / driver stops"),
    ("pending", "suspended", "save operational or reconciliation reason"),
    ("suspended", "pending", "compatible drive resolves blocking evidence"),
    ("pending", "finished", "save terminal outcome"),
    ("suspended", "suspended", "evidence remains unresolved"),
  ), caption: [Cancellation is an independent intent axis. Driver timeout, death, or lost ownership leaves the latest saved state authoritative and does not introduce cancellation.])
  #answers(title: "Persistence capability", responsibility: [Check an existing Workflow's eligibility and restore-compatible identity.], interface: [`durable.new` with labeled root codecs; with_version, with_config, with_max_checkpoint_bytes; step recoverable/restore_undo/resolvers.], interactions: [Uses step versions, graph wiring, and codec identities to compute the stamp; attaches no second graph.], invariants: [Every persistent step is recoverable; every compensating step declares pure undo reconstruction including explicit NoUndo. Compatibility precedes application decoding.], failure: [Missing/empty persistent capabilities are source defects and new panics with all problems. Changed stamps return IncompatibleDefinition before decoder callbacks.])
  #answers(title: "Durable handle and driver", responsibility: [Create/reconnect one saved execution and advance it under a current claim.], interface: [`start_or_reconnect`, `reconnect`, `read`, `cancel`, `drive`, `unfinished`; growing Error with stable error_kind.], interactions: [One Storage serves all ids; drive claims, restores, runs, saves outcome, stops helpers, and releases.], invariants: [Create/reconnect runs no effects; same id/definition/encoded input is idempotent. Finished executions return saved Outcome without another effect.], failure: [Busy, Transient, NeedsReconciliation, Incompatible, and Defect remain separate classifications; SuspensionNotSaved retains distinct original and recording failures.])
  #points([One optional persistence model and the delivery boundary are recorded in #adr(4).])
])

#section(title: "Checkpoint and effect permission", lead: "Admission, checked input, and effect outcome are separate durable boundaries.", visual: sequence(
  title: "Commit before effect permission", participants: (
    (id: "runner", label: "Runner", shape: "control"),
    (id: "store", label: "Storage", shape: "database"),
    (id: "worker", label: "Worker", shape: "participant"),
    (id: "provider", label: "Effect owner", shape: "boundary"),
  ), steps: (
    seq-msg("runner", "store", "commit admitted action"),
    seq-msg("store", "runner", "admission saved", dashed: true),
    seq-msg("runner", "worker", "prepare typed input", activate: true),
    seq-msg("worker", "runner", "checked encoded input", dashed: true),
    seq-msg("runner", "store", "commit input before dispatch"),
    seq-msg("store", "runner", "checked input saved", dashed: true),
    seq-msg("runner", "worker", "effect permission"),
    seq-msg("worker", "provider", "effect with stable key"),
    seq-msg("provider", "worker", "observed result", dashed: true),
    seq-msg("worker", "runner", "native result and undo", dashed: true, deactivate: true),
    seq-msg("runner", "store", "commit outcome and new readiness"),
  ), caption: [Loss after permission but before the accepted outcome creates an interrupted admitted action. The stable key and resolver must establish whether its effect happened.],
), body: [
  #answers(title: "Checkpoint interpreter", responsibility: [Save complete execution values and reconstruct compatible run state.], interface: [Opaque storage bytes; versioned envelope, typed checkpoint problems, and root/step codec boundaries.], interactions: [Serializes step progress, inputs/outputs, selected decisions, retries/deadline, failure/rollback intent, journal order, settlement, and uncertainty.], invariants: [Never saves executable closures, PIDs, monitors, sockets, timers, worker leases, or credentials. Every commit rewrites a complete bounded snapshot.], failure: [Malformed/foreign/graph-mismatched state, insufficient restored concurrency, missing compensation input, or nonrestorable undo fails closed.])
  #points(
    [The compatibility stamp includes workflow name/version, ordered occurrences, addresses, dependencies/choice wiring, step versions, codec versions, attempt/time budgets, and declared recovery capabilities. Runtime node ids do not enter it.],
    [Caller changes to callback meaning require version changes even with unchanged native types. A stamp checks declared identity, not semantic equivalence of arbitrary closures.],
    [Codec encoding checks that produced text decodes; it does not prove semantic equality or determinism. The codec author owns those laws and any external resources captured by callbacks.],
    [Undo reconstruction must be pure and may run during validation and restoration. A Continue that carries undo cannot commit if declared reconstruction returns NoUndo.],
    [Codec attachment, undo/resolver configuration, and error mapping preserve recovery capabilities across modifier order. A Failed resolver added after error mapping needs a compensation decider in the mapped vocabulary or fails DeciderMissingAfterMapping.],
    [Checkpoint format 2 retains correlation. Format 1 opens with correlation.from_key(id); the next commit uses format 2. Earlier binaries cannot read the newer envelope; no automatic arbitrary-definition migration is implied.],
  )
])

#section(title: "Recovery rules", lead: "Only explicit evidence authorizes replay of an interrupted admitted action.", body: [
  #answers(title: "Recovery resolvers", responsibility: [Establish results or proven absence after interruption without repeating uncertain callbacks.], interface: [Attempt and undo Evidence: Completed, Failed, NotSent, MaybeSent; compensation resolver: Some(Recovery) or None.], interactions: [Receive retained native input/output and the original EffectKey; applications query downstream receipts or enforce idempotency.], invariants: [Callbacks must be repeat-safe. Known saved success is reused; NotSent alone authorizes replay; MaybeSent suspends.], failure: [Missing or unresolved reconciliation saves RecoveryRequired with address, action, and key. Original compensation callback is never blindly replayed.])
  #md-table(3, (
    [*Saved boundary*], [*Recovery action*], [*Constraint*],
    [Success committed], [Decode and reuse], [No invocation of the completed step.],
    [Admitted before checked input], [Prepare again], [No effect permission was issued.],
    [Checked input admitted without result], [Attempt resolver], [Completed/Failed restore result; NotSent permits original-key replay; MaybeSent suspends.],
    [Undo admitted without result], [Undo resolver], [Missing/MaybeSent suspends; NotSent permits saved undo; native undo failures retained.],
    [Compensation admitted without decision], [Compensation resolver], [Some applies original-budget decision; None suspends; never repeats original decider.],
    [Marked uncertain returned error], [Retain unknown evidence and saved decision], [Uncommitted attempt uses effect resolver; interrupted decision uses compensation resolver.],
    [Cancellation already recorded], [Reconcile admitted actions before rollback], [NotSent does not start a new effect during cancellation.],
  ))
  #behavior(title: "A saved success is reused after runner loss", area: "Recovery", level: "boundary")[
    #given[A step's successful output was committed before the runner stopped.]
    #when[A compatible drive resumes the execution.]
    #then[The saved output is reused and the successful step does not execute again.]
  ]
  #behavior(title: "Unknown admitted work cannot silently replay", area: "Recovery", level: "boundary")[
    #given[An admitted action has no saved result.]
    #when[A drive resumes it.]
    #then[Explicit resolver evidence determines its result or permits replay by proven absence.]
    #then[Unresolved evidence suspends without repeating the external action.]
  ]
  #points(
    [Restored max_concurrency must accommodate saved in-flight attempts and compensation decisions. A smaller setting returns ConcurrencyBelowInFlight rather than pretending those admitted actions vanished.],
    [A storage/codec/size/refusal failure stops workers and dispatch. If suspension recording also fails distinctly, both causes survive; otherwise the last accepted checkpoint remains authority.],
    [Cancellation becomes visible at the next checkpoint and may therefore include a current action's timeout. A cancellation flag changed before admission or terminal commit wins the conditional-write race.],
  )
])

#section(title: "Storage and claim lifetime", lead: "The adapter supplies atomic revocable ownership; Saga supplies workflow transitions.", body: [
  #state-type(id: "claim-status", title: "Claim status", variants: ("unowned", "owned"))
  #entity(id: "execution-ownership", title: "Execution ownership", description: [The revocable ownership state for one saved execution.], kind: "entity", owner: "Storage adapter", lifecycle: "stateful", domain: "workflow", tint: "violet")[
    #attribute(id: "status", name: "Status", type: "Claim status", provenance: "derived", state-type: "claim-status", state-machine: "claim-lifecycle")[Vacant ownership permits a new claim; adapter-supported owner-loss detection can revoke a current claim.]
    #attribute(name: "Claim identity", type: "Execution id × generation × token", provenance: "derived")[New claims increase generation and obtain a distinct adapter proof. A token alone does not authenticate an external business caller.]
    #relates(cardinality: "1 : 1")[Guards checkpoint changes of one saved execution.]
  ]
  #state-machine(id: "claim-lifecycle", subject: "execution-ownership", state-field: "status", state-type: "claim-status", title: "Current ownership can be revoked", initial: "unowned", accepting: (), states: ("unowned", "owned"), transitions: (
    ("unowned", "owned", "claim; increment generation"),
    ("owned", "owned", "valid renewal / current commit"),
    ("owned", "unowned", "release / detected owner loss"),
  ), caption: [Lease expiry makes takeover possible under an adapter's policy. It cannot retract an external call already sent by an earlier runner; fencing guards saved writes.])
  #answers(title: "Storage port", responsibility: [Persist atomic execution bytes, claims, cancellation intent, and discovery.], interface: [Storage.new supplies create/load/claim/commit/release/cancel/unfinished; optional renewal and operation Duration.], interactions: [Returns opaque Stored and Claim values; Saga commits expected revision and observed cancellation with phase/data.], invariants: [Ownership is the current claim value, not the committing process. Each refused operation leaves checkpoint bytes/revision unchanged.], failure: [StaleOwner precedes CancellationChanged, which precedes Conflict; Busy excludes a competing live claim; unavailable, corrupt, timed-out, and missing records stay distinct.])
  #md-table(3, (
    [*Operation*], [*Accepted effect*], [*Refusal or idempotence*],
    [create], [Revision/generation zero; uncancelled Pending bytes], [AlreadyExists; never overwrites another execution.],
    [load], [Read latest bytes/revision/generation/cancellation], [NotFound or Corrupt distinct from availability.],
    [claim], [Advance generation; return Claim and Stored], [Busy while live claim excludes competition.],
    [commit], [Current claim + seen cancellation + revision → new bytes/phase and revision+1], [Ordered StaleOwner → CancellationChanged → Conflict.],
    [release], [Remove current ownership], [StaleOwner for another claim.],
    [cancel], [Set cancellation flag without changing bytes or revision], [Idempotent; durable cancel is a no-op for saved Finished.],
    [unfinished], [Up to limit unowned Pending/Suspended ids], [Finished excluded; oldest first where adapter can tell.],
  ))
  #points(
    [A heartbeat linked to the durable runner renews lease-based storage independently of checkpoint traffic. Retry backoff or a long attempt cannot depend on commit-only renewal.],
    [StaleOwner on renewal stops the runner. Unavailable/TimedOut renewal retries at the next interval; all later writes remain fenced if the lease expires meanwhile. A wrapper rebuilding Storage must preserve renewal.],
    [Drive uses a watchdog for synchronous runner storage operations; other durable operations use bounded helper calls. Timeout stops waiting and kills the helper/runner but does not itself prove whether a remote store applied an operation.],
    [Storage claim constructors expose adapter proofs but cannot establish validity alone. The adapter enforces current generation/token and its declared deployment scope at every mutation.],
    [The protocol decision is #adr(5). Adapter-specific persistence and distribution guarantees remain separate from the core contract.],
  )
])

#section(title: "Storage adapters and delivery", lead: "Reference adapters differ in failure scope; delivery remains an application lifetime.", body: [
  #components(
    component(name: "Memory storage", tint: "violet", mission: "Keep durable records within one supervised or linked VM actor.", answers: answers-data(
      responsibility: [Serialize the Storage protocol in one actor and monitor claiming process loss.],
      interface: [start, supervised(name), named(name), stop, and storage(handle).],
      interactions: [One actor serves all ids; named handles locate a restarted actor.],
      invariants: [Current claim may be used by any process; claiming-process monitor controls owner loss. Restart discards all saved executions.],
      failure: [Five-second operation wait returns TimedOut; stopped/missing actor returns Unavailable. Runner loss can be survived; VM/store loss cannot.],
    )),
    component(name: "Reference file storage", tint: "violet", mission: "Recover checkpoint bytes in a fresh VM with one directory owner.", answers: answers-data(
      responsibility: [Apply atomic per-execution mutations using canonical-path locking and synced temporary-file replacement.],
      interface: [Pure file.open(canonical_absolute_directory) returns Storage; caller creates and owns directory.],
      interactions: [Erlang FFI reads/writes records; a claim records this VM's claiming process. Earlier-VM claims are unowned at recovery.],
      invariants: [One VM at a time uses a canonical directory; lock wait is bounded. Atomic replacement preserves complete snapshots.],
      failure: [Malformed/read/write/lock failures remain typed. Concurrent VMs, path aliases, and power-loss directory durability are outside its guarantee.],
    )),
  )
  #answers(title: "PostgreSQL persistence adapter", responsibility: [Implement Storage across VMs with database-backed fenced claims.], interface: [The separate saga_postgres package's public configuration/storage/migration port; caller-owned pool.], interactions: [Saga owns heartbeat/drive; the adapter owns database clock, lease, schema, transactions, and generation/token checks.], invariants: [No second pool is implicitly allocated by Saga; core Saga gains no database dependency.], failure: [Lease loss permits recovery under the adapter's contract; external-effect uncertainty still belongs to Saga/application reconciliation.])
  #points([Detailed PostgreSQL design belongs to `integrations/saga_postgres/docs/design/design.typ`. The core owns only the persistence port and its lifetime obligations.])
  #answers(title: "Delivery integration", responsibility: [Wake stable executions and map delivery interruption into explicit Saga operations.], interface: [Application-owned stable id, reconnect/read/drive/cancel, unfinished scanning, and error_kind mapping.], interactions: [A Grind job supplies capacity and wakeup; Saga's saved outcome supplies workflow completion authority. Fabric supplies parent policy and continuation.], invariants: [Driver stop is resumable interruption; cancel is recorded intent. No automatic full-local-workflow retry follows redelivery.], failure: [Busy/Transient can be retried deliberately; NeedsReconciliation needs operator/application evidence; Incompatible/Defect require correction. Atomic job admission needs an application transaction/outbox.])
  #points(
    [The checkout application selects Grind cancellation while driving Saga and explicitly calls durable.cancel when cancellation is intended. Stopping drive alone must leave the execution recoverable.],
    [Delivery deadlines reserve time for Saga stop, drain, release, settlement, and any persistence owner-loss window. Retrying a completed failed workflow is an explicit application policy.],
    [An application using a shared pool starts migrations and stops infrastructure under its own lifecycle. Store reuse does not merge job, agent, and workflow claim authority.],
  )
])

#section(title: "Observation and verification ports", lead: "Observations describe transitions; executable consumers test guarantees at their actual boundary.", body: [
  #answers(title: "Telemetry", responsibility: [Emit package-owned lifecycle facts after corresponding transitions.], interface: [Six typed event descriptors: run_started, run_stopped, step_started, step_stopped, compensation_stopped, undo_stopped.], interactions: [Sinal transports typed measurements/metadata; step clients read the same correlation from EffectKey.], invariants: [Every event carries correlation; durable metadata includes execution id; no telemetry value drives workflow state.], failure: [Handler raises are isolated by Sinal. Synchronous slow handlers delay coordinator processing and therefore timing; no guaranteed durable observation delivery is implied.])
  #points(
    [The local correlation defaults to unique(); the first durable drive saves an explicit handle correlation or from_key(id). Later handles cannot change it; #adr(6) records the identity contract.],
    [Saga emits no separate claim/release/lease/checkpoint event family. Applications obtain those facts from durable/storage results rather than interpreting a missing event as absence of an effect.],
  )
  #answers(title: "Testing and conformance", responsibility: [Expose observation and extension checks without private imports.], interface: [`testing.wait_until` polls public Progress with an explicit timeout; storage.conformance.run accepts fresh fixtures and separate owner-loss allowance.], interactions: [Consumer tests supply native records/errors and public storage/resolver fakes; fresh VM probes exercise file recovery.], invariants: [Conformance checks atomic create, claims-as-values, live/lost ownership, stale writes, cancellation race, listing, release, and killed-runner recovery.], failure: [Located suite failures report the violated contract. A one-VM pass proves neither distributed fencing nor media durability; adapters need separate deployment tests.])
  #md-table(3, (
    [*Evidence*], [*Guarantee checked*], [*Boundary*],
    [examples/order_consumer], [Common/advanced native use, mapped errors, failures/undo, durable recovery, Blueprint bridge, full reporting], [Separate package using public modules only.],
    [fixtures/negative + positive controls], [Opaque Port/EffectKey/Execution and wrong error/choice/Continue types], [Actual compiler failures with expected diagnostics.],
    [test scheduling/lifecycle/rollback/retry/unknown/reporting], [Admission, timers, owner loss, reverse undo, evidence and mailbox cleanup], [Public behavioral and process probes.],
    [scripts/check_durable_restart.sh], [Concurrent effect/compensation receipts and stable-key reconciliation], [Actual VM kill and fresh definition in another VM.],
    [oracle + PROVENANCE], [Seven scoped Reactor scenarios; reverse undo and sibling settlement are deliberate differences], [Reactor 1.0.6; not full Reactor API parity.],
    [bench], [Build-once/store/readiness cost and correctness checks], [Measured evidence; no universal latency guarantee.],
  ))
  #points([Operational commands remain in README, AGENTS, DURABILITY, and oracle provenance. The design gate checks notation, links, and PDF freshness; it does not replace runtime, fault, compiler, or adapter evidence.])
])

#section(title: "Retained composition contracts", lead: "Expanded capabilities retain explicit ownership instead of changing the static facade's meaning.", body: [
  #subsection(title: "Value-dependent traversal and local continuation")[
    #answers(title: "Typed expansion", responsibility: [Compose known native types after a value arrives and traverse a homogeneous runtime collection.], interface: [A future value-dependent composition/traversal surface over the canonical Workflow; no accepted replacement signatures.], interactions: [Expands typed local fragments; durable fragments need stable factory identity/version and saved branch selection.], invariants: [Ordering, graph-size, cancellation, and shared/partitioned capacity must be explicit. Arbitrary heterogeneous steps and named native-result lookup remain excluded.], failure: [Unsupported durable local closures are refused at eligibility; runtime collection/resource bounds must fail explicitly before uncontrolled expansion.])
    #points([Explicit local halt/resume retains typed state and control authority. Copyable execution values must not permit repeated external effects merely because a completion or wake token matches.],
      [A future paused or blocked recovery retains native input, original cause, continuation, and completed journal. Retry permission, cancellation/rollback permission, and reconciliation authority remain separate; relative wake tokens reject stale or duplicate delivery. The delivered Hold is a terminal report, not this resumable recovery surface.])
  ]
  #subsection(title: "Approvals and historical review")[
    #answers(title: "Approval continuation", responsibility: [Suspend at an authenticated answer boundary without treating waiting as business failure.], interface: [Retained input, wait identity/revision, authenticated answer, command identity, deadline, and explicit continuation owner.], interactions: [Commits waiting state and releases worker capacity; Fabric owns agent approval policy while Saga owns its workflow boundary.], invariants: [Stale/duplicate signals cannot resume another revision. Forking from history cannot silently reuse compensation authority over prior effects.], failure: [Expired, rejected, cancelled, stale, incompatible, corrupt, and ambiguous-effect outcomes stay distinct. Prior-effect policy for forks remains a ruling.])
  ]
  #subsection(title: "Independent children and compensation authority")[
    #answers(title: "Child boundary", responsibility: [Attach an independently owned execution without flattening its journal into its parent.], interface: [Typed child contract with actual input/output/error/undo-error codecs; prepared command; checked admission receipt; observation and cancellation intent.], interactions: [Parent persists route, exact request, stable command, and attachment; after restart it reattaches to compatible child owner state.], invariants: [Compatibility and authority precede decoding. Waiting releases capacity needed by child; parent and child recovery remain distinct.], failure: [Unknown/invalid admission preserves exact request for reconciliation; later failed retry does not resolve earlier uncertainty. Missing owner/run, pruned output, incompatibility, and corrupt result differ.])
    #answers(title: "Compensation authority", responsibility: [Authorize one logical compensation operation for a retained successful execution.], interface: [Owner-backed exclusive claim, durable receipt reference, typed rebound undo-error codec, stable request command, and separate compensation observation.], interactions: [Owner checks success, journal retention, claimant identity, and current claim; parent retains attachment and receipt references.], invariants: [Success alone grants no authority. Copyable handles never enforce single use; storage admits at most one logical operation per claim and replays its original admission.], failure: [Retention expiry/pruning, competing claim, unauthorized caller, unavailable owner, incompatible contract, uncertain admission, and undo failure remain distinct.])
    #md-table(3, (
      [*Independent state axis*], [*States*], [*Rule*],
      [Historical child result], [Pending / completed / failed / unresolved; output may be pruned], [Compensation does not rewrite historical success.],
      [Compensation operation], [Pending / running / succeeded / failed / unresolved], [One logical admission per authority claim.],
      [Result and journal retention], [Available / expired / pruned], [A retained journal may outlive output; replayed admitted request retains identity after fresh-admission expiry.],
    ))
    #points([The laboratory establishes these distinctions through scripted immutable owner records. Concurrent storage enforcement, authentication, actual compensation, restart, and local post-success authority remain the pending implementation boundary.])
  ]
])

#section(title: "Runtime-authored graphs", lead: "Finite data contracts type runtime wiring without pretending to create new native Gleam types.", body: [
  #answers(title: "Runtime graph compiler", responsibility: [Turn an editable graph into a checked immutable executable definition.], interface: [Versioned node registry, parametric port expressions, configuration contracts, draft graph, publication errors, and PublishedGraph.], interactions: [Blueprint owns finite runtime contracts/validated values; Saga owns registration, instantiation, wiring, graph validation, and execution.], invariants: [Only PublishedGraph executes. Runtime data remains opaque validated values; static adapters retain actual native codecs.], failure: [Unsupported schemas, unresolved versions/types, invalid configuration, illegal connections, cycles, missing inputs, and resource limits are located publication failures.])
  #state-type(id: "runtime-graph-status", title: "Runtime graph status", variants: ("draft", "published", "retired"))
  #entity(id: "runtime-graph-definition", title: "Runtime graph definition", description: [The retained runtime-authoring model with editable and immutable states.], kind: "aggregate", owner: "Saga runtime graph compiler", lifecycle: "stateful", domain: "workflow", tint: "violet")[
    #attribute(id: "status", name: "Status", type: "Runtime graph status", provenance: "derived", state-type: "runtime-graph-status", state-machine: "runtime-graph-lifecycle")[Publication derives executable capability; retirement and existing-run retention require an explicit policy before implementation.]
    #attribute(name: "Node contract references", type: "Registry kind × version × finite port contracts", provenance: "authored")[Deployed implementations remain separate from serializable graph data.]
    #attribute(name: "Resolved schema bindings", type: "Concrete finite runtime contracts", provenance: "derived")[Publication resolves every variable and validates values/connections before execution.]
  ]
  #state-machine(id: "runtime-graph-lifecycle", subject: "runtime-graph-definition", state-field: "status", state-type: "runtime-graph-status", title: "Runtime graph publication", initial: "draft", accepting: ("retired",), states: ("draft", "published", "retired"), transitions: (
    ("draft", "draft", "edit / rejected publication"),
    ("draft", "published", "all publication checks pass"),
    ("published", "retired", "retire version"),
  ), caption: [A changed executable graph is a new version derived from a draft. Treatment of active executions and registry retention on retirement remains explicit design work.])
  #subsection(title: "Parametric node model")[
    #md-table(2, (
      [*Family*], [*Type relation*],
      [Identity / Delay], [T → T],
      [Choose], [Bool × T × T → T],
      [Collect / Constant / Validate], [T… → Array(T); () → T; wire value → validated T],
      [Map], [Array(A) × Graph(A, B) → Array(B)],
      [LLM], [Input → Output under declared finite contracts],
    ))
    #points(
      [A node definition retains kind/version, input/output port definitions, configuration schema, and deployed implementation. Type expressions include concrete contract, variable, array, optional, and finite object.],
      [Instantiation binds variables through incoming connections, including array/optional/object structure. The first compatibility rule is exact structural equality with explicit transformation nodes; width subtyping and broader assignability require separate acceptance.],
      [Every runtime port has an opaque identity and validated contract; every node output is validated against its output contract. Required field access derives the field contract and returns a located failure.],
      [A native adapter validates and decodes input before its native callback, then checks encoding and validates output. A custom codec with unavailable schema cannot silently claim compatibility.],
      [The LLM node composes llm_wire provider execution and Blueprint output validation. Fabric can supply bounded agent policy; it does not become Saga's provider transport.],
    )
  ]
  #answers(title: "Runtime node execution", responsibility: [Execute a published node through its checked concrete port contracts.], interface: [Opaque port identifiers and maps of Blueprint validated values; concrete input/output contracts and located node errors. Native adapters decode typed input, invoke their original handler, encode output, and validate the encoded result.], interactions: [Saga schedules the registered implementation; Blueprint checks contract values; application services own actual remote effects and reconciliation.], invariants: [A successful node output conforms to its declared contract. Registry identity and publication remain fixed for one execution; a dictionary never permits unchecked native-value lookup.], failure: [Input decoding, native handler failure, output encoding, invalid node output, timeout, and uncertain remote effects remain distinct. Publication does not prove external provider deduplication.])
  #subsection(title: "Publication and hostile input")[
    #md-table(2, (
      [*Publication stage*], [*Required acceptance*],
      [Configuration and registry], [Decode every configuration; resolve definition/version; validate constants and defaults.],
      [Type derivation], [Instantiate parameters; unify connected ports; derive output schemas.],
      [Graph structure], [Detect cycles; reject missing producers and undeclared duplicate writers; require associative declared reducers.],
      [Boundary completeness], [Check required inputs; validate graph input/output contracts; reject unreachable outputs.],
      [Executable publication], [Produce one immutable PublishedGraph with all type variables resolved.],
    ))
    #points(
      [Finite contracts forbid cyclic references for this surface; arrays and nested objects still remain finite schema trees. Schema depth/size, graph nodes/edges, traversal, and execution resource budgets need explicit bounds before hostile-input acceptance.],
      [Serialization saves graph data and versioned implementation identity, never closure code. Restore checks registry versions and schema/value compatibility before execution.],
      [Visual editors, diagnostic metadata, exported graphs, hostile publication tests, and persistence/recovery scenarios belong to this retained surface; static Port compiler tests do not verify it.],
    )
  ]
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
