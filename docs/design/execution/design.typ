#import "../.render/designlib.typ": *

#let title = [Execution]
#let accent = "violet"
#let body = [
#section(title: "Foundation", lead: "Execution owns typed dependency scheduling, effect evidence, and settlement.", body: [
  #goal(title: "Execute native typed workflows with complete evidence")[One definition supports shared dependencies, concurrency, retry, compensation, and undo while retaining caller-owned values and failure types.]
  #invariant(title: "A native dependency preserves its producer type", enforcement: "mechanism")[An opaque Port binds one produced value type to every consumer. Only construction of that Port can establish its node identity and typed read capability.]
  #invariant(title: "An uncertain action remains recorded after recovery", enforcement: "mechanism")[A later retry, replacement, abort, or hold does not erase an earlier unknown effect. Completion with such evidence has a distinct outcome.]
  #invariant(title: "Only known completions enter the undo journal", enforcement: "mechanism")[An interrupted attempt is never reported as undone. Successful undo entries run in reverse actual completion order and retain every failure.]
  #principle(title: "Preserve local capability without persistence ceremony")[Native local execution requires no codecs or storage. Persistence attaches to the same definition through the #ctx("durability") context and checks its additional obligations.]
])


#pending-ledger(
  pending-entry(title: "Value-dependent composition and bounded traversal", kind: "build", since: "2026-10-06", adr: [#adr(8)])[
    Typed value-dependent composition and homogeneous traversal need ordering, graph-size, cancellation, and nested-capacity contracts. Arbitrary local typed fragments stay available; their exact admission and durable eligibility need a ruling before an API is accepted.
  ],
  pending-entry(title: "Approval and historical fork authority", kind: "ruling", since: "2026-10-06", adr: [#adr(8)])[
    Durable approval waits, authenticated revision-checked deduplicated signals, and historical review forks need two owner decisions. One decision is how a changed answer treats prior effects; the other is which continuation owner suspends a Fabric agent.
  ],
  pending-entry(title: "Independent children and completed-run compensation", kind: "build", since: "2026-10-06", adr: [#adr(8)])[
    Typed admission, attachment, observation, command reconciliation, journal retention, and exclusive compensation authority are designed and unbuilt. Scripted laboratory records establish the intended distinctions but not atomic admission, authorization, races, or recovery.
  ],
  pending-entry(title: "Runtime graph publication and execution", kind: "build", since: "2026-10-06", adr: [#adr(8)])[
    Finite-contract registered graphs, parametric node instantiation, exact connection compatibility, publication checks, and execution are designed and unbuilt. Generic node families, native adapters, LLM nodes, visualization, and persistence remain required within this surface.
  ],
  pending-entry(title: "Extended graph validation and introspection", kind: "build", since: "2026-10-06", adr: [#adr(8)])[
    Declared associative multiple-writer reducers, separate diagnostic labels and metadata, and graph visualization export are unbuilt. Pure authoring keeps working without OTP startup; static construction already excludes cycles and missing producers.
  ],
  pending-entry(title: "Shared capacity and explicit lifecycle extensions", kind: "ruling", since: "2026-10-06", adr: [#adr(8)])[
    Shared or partitioned budgets across runs and children, coordinator supervision, explicit in-memory halt/resume, and undo retry policy await a decision. Concurrency is per run and each undo is attempted once.
  ],
  pending-entry(title: "The common example must retain typed failures", kind: "ruling", since: "2026-10-06", adr: [#adr(9)])[
    The execution module example reduces all noncompletion to unknown-effects lists and admission failure to an empty list. Correcting the executable example and its consumer assertion requires a coordinated example change; the library already returns full typed reports.
  ],
)

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
  #subsection(title: "Callback records, keys, and correlation")[
    #points(
      [A step callback receives one record. `step` receives the input only; `effect` receives the input and an opaque #term("term-effect-key"); `undo` receives UndoRequest(input, output, key); `compensate` receives FailedAttempt(input, failure, attempt, attempts_left, key). Callers read records by label and the key through its accessors (idempotency_key, attempt_number, attempt_key, correlation_of).],
      [The #term("term-correlation") of a run is mandatory and reaches every callback through its key. A local run takes a fresh unique correlation unless configured; a durable execution takes the saved one, or the key-derived default. A step's own clients attach it to join the run's observations.],
      [A local key belongs to one run: its #term("term-idempotency-key") derives from the run id, which each local run creates anew. A durable execution derives its keys from its execution id, so they survive restart.],
      [`define` reports every definition defect together and panics naming the workflow and each offending step. `try_define` returns every DefinitionError for definitions built from runtime data, and `describe_definition_error` renders one. `map_errors` and `map_step_errors` adapt error vocabulary; no saga-owned error type is ever required.],
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
  #points(
    [A safe summary keeps addresses visible. Typed cause accessors and caller-provided renderers may expose private application data; the application owns disclosure. #adr(7) records why reporting retains full reports before projection.],
    [Each action of a run (a step attempt, a compensation decision, an undo) ends with a known result or an unknown effect. `unknown_effects(outcome)` lists every unknown effect as (step, action, ending) and is empty exactly when every effect is known. An action is recorded when it ends, so a later decision cannot hide it: a crashed or maybe-sent attempt retried to success yields CompletedWithUnknownEffects.],
    [A retry decided after the run began settling for another reason is reported as retry-superseded, never as an exhausted budget.],
    [Settling ends as soon as nothing is in flight. A settle window of zero rolls back at once and reports in-flight steps as interrupted. The window is set per run, never per cancellation.],
  )
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
    [Coordinator startup handshake], [5 seconds], [Not configurable; separate from the public control handshake.],
    [Observation wait (await, progress, testing wait)], [Required caller Duration], [A timeout limits the caller's wait and never cancels the run.],
    [Synchronous run], [Until an outcome or coordinator loss], [A caller needing a bound uses start and await.],
    [Receiver readiness], [5 seconds], [Proven prelaunch failure returns typed reporting error.],
    [Reporting rollback / notification], [Caller Duration], [1 through 2^32−1 milliseconds; each callback has this bound.],
    [Sinal handlers], [Synchronous in the coordinator], [A slow handler delays coordinator processing; a forwarder route removes that coupling.],
  ))
  #points([Every timeout, deadline, interval, and lease is a Duration; no public function takes milliseconds as an integer. A bound that may be lifted takes After(Duration) or an explicit Infinity, so unbounded is never implicit. The storage, checkpoint, and drive bounds belong to the #ctx("durability") context.])
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
    [The local start path contains an internal readiness wait and a separate public control-handshake wait, each five seconds. Their sum is not a total-start bound.],
    [Persistent deadlines and backoff use wall-clock timestamps across restart; run timers use monotonic timing. The deployment owns wall-clock suitability and cannot infer cross-node agreement from local Duration types.],
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
    [`start_reporting` allocates no outcome monitor/mail in the starting process. Its Subject receives at most one terminal report after settlement; coordinator death without a report remains distinguishable through monitoring the coordinator pid, whose Down message always arrives after its outcome.],
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

#section(title: "Observation and verification", lead: "Observations describe transitions; oracle and benchmark evidence state exactly what they compare.", body: [
  #answers(title: "Telemetry", responsibility: [Emit package-owned lifecycle facts after corresponding transitions.], interface: [Six typed event descriptors: run_started, run_stopped, step_started, step_stopped, compensation_stopped, undo_stopped.], interactions: [Sinal transports typed measurements and metadata; step clients read the same correlation from EffectKey. Metadata carries the workflow name, this VM's run id, the durable execution id when one exists, and the correlation.], invariants: [Every event carries a correlation; no telemetry value drives workflow state. A compensation event reports the retry delay a RetryAfter decision scheduled and whether the cap shortened it.], failure: [Handler raises are isolated by Sinal. Synchronous slow handlers delay coordinator processing and therefore timing; no guaranteed durable observation delivery is implied.])
  #points(
    [The local correlation defaults to a unique value; the first durable drive saves an explicit handle correlation or the execution-id-derived default. Later handles cannot change it; #adr(6) records the identity contract.],
    [Saga emits no separate claim, release, lease, or checkpoint event family. Applications obtain those facts from durable and storage results, and never interpret a missing event as absence of an effect.],
    [A route for the saga event prefix to a Sinal forwarder moves handler work out of the coordinator.],
  )
  #answers(title: "Run synchronization for tests", responsibility: [Let a test wait for an observable run state without a guessed sleep.], interface: [`testing.wait_until(execution, matching, within)` polls public Progress until the predicate accepts a snapshot or the overall bound elapses.], interactions: [Polling uses a short internal interval and the monotonic clock for its deadline. A consumer test supplies its own blocking gate for step bodies; the order consumer's test support holds the maintained gate.], invariants: [Progress is a read-only observation and never a control or recovery token.], failure: [WaitTimedOut means the predicate never matched in time. RunEnded means the coordinator was already gone.])
  #points(
    [A poll sees only the snapshot current at that instant, so a quickly passing state can be missed. A predicate accepts its target state or any state beyond it: waiting for a step in compensation also accepts a scheduled retry.],
    [Saga ships no gate for a step body to block on, because that is generic process coordination. A gate registers a reply subject owned by the entering process, because a step body runs in a fresh task process on every attempt, and it queues a release that arrives before any waiter.],
  )
  #subsection(title: "Reactor oracle")[
    #points(
      [Reactor 1.0.6 is the pinned behavioral oracle for scheduling and rollback. The pin is a hex-locked dependency in the oracle project plus the upstream commit recorded in #adr(10). Saga's native contracts, not Reactor's API, define the product.],
      [A scenario runs against live Reactor and against Saga, normalizes both outputs to one language-neutral text format, and compares them mechanically. A claimed match must produce an empty difference. A claimed deliberate difference must byte-match a checked-in expected difference. A change in either difference fails the check instead of silently reclassifying behavior.],
      [Captured raw Reactor output is retained separately as evidence of the Reactor side and is verified against a live run; it is not the input to the comparison.],
      [An expected difference is recomputed after a reviewed behavior change, never edited by hand.],
    )
    #points(
      [Seven scenarios are compared: sequential chain (D1), undo after a failing fourth step (D2), a slow, a fast-failing and a quick sibling (D3), two failures then success (D4), compensation that continues with a replacement (D5), ten independent steps under concurrency three (D6) and a diamond with one producer (D7). Two differences are deliberate: Saga undoes in reverse completion order (D2) and settles siblings, undoing a late completion once it is known (D3). The rest match.],
      [Halt and resume, abandoned steps, whole-run undo, dynamic steps, the DSL transformer suite, and graph export are not compared; the composition extension contracts own them. The decision is #adr(10).],
      [The pin, the mapping of each adapted upstream test, every compared fact and the limits of the claim are retained in #lnk("../../ORACLE.md")[the oracle reference].],
    )
  ]
  #subsection(title: "Run-cost benchmark")[
    #points(
      [The harness is a separate package that measures definition and execution through public modules. It measures a chain, a shared producer with fan-out and fan-in, and a sliding window of four dependency reads, at sizes from 10 to 2000 steps.],
      [Each cell measures definition once, one separate warm-up, then 20 runs below 1000 steps or 15 runs at larger sizes. Monotonic timings are microseconds with nearest-rank median and p95.],
      [Every timed run must complete with the shape's expected output, and an atomic counter checks builder invocations across definition and runs. A cell whose warm-up exceeds 60 seconds is skipped, and larger sizes of that shape stop.],
      [Trivial integer steps isolate graph and scheduling cost. The harness establishes no remote-effect latency, rollback cost, storage throughput, memory limit, or production tail latency, and a doubling ratio supports a cost hypothesis rather than an asymptotic bound. The decision is #adr(11).],
      [The run command, retained results, capture caveats and limits are retained in #lnk("../../BENCHMARKS.md")[the benchmark reference]; results are observational and no timing threshold fails a gate.],
    )
  ]
  #subsection(title: "Usage examples")[
    #points(
      [Every documented usage topic exists as compiled, runnable code in the order consumer, one module per topic, and each module asserts what it shows. The full gate runs the consumer tests and the consumer program, so an API change that breaks an example fails the build.],
      [The README keeps a short usage section that mirrors the examples. No prose usage guide exists; #adr(14) records the decision.],
      [Operating guidance for durable runs is expressed as examples: a stable id that reconnects to the saved run, error kinds mapped to application actions, discovery of unfinished runs, and recovery of a committed local effect through an attempt resolver. The durability contracts they exercise are owned by the durability context.],
      [The topic map is in #lnk("../../../examples/order_consumer/README.md")[the order consumer README].],
    )
  ]
])

#section(title: "Composition extension contracts", lead: "Extension capabilities carry explicit ownership instead of changing the static facade's meaning.", body: [
  #subsection(title: "Value-dependent traversal and local continuation")[
    #answers(title: "Typed expansion", responsibility: [Compose known native types after a value arrives and traverse a homogeneous runtime collection.], interface: [A value-dependent composition and traversal surface over the canonical Workflow; no signatures are accepted.], interactions: [Expands typed local fragments; durable fragments need stable factory identity/version and saved branch selection.], invariants: [Ordering, graph-size, cancellation, and shared/partitioned capacity must be explicit. Arbitrary heterogeneous steps and named native-result lookup remain excluded.], failure: [Unsupported durable local closures are refused at eligibility; runtime collection/resource bounds must fail explicitly before uncontrolled expansion.])
    #points([Explicit local halt/resume retains typed state and control authority. Copyable execution values must not permit repeated external effects merely because a completion or wake token matches.],
      [A paused or blocked recovery retains native input, original cause, continuation, and completed journal. Retry permission, cancellation/rollback permission, and reconciliation authority remain separate; relative wake tokens reject stale or duplicate delivery. Hold is a terminal report, never a resumable recovery surface.])
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
    #points([These distinctions are contracts. Concurrent storage enforcement, authentication, actual compensation, restart, and local post-success authority are the build boundary recorded in the pending ledger.])
  ]
])

#section(title: "Runtime-authored graphs", lead: "Finite data contracts type runtime wiring without pretending to create new native Gleam types.", body: [
  #answers(title: "Runtime graph compiler", responsibility: [Turn an editable graph into a checked immutable executable definition.], interface: [Versioned node registry, parametric port expressions, configuration contracts, draft graph, publication errors, and PublishedGraph.], interactions: [Blueprint owns finite runtime contracts/validated values; Saga owns registration, instantiation, wiring, graph validation, and execution.], invariants: [Only PublishedGraph executes. Runtime data remains opaque validated values; static adapters retain actual native codecs.], failure: [Unsupported schemas, unresolved versions/types, invalid configuration, illegal connections, cycles, missing inputs, and resource limits are located publication failures.])
  #state-type(id: "runtime-graph-status", title: "Runtime graph status", variants: ("draft", "published", "retired"))
  #entity(id: "runtime-graph-definition", title: "Runtime graph definition", description: [The runtime-authoring model with editable and immutable states.], kind: "aggregate", owner: "Saga runtime graph compiler", lifecycle: "stateful", domain: "workflow", tint: "violet")[
    #attribute(id: "status", name: "Status", type: "Runtime graph status", provenance: "derived", state-type: "runtime-graph-status", state-machine: "runtime-graph-lifecycle")[Publication derives executable capability; retirement and existing-run retention require an explicit policy before implementation.]
    #attribute(name: "Node contract references", type: "Registry kind × version × finite port contracts", provenance: "authored")[Deployed implementations remain separate from serializable graph data.]
    #attribute(name: "Resolved schema bindings", type: "Concrete finite runtime contracts", provenance: "derived")[Publication resolves every variable and validates values/connections before execution.]
  ]
  #state-machine(id: "runtime-graph-lifecycle", subject: "runtime-graph-definition", state-field: "status", state-type: "runtime-graph-status", title: "Runtime graph publication", initial: "draft", accepting: ("retired",), states: ("draft", "published", "retired"), transitions: (
    ("draft", "draft", "edit / rejected publication"),
    ("draft", "published", "all publication checks pass"),
    ("published", "retired", "retire version"),
  ), caption: [A changed executable graph is a new version derived from a draft. Retirement states how active executions and registry retention are treated.])
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
      [Visual editors, diagnostic metadata, exported graphs, hostile publication tests, and persistence/recovery scenarios belong to this surface; static Port compiler tests do not verify it.],
    )
  ]
])

]
