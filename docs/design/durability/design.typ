#import "../.render/designlib.typ": *

#let title = [Durability]
#let accent = "teal"
#let body = [
#section(title: "Foundation", lead: "Durability owns saved execution progress, claim-fenced storage, and recovery under explicit evidence.", body: [
  #goal(title: "Recover saved execution without guessing effect absence")[Optional persistence retains progress and stable identities across runner or VM loss. Recovery uses explicit application evidence before repeating admitted work.]
  #invariant(title: "Durable effect permission follows a committed state", enforcement: "mechanism")[Admission and checked input are saved before a worker receives permission for the external action. A refused commit stops further dispatch.]
  #invariant(title: "A current claim guards every checkpoint write", enforcement: "mechanism")[The adapter checks claim identity, cancellation observation, and revision atomically. A released or replaced claim cannot write progress.]
  #principle(title: "Resolve interrupted effects by evidence, never by replay")[Only explicit application evidence permits repeating an admitted action whose result is unsaved. Losing a driver, a runner, or a delivery never establishes that an effect did not happen.]
])


#pending-ledger(
  pending-entry(title: "Checkpoint migration and delivery transaction", kind: "ruling", since: "2026-10-06", adr: [#adr(4)])[
    Explicit definition and checkpoint migration, and any optional Saga/Grind runner or outbox, await a decision. An application owns wakeups, transaction boundaries, and redelivery; no generic durable runtime is inferred from similar claims or durations.
  ],
  pending-entry(title: "A total drive return deadline needs an owner decision", kind: "ruling", since: "2026-10-06", adr: [#adr(9)])[
    The drive execution budget is followed by synchronous runner drain and possible bounded claim release. A stronger total-return promise requires an explicit cleanup owner and takeover contract.
  ],
  pending-entry(title: "Durability timing margins and recovery rules are unchecked", kind: "verify", since: "2026-10-17")[
    The timing margins, the stable-identity rules and the committed-local-effect recovery rules came from the retired durability guide. The compiled durable-run examples exercise stable identity, reconnect, unfinished-run discovery and committed-local-effect recovery. The timing margins need a comparison with source.
  ],
)

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

#section(title: "Durable time and capacity", lead: "Each durable bound limits one operation; none promises a total return time.", body: [
  #md-table(3, (
    [*Bound*], [*Default*], [*Ownership and limit*],
    [Storage operation], [5 seconds, then TimedOut], [`with_call_timeout`; minimum one millisecond; a slow call suspends and stops the runner.],
    [Memory adapter call], [5 seconds], [Not configurable.],
    [File adapter mutation lock], [5 seconds, then Busy], [Not configurable.],
    [Checkpoint bytes], [16 MiB (16 777 216 bytes)], [Whole saved snapshot; `with_max_checkpoint_bytes` changes it; an oversize snapshot returns CheckpointTooLarge.],
    [Drive execution], [Required caller Duration], [Positive; after expiry, runner drain and claim release precede return.],
    [Drive caller exit], [Runner stops], [The checkpoint is kept and no cancellation is recorded.],
    [PostgreSQL claim lease], [30 seconds, renewed every 10 seconds], [Owned by the adapter's own layer.],
    [Conformance owner-loss window], [Declared by the adapter], [Passed to the #term("term-conformance-suite") as the owner-loss allowance.],
  ))
  #points(
    [A drive starts its execution-budget clock after spawning and monitoring the runner. Its drain receives wait up to five seconds each and reset after a claim or result message; the finite protocol has at most one claim and one result. An abnormal exit with a known claim then performs one separately bounded release call.],
    [A drive may therefore return after its requested Duration. Drain scheduling and release timing are margin obligations when an application nests a drive under a job worker deadline. The decision is #adr(9); a stronger total deadline is a pending ruling.],
    [The storage call timeout, lease, renewal interval, effect timeout, and delivery retry policy must be compatible. A lost VM can leave Busy until the adapter detects owner loss, so a retry window must outlast the adapter's owner-loss policy.],
  )
])

#section(title: "Operating a durable execution", lead: "The application owns identity, wakeups, and evidence; Saga owns the saved protocol.", body: [
  #subsection(title: "Admission and drive")[
    #points(
      [The application chooses one stable execution id before job admission or external delivery. Reuse of an id requires the same compatible definition and the same encoded input; another input returns InputMismatch.],
      [`durable.new` checks that every step is recoverable and panics naming each offending step and codec, because that is a source defect. The workflow version defaults to "1"; `with_version` changes it whenever the workflow's behavior changes, so a saved execution of the old behavior returns IncompatibleDefinition.],
      [A drive claims the execution, restores it, runs, saves the outcome, stops its helpers, and releases the claim. Timeout, caller exit, and runner loss each stop the runner: in-flight attempts are killed, the claim is released at once, and the next compatible drive resumes from the last checkpoint by asking each interrupted attempt's resolver. Only the loss of the runner's VM leaves the claim to the adapter's expiry. Only `durable.cancel` cancels.],
      [Correlation is set before the first drive, which saves it. The caller inspects the full typed Outcome, because a returned report can describe failure, cancellation, or unresolved effects.],
    )
  ]
  #behavior(title: "A repeated admission reuses the saved execution", area: "Admission", level: "boundary")[
    #given[An execution is saved under an id.]
    #when[The same id and the same encoded input are admitted again.]
    #then[The saved execution is attached and no effect runs.]
    #then[A different input for that id is refused as an input mismatch.]
  ]
  #behavior(title: "Drive interruption preserves resumable state", area: "Admission", level: "boundary")[
    #given[A drive stops because its budget expires, its caller exits, or its runner is lost.]
    #when[A compatible drive later resumes the execution.]
    #then[The execution continues from its last checkpoint and no cancellation is recorded.]
  ]
  #subsection(title: "Failure classes and caller action")[
    #md-table(2, (
      [*Error kind*], [*Application action*],
      [Busy], [Retry no sooner than the adapter's owner-loss window, under the application's delivery policy.],
      [Transient], [Retain the stable id and retry deliberately; inspect the saved state before assuming work absent.],
      [NeedsReconciliation], [Establish downstream effect evidence or request operator attention.],
      [Incompatible], [Supply the matching definition and input, or use an explicitly approved migration.],
      [Defect], [Correct configuration, codec, definition, checkpoint, or adapter behavior.],
    ))
    #points(
      [Typed StorageFailure, CodecFailure, InvalidCheckpoint, CheckpointTooLarge, and RecoveryRequired detail stays available. SuspensionNotSaved retains both causes when recording the suspension fails differently from the original cause.],
      [NotSent requires proof of absence and MaybeSent preserves suspension. A remote effect is never replayed because an old driver disappeared or a delivery system retried.],
    )
  ]
  #subsection(title: "Committed local database effect")[
    #sequence(
      title: "A committed posting survives runner loss", participants: (
        (id: "lost", label: "Lost runner", shape: "control"),
        (id: "app", label: "Application record", shape: "database"),
        (id: "resolver", label: "Restarted resolver", shape: "control"),
      ), steps: (
        seq-msg("lost", "app", "posting transaction commits"),
        seq-note("lost", "runner dies before checkpointing completion"),
        seq-msg("resolver", "app", "read posting record"),
        seq-msg("app", "resolver", "recorded result", dashed: true),
        seq-note("resolver", "Completed restores the result; the posting callback is not repeated"),
      ), caption: [The application's posting record, not a missing checkpoint, establishes the result.],
    )
    #points(
      [Sharing a database or a pool does not make an application's SQL transaction atomic with Saga's later checkpoint. The adapter's commit boundaries keep those writes separate.],
      [The application keeps a stable business operation id and an authoritative posting record inside its own transaction, makes the effect idempotent, and makes it discoverable under that identity. A missing checkpoint is never evidence that SQL rolled back.],
      [The attempt resolver returns Completed only when the application record establishes the result. A failed lookup or inconclusive absence stays MaybeSent and authorizes no further effect.],
      [A delivery system that redelivers the coordinator resumes Saga recovery; it never authorizes resubmitting a payment. The application owns financial evidence, reservation, and posting independently of both libraries. The decision is #adr(12).],
    )
  ]
  #subsection(title: "Discovery and reading")[
    #points(
      [`durable.unfinished(storage, limit)` lists candidates without reserving them, and reconnect plus drive can still meet Busy. Saga supplies listing and reconnect, never a sweeper or delivery scheduler.],
      [`durable.read(run)` returns Pending, Suspended with its reason, or Finished with the saved report, without a live runner. The saved Outcome is the completion authority; notifications and telemetry are observations.],
    )
  ]
])

#section(title: "Storage verification", lead: "Protocol conformance and deployment durability need different evidence.", body: [
  #answers(title: "Storage conformance", responsibility: [Expose extension checks for any storage adapter without private imports.], interface: [`storage.conformance.run` accepts a fresh fixture per scenario, cleanup, an operation timeout, and the adapter's declared owner-loss allowance.], interactions: [Consumer tests supply public storage and resolver fakes; fresh-VM probes exercise file recovery.], invariants: [The suite checks atomic create, claims as values, live and lost ownership, stale writes, the cancellation race, listing, release, and killed-runner recovery.], failure: [A located suite failure names the violated contract. A one-VM pass proves neither distributed fencing nor media durability, so adapters need separate deployment tests.])
  #points(
    [The restart probe kills a VM during concurrent effect and compensation work, then recovers from fresh definitions and saved receipts in another VM. It stays in the verification set because no in-process test can establish that recovery.],
    [Evidence across both contexts appears in the root verification gates.],
  )
])
]
