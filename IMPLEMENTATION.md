# Optional persistence implementation

Accepted on 2026-09-29: one typed `saga.Workflow`, one concurrent execution
engine, optional persistence through a storage contract. Governing anchors:
`../oversight/saga-design.md#the-core-abstraction` and
`../oversight/saga-design.md#durable-execution-and-freezethaw`, refined by the
owner's instruction that Saga remains independently useful in memory.

## Ownership and behavior

Saga owns dependency readiness, bounded admission, retry decisions, closed
branch selection, cancellation, outcomes, and reverse completion-order undo.
A storage adapter owns atomic create, revision checks, execution ownership,
and saved bytes. Delivery systems may wake a runner; they never decide Saga
progress. Local workflows need no codecs. Persistence admission requires
versioned codecs and reconstruction/reconciliation callbacks. Saved records
contain values and stable identities only; callbacks, PIDs, timers, and
credentials are reconstructed by the caller's definition.

An admitted action with no committed outcome is uncertain after interruption.
Resume must reconcile it or return unresolved; it must never blindly replay
an effect. A saved success is reused. Selection precedes branch effects.
Every dispatch follows a successful state commit. Revision or ownership
failure stops dispatch. Restart preserves compensation order and intent.

## Delivery sequence and coverage

1. Canonical closed choice in `saga.Workflow`; local runtime semantics retained.
2. Explicit checkpoint state and dispatch boundaries in the existing runner.
3. Optional storage contract, memory adapter, and persistence admission.
4. File adapter and fresh-process recovery of concurrent workflows.
5. Document the optional Grind boundary; no Grind/Fabric implementation.

The authoring API, transition state, storage contract, and recovery integration
are captured above and in DURABILITY.md. Database adapters, dynamic traversal,
Fabric changes, and distributed execution delivery are out of scope.

## Acceptance

Public tests cover unchosen effects, shared dependencies, concurrency limits,
retries, duplicate starts, conflicts, saved results, interrupted effects and
undo, persistent cancellation and outcome reads, incompatible definitions,
and the same definition executed locally and through memory/file adapters.
The external consumer uses only public modules. A real VM-kill probe verifies
recovery from a fresh definition. Existing local lifecycle gates remain.

Commands: `gleam format --check src test`, `gleam build --warnings-as-errors`,
`gleam test`; in examples/order_consumer: `gleam format --check`,
`gleam build --warnings-as-errors`, `gleam test`, `gleam run`;
`scripts/check_negative.sh`, `scripts/check_durable_restart.sh`, `nix fmt`,
`nix flake check`. No separate lint channel is configured.

## Delivery evidence

Implemented the five-step boundary: canonical `Workflow` choices, checkpoint
values and gated dispatch in the existing coordinator, a public storage
contract with memory/file adapters, concurrent restart recovery, and a documented
optional Grind/Fabric integration contract. The old separate plan language and
its file-specific runner have been removed.

The full gate passed on 2026-09-29: 149 Saga tests, 12 external consumer tests,
four consumer scenarios, two positive compiler controls, six negative fixtures,
and a killed-VM concurrent recovery probe. Formatting, warnings-as-errors build,
and `nix flake check` passed on aarch64-darwin; Nix skipped other host systems.
No separate lint command or design renderer is configured in this repository.

Acceptance review: shared authoring/runtime and public adapter contracts are
implemented; tests exercise public behavior and actual process/VM loss; no
Grind, Fabric, or database dependencies were added. Documentation records the
single-VM file guarantee, whole-snapshot cost, explicit undo reconstruction,
and conservative suspension of interrupted compensation callbacks. No tracker
issue, formal pending ledger, commit, or publication was part of this delivery.

## Recovery hardening goal (2026-09-29)

Owner-authorized sequence: fix configuration-order loss, late Continue undo
eligibility, and erased failure types; complete compensation recovery; publish
an adapter conformance suite. Governing ownership and admission rules above
remain unchanged. Work stays within Saga, with no new database or delivery
integration.

Accepted behavior for this increment:

- Undo and compensation reconciliation are step configuration, independent of
  whether codecs were attached earlier or later. Reattaching codecs preserves
  both callbacks; error mapping transforms callback results without inversion.
- Persistent compensating steps explicitly declare `restore_undo`, including
  an explicit `NoUndo` contract. Preparation rejects missing declarations before
  effects. Actual Continue undo capability is checked before its outcome is
  committed. Local compensation retains its unrestricted closure capability.
- Storage failures, codec failures, invalid checkpoints, and uncertain actions
  retain distinct typed values through drive and persistent suspended reads.
  If recording a suspension fails with a distinct error, the caller receives
  both failure causes. An identical repeated error is returned once.
- A compensation resolver receives the saved input, attempt, and stable
  compensation key, and returns a resolved Recovery or unknown. It does not
  silently replay the original callback. Resolved Retry/RetryAfter, Continue,
  Abort, AbortAfterCleanupFailure, and Hold use the existing transition rules.
  Restart retains attempt budgets, undo order, and cancellation semantics.
- A reusable public storage conformance runner checks atomic creation, load,
  claims, revision checks, stale generations, cancellation races, release, and
  ownership recovery after process loss. Memory and file adapters run the same
  suite; adapter-specific deployment guarantees remain explicit.

Coverage: authoring/recovery behavior belongs to Saga and DURABILITY.md;
conformance belongs to the storage contract. Public integration tests cover
these rules, actual worker interruption, and fresh-VM recovery. Gates remain
those listed above. Status: complete.

Delivered all three API corrections, explicit compensation recovery, and the
public adapter conformance runner. Persistent compensation requires an explicit
undo declaration; actual Continue undo is checked before commit. Resolver
configuration survives codec reattachment, and mapped resolver results preserve
the application's error types. Unknown decisions remain suspended. The
checkpoint format starts at 1 for the first release. Development snapshots are
disposable; no deployed or published format requires migration.

The full gate passed on 2026-09-29: 164 Saga tests, 14 external consumer tests,
four consumer scenarios, two positive compiler controls, six negative compiler
fixtures, and two actual VM-kill recovery probes (concurrent effects and
concurrent compensation). Both memory and file adapters pass the same public
conformance suite; a deliberately broken ownership adapter is rejected.
Formatting, warnings-as-errors builds, and diff whitespace checks passed.
`nix flake check` passed on aarch64-darwin and omitted incompatible systems.

The full gate exposed a pre-existing report-test race: the run could exit before
the test attached its monitor. The test now attaches the monitor before opening
its existing gate, preserving its normal-exit assertion. No production behavior
changed for that correction.

Package boundaries remain unchanged. No database or delivery integration was
added. The file adapter still supports one VM per canonical path; conformance
checks do not establish distributed fencing or power-loss durability. Changes
are ready for the owner-requested local commit. Publication remains out of scope.

## Final review and first-release baseline

The owner requested a final gap check and local commit. Adherence to the accepted
optional-persistence design, fulfillment of the three recovery fixes and adapter
suite, repository standards, and implementation craft have no remaining blockers
in this delivery. Database adapters, delivery integrations, checkpoint migrations,
and cross-VM file coordination remain documented future work or adapter limits.
The unpublished checkpoint format has been reset to 1; the package remains at
its initial version, 0.1.0.
