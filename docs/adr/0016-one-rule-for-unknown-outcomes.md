# One rule for unknown outcomes

<a id="adr-0016"></a>

Accepted 2026-11-25 by owner ruling. Supersedes the crash and timeout part of
[0003-uncertainty-and-rollback-authority.md](0003-uncertainty-and-rollback-authority.md);
the rest of 0003 remains in force.

## Decision

- A step's outcome is unknown when its attempt crashes or exits, outlives its timeout, or returns an error that `unknown_when` classified, and when its compensation decider crashes or times out.
- Every unknown outcome follows the step's `on_unknown` policy when no decision settles it: the step has no decider, the decider crashed or timed out, or the decider asked for a retry the budget no longer allows. The default `Reconcile` ends the run `Unresolved` and undoes nothing; `RollBack` fails the run and undoes known completions.
- An explicit decider decision still applies: `Abort` permits rollback, `Hold` holds.
- `Unresolved` carries the held failure (`Returned(error)`, `Crashed(crash)` or `TimedOut`). `Hold` takes no argument: the held evidence is the failure itself, not a second value the decider invents.

## Rationale and alternatives

- 0003 made Hold the default for a classified error because rollback after an uncertain payment can release inventory while the charge status is unknown. A crash or a timeout is the most common unknown outcome, yet it rolled back by default: a provider that answered after the 60 second default step timeout left a possibly taken charge with its reservation released.
- A per-step `on_interrupted` policy was rejected: two policies for one concept leave the author to keep them in agreement.
- Keeping `Hold(evidence)` with a crash mapped to some error value was rejected: the run would report a value the attempt never produced.

## Consequences

- Breaking: a crash or timeout without a decider now ends `Unresolved` instead of `Failed(StepCrashed | StepTimedOut)`; add `on_unknown(RollBack)` to keep rollback. `Unresolved.evidence: e` became `failure: AttemptFailure(e)`, and `Hold(e)` became `Hold`.
- `StepCrashed` and `StepTimedOut` remain the causes a `RollBack` step fails with.
- Checkpoints saved with the earlier held error read as `Returned(error)`.
