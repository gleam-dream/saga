# Retain uncertainty independently from retry and rollback authority

<a id="adr-0003"></a>

## Decision

- Record crashed, timed-out, interrupted, and application-marked uncertain actions independently from later recovery.
- Failed-attempt compensation chooses retry, replacement with its own undo, abort, cleanup-failing abort, or Hold. Undo addresses known completion separately.
- Hold retains prior effects without rollback authority. Marked uncertain errors default to Hold absent explicit decision or RollBack policy.
- Reverse actual completion order defines undo; attempt all entries and retain every failure. Settle active siblings before returning stopped outcomes.

## Rationale and alternatives

- A clean final retry cannot prove an earlier possible effect absent. CompletedWithUnknownEffects prevents that evidence loss.
- Ordinary rollback after uncertain payment can release inventory while charge status remains unknown. Explicit permission preserves application policy.
- Reactor 1.0.6 forward undo and early sibling return conflict with the retained contract; exact parity applies only to mapped scenarios.

## Evidence and history

- Native lifecycle dates to 2026-09-23. Retry admission/interruption fixes: `072d20d2da51542fd499c68f176143aaa35f998a`, `a28805d237d33ec05ff0a4d57c342cfe881e2916`.
- Marked uncertainty/stable keys: `f27b6287fd0002b0be3b54f0cc36050f40fe5fbe`, 2026-10-02. Default Hold correction: `92612fd0759260b42ec806f0a7dae03fb6519278`, same date.
- Evidence: unknown/rollback/retry tests, public consumer, `PROVENANCE.md`, mechanically checked oracle `d2.diff` and `d3.diff`.
- Prior contracts: Oversight composition research, interface-lab ACTIVITY-RECOVERY, Saga compensation/undo design. Broader continuation intent does not assert delivered halt/resume.
