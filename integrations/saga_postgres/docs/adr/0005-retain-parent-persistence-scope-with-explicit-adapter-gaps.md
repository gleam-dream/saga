# Retain parent persistence scope and expose missing adapter authority

<a id="adr-0005"></a>

## Decision

- Preserve the existing complete-checkpoint adapter and its indefinitely retained execution rows. No pruning, approval-answer ledger, independent journal lifetime, or completed-run compensation claim is supplied by the current schema.
- Keep Saga's retained checkpoint conversion, historical forks, durable approvals, independent children, result/journal retention, and compensation authority visible as adapter extension requirements. Their final database admission/retention rules need an owner decision before implementation.
- Move standing PostgreSQL architecture and vocabulary into this native layer, and retain history/rationale in these ADRs. Keep README as the common usage and operations entry point; retain the SQL, tests, harness, and package metadata unchanged.

## Rationale and alternatives

- Treating the current row schema as the whole intended product would erase parent capabilities. Treating copyable handles or serialized bytes as sufficient authority would silently bless unimplemented atomic admission and retention semantics.
- The parent owns workflow meaning and its future contracts. A duplicate adapter glossary or a second scheduling/tracker/runtime layer would create another competing authority.
- Deleting finished rows without an explicit retention contract can invalidate result references, compensation evidence, and idempotent reconnect. A TTL is therefore a semantic choice rather than an infrastructure-only cleanup.
- One timeless standing layer avoids maintaining the old README architecture, changing release-plan status, and schema prose as separate authoritative descriptions. Operational commands remain in README; versioned source history preserves removed prose.

## Evidence and historical limits

- Sources captured: adapter README/CHANGELOG/source/tests at Saga `2a6bc4bf380145e62015e66e15d216f9a6d7d1d4`; parent `DURABILITY.md` at that revision; Oversight Saga durability/children/capability scope, API rows, release decisions/plan, workflow/composition/child-lifecycle research.
- Immutable source: [Saga's parent persistence document](https://github.com/gleam-dream/saga/blob/2a6bc4bf380145e62015e66e15d216f9a6d7d1d4/DURABILITY.md), [Oversight Saga scope](https://github.com/gleam-dream/oversight/blob/3baff7030a96d5b6cf78b2335c16d8c203727da5/saga-design.md), and [child lifecycle research](https://github.com/gleam-dream/oversight/blob/3baff7030a96d5b6cf78b2335c16d8c203727da5/research/child-lifecycle-contract.md).
- Oversight's early PUBLIC-API and API-COVERAGE Saga rows still described persistence as deferred despite the delivered adapter. Current code/public consumers resolve that stale implementation claim; they do not remove future requirements.
- The parent [retained-scope ADR](../../../../docs/adr/0008-retain-expanded-workflow-scope.md#adr-0008) and [composition design](../../../../docs/design/design.typ#retained-composition-contracts) own those requirements and their oracle limits.
- User-authorized documentation consolidation is recorded 2026-10-06. No historical acceptance date or rationale is invented for an unresolved future database contract.
