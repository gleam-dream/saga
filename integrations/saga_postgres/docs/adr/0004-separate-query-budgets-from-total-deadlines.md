# Separate implemented query budgets from a total-operation deadline

<a id="adr-0004"></a>

## Decision and unresolved ruling

- Document the implemented 4.5-second setting as the normal pool-backed query attempt budget. Keep Saga's separately configurable operation wait explicit.
- Do not claim that this setting bounds the whole adapter operation or migration. A stronger total deadline and the admitted connection topology need an owner ruling; no runtime change is accepted by this record.

## Evidence and consequences

- The store's run helper starts with three attempts and retries SQLSTATE 40001/40P01 with a new budget. A failed conditional mutation can also issue an independently timed owner read.
- pog 4.1.0 `pog_ffi.query` passes timeout to pgo's pool options. pgo 0.20.0 `pgo_pool` starts a deadline for a checked-out connection loan; already checked-out `SingleConnection` queries bypass that timeout path.
- Package migration uses `pog.transaction`, whose callback receives an already checked-out connection. Its 60-second query arguments therefore do not create independent statement deadlines on that path; transaction checkout/connection lifetime remains driver-owned.
- The blocked-row test demonstrates TimedOut before Saga's default call wait on the ordinary pooled path. It does not prove a total multi-query, transaction, or TCP-fault bound.
- Saga's watchdog stops waiting and can terminate the helper/runner. A database write or a final transaction commit can have applied before the reply was lost; a timeout is not proof of absence.

## Alternatives for a future ruling

- Restrict construction to a pooled capability with an enforced topology check. This would make ordinary query-loan assumptions explicit but would change accepted construction behavior.
- Carry one remaining budget across retries and diagnosis. This would establish an adapter-owned total budget but still require a remote mutation reconciliation contract.
- Retain per-query budgets and require callers to use Saga's watchdog. This preserves the facade while direct callback users and migration remain dependent on driver timing and deployment policy.

## Provenance

- Source-derived documentation finding recorded 2026-10-06 against Saga `2a6bc4bf380145e62015e66e15d216f9a6d7d1d4` and the checked-in adapter/manifest.
- The public README and module comments used broader query wording. They are clarified in the staged documentation; production source, query timeouts, SQL, examples, and tests are unchanged.
- Parent [drive-budget ADR](../../../../docs/adr/0009-drive-budget-and-example-gaps.md#adr-0009) owns drain/release after drive timeout. This record owns the distinct adapter query/transaction topology limit.
