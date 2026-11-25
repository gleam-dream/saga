# Store checkpoint records as rows

<a id="adr-0006"></a>

Accepted 2026-11-25. Supersedes the single checkpoint column of [0002-use-atomic-row-writes-and-numbered-migrations.md](0002-use-atomic-row-writes-and-numbered-migrations.md); its conditional writes and numbered migrations remain in force.

## Decision

- Keep one `saga_executions` row per execution for revision, ownership, cancellation and phase, and one `saga_records` row per checkpoint record, keyed by execution and record key.
- A commit is one statement: a data-modifying CTE updates the execution row under the claim, revision and cancellation predicates, and upserts the named records only from the row it updated.
- List one phase in id byte order (`COLLATE "C"`) after a cursor id, served by an index on phase then id.
- Migration 2 drops the checkpoint column. Checkpoints of the earlier format are not converted, because no current saga reads them.

## Rationale and alternatives

- Saga's storage port now commits only changed records (parent ADR 0017). Rewriting one bytea column per commit wrote the whole checkpoint each time, and each write left a dead row version of that size.
- A transaction of two statements would also be atomic, but it holds a connection across statements; one statement keeps every accepted mutation a single round trip.
- Creation order is not part of the listing order, because a keyset cursor on id alone pages correctly and needs no second column.

## Evidence

- The conformance suite (record merging, refused commits write no record, paged listing by phase), `store_test`, and `migrate_test`, which migrates a version 1 schema with a saved execution.
