# Use conditional execution-row writes and numbered schema migrations

<a id="adr-0002"></a>

## Decision

- Represent each execution as one row containing opaque checkpoint bytes and independent revision, ownership, cancellation, and discovery state.
- Use one conditional statement for an accepted mutation. Diagnose a no-row refusal through a later read while preserving the parent refusal precedence.
- Apply numbered forward schema steps and their version markers in one READ COMMITTED transaction under a schema-specific advisory lock. Ship equivalent SQL up statements for application migration tooling.

## Rationale and alternatives

- A read-then-unconditional-write adapter cannot exclude competing owners or cancellation/progress races. A conditional mutation makes acceptance atomic at the database boundary.
- The single-row representation fits the existing complete-checkpoint port. A per-step journal would need a different persistence protocol, retention model, and migration contract.
- A follow-up read distinguishes missing rows and ownership/cancellation/progress disagreements. It costs another query and can observe later state rather than the failed statement's original snapshot.
- A transaction and shared lock prevent concurrent package startup from independently applying the same unapplied schema step. An unlocked create-if-missing sequence would retain races between callers.
- Package migration supplies a common path; external SQL supports application-controlled migration deployment. Keeping both forms requires a byte/statement-equivalence test and coordination on lock, transaction, search path, and applied versions.
- Maximum-version selection is small and forward-compatible with a newer marker. It does not detect holes, changed released migration text, or actual schema incompatibility.

## Evidence and historical limits

- Saga commit `2ee5e0add00b94ea6da43734d28d01491b0bc6f1`, 2026-10-02, introduced `internal/store.gleam`, `internal/migrations.gleam`, and the version-one SQL file.
- Current implementation evidence: mutation predicates and `owner` diagnosis; migrate's isolation/lock/search-path sequence; version-one table constraints/index; migration tests for concurrency, idempotence, newer watermark, schema separation, and SQL equivalence.
- The inspected commit records implementation, not a detailed comparison of every candidate schema or lock design. The alternatives above explain the observable trade-off; they are not attributed as a recovered historical debate.
- The SQL migration filename carries its original timestamp. Migration text is preserved unchanged by this documentation migration.
