# Keep PostgreSQL storage separate and borrow the application pool

<a id="adr-0001"></a>

## Decision

- Keep `saga_postgres` as a separate Erlang-target package implementing Saga's storage port. Core Saga owns workflow execution and gains no PostgreSQL dependency.
- Accept the application's `pog.Connection` instead of starting an adapter-owned pool. One storage value serves all execution ids in its selected schema.
- Keep pog `>= 4.1.0 and < 4.2.0` and pgo `>= 0.20.0 and < 0.21.0` aligned with Grind while these packages compose through one resolved pool implementation.

## Rationale and alternatives

- Applications already need a pool for business queries. An adapter-owned pool would add another capacity, supervision, credential, and shutdown owner.
- Keeping substantial SQL, migration, and lease behavior in an adapter isolates infrastructure without hiding the meaning of Saga's claims. An in-core adapter would impose pog/pgo on local Saga consumers.
- A generic shared durability engine would conflate Saga execution claims, Grind job claims, and Fabric compare-and-set authority. Similar fields do not establish equal invariants.
- Exact minor alignment carries upgrade coordination cost. The recorded reason is Grind's coupling to private pog/pgo connection shapes; this adapter itself uses pog's public query/transaction interface.

## Evidence and historical limits

- Adapter introduction: Saga commit `2ee5e0add00b94ea6da43734d28d01491b0bc6f1`, 2026-10-02. Its package comments, public module, and shared-pool consumer record the ownership and dependency rationale.
- The Oversight release plan recorded the owner choice to ship this adapter in the first package set. The later Round 9 decision retained both PostgreSQL adapters because their correctness-critical infrastructure justified package isolation.
- Sources captured before retirement: `oversight/docs/release-api/saga.md` SAGA-R2/R3 and questions 2/7; `DECISIONS.md` Round 9; `PLAN.md` Saga PostgreSQL and Round 9 rows; `PUBLIC-API-GUIDELINES.md` ownership/composition rules.
- Immutable source: [Oversight release decisions](https://github.com/gleam-dream/oversight/blob/3baff7030a96d5b6cf78b2335c16d8c203727da5/docs/release-api/DECISIONS.md) and [Saga release review](https://github.com/gleam-dream/oversight/blob/3baff7030a96d5b6cf78b2335c16d8c203727da5/docs/release-api/saga.md).
- The exact original discussion leading to each SQL representation is not recoverable from the inspected source/history. The owner-authorized package boundary is recorded; no additional historical approval is inferred.
- Parent claim semantics and their rationale remain in [Saga's storage ADR](../../../../docs/adr/0005-storage-claims-as-values.md#adr-0005).
