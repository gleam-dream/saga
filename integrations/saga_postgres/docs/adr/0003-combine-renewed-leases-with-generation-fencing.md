# Combine renewed database-clock leases with generation and token fencing

<a id="adr-0003"></a>

## Decision

- Make lease expiry permit takeover under the database clock. Preserve the unreplaced claim's commit/renew/release authority until replacement or release.
- Increment generation and generate a distinct token on claim. Match both on owned operations; neither a lease timestamp nor generation alone is the ownership proof.
- Declare renewal every third of the configured lease and leave the linked heartbeat to Saga. Prompt release handles detectable runner loss; lease expiry handles lost nodes or lost release.

## Rationale and alternatives

- Database-clock expiry avoids comparing runner-node clocks for takeover. It still relies on the database clock and can be affected by clock changes.
- Commit-only refresh can expire during a long step or retry backoff. Independent renewal separates liveness from checkpoint traffic.
- Immediate expiry-based write revocation would refuse a merely slow current owner even before another owner exists. The selected contract permits its refresh while a conditional takeover can still replace it.
- Process-identity ownership would need adapter registries and would not establish cross-VM authority. Claim values allow transfer while database predicates exclude replaced proofs.
- Fencing saved progress cannot retract an already sent effect. A partitioned live runner can lose timely renewal and overlap an external action with a successor; the application/Saga resolver contract remains necessary.

## Evidence and historical limits

- Initial adapter: `2ee5e0add00b94ea6da43734d28d01491b0bc6f1`, 2026-10-02. Prompt killed-runner release in parent Saga and adapter tests: `92612fd0759260b42ec806f0a7dae03fb6519278`, 2026-10-02.
- `store_test` checks current renewal, stale replacement, missing rows, and commit after expiry before takeover. `durable_test` checks heartbeat over a long action, killed-runner release, and simulated lost release followed by resolver recovery.
- Parent [claim ADR](../../../../docs/adr/0005-storage-claims-as-values.md#adr-0005) records the protocol rationale. Inspection does not recover a separate historical decision discussion for permitting unreplaced expired claims.
- The test script runs one VM against PostgreSQL 16 with fsync and synchronous_commit disabled. It proves selected protocol behavior, not multi-node partition safety or media survival. Deployment evidence remains unresolved in the layer.
