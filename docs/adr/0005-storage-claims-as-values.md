# Storage ownership is a revocable claim value

<a id="adr-0005"></a>

## Decision

- One Storage serves every id in its scope. Claim combines execution id, generation, and adapter token; any process holding the current value may commit/release.
- Atomically refuse stale ownership before changed cancellation before revision conflict. Cancellation preserves checkpoint bytes/revision.
- Renew leases independently from checkpoint traffic through a runner-linked heartbeat. Adapter owner-loss detection and distribution/media guarantees remain explicit.

## Rationale and alternatives

- Same-process ownership required extra in-VM registries in application database adapters. Claim values preserve fencing without that state.
- Revision-only writes cannot exclude a replaced owner. Claim fencing guards saved writes but cannot retract an external call.
- Commit-only renewal expires during long actions/backoff. Runner heartbeat separates liveness from checkpoint frequency.

## Evidence and history

- Protocol: `1d965bd5819ae8acd61132d5e9542930a4e0ece8`, 2026-10-02. Refusal/renewal fix: `3ea5a75a7f5f4a7a4e6b9bc002d0c182304f7983`.
- Separate adapter: `2ee5e0add00b94ea6da43734d28d01491b0bc6f1`, same date. Detailed database rationale belongs to its nested layer.
- Evidence: storage source, memory/file, public conformance, durable tests and PostgreSQL conformance. Alternatives/costs: Oversight release Saga SAGA-R2/R3/R10 and PLAN Round 3.
