# Persistence attaches to the same workflow and delivery remains external

<a id="adr-0004"></a>

## Decision

- Keep common constructors total for programmer-authored definitions: define and durable.new name definition defects in a panic; try_define returns structured defects for runtime-authored definitions. Config is opaque and execution admission checks all violations.
- Attach checked codecs, versions, reconstruction, and reconciliation to the canonical Workflow. Save data/identities; reconstruct executable behavior from a compatible deployed definition.
- Commit admission and checked input before effect permission. Saved success is reused; interrupted admitted attempt/undo/compensation needs explicit evidence.
- Driver stop preserves resumable state; only explicit durable.cancel records intent. Applications own wakeups, scanning, redelivery, and admission transactions/outboxes.

## Rationale and alternatives

- Retrying a whole local workflow inside a job restarts effects and does not supply per-step durability. Whole-workflow retry requires deliberate policy.
- A shared durability engine would conflate job claims, workflow claims, and agent compare-and-set authority. Similar leases do not establish equal invariants.
- An unknown attempt default previously rolled back known inventory even when charge state remained uncertain; the explicit Hold correction is recorded in ADR 0003. Abnormal runner loss now releases a known claim promptly; lease expiry remains the node-loss fallback.
- Closure snapshots cannot reconstruct resources or establish replay safety. Compatible factories and explicit reconciliation expose those obligations.

## Evidence and history

- Optional persistence: `23f0bdcfacafbd25bf66c3c72393dc776b01d79d`, 2026-09-29. The implementation history records owner authorization and recovery hardening; the retired IMPLEMENTATION log is available in git history.
- Durable handle/owned drive: `1d965bd5819ae8acd61132d5e9542930a4e0ece8`, 2026-10-02.
- Evidence: durable/coordinator/checkpoint/codec source, durable tests/public consumer, real VM-kill restart script.
- Prior scope: Oversight durability/freeze-thaw, composition/child research, lab checkpoint/child evidence, release DECISIONS durability ownership. Migrations, approvals, forks, children, and transactional delivery remain intended contracts.
