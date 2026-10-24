# Build once and retain native values in a run-scoped store

<a id="adr-0002"></a>

## Decision

- Evaluate the builder once and reuse its immutable validated graph. Keep outputs/action records in a fresh opaque native store per run.
- Confine type erasure and identity coercion to `saga/internal/store`. Producer and opaque Port fetch bind node identity to one native type together; readiness admits reads only after output commit.
- Register each node once and assemble the reachable graph with one dependency walk rather than repeatedly merging transitive node maps. embed evaluates the embedded builder during the destination definition; map_errors reuses its validated graph.
- Use dependency counts and a ready min-heap. Retries enter the same capacity-controlled admission path.

## Rationale and alternatives

- Per-node mailbox cells required rebuilding per run and selective receives whose cost grew with retained messages. Sharing build-time cells across runs would violate isolation.
- Caller-visible Dynamic weakens the public contract; internal native storage changes the carrier while preserving producer-bound typing.
- Repeated transitive dictionary merges made wide-dependency definition cost superlinear; node registration removes that redundant work.
- Full graph scans were a separate admission cost; the readiness index removes them while retaining builder-order tie breaking.

## Evidence and history

- Build-once storage: `9e9350a0652d809a34eab594443aa858aba956c1`, 2026-09-23. Ready admission: `ea8479c5d5ee1bdce2614d106749e1422a2237fe`, same date.
- Explicit decision: `dd149515ad464d9e539563fd94ee790c8dbd32a5`, 2026-09-24. Builder-safe error mapping: `fff29d96f5ee2cd3cd39da90071f062d46195b25`, same date.
- Definition node accumulation: `bb21ebc5af60d5f7fc07d4403cdf5e4e79f9e4bd`, 2026-09-24. Harness resolution/correctness refinement: `9361e395d8ac2279daf8934fec50792421034027`, same date.
- Evidence: internal store/min_heap/coordinator, store-soundness/map-isolation/map-errors tests, and the separate public benchmark package. [Exact historical tables](https://github.com/gleam-dream/saga/blob/2a6bc4bf380145e62015e66e15d216f9a6d7d1d4/bench/RESULTS.md) remain pinned to their source revision.
- This supersedes Oversight's original no-central-map mechanism sketch. Native public typing remains required; measurements are scoped evidence, not a universal latency promise.

## Measured evidence and limits

- Original comparison used Apple M2 Max, OTP 28, 12 online schedulers, concurrency 64, and no run deadline. Chain, fan and four-wide dependency shapes covered 10–2000 steps; original large cells had five runs, later microsecond captures had fifteen and asserted exact output.
- Baseline `0517f1a` rebuilt on every run. Store change `9e9350a` reduced builder calls to one per Workflow and improved N=2000 median run time by 1.42–1.68 times; roughly fourfold doubling persisted because admission still scanned every node.
- Ready admission `ea8479c` reduced measured doubling to about 2.0–2.4 times. Combined N=2000 speedup was 14–49 times by shape. These machine-specific observations support the separate storage and admission hypotheses; they do not promise those speedups for application workloads.
- Wide-definition profiling attributed 35.28% of one N=2000 profile to 5992 `maps:merge/2` calls. Node registration reduced the measured construction sample from 31,904 to 6461 microseconds; the N=1000→2000 ratio changed from 3.24 to 1.89.
- Microsecond timing and expected-output assertions corrected under-resolved small cells and excluded completed-but-wrong runs. Dependency indexing and prepend/reverse accumulation removed other repeated work; map_errors reuses the validated graph rather than rerunning its builder.
- Construction had one sample per size. Small timer values, warm-up and system scheduling limit inference; observed near-linear growth does not prove an asymptotic bound or production tail latency. The benchmark contains trivial integers, no remote effects, rollback-heavy journal, persistence traffic or realistic resource contention.
