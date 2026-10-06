# Saga run-cost benchmark

This separate Gleam package measures trivial workflow construction and execution
through Saga's public modules. It runs separately from `gleam test`.
Run from the repository root:

```sh
nix develop
cd bench
gleam run > /tmp/saga-bench.md
```

Record the Saga commit, dependency lock, CPU, Erlang/OTP version, scheduler count
and system load alongside a capture. Compare revisions under the same environment
and harness method.

## Method

- The harness measures chain, shared-producer fan-out/fan-in, and a sliding window of four dependency reads. Sizes are 10, 50, 100, 250, 500, 1000 and 2000 steps.
- Each cell measures definition once, one separate warm-up, then 20 runs below 1000 steps or 15 runs at larger sizes. Monotonic timings are in microseconds; median and p95 use nearest rank.
- Every timed run must complete with the shape's expected output. A wrong result or stopped workflow fails the harness. An atomic counter measures builder invocations across definition and runs.
- Configuration uses concurrency 64 and the remaining execution defaults. A warm-up exceeding 60 seconds marks its cell skipped and stops larger sizes for that shape.
- Output includes construction, warm-up, median, p95, minimum, maximum, builder count, sample count and doubling ratios.

## Retained measurements

The [historical microsecond tables](https://github.com/gleam-dream/saga/blob/2a6bc4bf380145e62015e66e15d216f9a6d7d1d4/bench/RESULTS.md#after-independent-review-fixes)
record these execution timings after a separate warm-up, with 15 measured runs
per cell and nearest-rank median/p95. All steps perform trivial integer work.

| Shape                                       | Steps N | Median (µs) | p95 (µs) | Measured runs |
| ------------------------------------------- | ------- | ----------- | -------- | ------------- |
| Sequential chain                            | 1000    | 4241        | 4561     | 15            |
| Sequential chain                            | 2000    | 8301        | 8504     | 15            |
| Shared producer with N consumers and fan-in | 1000    | 7444        | 7890     | 15            |
| Shared producer with N consumers and fan-in | 2000    | 16125       | 16861    | 15            |
| Four-wide sliding dependency window         | 1000    | 5657        | 5968     | 15            |
| Four-wide sliding dependency window         | 2000    | 12142       | 16969    | 15            |

The capture reports Apple M2 Max, Erlang/OTP 28, 12 online schedulers,
concurrency 64 and no run deadline. It does not record the capture timestamp,
Gleam version, operating-system version, dependency-lock digest or system load.
The microsecond tables were introduced in commit
[`9361e3`](https://github.com/gleam-dream/saga/commit/9361e395d8ac2279daf8934fec50792421034027)
on 2026-09-24; that is the report's commit date, not a recorded measurement date.
The retained file revision is `2a6bc4b`; it does not identify the exact execution
revision for these tables. These results have not been rerun for the current checkout.

A later [construction comparison](https://github.com/gleam-dream/saga/blob/2a6bc4bf380145e62015e66e15d216f9a6d7d1d4/bench/RESULTS.md#wide-dependency-reads-define-time-beforeafter)
records one N=2000 wide-workflow definition sample changing from 31,904 to 6461 µs.
That is a separate single-sample construction observation, not one of the
execution timings above. [ADR 0002](../docs/adr/0002-build-once-run-scoped-values.md)
connects the measurements to the storage, admission and node-accumulation changes.

Use the command above to capture the current checkout. To reproduce the historical
report, run its recorded harness/revisions with the recorded configuration;
missing capture metadata prevents an exact environment reproduction.

## Limits

- Trivial integer steps isolate graph/scheduling overhead. These measurements do not establish remote-effect latency, rollback cost, durable-storage throughput, application memory limits or production tail latency.
- Definition has one sample per cell; small timings, compilation/JIT warm-up, scheduling and machine load affect comparisons. Doubling ratios and isolated profiles are evidence for a specific cost hypothesis, not an asymptotic proof.
- Historical findings and the exact earlier tables are linked from [ADR 0002](../docs/adr/0002-build-once-run-scoped-values.md). The [native design](../docs/design/design.typ#coordinator-internals) owns the storage/readiness contract.
