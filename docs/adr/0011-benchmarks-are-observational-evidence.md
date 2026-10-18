# Benchmarks are observational evidence with no latency threshold

<a id="adr-0011"></a>

## Decision

- The run-cost benchmark is a separate package that measures definition and execution through public modules. It runs outside `gleam test`, and its strict build is a gate obligation.
- Every timed run must produce the shape's expected output, so a wrong result or stopped workflow fails the harness. No timing threshold fails a gate.
- A capture records the Saga commit, dependency lock, CPU, OTP version, scheduler count, and system load. Revisions are compared under the same environment and harness method.
- The method, retained results, environment caveats and limits are kept in [docs/BENCHMARKS.md](../BENCHMARKS.md), the sibling of the oracle reference. Accepted findings and their rationale live in ADR 0002 and in this record. Raw captures, when produced, stay as non-markdown data.

## Rationale and alternatives

- Trivial integer steps isolate graph and scheduling overhead. They cannot establish remote-effect latency, rollback cost, storage throughput, memory limits, or production tail latency, so a threshold would assert more than the harness measures.
- Definition has one sample per cell, and timer resolution, warm-up, and machine load affect small values. A doubling ratio supports a cost hypothesis, not an asymptotic bound.
- A threshold would fail on machine variation. An observational run with correctness assertions detects wrong results without that false failure.

## Retained measurements

The retained execution table, the construction observation, the capture environment and the missing capture metadata are in [docs/BENCHMARKS.md](../BENCHMARKS.md). They have not been rerun for the current tree, and the retained file revision does not identify the exact execution revision, so an exact environment reproduction is not possible. The construction observation connects to ADR 0002.

## Evidence and history

- Harness: `bench/`. Method, results and limits: [docs/BENCHMARKS.md](../BENCHMARKS.md); the execution design's run-cost benchmark subsection states the method rules.
- Benchmarks run weekly and manually under the gate registry (ADR 0013).
