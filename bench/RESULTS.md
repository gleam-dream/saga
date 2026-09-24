# saga run-cost benchmark results

`bench/` is a throwaway benchmark package, a path dependency on `saga`
using only its public modules (`saga`, `saga/execution`). It is not part
of `gleam test`; run it with:

```sh
cd bench
gleam run
```

## Machine

- CPU: Apple M2 Max (`sysctl -n machdep.cpu.brand_string`)
- Erlang/OTP: 28
- Schedulers online: 12
- `execution.Config`: `max_concurrency: 64`, `deadline: None` (no run
  deadline), otherwise `execution.config()`'s defaults.

## Method

For each shape and each N in `[10, 50, 100, 250, 500, 1000, 2000]`:

- **`define` time**: one monotonic-clock measurement around `saga.define`.
- **Per-run wall time**: one warm-up `execution.run` (reported separately),
  then 20 measured runs (5 for N >= 1000, to keep total wall time
  reasonable), each timed with the monotonic clock. `median` and `p95` are
  nearest-rank over the measured samples.
- **Build invocations**: a concurrency-safe atomics counter (`bench/src/
counter.gleam`, `bench/src/bench_native.erl`), incremented once every
  time the workflow's build closure actually runs (once at `define`, and
  once per `execution.run` before the build-once refactor). Read after all
  measured runs for that shape/N.
- No size was dropped for exceeding the ~60s per-iteration budget in
  either the before or after run.

Shapes (`bench/src/shapes.gleam`), every step returning a trivial `Int`:

- **Chain**: N sequential steps, each reading the immediately preceding
  step's output.
- **Fan-in/fan-out**: one shared producer, read by N parallel consumer
  steps, recombined with `saga.all`.
- **Wide dependency reads**: each step (after 4 root steps) reads a
  sliding window of 4 prior outputs via `saga.both`/`saga.map`, folded
  through `saga.all` at the end.

## Before (commit 0517f1a)

Per-run re-evaluation of the workflow's build function, with per-run
outputs held in per-node mailbox cells read via selective receive
(O(N) per read, O(N^2) per run in the worst case for a shape whose reads
scale with N).

### Chain (N sequential steps, each reads the previous)

| N    | define (ms) | warmup (ms) | median (ms) | p95 (ms) | min (ms) | max (ms) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 6           | 5           | 0           | 1        | 0        | 1        | 22                | 20            |
| 50   | 0           | 1           | 0           | 1        | 0        | 1        | 22                | 20            |
| 100  | 0           | 2           | 1           | 2        | 1        | 2        | 22                | 20            |
| 250  | 0           | 7           | 7           | 8        | 7        | 8        | 22                | 20            |
| 500  | 1           | 25          | 25          | 26       | 24       | 26       | 22                | 20            |
| 1000 | 1           | 100         | 98          | 99       | 94       | 99       | 7                 | 5             |
| 2000 | 4           | 391         | 392         | 412      | 386      | 412      | 7                 | 5             |

Growth (median time at 2N / median time at N):

- 250 -> 500: 3.57x
- 500 -> 1000: 3.92x
- 1000 -> 2000: 4.00x

(50 -> 100 omitted: baseline median was 0ms.)

### Fan-in/fan-out (1 shared producer, N parallel consumers, `all`)

| N    | define (ms) | warmup (ms) | median (ms) | p95 (ms) | min (ms) | max (ms) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 0           | 0           | 0           | 1        | 0        | 1        | 22                | 20            |
| 50   | 0           | 1           | 1           | 1        | 0        | 1        | 22                | 20            |
| 100  | 0           | 1           | 2           | 2        | 1        | 2        | 22                | 20            |
| 250  | 0           | 7           | 7           | 7        | 6        | 7        | 22                | 20            |
| 500  | 1           | 22          | 21          | 22       | 20       | 22       | 22                | 20            |
| 1000 | 3           | 79          | 78          | 83       | 78       | 83       | 7                 | 5             |
| 2000 | 3           | 323         | 313         | 317      | 307      | 317      | 7                 | 5             |

Growth (median time at 2N / median time at N):

- 50 -> 100: 2.00x
- 250 -> 500: 3.00x
- 500 -> 1000: 3.71x
- 1000 -> 2000: 4.01x

### Wide dependency reads (`both`/`map` over a window of 4 prior outputs)

| N    | define (ms) | warmup (ms) | median (ms) | p95 (ms) | min (ms) | max (ms) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 0           | 1           | 0           | 0        | 0        | 1        | 22                | 20            |
| 50   | 1           | 0           | 1           | 1        | 0        | 1        | 22                | 20            |
| 100  | 1           | 2           | 2           | 2        | 1        | 2        | 22                | 20            |
| 250  | 1           | 9           | 9           | 9        | 8        | 10       | 22                | 20            |
| 500  | 3           | 30          | 31          | 32       | 30       | 33       | 22                | 20            |
| 1000 | 9           | 118         | 121         | 126      | 116      | 126      | 7                 | 5             |
| 2000 | 30          | 462         | 472         | 473      | 462      | 473      | 7                 | 5             |

Growth (median time at 2N / median time at N):

- 50 -> 100: 2.00x
- 250 -> 500: 3.44x
- 500 -> 1000: 3.90x
- 1000 -> 2000: 3.90x

### Before: analysis

All three shapes show median run time growing roughly 4x when N doubles
once N is large enough for scheduling/read overhead to dominate step work
(N >= 250) — consistent with O(N^2) total work per run (O(N) reads each
costing O(N) via the per-node cell's selective receive over the
coordinator's own mailbox). `define`'s own time stays small and roughly
linear in N (it runs the builder exactly once, with no repeated re-reads).
`build invocations` for a shape/N with 20 measured runs is 22 (1 call for
`define` plus 1 warm-up run plus 20 measured runs) and 7 for N >= 1000 (1
plus 1 plus 5) — i.e. the build function runs once per run, exactly as the
current per-run-re-evaluation design requires.

## After (commit 9e9350a)

Build function evaluated exactly once, at `define`; per-run outputs held
in a `Dict(Int, Native)` (`saga/internal/store`, where `Native` is that
module's own opaque, type-erased carrier -- never `gleam/dynamic.Dynamic`)
owned by the coordinator, read through one internal typed accessor
performing a single unsafe coerce, sound because the key (a node id) and
its element type both come from the same `Port`. Same machine, same
method, same `execution.Config` as "Before".

### Chain (N sequential steps, each reads the previous)

| N    | define (ms) | warmup (ms) | median (ms) | p95 (ms) | min (ms) | max (ms) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 7           | 5           | 0           | 0        | 0        | 1        | 1                 | 20            |
| 50   | 0           | 0           | 0           | 1        | 0        | 1        | 1                 | 20            |
| 100  | 1           | 1           | 1           | 1        | 0        | 1        | 1                 | 20            |
| 250  | 1           | 4           | 4           | 5        | 4        | 5        | 1                 | 20            |
| 500  | 1           | 16          | 16          | 17       | 16       | 17       | 1                 | 20            |
| 1000 | 2           | 64          | 65          | 65       | 63       | 65       | 1                 | 5             |
| 2000 | 4           | 276         | 276         | 276      | 274      | 276      | 1                 | 5             |

Growth (median time at 2N / median time at N):

- 250 -> 500: 4.00x
- 500 -> 1000: 4.06x
- 1000 -> 2000: 4.24x

### Fan-in/fan-out (1 shared producer, N parallel consumers, `all`)

| N    | define (ms) | warmup (ms) | median (ms) | p95 (ms) | min (ms) | max (ms) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 0           | 0           | 0           | 0        | 0        | 1        | 1                 | 20            |
| 50   | 1           | 0           | 0           | 1        | 0        | 1        | 1                 | 20            |
| 100  | 0           | 1           | 1           | 2        | 1        | 2        | 1                 | 20            |
| 250  | 0           | 5           | 5           | 5        | 4        | 5        | 1                 | 20            |
| 500  | 2           | 14          | 15          | 15       | 14       | 15       | 1                 | 20            |
| 1000 | 3           | 49          | 50          | 50       | 49       | 50       | 1                 | 5             |
| 2000 | 9           | 238         | 186         | 252      | 185      | 252      | 1                 | 5             |

Growth (median time at 2N / median time at N):

- 250 -> 500: 3.00x
- 500 -> 1000: 3.33x
- 1000 -> 2000: 3.72x

### Wide dependency reads (`both`/`map` over a window of 4 prior outputs)

| N    | define (ms) | warmup (ms) | median (ms) | p95 (ms) | min (ms) | max (ms) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 0           | 0           | 0           | 0        | 0        | 1        | 1                 | 20            |
| 50   | 0           | 1           | 0           | 1        | 0        | 1        | 1                 | 20            |
| 100  | 0           | 1           | 1           | 2        | 1        | 2        | 1                 | 20            |
| 250  | 1           | 5           | 5           | 5        | 4        | 5        | 1                 | 20            |
| 500  | 3           | 18          | 17          | 18       | 17       | 18       | 1                 | 20            |
| 1000 | 9           | 70          | 70          | 71       | 68       | 71       | 1                 | 5             |
| 2000 | 29          | 256         | 302         | 370      | 251      | 370      | 1                 | 5             |

Growth (median time at 2N / median time at N):

- 250 -> 500: 3.40x
- 500 -> 1000: 4.11x
- 1000 -> 2000: 4.31x

## Before/after comparison

Median run time (ms), before -> after, with speedup (before/after):

| Shape          | N=100          | N=250          | N=500            | N=1000            | N=2000             |
| -------------- | -------------- | -------------- | ---------------- | ----------------- | ------------------ |
| Chain          | 1 -> 1 (1.00x) | 7 -> 4 (1.75x) | 25 -> 16 (1.56x) | 98 -> 65 (1.51x)  | 392 -> 276 (1.42x) |
| Fan-in/fan-out | 2 -> 1 (2.00x) | 7 -> 5 (1.40x) | 21 -> 15 (1.40x) | 78 -> 50 (1.56x)  | 313 -> 186 (1.68x) |
| Wide reads     | 2 -> 1 (2.00x) | 9 -> 5 (1.80x) | 31 -> 17 (1.82x) | 121 -> 70 (1.73x) | 472 -> 302 (1.56x) |

Build invocations per shape/N (20 measured runs, 5 for N >= 1000; counts
1 `define` + 1 warm-up + N measured runs before, vs. exactly 1 total
after):

| N      | Before | After |
| ------ | ------ | ----- |
| <1000  | 22     | 1     |
| >=1000 | 7      | 1     |

### After: analysis, and what did not improve as expected

Absolute median run time improved by roughly 1.4x-2.0x across all three
shapes at every measured N, and build invocations dropped from one per
run (22, or 7 for the smaller 5-run sample at N>=1000) to exactly one
per `Workflow`, confirmed directly by the counter and consistent with
`saga/internal/store`'s O(1) map-lookup read replacing the O(N)
selective-receive read.

**What did not improve as expected: the O(N^2) growth curve itself
persists after this refactor** — median time still grows roughly
3.0x-4.3x when N doubles, close to the "before" ratios (3.0x-4.0x). The
per-value-read cost this task targeted (`cell.read`'s O(N) selective
receive, replaced by `store.get`'s O(1) map lookup) was **not** the
whole story: `saga/internal/coordinator`'s `admit` function also scans
the _entire_ ordered node list on every admission decision
(`list.filter(state.order, ...)` in `admit`, `src/saga/internal/
coordinator.gleam`), which is itself O(N) per call and is invoked once
per completed node, giving a separate O(N^2) admission-bookkeeping cost
untouched by the build-once/store refactor. This is why absolute times
dropped (each read is now cheap) but the growth _shape_ did not flatten
to linear (each commit still triggers an O(N) scan over the whole node
order). Fixing that scan (e.g. an explicit ready-queue instead of
filtering `state.order` from scratch each time) is a separate,
follow-on optimization, out of scope for this build-once/store-by-node-id
change; it was not attempted here and no code changes toward it are
included in this commit.

## After admission fix (commit ea8479c)

`coordinator.admit`'s full scan of `state.order` on every admission
decision (the follow-on quadratic cost identified above) is replaced with
a min-heap (`saga/internal/min_heap`) of ready node ids, pushed to exactly
when a node becomes admittable and popped by `admit` — O(log N) per
operation instead of an O(N) scan, admission order unchanged (verified by
the oracle's byte-identical traces and a new direct regression test).
`all_nodes_done`/`rollback_complete`'s own O(N) scans (checked on every
message the coordinator processes, not just completions) are likewise
replaced with incrementally maintained counters/flags. Same machine,
method, and `execution.Config` as "Before"/"After (commit 9e9350a)".

### Chain (N sequential steps, each reads the previous)

| N    | define (ms) | warmup (ms) | median (ms) | p95 (ms) | min (ms) | max (ms) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 6           | 6           | 0           | 0        | 0        | 1        | 1                 | 20            |
| 50   | 1           | 0           | 0           | 1        | 0        | 1        | 1                 | 20            |
| 100  | 0           | 1           | 0           | 1        | 0        | 1        | 1                 | 20            |
| 250  | 0           | 1           | 1           | 1        | 1        | 2        | 1                 | 20            |
| 500  | 1           | 3           | 2           | 2        | 2        | 2        | 1                 | 20            |
| 1000 | 2           | 5           | 4           | 5        | 4        | 5        | 1                 | 5             |
| 2000 | 4           | 9           | 8           | 9        | 8        | 9        | 1                 | 5             |

Growth (median time at 2N / median time at N): 250->500 2.00x,
500->1000 2.00x, 1000->2000 2.00x — linear, not quadratic.

### Fan-in/fan-out (1 shared producer, N parallel consumers, `all`)

| N    | define (ms) | warmup (ms) | median (ms) | p95 (ms) | min (ms) | max (ms) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 0           | 0           | 0           | 0        | 0        | 1        | 1                 | 20            |
| 50   | 0           | 1           | 0           | 1        | 0        | 1        | 1                 | 20            |
| 100  | 0           | 1           | 1           | 1        | 0        | 1        | 1                 | 20            |
| 250  | 1           | 2           | 2           | 2        | 1        | 2        | 1                 | 20            |
| 500  | 2           | 4           | 4           | 5        | 4        | 5        | 1                 | 20            |
| 1000 | 3           | 9           | 9           | 10       | 9        | 10       | 1                 | 5             |
| 2000 | 9           | 25          | 22          | 24       | 21       | 24       | 1                 | 5             |

Growth (median time at 2N / median time at N): 250->500 2.00x,
500->1000 2.25x, 1000->2000 2.44x.

### Wide dependency reads (`both`/`map` over a window of 4 prior outputs)

| N    | define (ms) | warmup (ms) | median (ms) | p95 (ms) | min (ms) | max (ms) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 0           | 0           | 0           | 0        | 0        | 1        | 1                 | 20            |
| 50   | 0           | 1           | 0           | 1        | 0        | 1        | 1                 | 20            |
| 100  | 0           | 1           | 1           | 1        | 0        | 1        | 1                 | 20            |
| 250  | 1           | 2           | 1           | 2        | 1        | 2        | 1                 | 20            |
| 500  | 3           | 4           | 3           | 3        | 2        | 3        | 1                 | 20            |
| 1000 | 9           | 6           | 6           | 7        | 6        | 7        | 1                 | 5             |
| 2000 | 30          | 15          | 13          | 14       | 13       | 14       | 1                 | 5             |

Growth (median time at 2N / median time at N): 250->500 3.00x,
500->1000 2.00x, 1000->2000 2.16x (the 250->500 blip is measurement
noise at low absolute millisecond counts, not a growth-shape signal).

## Three-column comparison: before / after-store / after-admission

Median run time (ms) at each stage, with cumulative speedup
(before/after-admission) in the last column:

| Shape          | N    | Before (0517f1a) | After store (9e9350a) | After admission (ea8479c) | Cumulative speedup                                    |
| -------------- | ---- | ---------------- | --------------------- | ------------------------- | ----------------------------------------------------- |
| Chain          | 100  | 1                | 1                     | 0                         | below timer resolution (ms) -- see the µs rerun below |
| Chain          | 250  | 7                | 4                     | 1                         | 7.00x                                                 |
| Chain          | 500  | 25               | 16                    | 2                         | 12.50x                                                |
| Chain          | 1000 | 98               | 65                    | 4                         | 24.50x                                                |
| Chain          | 2000 | 392              | 276                   | 8                         | 49.00x                                                |
| Fan-in/fan-out | 100  | 2                | 1                     | 1                         | 2.00x                                                 |
| Fan-in/fan-out | 250  | 7                | 5                     | 2                         | 3.50x                                                 |
| Fan-in/fan-out | 500  | 21               | 15                    | 4                         | 5.25x                                                 |
| Fan-in/fan-out | 1000 | 78               | 50                    | 9                         | 8.67x                                                 |
| Fan-in/fan-out | 2000 | 313              | 186                   | 22                        | 14.23x                                                |
| Wide reads     | 100  | 2                | 1                     | 1                         | 2.00x                                                 |
| Wide reads     | 250  | 9                | 5                     | 1                         | 9.00x                                                 |
| Wide reads     | 500  | 31               | 17                    | 3                         | 10.33x                                                |
| Wide reads     | 1000 | 121              | 70                    | 6                         | 20.17x                                                |
| Wide reads     | 2000 | 472              | 302                   | 13                        | 36.31x                                                |

Growth ratios (median time at 2N / median time at N), all three stages:

| Shape          | Before (500->1000->2000) | After store  | After admission |
| -------------- | ------------------------ | ------------ | --------------- |
| Chain          | 3.92x, 4.00x             | 4.06x, 4.24x | 2.00x, 2.00x    |
| Fan-in/fan-out | 3.71x, 4.01x             | 3.33x, 3.72x | 2.25x, 2.44x    |
| Wide reads     | 3.90x, 3.90x             | 4.11x, 4.31x | 2.00x, 2.16x    |

### After admission fix: analysis

The admission fix closes the gap the store refactor alone left open.
Growth ratios drop from ~3.7x-4.3x per doubling (consistent with O(N^2))
to ~2.0x-2.4x (consistent with O(N), the expected shape for N independent
or chained trivial steps under a scheduling loop that does O(1) work per
admission). Cumulative speedup at N=2000 ranges from 14x (fan-in/fan-out,
the shape with the least dependency-chain depth to amortize) to 49x
(chain, the shape that hits `admit` hardest per run since each step
strictly serializes the next). No profiling was needed beyond the
benchmark itself: the growth ratios already confirm the fix, and no
further superlinear behavior was observed at N=2000 in any shape.

## After independent review fixes

An independent review of the perf work above approved it pending fixes,
none of which touch the benchmarked shapes' own algorithmic cost, but two
of which affect this file directly and the bench harness itself:

- The bench's own clock moved from millisecond to **microsecond**
  resolution (`bench/src/bench_native.erl`'s `monotonic_time/0`), since
  the millisecond clock under-resolved every small-N cell to 0-1ms (hence
  the "n/a (baseline 0ms)"/"inf" growth-ratio placeholders in the tables
  above). All timings below are in µs.
- Every timed run now asserts `execution.Completed` with the shape's own
  expected output (`shapes.chain_expected_output`/`fan_expected_output`/
  `wide_expected_output`), not just that a run finished — a
  completed-but-wrong-value run panics the bench immediately instead of
  silently contaminating a timing.
- N >= 1000 now takes 15 measured runs (up from 5), since each run is
  cheap enough post-fix (single-digit milliseconds) that more samples cost
  little additional wall time (the whole three-shape, seven-size sweep
  still completes in ~2-3 seconds).
- Two remaining O(N) sources the admission fix had not addressed were
  fixed in the scheduler itself: `build_dependents` (the reverse-dependency
  index) is now computed once at `define`, not once per run, and its own
  per-dependent `list.append` became a prepend + single reverse;
  `record_undone`/`record_undo_failure`/`record_not_undoable`/
  `record_held` now prepend and reverse once at the point a `Settlement`
  becomes final, instead of appending per journal entry; `saga.all`'s
  per-element `list.append` became a prepend + single reverse. None of
  these are on the _admission_ hot path the previous section measured, so
  they were not expected to change the growth ratios materially for these
  three shapes (none has enough sibling fan-in/fan-out or rollback depth
  at these N to make the difference visible against admission's own cost)
  — this section's numbers confirm that: still ~2.0x per doubling, same as
  "After admission fix" above.
- `map_errors` no longer re-runs a workflow's build function to compute
  its own graph (see README's "Design decisions" section and the
  `perf: reuse the validated graph in map_errors` commit) — this shape
  never used `map_errors`, so it has no bearing on these numbers, but is
  recorded here since it was found during the same review pass.

Same machine as all prior sections; `execution.Config` unchanged
(`max_concurrency: 64`, `deadline: None`).

### Chain (N sequential steps, each reads the previous)

| N    | define (µs) | warmup (µs) | median (µs) | p95 (µs) | min (µs) | max (µs) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 8561        | 7025        | 44          | 54       | 40       | 73       | 1                 | 20            |
| 50   | 177         | 276         | 208         | 249      | 200      | 254      | 1                 | 20            |
| 100  | 219         | 493         | 433         | 458      | 371      | 519      | 1                 | 20            |
| 250  | 519         | 1047        | 1029        | 1097     | 957      | 1121     | 1                 | 20            |
| 500  | 1143        | 2160        | 2092        | 2125     | 2033     | 2131     | 1                 | 20            |
| 1000 | 2425        | 4444        | 4241        | 4561     | 3966     | 4561     | 1                 | 15            |
| 2000 | 4706        | 8647        | 8301        | 8504     | 8022     | 8504     | 1                 | 15            |

Growth (median time at 2N / median time at N): 50->100 2.09x, 250->500
2.03x, 500->1000 2.03x, 1000->2000 1.96x.

### Fan-in/fan-out (1 shared producer, N parallel consumers, `all`)

| N    | define (µs) | warmup (µs) | median (µs) | p95 (µs) | min (µs) | max (µs) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 35          | 129         | 73          | 101      | 56       | 121      | 1                 | 20            |
| 50   | 128         | 341         | 380         | 427      | 340      | 432      | 1                 | 20            |
| 100  | 221         | 848         | 763         | 856      | 682      | 858      | 1                 | 20            |
| 250  | 486         | 1800        | 1847        | 2061     | 1720     | 2198     | 1                 | 20            |
| 500  | 1772        | 4239        | 3867        | 4325     | 3624     | 4416     | 1                 | 20            |
| 1000 | 3042        | 7892        | 7444        | 7890     | 7150     | 7890     | 1                 | 15            |
| 2000 | 8010        | 15736       | 16125       | 16861    | 15459    | 16861    | 1                 | 15            |

Growth (median time at 2N / median time at N): 50->100 2.01x, 250->500
2.09x, 500->1000 1.93x, 1000->2000 2.17x.

### Wide dependency reads (`both`/`map` over a window of 4 prior outputs)

| N    | define (µs) | warmup (µs) | median (µs) | p95 (µs) | min (µs) | max (µs) | build invocations | measured runs |
| ---- | ----------- | ----------- | ----------- | -------- | -------- | -------- | ----------------- | ------------- |
| 10   | 32          | 81          | 45          | 62       | 44       | 63       | 1                 | 20            |
| 50   | 156         | 285         | 256         | 295      | 245      | 324      | 1                 | 20            |
| 100  | 402         | 597         | 510         | 541      | 500      | 573      | 1                 | 20            |
| 250  | 1086        | 1385        | 1372        | 1465     | 1261     | 1527     | 1                 | 20            |
| 500  | 3166        | 2900        | 2853        | 2979     | 2688     | 3049     | 1                 | 20            |
| 1000 | 9850        | 6152        | 5657        | 5968     | 5463     | 5968     | 1                 | 15            |
| 2000 | 31904       | 13588       | 12142       | 16969    | 11789    | 16969    | 1                 | 15            |

Growth (median time at 2N / median time at N): 50->100 1.99x, 250->500
2.08x, 500->1000 1.98x, 1000->2000 2.15x.

### After independent review fixes: analysis

With microsecond resolution and correctness assertions in place, every
cell down to N=10 now reports a meaningful, non-zero median, and no
"n/a"/"inf" placeholder remains anywhere in this file. Growth per doubling
stays consistently ~2.0x-2.2x across all three shapes at every measured N
(linear), matching "After admission fix" above within measurement noise —
confirming that fixing `build_dependents`/`Settlement`-field/`saga.all`
accumulation did not regress the admission fix's own linear scheduling
cost, as expected (none of those three fixes touch the per-node admission
path itself; they remove separate, smaller O(N) or O(k^2) costs that
these particular shapes' N and fan-in/fan-out width do not make visible
against admission's own cost at these sizes). No panics occurred across
any shape/N/run, confirming every timed run's output was correct, not
merely "completed."
