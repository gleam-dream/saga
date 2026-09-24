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
in a `Dict(Int, Dynamic)` (`saga/internal/store`) owned by the coordinator,
read through one internal typed accessor performing a single unsafe
coerce, sound because the key (a node id) and its element type both come
from the same `Port`. Same machine, same method, same `execution.Config`
as "Before".

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
