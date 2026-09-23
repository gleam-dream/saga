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
