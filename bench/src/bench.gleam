/// Benchmark harness for saga's run cost against step count. Not part of
/// `gleam test` -- this is a separate package (`bench/`), a path dependency
/// on saga using only its public modules, run with `gleam run`. Prints a
/// Markdown table to stdout; redirect it into a file to record results.
import bench_ffi
import counter.{type Counter}
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{None}
import gleam/string
import saga.{type DefinitionError, type Workflow}
import saga/execution
import shapes.{type BenchError}
import stats.{type Summary}

const sizes = [10, 50, 100, 250, 500, 1000, 2000]

/// Wall-clock budget per one bench cell (one shape at one N): once a single
/// iteration's own run time exceeds this, larger sizes for that shape are
/// skipped rather than run, and the skip is reported in the table.
const max_iteration_ms = 60_000

/// Warm-up iterations before measurement, then this many measured runs per
/// shape/N (fewer for very large N, to keep the whole suite's wall time
/// reasonable).
const measured_runs = 20

const measured_runs_huge = 5

const huge_n_threshold = 1000

/// `gleam_stdlib` in this lockfile has no `list.range`.
fn range(from: Int, to: Int) -> List(Int) {
  range_acc(from, to, [])
}

fn range_acc(from: Int, to: Int, acc: List(Int)) -> List(Int) {
  case from > to {
    True -> list.reverse(acc)
    False -> range_acc(from, to - 1, [to, ..acc])
  }
}

pub type Row {
  Row(
    n: Int,
    define_ms: Int,
    warmup_ms: Int,
    summary: Summary,
    build_invocations: Int,
    skipped: Bool,
  )
}

pub fn main() {
  io.println("# saga run-cost benchmark\n")

  let chain_rows = bench_shape(fn(n, c) { shapes.chain(n, c) })
  let fan_rows = bench_shape(fn(n, c) { shapes.fan(n, c) })
  let wide_rows = bench_shape(fn(n, c) { shapes.wide(n, c) })

  print_table("Chain (N sequential steps, each reads the previous)", chain_rows)
  print_table(
    "Fan-in/fan-out (1 shared producer, N parallel consumers, `all`)",
    fan_rows,
  )
  print_table(
    "Wide dependency reads (`both`/`map` over a window of 4 prior outputs)",
    wide_rows,
  )
}

fn bench_shape(
  build: fn(Int, Counter) ->
    Result(Workflow(Int, o, BenchError, Nil), List(DefinitionError)),
) -> List(Row) {
  list_fold_until(sizes, [], fn(acc, n) {
    let build_count = counter.new()
    let define_start = bench_ffi.monotonic_time()
    let built = build(n, build_count)
    let define_ms = bench_ffi.monotonic_time() - define_start
    // `define` itself invokes the builder once; that invocation is part of
    // this cell's own build-invocation count like any other.
    case built {
      Error(_errors) -> Stop(acc)
      Ok(workflow) -> {
        let run_count = case n >= huge_n_threshold {
          True -> measured_runs_huge
          False -> measured_runs
        }
        let config =
          execution.Config(
            ..execution.config(),
            max_concurrency: 64,
            deadline: None,
          )
        // One warm-up run, timed and reported separately, then `run_count`
        // measured runs.
        let warmup_start = bench_ffi.monotonic_time()
        let _ = execution.run(workflow, n, config)
        let warmup_ms = bench_ffi.monotonic_time() - warmup_start

        case warmup_ms > max_iteration_ms {
          True ->
            Stop([
              Row(
                n: n,
                define_ms: define_ms,
                warmup_ms: warmup_ms,
                summary: stats.summarize([]),
                build_invocations: counter.read(build_count),
                skipped: True,
              ),
              ..acc
            ])
          False -> {
            let samples =
              range(1, run_count)
              |> list.map(fn(_i) {
                let start = bench_ffi.monotonic_time()
                let _ = execution.run(workflow, n, config)
                bench_ffi.monotonic_time() - start
              })
            let row =
              Row(
                n: n,
                define_ms: define_ms,
                warmup_ms: warmup_ms,
                summary: stats.summarize(samples),
                build_invocations: counter.read(build_count),
                skipped: False,
              )
            Continue([row, ..acc])
          }
        }
      }
    }
  })
  |> list.reverse
}

// A tiny local fold-until (gleam_stdlib's `list.fold_until` uses
// `list.ContinueOrStop`, imported here under local names for clarity at
// call sites above).
type FoldSignal(acc) {
  Continue(acc)
  Stop(acc)
}

fn list_fold_until(
  items: List(a),
  initial: acc,
  f: fn(acc, a) -> FoldSignal(acc),
) -> acc {
  case items {
    [] -> initial
    [first, ..rest] ->
      case f(initial, first) {
        Continue(next) -> list_fold_until(rest, next, f)
        Stop(final) -> final
      }
  }
}

fn print_table(title: String, rows: List(Row)) -> Nil {
  io.println("## " <> title <> "\n")
  io.println(
    "| N | define (ms) | warmup (ms) | median (ms) | p95 (ms) | min (ms) | max (ms) | build invocations | measured runs |",
  )
  io.println("|---|---|---|---|---|---|---|---|---|")
  list.each(rows, fn(r) {
    case r.skipped {
      True ->
        io.println(
          "| "
          <> int.to_string(r.n)
          <> " | "
          <> int.to_string(r.define_ms)
          <> " | "
          <> int.to_string(r.warmup_ms)
          <> " (SKIPPED: exceeded "
          <> int.to_string(max_iteration_ms)
          <> "ms budget) | - | - | - | "
          <> int.to_string(r.build_invocations)
          <> " | 0 |",
        )
      False ->
        io.println(
          "| "
          <> int.to_string(r.n)
          <> " | "
          <> int.to_string(r.define_ms)
          <> " | "
          <> int.to_string(r.warmup_ms)
          <> " | "
          <> int.to_string(r.summary.median)
          <> " | "
          <> int.to_string(r.summary.p95)
          <> " | "
          <> int.to_string(r.summary.min)
          <> " | "
          <> int.to_string(r.summary.max)
          <> " | "
          <> int.to_string(r.build_invocations)
          <> " | "
          <> int.to_string(r.summary.n)
          <> " |",
        )
    }
  })
  io.println("")
  print_growth(rows)
}

fn print_growth(rows: List(Row)) -> Nil {
  let by_n =
    list.filter_map(rows, fn(r) {
      case r.skipped {
        True -> Error(Nil)
        False -> Ok(#(r.n, r.summary.median))
      }
    })
  let lines =
    list.filter_map(by_n, fn(entry) {
      let #(n, median) = entry
      case list.find(by_n, fn(other) { other.0 == n * 2 }) {
        Ok(#(double_n, double_median)) -> {
          let ratio_str = case median {
            0 -> "n/a (baseline 0ms)"
            _ -> ratio_string(double_median, median)
          }
          Ok(
            "- "
            <> int.to_string(n)
            <> " -> "
            <> int.to_string(double_n)
            <> ": "
            <> ratio_str
            <> "x",
          )
        }
        Error(_) -> Error(Nil)
      }
    })
  case lines {
    [] -> Nil
    _ -> {
      io.println("Growth (median time at 2N / median time at N):\n")
      io.println(string.join(lines, "\n"))
      io.println("")
    }
  }
}

fn ratio_string(numerator: Int, denominator: Int) -> String {
  let scaled = { numerator * 100 } / denominator
  let whole = scaled / 100
  let frac = scaled % 100
  let frac_str = case frac < 10 {
    True -> "0" <> int.to_string(frac)
    False -> int.to_string(frac)
  }
  int.to_string(whole) <> "." <> frac_str
}
