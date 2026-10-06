/// Benchmark harness for saga's run cost against step count. Not part of
/// `gleam test` -- this is a separate package (`bench/`), a path dependency
/// on saga using only its public modules, run with `gleam run`. Prints a
/// Markdown table to stdout; redirect it into a file to record results.
///
/// Every timed run asserts `execution.Completed` with the shape's own
/// expected output (see `shapes.chain_expected_output`/
/// `fan_expected_output`/`wide_expected_output`), not just that a run
/// finished: a completed-but-wrong-value run would otherwise silently
/// contaminate the benchmark instead of failing loudly.
import bench_ffi
import counter.{type Counter}
import gleam/int
import gleam/io
import gleam/list
import gleam/string
import saga.{type DefinitionError, type Workflow}
import saga/execution
import shapes.{type BenchError}
import stats.{type Summary}

const sizes = [10, 50, 100, 250, 500, 1000, 2000]

/// If a cell's warm-up exceeds this microsecond budget, report it as skipped
/// and skip larger sizes for that shape.
const max_iteration_us = 60_000_000

/// Measured runs after one separately timed warm-up: 20 below 1000 steps,
/// 15 at larger sizes.
const measured_runs = 20

const measured_runs_huge = 15

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
    define_us: Int,
    warmup_us: Int,
    summary: Summary,
    build_invocations: Int,
    skipped: Bool,
  )
}

pub fn main() {
  io.println("# saga run-cost benchmark\n")
  io.println("All timings in microseconds (µs) unless noted otherwise.\n")

  let bench_input = 7

  let chain_rows =
    bench_shape(fn(n, c) { shapes.chain(n, c) }, bench_input, fn(n, output) {
      output == shapes.chain_expected_output(n, bench_input)
    })
  let fan_rows =
    bench_shape(fn(n, c) { shapes.fan(n, c) }, bench_input, fn(n, output) {
      output == shapes.fan_expected_output(n, bench_input)
    })
  let wide_rows =
    bench_shape(fn(n, c) { shapes.wide(n, c) }, bench_input, fn(n, output) {
      output == shapes.wide_expected_output(n, bench_input)
    })

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
  input: Int,
  output_correct: fn(Int, o) -> Bool,
) -> List(Row) {
  list_fold_until(sizes, [], fn(acc, n) {
    let build_count = counter.new()
    let define_start = bench_ffi.monotonic_time()
    let built = build(n, build_count)
    let define_us = bench_ffi.monotonic_time() - define_start
    // `define` itself invokes the builder once; that invocation is part of
    // this cell's own build-invocation count like any other.
    case built {
      Error(_errors) -> Stop(acc)
      Ok(workflow) -> {
        let run_count = case n >= huge_n_threshold {
          True -> measured_runs_huge
          False -> measured_runs
        }
        let config = execution.config() |> execution.with_max_concurrency(64)
        // One warm-up run, timed and reported separately, then `run_count`
        // measured runs. Every run is checked for correctness, not just
        // timed: a run that fails to complete with the expected output
        // panics the bench immediately rather than silently recording a
        // bogus timing.
        let warmup_start = bench_ffi.monotonic_time()
        assert_run_correct(workflow, input, config, n, output_correct)
        let warmup_us = bench_ffi.monotonic_time() - warmup_start

        case warmup_us > max_iteration_us {
          True ->
            Stop([
              Row(
                n: n,
                define_us: define_us,
                warmup_us: warmup_us,
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
                assert_run_correct(workflow, input, config, n, output_correct)
                bench_ffi.monotonic_time() - start
              })
            let row =
              Row(
                n: n,
                define_us: define_us,
                warmup_us: warmup_us,
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

fn assert_run_correct(
  workflow: Workflow(Int, o, BenchError, Nil),
  input: Int,
  config: execution.Config,
  n: Int,
  output_correct: fn(Int, o) -> Bool,
) -> Nil {
  case execution.run(workflow, input, config) {
    Ok(execution.Completed(output)) ->
      case output_correct(n, output) {
        True -> Nil
        False ->
          panic as "bench: run completed with an unexpected output (see shapes.*_expected_output)"
      }
    other -> {
      io.println(string.inspect(other))
      panic as "bench: run did not complete as expected"
    }
  }
}

// Stops processing larger sizes when the warm-up budget is exceeded.
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
    "| N | define (µs) | warmup (µs) | median (µs) | p95 (µs) | min (µs) | max (µs) | build invocations | measured runs |",
  )
  io.println("|---|---|---|---|---|---|---|---|---|")
  list.each(rows, fn(r) {
    case r.skipped {
      True ->
        io.println(
          "| "
          <> int.to_string(r.n)
          <> " | "
          <> int.to_string(r.define_us)
          <> " | "
          <> int.to_string(r.warmup_us)
          <> " (SKIPPED: exceeded "
          <> int.to_string(max_iteration_us)
          <> "µs budget) | - | - | - | "
          <> int.to_string(r.build_invocations)
          <> " | 0 |",
        )
      False ->
        io.println(
          "| "
          <> int.to_string(r.n)
          <> " | "
          <> int.to_string(r.define_us)
          <> " | "
          <> int.to_string(r.warmup_us)
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
            0 -> "below timer resolution"
            _ -> ratio_string(double_median, median)
          }
          let suffix = case median {
            0 -> ""
            _ -> "x"
          }
          Ok(
            "- "
            <> int.to_string(n)
            <> " -> "
            <> int.to_string(double_n)
            <> ": "
            <> ratio_str
            <> suffix,
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
