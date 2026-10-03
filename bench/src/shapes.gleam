/// The three workflow shapes benchmarked: a sequential chain, a
/// fan-in/fan-out (one producer, N parallel consumers, combined with
/// `saga.all`), and wide dependency reads (each step reads several earlier
/// outputs via `saga.both`). Every step does trivial work (returns an Int)
/// so the measured cost is scheduling/bookkeeping overhead, not step work.
///
/// Every shape's builder closure increments `build_count` (a `Counter`)
/// each time it runs, so a bench run can report how many times the
/// workflow's build function was actually invoked across many runs of the
/// same definition -- the number this whole benchmark exists to drive down.
import counter.{type Counter}
import gleam/int
import gleam/list
import saga.{type Workflow}

pub type BenchError {
  BenchError(String)
}

/// `gleam_stdlib` in this lockfile has no `list.range`; this is the bench
/// package's own tiny inclusive-range helper (`from` and `to` both
/// inclusive, empty if `from > to`).
fn range(from: Int, to: Int) -> List(Int) {
  range_acc(from, to, [])
}

fn range_acc(from: Int, to: Int, acc: List(Int)) -> List(Int) {
  case from > to {
    True -> list.reverse(acc)
    False -> range_acc(from, to - 1, [to, ..acc])
  }
}

/// The output a `chain(n, _)` run must produce for input `x`, so the bench
/// harness can assert correctness (not just completion) on every timed run.
pub fn chain_expected_output(n: Int, input: Int) -> Int {
  input + n
}

/// The output a `fan(n, _)` run must produce for input `x`: consumer `i`
/// (1-indexed) returns `x + i`, and `saga.all` combines them in that order.
pub fn fan_expected_output(n: Int, input: Int) -> List(Int) {
  range(1, n) |> list.map(fn(i) { input + i })
}

/// The output a `wide(n, _)` run must produce for input `x`. Mirrors the
/// builder's own arithmetic and fold shape line for line (root `i` returns
/// `x + i`; step `i` beyond the root window sums its 4-wide window and
/// adds `i`; the fold's own final accumulator, not an independently
/// re-derived running total, is what `saga.all` actually combines below),
/// so it exists to catch a *regression* between what this shape computes
/// and what a real run returns, not to double as an independent
/// specification of what "wide" should compute. `produced` deliberately
/// ends up as just the final iteration's `[step_port, ..window]` (5
/// elements, not growing with N) because that is what the actual builder
/// below folds into and returns via `saga.all` -- the fold's second
/// accumulator slot is computed but never threaded forward, in both the
/// real builder and this mirror.
pub fn wide_expected_output(n: Int, input: Int) -> List(Int) {
  let window = 4
  let roots = range(1, window) |> list.map(fn(i) { input + i })
  let #(_prior, produced) =
    range(window + 1, n)
    |> list.fold(#(roots, roots), fn(acc, i) {
      let #(prior, _all_ports) = acc
      let value = list.fold(prior, 0, fn(a, b) { a + b }) + i
      let next_window = list.append(list.drop(prior, 1), [value])
      #(next_window, [value, ..prior])
    })
  produced
}

/// N sequential steps, each reading the previous step's output and adding
/// one to it.
pub fn chain(
  n: Int,
  build_count: Counter,
) -> Result(Workflow(Int, Int, BenchError, Nil), List(saga.DefinitionError)) {
  saga.try_define("chain_" <> int.to_string(n), fn(input) {
    counter.increment(build_count)
    range(1, n)
    |> list.fold(input, fn(port, i) {
      saga.perform(
        port,
        saga.step("step_" <> int.to_string(i), fn(x: Int) { Ok(x + 1) }),
      )
    })
  })
}

/// One shared producer, read by N parallel steps, combined back with
/// `saga.all`. Exercises fan-out (N steps depending on the same node) and
/// fan-in (`all` depending on all N).
pub fn fan(
  n: Int,
  build_count: Counter,
) -> Result(
  Workflow(Int, List(Int), BenchError, Nil),
  List(saga.DefinitionError),
) {
  saga.try_define("fan_" <> int.to_string(n), fn(input) {
    counter.increment(build_count)
    let produced =
      saga.perform(input, saga.step("produce", fn(x: Int) { Ok(x) }))
    let consumers =
      range(1, n)
      |> list.map(fn(i) {
        saga.perform(
          produced,
          saga.step("consume_" <> int.to_string(i), fn(x: Int) { Ok(x + i) }),
        )
      })
    case consumers {
      [first, ..rest] -> saga.all(first, rest)
      [] -> panic as "bench: fan(0) has no consumers"
    }
  })
}

/// N steps each reading several earlier outputs (a small fixed-size window
/// of previous steps) via `saga.both`, then combined with `saga.all`. Not a
/// chain (steps do not depend on their immediate predecessor alone) and not
/// a single fan-in (each step depends on a *window* of prior steps, not the
/// same one shared producer), so this exercises a wider dependency graph
/// shape than either of the other two.
pub fn wide(
  n: Int,
  build_count: Counter,
) -> Result(
  Workflow(Int, List(Int), BenchError, Nil),
  List(saga.DefinitionError),
) {
  let window = 4
  saga.try_define("wide_" <> int.to_string(n), fn(input) {
    counter.increment(build_count)
    let roots =
      range(1, window)
      |> list.map(fn(i) {
        saga.perform(
          input,
          saga.step("root_" <> int.to_string(i), fn(x: Int) { Ok(x + i) }),
        )
      })
    let #(_prior, produced) =
      range(window + 1, n)
      |> list.fold(#(roots, roots), fn(acc, i) {
        let #(prior, _all_ports) = acc
        // Read this step's small fixed window of prior outputs (the last
        // `window` ports produced), combining pairs with `both`.
        let combined = case prior {
          [first, ..rest] ->
            list.fold(rest, saga.map(first, fn(a) { [a] }), fn(sofar, port) {
              saga.both(sofar, port)
              |> saga.map(fn(pair) { list.append(pair.0, [pair.1]) })
            })
          [] -> panic as "bench: wide window is empty"
        }
        let step_port =
          saga.perform(
            combined,
            saga.step("wide_" <> int.to_string(i), fn(values: List(Int)) {
              Ok(list.fold(values, 0, fn(a, b) { a + b }) + i)
            }),
          )
        let next_window = list.append(list.drop(prior, 1), [step_port])
        #(next_window, [step_port, ..prior])
      })
    case produced {
      [first, ..rest] -> saga.all(first, rest)
      [] -> panic as "bench: wide(n) has no produced steps"
    }
  })
}
