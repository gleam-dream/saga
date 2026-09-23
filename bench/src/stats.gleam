/// Simple descriptive statistics over a list of millisecond durations:
/// median and p95 (nearest-rank), used to summarize many runs of one bench
/// shape/N without pulling in a stats dependency.
import gleam/int
import gleam/list

pub type Summary {
  Summary(median: Int, p95: Int, min: Int, max: Int, n: Int)
}

pub fn summarize(samples: List(Int)) -> Summary {
  let sorted = list.sort(samples, int.compare)
  let count = list.length(sorted)
  Summary(
    median: percentile(sorted, count, 50),
    p95: percentile(sorted, count, 95),
    min: first_or_zero(sorted),
    max: first_or_zero(list.reverse(sorted)),
    n: count,
  )
}

fn first_or_zero(values: List(Int)) -> Int {
  case values {
    [first, ..] -> first
    [] -> 0
  }
}

/// Nearest-rank percentile over an already-sorted list.
fn percentile(sorted: List(Int), count: Int, p: Int) -> Int {
  case count {
    0 -> 0
    _ -> {
      let rank = { p * count + 99 } / 100
      let index = int.max(0, int.min(count - 1, rank - 1))
      case list.drop(sorted, index) {
        [value, ..] -> value
        [] -> 0
      }
    }
  }
}
