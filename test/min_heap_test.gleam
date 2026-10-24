import gleeunit/should
import saga/internal/min_heap

pub fn empty_heap_extracts_nothing_test() {
  min_heap.new() |> min_heap.extract_min |> should.equal(Error(Nil))
}

pub fn single_value_round_trips_test() {
  let heap = min_heap.new() |> min_heap.insert(42)
  let assert Ok(#(value, rest)) = min_heap.extract_min(heap)
  value |> should.equal(42)
  min_heap.is_empty(rest) |> should.equal(True)
}

pub fn extracts_in_ascending_order_test() {
  let heap =
    min_heap.new()
    |> min_heap.insert(5)
    |> min_heap.insert(3)
    |> min_heap.insert(9)
    |> min_heap.insert(1)
    |> min_heap.insert(7)
    |> min_heap.insert(3)

  min_heap.to_sorted_list(heap) |> should.equal([1, 3, 3, 5, 7, 9])
}

pub fn interleaved_insert_and_extract_preserves_order_test() {
  let heap = min_heap.new() |> min_heap.insert(10) |> min_heap.insert(20)
  let assert Ok(#(first, heap)) = min_heap.extract_min(heap)
  first |> should.equal(10)
  let heap = heap |> min_heap.insert(5) |> min_heap.insert(30)
  min_heap.to_sorted_list(heap) |> should.equal([5, 20, 30])
}

pub fn large_reverse_insert_extracts_ascending_test() {
  let values = reverse_range(500, 1)
  let heap =
    values
    |> fold_insert(min_heap.new())
  min_heap.to_sorted_list(heap) |> should.equal(range(1, 500))
}

fn reverse_range(from: Int, to: Int) -> List(Int) {
  reverse_range_acc(from, to, [])
}

fn reverse_range_acc(from: Int, to: Int, acc: List(Int)) -> List(Int) {
  case from < to {
    True -> acc
    False -> reverse_range_acc(from - 1, to, [from, ..acc])
  }
}

fn range(from: Int, to: Int) -> List(Int) {
  range_acc(to, from, [])
}

fn range_acc(from: Int, to: Int, acc: List(Int)) -> List(Int) {
  case from < to {
    True -> acc
    False -> range_acc(from - 1, to, [from, ..acc])
  }
}

fn fold_insert(values: List(Int), heap: min_heap.MinHeap) -> min_heap.MinHeap {
  case values {
    [] -> heap
    [first, ..rest] -> fold_insert(rest, min_heap.insert(heap, first))
  }
}
