/// A minimal binary min-heap of `Int`, used by `saga/internal/coordinator`
/// to admit ready nodes in ascending node-id order (== builder-call order,
/// since `saga.gleam`'s `resolve_addresses` assigns and sorts node ids that
/// way) without re-scanning the whole node order on every admission
/// decision. `insert`/`extract_min` are both O(log n); a run with N nodes
/// therefore does O(N log N) total admission work across its whole
/// lifetime, not O(N) *per* admission decision (O(N^2) total).
///
/// Backed by a `Dict(Int, Int)` (heap index -> value) rather than an array,
/// since Gleam has no native mutable/O(1)-index array; `dict.get`/
/// `dict.insert` on small integer keys are still effectively O(1) on the
/// BEAM's dict implementation for the heap sizes admission ever holds
/// (bounded by how many nodes can be simultaneously ready, never more than
/// the total node count).
import gleam/dict.{type Dict}

pub opaque type MinHeap {
  MinHeap(values: Dict(Int, Int), size: Int)
}

pub fn new() -> MinHeap {
  MinHeap(dict.new(), 0)
}

pub fn is_empty(heap: MinHeap) -> Bool {
  heap.size == 0
}

/// Inserts `value`. Callers must not insert a value already present in the
/// heap (the coordinator never does: a node id is only pushed while it is
/// not already pending admission, guarded by its own `NodeRunState`).
pub fn insert(heap: MinHeap, value: Int) -> MinHeap {
  let index = heap.size
  let values = dict.insert(heap.values, index, value)
  sift_up(MinHeap(values, heap.size + 1), index)
}

/// Removes and returns the smallest value, or `Error(Nil)` if empty.
pub fn extract_min(heap: MinHeap) -> Result(#(Int, MinHeap), Nil) {
  case heap.size {
    0 -> Error(Nil)
    _ -> {
      let assert Ok(min) = dict.get(heap.values, 0)
      let last_index = heap.size - 1
      case last_index {
        0 -> Ok(#(min, MinHeap(dict.new(), 0)))
        _ -> {
          let assert Ok(last) = dict.get(heap.values, last_index)
          let values =
            dict.insert(heap.values, 0, last) |> dict.delete(last_index)
          let heap = sift_down(MinHeap(values, heap.size - 1), 0)
          Ok(#(min, heap))
        }
      }
    }
  }
}

fn sift_up(heap: MinHeap, index: Int) -> MinHeap {
  case index {
    0 -> heap
    _ -> {
      let parent_index = { index - 1 } / 2
      let assert Ok(current) = dict.get(heap.values, index)
      let assert Ok(parent) = dict.get(heap.values, parent_index)
      case current < parent {
        True -> {
          let values =
            heap.values
            |> dict.insert(index, parent)
            |> dict.insert(parent_index, current)
          sift_up(MinHeap(values, heap.size), parent_index)
        }
        False -> heap
      }
    }
  }
}

fn sift_down(heap: MinHeap, index: Int) -> MinHeap {
  let left = 2 * index + 1
  let right = 2 * index + 2
  let assert Ok(current) = dict.get(heap.values, index)
  let #(smallest_index, smallest_value) = #(index, current)
  let #(smallest_index, smallest_value) = case left < heap.size {
    False -> #(smallest_index, smallest_value)
    True -> {
      let assert Ok(left_value) = dict.get(heap.values, left)
      case left_value < smallest_value {
        True -> #(left, left_value)
        False -> #(smallest_index, smallest_value)
      }
    }
  }
  let #(smallest_index, smallest_value) = case right < heap.size {
    False -> #(smallest_index, smallest_value)
    True -> {
      let assert Ok(right_value) = dict.get(heap.values, right)
      case right_value < smallest_value {
        True -> #(right, right_value)
        False -> #(smallest_index, smallest_value)
      }
    }
  }
  case smallest_index == index {
    True -> heap
    False -> {
      let values =
        heap.values
        |> dict.insert(index, smallest_value)
        |> dict.insert(smallest_index, current)
      sift_down(MinHeap(values, heap.size), smallest_index)
    }
  }
}

/// Used only by tests, to assert heap contents irrespective of internal
/// layout.
pub fn to_sorted_list(heap: MinHeap) -> List(Int) {
  to_sorted_list_loop(heap, [])
  |> int_list_reverse
}

fn to_sorted_list_loop(heap: MinHeap, acc: List(Int)) -> List(Int) {
  case extract_min(heap) {
    Error(Nil) -> acc
    Ok(#(value, rest)) -> to_sorted_list_loop(rest, [value, ..acc])
  }
}

fn int_list_reverse(items: List(Int)) -> List(Int) {
  int_list_reverse_acc(items, [])
}

fn int_list_reverse_acc(items: List(Int), acc: List(Int)) -> List(Int) {
  case items {
    [] -> acc
    [first, ..rest] -> int_list_reverse_acc(rest, [first, ..acc])
  }
}
