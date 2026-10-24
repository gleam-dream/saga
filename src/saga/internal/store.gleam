/// Stores completed outputs and action records in a fresh native map for each
/// run. The coordinator owns and threads the Store; concurrent runs never
/// share it. Values use the opaque Native carrier without decoding their shape.
/// See docs/adr/0002-build-once-run-scoped-values.md for the storage decision.
///
/// Soundness of get's identity cast depends on producer-bound typing. A node
/// identity and its native type are bound when perform creates the producer
/// and its opaque Port. That Port's fetch closure reads the same identity at
/// the same type; callers cannot reconstruct a fetch from a bare node id.
/// The coordinator admits a dependent only after its producers commit outputs.
import gleam/dict.{type Dict}

pub opaque type Store {
  Store(values: Dict(Int, Native), records: Dict(Int, Native))
}

/// The type-erased representation held in the map. Never constructed or
/// inspected directly outside `put`/`get` below.
type Native

pub fn new() -> Store {
  Store(dict.new(), dict.new())
}

/// Records a completed value. Must be called at most once per node id per
/// run; a second put overwrites the first.
pub fn put(store: Store, node_id: Int, value: a) -> Store {
  Store(..store, values: dict.insert(store.values, node_id, to_native(value)))
}

/// Reads a committed value as its producer-bound native type. Panics if the
/// value is absent. Dependency readiness and opaque Port construction prevent
/// an absent or differently typed read in a well-formed run.
pub fn get(store: Store, node_id: Int) -> a {
  case dict.get(store.values, node_id) {
    Ok(value) -> from_native(value)
    Error(Nil) ->
      panic as "saga: store.get read a node id with no committed value"
  }
}

@external(erlang, "saga_ffi", "identity")
fn to_native(value: a) -> Native

@external(erlang, "saga_ffi", "identity")
fn from_native(value: Native) -> a

/// The node that binds its input/output types owns both accessors.
pub fn put_record(store: Store, id: Int, value: a) -> Store {
  Store(..store, records: dict.insert(store.records, id, to_native(value)))
}

pub fn get_record(store: Store, id: Int) -> Result(a, Nil) {
  case dict.get(store.records, id) {
    Ok(value) -> Ok(from_native(value))
    Error(Nil) -> Error(Nil)
  }
}
