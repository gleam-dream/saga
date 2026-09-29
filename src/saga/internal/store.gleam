/// The coordinator's per-run value store: one `Dict(Int, Native)` keyed by
/// node id (`Native` is this module's own opaque, type-erased carrier, not
/// `gleam/dynamic.Dynamic` — nothing here ever decodes or inspects a
/// value's shape, it only ever casts a value back to the exact type it was
/// stored as), holding every node's completed output for the lifetime of
/// one run. Before the build-once refactor, a node's per-run output lived
/// in its own single-value mailbox cell (a `Subject`, allocated fresh each
/// time the workflow's graph was built). Once a `Workflow`'s graph is built
/// exactly once, at `define`, the same node closures run across every
/// subsequent run of that definition, so a value holder allocated at build
/// time can no longer be run-scoped -- two concurrent (or successive) runs
/// sharing one `Subject` per node would corrupt or block on each other's
/// writes. A plain `Dict`, freshly created per run and threaded through the
/// coordinator's own state, is run-scoped by construction: nothing here is
/// shared across runs, or across processes beyond the coordinator that owns
/// one run's `Store`. See the design-decisions note in README.md and
/// `bench/RESULTS.md` for why this centralization was worth making (O(N^2)
/// scheduling cost collapsing to linear) despite departing from
/// saga-design.md's original no-central-map stance.
///
/// **Soundness of `get`'s coercion.** `get`'s only unsafe operation in this
/// entire package is a native identity cast from the value that was
/// `put` at a given node id back to the caller's expected type `a`. This is
/// sound *by construction*, not merely by convention: every `Port(a, e, u)`
/// created by `saga.perform`/`saga.map`/`saga.both`/`saga.all`/`define`'s
/// root input closes over one specific node id and is the *only* thing that
/// ever calls `get` for that id, with its own `a`. The node id and its
/// element type `a` are bound together once, in the same `perform` call
/// that both creates the id and returns the `Port(a, e, u)` whose `fetch`
/// reads it back -- there is no path by which a `Port(a, ..)` for one node
/// id can be exchanged for a differently-typed read of the same id, because
/// `Port` is opaque and its `fetch` closure is built alongside the `put`
/// call it corresponds to, never reconstructed from a bare `Int`. The same
/// invariant already justified the previous per-node `Subject`-per-node
/// mailbox-cell design (typed at allocation); `Store` only changes *where*
/// the value lives (a run-scoped map instead of a build-time mailbox), not
/// who is allowed to read it as what type.
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

/// Records `node_id`'s completed value. Must be called at most once per
/// node id per run (the coordinator only ever commits a node's output
/// once); a second `put` for the same id overwrites the first.
pub fn put(store: Store, node_id: Int, value: a) -> Store {
  Store(..store, values: dict.insert(store.values, node_id, to_native(value)))
}

/// Reads back `node_id`'s value, coerced to the caller's expected type `a`.
/// Panics if nothing was ever `put` at this id -- which never happens for a
/// well-formed run, since the coordinator only admits a node once every one
/// of its dependencies has already committed a value (see
/// `saga/internal/coordinator`'s admission loop), and every dependency read
/// goes through a `Port` created for that exact id. See this module's doc
/// comment for why the coercion itself is sound.
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
