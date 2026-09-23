/// A per-node mailbox cell: a `Subject(a)` owned by whichever process
/// evaluates a `Workflow`'s builder (the caller during `define`'s dry run,
/// the coordinator during a real run). Writing sends the value to the
/// owning process's own mailbox; reading is a selective receive that leaves
/// the message in place so every dependent read observes the same value.
import gleam/erlang/process.{type Subject}

pub type Cell(a) {
  Cell(subject: Subject(a))
}

/// Allocates a fresh cell owned by the calling process. Nothing is sent
/// until `write` is called; during a `define`-time dry run this subject is
/// never written to or read from and is simply discarded.
pub fn new() -> Cell(a) {
  Cell(process.new_subject())
}

/// Writes `value` into the cell. Must be called by the cell's owning
/// process. Only one write per cell is expected per run.
pub fn write(cell: Cell(a), value: a) -> Nil {
  process.send(cell.subject, value)
}

/// Reads the value from the cell without consuming it, so that multiple
/// dependents can each read the same completed value. Must be called by the
/// cell's owning process, strictly after `write`.
pub fn read(cell: Cell(a)) -> a {
  let value = process.receive_forever(cell.subject)
  process.send(cell.subject, value)
  value
}

// ---------------------------------------------------------------------------
// Registry: an append-only accumulator for one builder evaluation.
// ---------------------------------------------------------------------------

/// An append-only list accumulator, synchronously owned by the single
/// process evaluating one `Workflow` builder (the caller during `define`'s
/// dry run, the coordinator during `for_run`). Used to track every node
/// created during that one evaluation — including ones later discarded by
/// the builder (never merged into the final output port) — so orphaned
/// steps can be reported. Built on the same mailbox trick as `Cell`, but
/// mutated (read-modify-write) rather than written once.
pub type Registry(a) {
  Registry(subject: Subject(List(a)))
}

/// Allocates a fresh, empty registry owned by the calling process.
pub fn new_registry() -> Registry(a) {
  let registry = Registry(process.new_subject())
  process.send(registry.subject, [])
  registry
}

/// Appends `item` to the registry. Must be called by the registry's owning
/// process. A registration arriving after `close` has already drained this
/// registry (only possible through a foreign-port misuse — a `Port`, and
/// therefore its scope's registry, captured from one `define` evaluation
/// and fed into a different one, which `perform` itself flags as
/// `ForeignPort`) finds nothing to receive; rather than block forever, it
/// silently re-seeds the registry with just this one item, since the
/// eventual `ForeignPort` definition error already reports the real
/// problem and an orphan check against a foreign, already-closed registry
/// would not be meaningful anyway.
pub fn register(registry: Registry(a), item: a) -> Nil {
  case process.receive(registry.subject, 0) {
    Ok(existing) -> process.send(registry.subject, [item, ..existing])
    Error(_) -> process.send(registry.subject, [item])
  }
}

/// Every item registered so far, in registration order. Must be called by
/// the registry's owning process. Leaves the registry unchanged (like
/// `Cell.read`) so a still-in-scope registry may keep being registered
/// into — see `close`, which is what actually retires a registry once its
/// owning evaluation is done.
pub fn all_registered(registry: Registry(a)) -> List(a) {
  let existing = process.receive_forever(registry.subject)
  process.send(registry.subject, existing)
  list_reverse(existing)
}

/// Retires a registry once its owning builder evaluation is fully done
/// (after `all_registered` has already been read for the last time):
/// drains its one outstanding message so it is not left sitting in the
/// owning process's mailbox forever. Unlike `Cell`, whose `define`-time
/// subjects are never written to at all (a dry run never calls
/// `write`/`read`), a registry's whole purpose is to be written to and
/// read *during* `define`'s dry run (to catch orphaned steps), so its
/// owning process — typically the long-lived caller of `define`, not a
/// coordinator that exits and takes its mailbox with it — needs this
/// explicit cleanup. Safe to call even if nothing was ever registered (the
/// registry always holds exactly one message, written by `new_registry`).
pub fn close(registry: Registry(a)) -> Nil {
  let _ = process.receive(registry.subject, 0)
  Nil
}

fn list_reverse(items: List(a)) -> List(a) {
  list_reverse_acc(items, [])
}

fn list_reverse_acc(items: List(a), acc: List(a)) -> List(a) {
  case items {
    [] -> acc
    [first, ..rest] -> list_reverse_acc(rest, [first, ..acc])
  }
}
