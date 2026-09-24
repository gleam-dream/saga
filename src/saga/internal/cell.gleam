/// An append-only list accumulator (`Registry`), synchronously owned by the
/// single process evaluating one `Workflow` builder (the caller during
/// `define`'s one evaluation). Used to track every node `perform` creates
/// during that one evaluation — including ones later discarded by the
/// builder (never merged into the final output port) — so orphaned steps
/// can be reported at `define` time. Built on the mailbox-as-mutable-cell
/// trick: a `Subject` owned by the calling process, holding the
/// accumulator's current value as the one message in its own mailbox,
/// read-modify-written on each `register`.
///
/// This module used to also provide `Cell` (a single-value version of the
/// same trick, `new`/`write`/`read`), which every node's per-run output
/// once lived in. Per-run values now live in a single, run-scoped
/// `saga/internal/store.Store` instead (see that module's doc comment for
/// why), so `Cell` had no remaining caller and was removed; `Registry`
/// alone remains, since it is still how `define` accumulates the nodes one
/// builder evaluation creates.
import gleam/erlang/process.{type Subject}

/// An append-only list accumulator, synchronously owned by the single
/// process evaluating one `Workflow` builder (the caller during `define`'s
/// one evaluation). Used to track every node created during that one
/// evaluation — including ones later discarded by the builder (never
/// merged into the final output port) — so orphaned steps can be reported.
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
/// the registry's owning process. Leaves the registry unchanged so a
/// still-in-scope registry may keep being registered into — see `close`,
/// which is what actually retires a registry once its owning evaluation is
/// done.
pub fn all_registered(registry: Registry(a)) -> List(a) {
  let existing = process.receive_forever(registry.subject)
  process.send(registry.subject, existing)
  list_reverse(existing)
}

/// Retires a registry once its owning builder evaluation is fully done
/// (after `all_registered` has already been read for the last time):
/// drains its one outstanding message so it is not left sitting in the
/// owning process's mailbox forever — the registry's whole purpose is to be
/// written to and read *during* `define`'s one evaluation, so its owning
/// process (typically the long-lived caller of `define`) needs this
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
