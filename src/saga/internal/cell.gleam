/// Collects every node created during one workflow definition, including
/// nodes absent from the final output, so define can report orphaned steps.
/// The builder's process owns the Registry. Its Subject retains one message
/// containing the current list; register replaces that message synchronously.
/// Run outputs belong to saga/internal/store, independently of this registry.
import gleam/erlang/process.{type Subject}

/// An append-only node registry owned by the process evaluating the builder.
pub type Registry(a) {
  Registry(subject: Subject(List(a)))
}

/// Allocates a fresh, empty registry owned by the calling process.
pub fn new_registry() -> Registry(a) {
  let registry = Registry(process.new_subject())
  process.send(registry.subject, [])
  registry
}

/// Registers item. Must be called by the registry's owning process.
/// A foreign Port can refer to a closed registry. Reseeding an empty registry
/// avoids blocking while definition validation reports ForeignPort; an orphan
/// check against that foreign registry would not describe the current builder.
pub fn register(registry: Registry(a), item: a) -> Nil {
  case process.receive(registry.subject, 0) {
    Ok(existing) -> process.send(registry.subject, [item, ..existing])
    Error(_) -> process.send(registry.subject, [item])
  }
}

/// Returns items in registration order without closing the registry.
/// Must be called by the registry's owning process.
pub fn all_registered(registry: Registry(a)) -> List(a) {
  let existing = process.receive_forever(registry.subject)
  process.send(registry.subject, existing)
  list_reverse(existing)
}

/// Drains the registry after the builder's final read, so its message does
/// not remain in the caller's mailbox. Safe even when no node was registered:
/// new_registry seeds the empty-list message.
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
