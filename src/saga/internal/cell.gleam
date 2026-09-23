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
