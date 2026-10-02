//// Defines the storage contract that `saga/durable` saves one execution's
//// checkpoint through.
////
//// Use this module to write a storage adapter, or to name its errors. A
//// `Storage` value addresses one execution and supplies six atomic
//// operations: `create` saves the first record once, `load` reads it,
//// `claim` takes exclusive ownership and advances the ownership
//// generation, `commit` saves new bytes only for the current owner,
//// expected revision and observed cancellation flag, `release` gives up
//// ownership, and `cancel` records cancellation without overwriting
//// progress. An adapter must enforce revisions, cancellation observations
//// and ownership generations atomically; no workflow rules live here.
////
//// `saga/storage/memory` and `saga/storage/file` implement this contract,
//// and `saga/storage/conformance` checks an adapter against it. See
//// DURABILITY.md's "Storage contract" for the full rules.

/// Why a storage operation failed.
pub type Error {
  NotFound
  AlreadyExists
  Busy
  Conflict
  StaleOwner
  CancellationChanged
  Corrupt
  Io(String)
}

/// One saved execution: its revision, ownership generation, cancellation
/// flag and checkpoint bytes.
pub type Record {
  Record(revision: Int, generation: Int, cancelled: Bool, data: BitArray)
}

/// The operations that address one saved execution.
pub type Storage {
  Storage(
    create: fn(BitArray) -> Result(Record, Error),
    load: fn() -> Result(Record, Error),
    claim: fn() -> Result(Record, Error),
    commit: fn(Int, Int, Bool, BitArray) -> Result(Record, Error),
    release: fn(Int) -> Result(Nil, Error),
    cancel: fn() -> Result(Nil, Error),
  )
}
