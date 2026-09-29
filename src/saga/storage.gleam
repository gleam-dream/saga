/// Storage for one execution. Adapters must atomically enforce revisions,
/// cancellation observations, and ownership tokens. No workflow rules live here.
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

pub type Record {
  Record(revision: Int, generation: Int, cancelled: Bool, data: BitArray)
}

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
