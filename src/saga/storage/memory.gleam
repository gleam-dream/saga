/// An in-memory adapter owned by a dedicated process. It outlives execution
/// workers, but not the VM. Dropping it explicitly releases its resources.
import saga/storage.{type Storage}

pub type Memory

@external(erlang, "saga_storage", "memory_new")
pub fn new() -> Memory

@external(erlang, "saga_storage", "memory_close")
pub fn close(memory: Memory) -> Nil

pub fn storage(memory: Memory) -> Storage {
  storage.Storage(
    create: fn(data) { create(memory, data) },
    load: fn() { load(memory) },
    claim: fn() { claim(memory) },
    commit: fn(token, revision, cancelled, data) {
      commit(memory, token, revision, cancelled, data)
    },
    release: fn(token) { release(memory, token) },
    cancel: fn() { cancel(memory) },
  )
}

@external(erlang, "saga_storage", "memory_create")
fn create(
  memory: Memory,
  data: BitArray,
) -> Result(storage.Record, storage.Error)

@external(erlang, "saga_storage", "memory_load")
fn load(memory: Memory) -> Result(storage.Record, storage.Error)

@external(erlang, "saga_storage", "memory_claim")
fn claim(memory: Memory) -> Result(storage.Record, storage.Error)

@external(erlang, "saga_storage", "memory_commit")
fn commit(
  memory: Memory,
  token: Int,
  revision: Int,
  cancelled: Bool,
  data: BitArray,
) -> Result(storage.Record, storage.Error)

@external(erlang, "saga_storage", "memory_release")
fn release(memory: Memory, token: Int) -> Result(Nil, storage.Error)

@external(erlang, "saga_storage", "memory_cancel")
fn cancel(memory: Memory) -> Result(Nil, storage.Error)
