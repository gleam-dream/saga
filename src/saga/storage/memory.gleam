//// Stores one execution's checkpoint in memory, in a dedicated process.
////
//// Use this adapter for tests and for `saga/durable` runs that must survive
//// the loss of their runner but not of the VM. `new` spawns an unlinked
//// process that holds the record; it outlives the runners that use it and
//// stops only when `close` is called or the VM stops. Use
//// `saga/storage/file` when a run must survive VM shutdown.
////
//// ```gleam
//// import saga/storage/memory
////
//// let memory = memory.new()
//// let storage = memory.storage(memory)
//// // ... durable.start_or_reconnect(storage, ..) and durable.drive(storage, ..)
//// memory.close(memory)
//// ```

import saga/storage.{type Storage}

/// The process that holds one in-memory record.
pub type Memory

/// Spawns an unlinked process holding no record.
@external(erlang, "saga_storage", "memory_new")
pub fn new() -> Memory

/// Stops the process and discards its record.
@external(erlang, "saga_storage", "memory_close")
pub fn close(memory: Memory) -> Nil

/// Returns the `Storage` operations for this memory's record.
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
