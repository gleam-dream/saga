//// Stores one execution's checkpoint in a file, so a durable run can be
//// recovered in a fresh VM.
////
//// Use this adapter when a `saga/durable` run must survive VM shutdown and
//// one VM at a time accesses the file. It writes a synced temporary file
//// and atomically renames it over the path, and keeps ownership locks
//// within the VM. Paths must be canonical absolute paths, and the caller
//// creates the parent directory. Concurrent VMs, path aliases and
//// power-loss durability of the directory are unsupported. Use
//// `saga/storage/memory` when a run need not outlive the VM.
////
//// ```gleam
//// import saga/storage/file
////
//// let storage = file.open("/var/lib/shop/checkout-123.saga")
//// ```

import saga/storage.{type Storage}

/// Returns the `Storage` for the execution saved at `path`. Opening does no
/// I/O; each operation reads or writes the file.
pub fn open(path: String) -> Storage {
  storage.Storage(
    create: fn(data) { create(path, data) },
    load: fn() { load(path) },
    claim: fn() { claim(path) },
    commit: fn(token, revision, cancelled, data) {
      commit(path, token, revision, cancelled, data)
    },
    release: fn(token) { release(path, token) },
    cancel: fn() { cancel(path) },
  )
}

@external(erlang, "saga_storage", "file_create")
fn create(path: String, data: BitArray) -> Result(storage.Record, storage.Error)

@external(erlang, "saga_storage", "file_load")
fn load(path: String) -> Result(storage.Record, storage.Error)

@external(erlang, "saga_storage", "file_claim")
fn claim(path: String) -> Result(storage.Record, storage.Error)

@external(erlang, "saga_storage", "file_commit")
fn commit(
  path: String,
  token: Int,
  revision: Int,
  cancelled: Bool,
  data: BitArray,
) -> Result(storage.Record, storage.Error)

@external(erlang, "saga_storage", "file_release")
fn release(path: String, token: Int) -> Result(Nil, storage.Error)

@external(erlang, "saga_storage", "file_cancel")
fn cancel(path: String) -> Result(Nil, storage.Error)
