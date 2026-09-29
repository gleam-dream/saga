/// A reference file adapter. One Erlang VM may access a path at a time.
/// Uses synced writes and atomic replacement; paths must be canonical absolute
/// paths. Cross-VM writers and power-loss directory durability are unsupported.
import saga/storage.{type Storage}

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
