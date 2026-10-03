//// Storage wrappers for durable tests: each delegates to a real adapter
//// and changes one operation.

import gleam/erlang/process
import saga/storage.{type Claim, type Commit, type Storage, type Stored}

pub fn with_claim(
  backend: Storage,
  claim: fn(String) -> Result(#(Claim, Stored), storage.Error),
) -> Storage {
  rebuild(backend, claim, fn(owner, change) {
    storage.do_commit(backend, owner, change)
  })
}

pub fn with_commit(
  backend: Storage,
  commit: fn(Claim, Commit) -> Result(Stored, storage.Error),
) -> Storage {
  rebuild(backend, fn(id) { storage.do_claim(backend, id) }, commit)
}

/// Reports the process that claims (the runner) to `owner`.
pub fn watched(
  backend: Storage,
  owner: process.Subject(process.Pid),
) -> Storage {
  with_claim(backend, fn(id) {
    let result = storage.do_claim(backend, id)
    case result {
      Ok(_) -> process.send(owner, process.self())
      Error(_) -> Nil
    }
    result
  })
}

fn rebuild(
  backend: Storage,
  claim: fn(String) -> Result(#(Claim, Stored), storage.Error),
  commit: fn(Claim, Commit) -> Result(Stored, storage.Error),
) -> Storage {
  storage.new(
    create: fn(id, data) { storage.do_create(backend, id, data) },
    load: fn(id) { storage.do_load(backend, id) },
    claim: claim,
    commit: commit,
    release: fn(owner) { storage.do_release(backend, owner) },
    cancel: fn(id) { storage.do_cancel(backend, id) },
    unfinished: fn(limit) { storage.do_unfinished(backend, limit) },
  )
}
