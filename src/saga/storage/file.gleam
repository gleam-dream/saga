//// Stores executions' checkpoints as files in one directory, so a durable
//// run can be recovered in a fresh VM.
////
//// Use this adapter when a `saga/durable` run must survive VM shutdown and
//// one VM at a time uses the directory. Each execution is a subdirectory,
//// named after its id, with one file per record and a head file that
//// indexes them. A commit writes its records to new synced files, then
//// atomically renames a new head file over the old one, so it writes only
//// the records that changed. A load takes no lock: when a concurrent commit
//// removed a record file it needed, it reads the new head and the files it
//// lacks, so it returns one committed revision. A claim lasts while the
//// claiming process lives in this VM; a file written by an earlier VM is
//// unowned, so a fresh VM can resume it at once. The directory must be a canonical absolute
//// path, and the caller creates it. Concurrent VMs, path aliases and
//// power-loss durability of the directory are unsupported; use the
//// `saga_postgres` package to share executions across VMs.
////
//// ```gleam
//// import saga/storage/file
////
//// let storage = file.open("/var/lib/shop/checkouts")
//// ```

import gleam/dict.{type Dict}
import saga/storage.{type Storage}

/// Returns the `Storage` for the executions saved in `directory`. Opening
/// does no I/O; each operation reads or writes one execution's files.
pub fn open(directory: String) -> Storage {
  storage.new(
    create: fn(id, records) { create(directory, id, records) },
    load: fn(id) { load(directory, id) },
    claim: fn(id) { claim(directory, id) },
    commit: fn(owner, change) { commit(directory, owner, change) },
    release: fn(claim) { release(directory, claim) },
    cancel: fn(id) { cancel(directory, id) },
    list: fn(listing) { list(directory, listing) },
  )
}

@external(erlang, "saga_storage", "file_create")
fn create(
  directory: String,
  id: String,
  records: Dict(String, BitArray),
) -> Result(Nil, storage.Error)

@external(erlang, "saga_storage", "file_load")
fn load(directory: String, id: String) -> Result(storage.Stored, storage.Error)

@external(erlang, "saga_storage", "file_claim")
fn claim(
  directory: String,
  id: String,
) -> Result(#(storage.Claim, storage.Stored), storage.Error)

@external(erlang, "saga_storage", "file_commit")
fn commit(
  directory: String,
  claim: storage.Claim,
  commit: storage.Commit,
) -> Result(Nil, storage.Error)

@external(erlang, "saga_storage", "file_release")
fn release(
  directory: String,
  claim: storage.Claim,
) -> Result(Nil, storage.Error)

@external(erlang, "saga_storage", "file_cancel")
fn cancel(directory: String, id: String) -> Result(Nil, storage.Error)

@external(erlang, "saga_storage", "file_list")
fn list(
  directory: String,
  listing: storage.Listing,
) -> Result(List(String), storage.Error)
