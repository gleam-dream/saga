import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/time/duration
import gleeunit/should
import saga/storage
import saga/storage/conformance
import saga/storage/file
import saga/storage/memory
import support/stores

@external(erlang, "saga_ffi", "unique_integer")
fn unique() -> Int

@external(erlang, "saga_test_files", "fresh_directory")
fn fresh_directory(path: String) -> Nil

@external(erlang, "saga_test_files", "remove_directory")
fn remove_directory(path: String) -> Nil

fn memory_fixture() -> Result(conformance.Fixture, String) {
  let assert Ok(store) = memory.start()
  Ok(
    conformance.fixture(memory.storage(store), cleanup: fn() {
      memory.stop(store)
    }),
  )
}

pub fn memory_adapter_conformance_test() {
  conformance.run(
    memory_fixture,
    timeout: duration.seconds(5),
    owner_loss_within: duration.milliseconds(200),
  )
  |> should.equal(Ok(Nil))
}

pub fn file_adapter_conformance_test() {
  conformance.run(
    fn() {
      let directory = "/tmp/saga-conformance-" <> int.to_string(unique())
      fresh_directory(directory)
      Ok(
        conformance.fixture(file.open(directory), cleanup: fn() {
          remove_directory(directory)
        }),
      )
    },
    timeout: duration.seconds(5),
    owner_loss_within: duration.milliseconds(200),
  )
  |> should.equal(Ok(Nil))
}

/// A storage whose release ignores ownership is caught.
pub fn conformance_detects_broken_ownership_test() {
  conformance.run(
    fn() {
      let assert Ok(store) = memory.start()
      let backend = memory.storage(store)
      let broken =
        storage.new(
          create: fn(id, records) { storage.do_create(backend, id, records) },
          load: fn(id) { storage.do_load(backend, id) },
          claim: fn(id) { storage.do_claim(backend, id) },
          commit: fn(claim, commit) {
            storage.do_commit(backend, claim, commit)
          },
          release: fn(_) { Ok(Nil) },
          cancel: fn(id) { storage.do_cancel(backend, id) },
          list: fn(listing) { storage.do_list(backend, listing) },
        )
      Ok(conformance.fixture(broken, cleanup: fn() { memory.stop(store) }))
    },
    timeout: duration.seconds(5),
    owner_loss_within: duration.milliseconds(200),
  )
  |> should.equal(Error(conformance.UnexpectedResult("forged release")))
}

/// A storage that accepts a rebuilt claim with another token is caught:
/// a generation alone must grant no ownership.
pub fn conformance_detects_forgeable_claims_test() {
  conformance.run(
    fn() {
      let assert Ok(store) = memory.start()
      let backend = memory.storage(store)
      let forgeable =
        storage.new(
          create: fn(id, records) { storage.do_create(backend, id, records) },
          load: fn(id) { storage.do_load(backend, id) },
          claim: fn(id) {
            let result = storage.do_claim(backend, id)
            case result {
              Ok(#(claim, _)) -> remember_claim(id, claim)
              Error(_) -> Nil
            }
            result
          },
          // Commits with the genuine claim whatever token it was given.
          commit: fn(claim, commit) {
            case genuine_claim(storage.claim_id(claim)) {
              Ok(genuine) ->
                case
                  storage.claim_generation(genuine)
                  == storage.claim_generation(claim)
                {
                  True -> storage.do_commit(backend, genuine, commit)
                  False -> storage.do_commit(backend, claim, commit)
                }
              Error(Nil) -> storage.do_commit(backend, claim, commit)
            }
          },
          release: fn(claim) { storage.do_release(backend, claim) },
          cancel: fn(id) { storage.do_cancel(backend, id) },
          list: fn(listing) { storage.do_list(backend, listing) },
        )
      Ok(conformance.fixture(forgeable, cleanup: fn() { memory.stop(store) }))
    },
    timeout: duration.seconds(5),
    owner_loss_within: duration.milliseconds(200),
  )
  |> should.equal(
    Error(conformance.UnexpectedResult("a generation alone grants no ownership")),
  )
}

@external(erlang, "saga_test_files", "remember_claim")
fn remember_claim(id: String, claim: storage.Claim) -> Nil

@external(erlang, "saga_test_files", "genuine_claim")
fn genuine_claim(id: String) -> Result(storage.Claim, Nil)

/// An adapter that never notices a lost owner fails within the declared
/// window instead of hanging.
pub fn conformance_detects_a_claim_that_never_ends_test() {
  conformance.run(
    fn() {
      let assert Ok(store) = memory.start()
      let backend = memory.storage(store)
      // Claims from a process that never exits, so a claim never ends.
      let sticky = stores.with_claim(backend, process_claim(backend, _))
      Ok(conformance.fixture(sticky, cleanup: fn() { memory.stop(store) }))
    },
    timeout: duration.seconds(5),
    owner_loss_within: duration.milliseconds(100),
  )
  |> should.equal(
    Error(conformance.UnexpectedResult(
      "reclaim within owner_loss_within after owner loss",
    )),
  )
}

pub fn conformance_rejects_invalid_windows_test() {
  conformance.run(
    memory_fixture,
    timeout: duration.milliseconds(0),
    owner_loss_within: duration.milliseconds(100),
  )
  |> should.equal(Error(conformance.InvalidTimeout))
  conformance.run(
    memory_fixture,
    timeout: duration.milliseconds(100),
    owner_loss_within: duration.milliseconds(0),
  )
  |> should.equal(Error(conformance.InvalidTimeout))
}

@external(erlang, "saga_test_files", "claim_from_immortal")
fn process_claim(
  storage: storage.Storage,
  id: String,
) -> Result(#(storage.Claim, storage.Stored), storage.Error)

/// A file load reads one consistent checkpoint while commits replace its
/// records: it never finds a record file that a concurrent commit removed.
pub fn file_load_is_consistent_with_concurrent_commits_test() {
  let directory = "/tmp/saga-load-race-" <> int.to_string(unique())
  fresh_directory(directory)
  let store = file.open(directory)
  // A load of many records spans several commits, each of which replaces
  // some of those records and removes their earlier files.
  let records = fn(n, count) {
    int.range(from: 0, to: count, with: dict.new(), run: fn(records, key) {
      dict.insert(records, int.to_string(key), <<n:size(32)>>)
    })
  }
  let record = fn(n) { records(n, 1000) }
  let assert Ok(Nil) = storage.do_create(store, "race", record(0))
  let assert Ok(#(claim, _)) = storage.do_claim(store, "race")
  let failures = process.new_subject()
  let stopper = process.new_subject()
  process.spawn(fn() {
    let stop = process.new_subject()
    process.send(stopper, stop)
    process.send(failures, load_until(store, stop, []))
  })
  let assert Ok(stop) = process.receive(stopper, 1000)
  int.range(from: 0, to: 100, with: Nil, run: fn(_, n) {
    let assert Ok(Nil) =
      storage.do_commit(
        store,
        claim,
        storage.Commit(
          expected_revision: n,
          observed_cancelled: False,
          phase: storage.Pending,
          records: records(n + 1, 20),
        ),
      )
    Nil
  })
  process.send(stop, Nil)
  process.receive(failures, 5000) |> should.equal(Ok([]))
  remove_directory(directory)
}

/// Loads "race" until told to stop and returns every failed load.
fn load_until(
  store: storage.Storage,
  stop: process.Subject(Nil),
  seen: List(storage.Error),
) -> List(storage.Error) {
  case process.receive(stop, 0) {
    Ok(Nil) -> seen
    Error(Nil) ->
      case storage.do_load(store, "race") {
        Ok(_) -> load_until(store, stop, seen)
        Error(error) -> load_until(store, stop, [error, ..seen])
      }
  }
}
