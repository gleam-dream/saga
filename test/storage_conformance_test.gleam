import gleam/int
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
  conformance.run(memory_fixture, timeout: 5000, owner_loss_within: 200)
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
    timeout: 5000,
    owner_loss_within: 200,
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
          create: fn(id, data) { storage.do_create(backend, id, data) },
          load: fn(id) { storage.do_load(backend, id) },
          claim: fn(id) { storage.do_claim(backend, id) },
          commit: fn(claim, commit) {
            storage.do_commit(backend, claim, commit)
          },
          release: fn(_) { Ok(Nil) },
          cancel: fn(id) { storage.do_cancel(backend, id) },
          unfinished: fn(limit) { storage.do_unfinished(backend, limit) },
        )
      Ok(conformance.fixture(broken, cleanup: fn() { memory.stop(store) }))
    },
    timeout: 5000,
    owner_loss_within: 200,
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
          create: fn(id, data) { storage.do_create(backend, id, data) },
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
          unfinished: fn(limit) { storage.do_unfinished(backend, limit) },
        )
      Ok(conformance.fixture(forgeable, cleanup: fn() { memory.stop(store) }))
    },
    timeout: 5000,
    owner_loss_within: 200,
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
    timeout: 5000,
    owner_loss_within: 100,
  )
  |> should.equal(
    Error(conformance.UnexpectedResult(
      "reclaim within owner_loss_within after owner loss",
    )),
  )
}

pub fn conformance_rejects_invalid_windows_test() {
  conformance.run(memory_fixture, timeout: 0, owner_loss_within: 100)
  |> should.equal(Error(conformance.InvalidTimeout))
  conformance.run(memory_fixture, timeout: 100, owner_loss_within: 0)
  |> should.equal(Error(conformance.InvalidTimeout))
}

@external(erlang, "saga_test_files", "claim_from_immortal")
fn process_claim(
  storage: storage.Storage,
  id: String,
) -> Result(#(storage.Claim, storage.Stored), storage.Error)
