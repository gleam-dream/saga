import gleam/int
import gleeunit/should
import saga/storage
import saga/storage/conformance
import saga/storage/file
import saga/storage/memory

@external(erlang, "saga_ffi", "unique_integer")
fn unique() -> Int

@external(erlang, "recovery_probe_ffi", "remove")
fn remove(path: String) -> Nil

pub fn memory_adapter_conformance_test() {
  conformance.run(
    fn() {
      let memory = memory.new()
      Ok(
        conformance.Fixture(memory.storage(memory), fn() {
          memory.close(memory)
        }),
      )
    },
    5000,
  )
  |> should.equal(Ok(Nil))
}

pub fn file_adapter_conformance_test() {
  conformance.run(
    fn() {
      let path = "/tmp/saga-conformance-" <> int.to_string(unique())
      remove(path)
      Ok(conformance.Fixture(file.open(path), fn() { remove(path) }))
    },
    5000,
  )
  |> should.equal(Ok(Nil))
}

pub fn conformance_detects_broken_ownership_test() {
  conformance.run(
    fn() {
      let memory = memory.new()
      let backend = memory.storage(memory)
      let broken = storage.Storage(..backend, release: fn(_) { Ok(Nil) })
      Ok(conformance.Fixture(broken, fn() { memory.close(memory) }))
    },
    5000,
  )
  |> should.equal(Error(conformance.UnexpectedResult("non-owner release")))
}
