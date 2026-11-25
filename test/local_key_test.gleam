//// A local run's idempotency key is sent to external providers, which
//// remember keys across this VM's lifetime. These tests start fresh Erlang
//// VMs and compare the keys their first runs produce.

import gleam/erlang/process
import gleam/list
import gleeunit/should
import saga
import saga/execution

/// The idempotency key of the first local run in the calling VM. Fresh VMs
/// call this through `local_key_test_ffi`, each as its first work after boot.
pub fn first_local_key() -> String {
  let keys = process.new_subject()
  let workflow =
    saga.define("checkout", fn(input) {
      input
      |> saga.perform(
        saga.effect("charge", fn(amount: Int, key) {
          process.send(keys, saga.idempotency_key(key))
          Ok(amount)
        }),
      )
    })
  let assert Ok(execution.Completed(1)) =
    execution.run(workflow, 1, execution.config())
  let assert Ok(key) = process.receive(keys, 1000)
  key
}

@external(erlang, "local_key_test_ffi", "first_keys_of_fresh_vms")
fn first_keys_of_fresh_vms(count: Int) -> List(String)

pub fn local_keys_differ_across_vm_restarts_test() {
  let keys = first_keys_of_fresh_vms(3)
  list.length(keys) |> should.equal(3)
  list.unique(keys) |> should.equal(keys)
}

pub fn local_keys_differ_between_runs_test() {
  { first_local_key() != first_local_key() } |> should.be_true
}
