//// Durable runs on PostgreSQL through saga's public API: a run finishes
//// and reads back as finished; a live runner keeps its claim past the
//// lease; a runner that dies mid-step loses its claim at lease expiry and
//// another drive resumes it through the step's resolver; `unfinished`
//// lists the executions that no live runner owns.

import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{None, Some}
import gleam/time/duration
import gleeunit/should
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage.{type Storage}
import saga_postgres
import saga_postgres/support

const lease = 1000

type Persistence =
  durable.Persistence(String, String, String, String)

fn prepare(workflow: saga.Workflow(String, String, String, String)) {
  let text = codec.text()
  let persistence =
    durable.new(
      workflow,
      input: text,
      output: text,
      error: text,
      undo_error: text,
    )
  persistence
  |> durable.with_config(
    execution.config() |> execution.with_step_timeout(execution.Infinity),
  )
}

/// A one-step workflow whose step runs `body`; its resolver reports the
/// interrupted attempt to `resolved` and answers `Completed`.
fn one_step(
  body: fn(String) -> Result(String, String),
  resolved: Subject(String),
) -> Persistence {
  let workflow =
    saga.define("one-step", fn(input) {
      saga.perform(
        input,
        saga.effect("work", fn(value, _key) { body(value) })
          |> durable.recoverable(
            version: "1",
            input: codec.text(),
            output: codec.text(),
            resolve: fn(value, _key) {
              process.send(resolved, value)
              durable.Completed(value <> " recovered")
            },
          ),
      )
    })
  prepare(workflow)
}

fn start(persistence: Persistence, store: Storage, id: String) {
  let assert Ok(run) =
    durable.start_or_reconnect(persistence, store, id:, input: "x")
  run
}

fn listed(store: Storage, id: String) -> Bool {
  let assert Ok(ids) = durable.unfinished(store, limit: 1000)
  list.contains(ids, id)
}

/// The same storage, reporting the process that claims (the runner) to
/// `owner`, with the same renewal.
fn watched(backend: Storage, owner: Subject(Pid)) -> Storage {
  watched_releasing(backend, owner, fn(claim) {
    storage.do_release(backend, claim)
  })
}

/// Like `watched`, with `release` replaced, to stand in for a release that
/// never reaches the database.
fn watched_releasing(
  backend: Storage,
  owner: Subject(Pid),
  release: fn(storage.Claim) -> Result(Nil, storage.Error),
) -> Storage {
  let rebuilt =
    storage.new(
      create: fn(id, data) { storage.do_create(backend, id, data) },
      load: fn(id) { storage.do_load(backend, id) },
      claim: fn(id) {
        let result = storage.do_claim(backend, id)
        case result {
          Ok(_) -> process.send(owner, process.self())
          Error(_) -> Nil
        }
        result
      },
      commit: fn(claim, change) { storage.do_commit(backend, claim, change) },
      release: release,
      cancel: fn(id) { storage.do_cancel(backend, id) },
      unfinished: fn(limit) { storage.do_unfinished(backend, limit) },
    )
  case storage.renewal(backend) {
    Some(#(every, renew)) -> storage.with_renewal(rebuilt, every:, renew:)
    None -> rebuilt
  }
}

fn kill_and_wait(pid: Pid) -> Nil {
  let monitor = process.monitor(pid)
  process.kill(pid)
  process.new_selector()
  |> process.select_specific_monitor(monitor, fn(_) { Nil })
  |> process.selector_receive_forever
}

fn listed_within(store: Storage, id: String, milliseconds: Int) -> Bool {
  case listed(store, id) {
    True -> True
    False if milliseconds <= 0 -> False
    False -> {
      process.sleep(50)
      listed_within(store, id, milliseconds - 50)
    }
  }
}

pub fn a_durable_run_finishes_on_postgres_test() {
  use connection <- support.using_pool(4)
  let schema = support.schema()
  let store =
    support.migrated(connection, schema, duration.milliseconds(lease))
    |> saga_postgres.storage
  let persistence =
    one_step(fn(value) { Ok(value <> "!") }, process.new_subject())
  let id = support.id()
  let run = start(persistence, store, id)
  durable.read(run) |> should.equal(Ok(durable.Pending))
  // Saved and not driven: it waits for a runner.
  listed(store, id) |> should.be_true
  durable.drive(run, timeout: duration.seconds(10))
  |> should.equal(Ok(execution.Completed("x!")))
  durable.read(run)
  |> should.equal(Ok(durable.Finished(execution.Completed("x!"))))
  listed(store, id) |> should.be_false
  // Reconnecting returns the saved outcome without running the step again.
  let again = start(persistence, store, id)
  durable.drive(again, timeout: duration.seconds(10))
  |> should.equal(Ok(execution.Completed("x!")))
  support.drop(connection, schema)
}

pub fn a_live_runner_keeps_its_claim_past_the_lease_test() {
  use connection <- support.using_pool(4)
  let schema = support.schema()
  let store =
    support.migrated(connection, schema, duration.milliseconds(lease))
    |> saga_postgres.storage
  let persistence =
    one_step(
      fn(value) {
        process.sleep(lease * 5 / 2)
        Ok(value <> "!")
      },
      process.new_subject(),
    )
  let id = support.id()
  let run = start(persistence, store, id)
  let result = process.new_subject()
  process.spawn(fn() {
    process.send(result, durable.drive(run, timeout: duration.seconds(10)))
  })
  // Past the lease, the renewed claim still excludes a second runner.
  process.sleep(lease * 3 / 2)
  durable.drive(run, timeout: duration.seconds(10))
  |> should.equal(Error(durable.StorageFailure(storage.Busy)))
  listed(store, id) |> should.be_false
  process.receive(result, 10_000)
  |> should.equal(Ok(Ok(execution.Completed("x!"))))
  support.drop(connection, schema)
}

/// A runner killed while `drive`'s caller lives loses its claim at once:
/// the next drive resumes without waiting for the lease.
pub fn a_killed_runner_frees_its_execution_at_once_test() {
  use connection <- support.using_pool(4)
  let schema = support.schema()
  let store =
    support.migrated(connection, schema, duration.seconds(30))
    |> saga_postgres.storage
  let owner = process.new_subject()
  let entered = process.new_subject()
  let resolved = process.new_subject()
  let persistence =
    one_step(
      fn(_value) {
        process.send(entered, Nil)
        process.sleep_forever()
        Error("unreachable")
      },
      resolved,
    )
  let id = support.id()
  let run = start(persistence, watched(store, owner), id)
  let result = process.new_subject()
  process.spawn_unlinked(fn() {
    process.send(result, durable.drive(run, timeout: duration.seconds(30)))
  })
  let assert Ok(runner) = process.receive(owner, 5000)
  let assert Ok(Nil) = process.receive(entered, 5000)
  kill_and_wait(runner)
  process.receive(result, 5000)
  |> should.equal(Ok(Error(durable.RunnerLost)))
  // The 30 s lease does not matter: drive released the claim.
  listed(store, id) |> should.be_true
  let assert Ok(resumed) = durable.reconnect(persistence, store, id:)
  durable.drive(resumed, timeout: duration.seconds(10))
  |> should.equal(Ok(execution.Completed("x recovered")))
  process.receive(resolved, 0) |> should.equal(Ok("x"))
  process.receive(entered, 0) |> should.equal(Error(Nil))
  support.drop(connection, schema)
}

/// When no release reaches the database, as when the runner's VM is lost,
/// the claim ends when its lease expires.
pub fn a_lost_release_falls_back_to_the_lease_test() {
  use connection <- support.using_pool(4)
  let schema = support.schema()
  let store =
    support.migrated(connection, schema, duration.milliseconds(lease))
    |> saga_postgres.storage
  let owner = process.new_subject()
  let entered = process.new_subject()
  let resolved = process.new_subject()
  let persistence =
    one_step(
      fn(_value) {
        process.send(entered, Nil)
        process.sleep_forever()
        Error("unreachable")
      },
      resolved,
    )
  let id = support.id()
  let run =
    start(persistence, watched_releasing(store, owner, fn(_) { Ok(Nil) }), id)
  let result = process.new_subject()
  process.spawn_unlinked(fn() {
    process.send(result, durable.drive(run, timeout: duration.seconds(30)))
  })
  let assert Ok(runner) = process.receive(owner, 5000)
  let assert Ok(Nil) = process.receive(entered, 5000)
  kill_and_wait(runner)
  process.receive(result, 5000)
  |> should.equal(Ok(Error(durable.RunnerLost)))
  // Its lease still holds: no runner may resume yet.
  durable.drive(run, timeout: duration.seconds(10))
  |> should.equal(Error(durable.StorageFailure(storage.Busy)))
  listed(store, id) |> should.be_false
  // At lease expiry the execution waits for a runner again.
  listed_within(store, id, lease + 1000) |> should.be_true
  let assert Ok(resumed) = durable.reconnect(persistence, store, id:)
  durable.drive(resumed, timeout: duration.seconds(10))
  |> should.equal(Ok(execution.Completed("x recovered")))
  process.receive(resolved, 0) |> should.equal(Ok("x"))
  process.receive(entered, 0) |> should.equal(Error(Nil))
  durable.read(resumed)
  |> should.equal(Ok(durable.Finished(execution.Completed("x recovered"))))
  support.drop(connection, schema)
}

pub fn cancel_is_observed_by_the_next_drive_test() {
  use connection <- support.using_pool(4)
  let schema = support.schema()
  let store =
    support.migrated(connection, schema, duration.milliseconds(lease))
    |> saga_postgres.storage
  let ran = process.new_subject()
  let persistence =
    one_step(
      fn(value) {
        process.send(ran, Nil)
        Ok(value)
      },
      process.new_subject(),
    )
  let run = start(persistence, store, support.id())
  durable.cancel(run) |> should.equal(Ok(Nil))
  let assert Ok(execution.Cancelled(..)) =
    durable.drive(run, timeout: duration.seconds(10))
  process.receive(ran, 0) |> should.equal(Error(Nil))
  support.drop(connection, schema)
}
