//// Adapter details beyond saga's conformance suite: the precedence of a
//// refused commit's reasons, renewal of a claim, the stored phase, and
//// the query timeout.

import gleam/dict
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import gleam/time/duration
import gleeunit/should
import pog
import saga/storage.{type Claim, type Storage}
import saga_postgres
import saga_postgres/support

fn fresh(connection: pog.Connection, schema: String) -> Storage {
  saga_postgres.storage(support.migrated(
    connection,
    schema,
    duration.seconds(30),
  ))
}

fn commit(
  store: Storage,
  claim: Claim,
  revision: Int,
  cancelled: Bool,
  phase: storage.Phase,
) -> Result(Int, storage.Error) {
  storage.do_commit(
    store,
    claim,
    storage.Commit(
      expected_revision: revision,
      observed_cancelled: cancelled,
      phase:,
      records: dict.from_list([#("checkpoint", <<"next":utf8>>)]),
    ),
  )
  |> result.try(fn(_) {
    storage.do_load(store, storage.claim_id(claim))
    |> result.map(storage.revision)
  })
}

/// The pending and suspended executions that no live claim holds.
fn unfinished(store: Storage) -> Result(List(String), storage.Error) {
  use pending <- result.try(storage.do_list(
    store,
    storage.Listing(storage.Pending, None, 10),
  ))
  use suspended <- result.try(storage.do_list(
    store,
    storage.Listing(storage.Suspended, None, 10),
  ))
  Ok(list.append(pending, suspended))
}

fn phase(connection: pog.Connection, schema: String, id: String) -> String {
  let assert Ok(returned) =
    pog.query(
      "SELECT phase FROM \"" <> schema <> "\".saga_executions WHERE id = $1",
    )
    |> pog.parameter(pog.text(id))
    |> pog.returning(decode.field(0, decode.string, decode.success))
    |> pog.execute(connection)
  let assert [phase] = returned.rows
  phase
}

/// A refused commit names the first failing check: ownership, then the
/// cancellation flag, then the revision.
pub fn a_refused_commit_names_ownership_then_cancellation_then_revision_test() {
  use connection <- support.using_pool(2)
  let schema = support.schema()
  let store = fresh(connection, schema)
  let id = support.id()
  let assert Ok(_) = storage.do_create(store, id, dict.new())
  let assert Ok(#(owner, _)) = storage.do_claim(store, id)
  let assert Ok(Nil) = storage.do_cancel(store, id)
  let forged =
    storage.claim(
      id:,
      generation: storage.claim_generation(owner),
      token: "forged",
    )
  commit(store, forged, 9, False, storage.Pending)
  |> should.equal(Error(storage.StaleOwner))
  commit(store, owner, 9, False, storage.Pending)
  |> should.equal(Error(storage.CancellationChanged))
  commit(store, owner, 9, True, storage.Pending)
  |> should.equal(Error(storage.Conflict))
  commit(store, owner, 0, True, storage.Pending) |> should.equal(Ok(1))
  support.drop(connection, schema)
}

pub fn a_commit_or_release_of_an_unknown_execution_is_not_found_test() {
  use connection <- support.using_pool(2)
  let schema = support.schema()
  let store = fresh(connection, schema)
  let ghost = storage.claim(id: support.id(), generation: 1, token: "t")
  commit(store, ghost, 0, False, storage.Pending)
  |> should.equal(Error(storage.NotFound))
  storage.do_release(store, ghost) |> should.equal(Error(storage.NotFound))
  support.drop(connection, schema)
}

pub fn commits_store_the_phase_that_list_reads_test() {
  use connection <- support.using_pool(2)
  let schema = support.schema()
  let store = fresh(connection, schema)
  let id = support.id()
  let assert Ok(_) = storage.do_create(store, id, dict.new())
  phase(connection, schema, id) |> should.equal("pending")
  let assert Ok(#(owner, _)) = storage.do_claim(store, id)
  commit(store, owner, 0, False, storage.Suspended) |> should.equal(Ok(1))
  phase(connection, schema, id) |> should.equal("suspended")
  commit(store, owner, 1, False, storage.Finished) |> should.equal(Ok(2))
  phase(connection, schema, id) |> should.equal("finished")
  let assert Ok(Nil) = storage.do_release(store, owner)
  unfinished(store) |> should.equal(Ok([]))
  storage.do_list(store, storage.Listing(storage.Finished, None, 10))
  |> should.equal(Ok([id]))
  storage.do_list(store, storage.Listing(storage.Finished, None, 0))
  |> should.equal(Ok([]))
  support.drop(connection, schema)
}

/// `renew` keeps the current claim, and reports a claim that was taken
/// over or whose execution is gone.
pub fn renewal_keeps_only_the_current_claim_test() {
  use connection <- support.using_pool(2)
  let schema = support.schema()
  let store =
    saga_postgres.storage(support.migrated(
      connection,
      schema,
      duration.milliseconds(500),
    ))
  let assert Some(#(_, renew)) = storage.renewal(store)
  let id = support.id()
  let assert Ok(_) = storage.do_create(store, id, dict.new())
  let assert Ok(#(first, _)) = storage.do_claim(store, id)
  // Renewed past its lease, the claim still excludes another.
  process.sleep(300)
  renew(first) |> should.equal(Ok(Nil))
  process.sleep(300)
  storage.do_claim(store, id) |> result_busy |> should.be_true
  // Left to expire, it is taken over and fenced.
  process.sleep(600)
  let assert Ok(#(second, _)) = storage.do_claim(store, id)
  renew(first) |> should.equal(Error(storage.StaleOwner))
  renew(second) |> should.equal(Ok(Nil))
  let assert Ok(_) =
    pog.query("DELETE FROM \"" <> schema <> "\".saga_executions WHERE id = $1")
    |> pog.parameter(pog.text(id))
    |> pog.execute(connection)
  renew(second) |> should.equal(Error(storage.NotFound))
  support.drop(connection, schema)
}

fn result_busy(result: Result(a, storage.Error)) -> Bool {
  result == Error(storage.Busy)
}

/// A query blocked longer than the query timeout fails with `TimedOut`
/// before saga's own call timeout.
pub fn a_blocked_query_times_out_below_the_call_timeout_test() {
  use connection <- support.using_pool(3)
  let schema = support.schema()
  let store = fresh(connection, schema)
  let id = support.id()
  let assert Ok(_) = storage.do_create(store, id, dict.new())
  let locked = process.new_subject()
  process.spawn(fn() {
    pog.transaction(connection, fn(connection) {
      let assert Ok(_) =
        pog.query(
          "SELECT id FROM \""
          <> schema
          <> "\".saga_executions WHERE id = $1 FOR UPDATE",
        )
        |> pog.parameter(pog.text(id))
        |> pog.execute(connection)
      process.send(locked, Nil)
      process.sleep(6000)
      Ok(Nil)
    })
  })
  let assert Ok(Nil) = process.receive(locked, 5000)
  storage.do_cancel(store, id) |> should.equal(Error(storage.TimedOut))
  process.sleep(1500)
  storage.do_cancel(store, id) |> should.equal(Ok(Nil))
  support.drop(connection, schema)
}

/// A claim whose lease expired stays current until another claim replaces
/// it: its holder may still commit, which renews the lease.
pub fn an_expired_claim_commits_until_it_is_replaced_test() {
  use connection <- support.using_pool(2)
  let schema = support.schema()
  let store =
    saga_postgres.storage(support.migrated(
      connection,
      schema,
      duration.milliseconds(100),
    ))
  let id = support.id()
  let assert Ok(_) = storage.do_create(store, id, dict.new())
  let assert Ok(#(owner, _)) = storage.do_claim(store, id)
  process.sleep(300)
  unfinished(store) |> should.equal(Ok([id]))
  commit(store, owner, 0, False, storage.Pending) |> should.equal(Ok(1))
  unfinished(store) |> should.equal(Ok([]))
  process.sleep(300)
  let assert Ok(#(_successor, _)) = storage.do_claim(store, id)
  commit(store, owner, 1, False, storage.Pending)
  |> should.equal(Error(storage.StaleOwner))
  support.drop(connection, schema)
}

/// A claim and the records it returns are one atomic unit: a claim whose
/// read of the records fails takes no ownership, so a retry claims at once,
/// and an accepted claim returns the records of its own generation.
pub fn a_claim_whose_read_fails_takes_no_ownership_test() {
  use connection <- support.using_pool(2)
  let schema = support.schema()
  let store = fresh(connection, schema)
  let id = support.id()
  let assert Ok(_) = storage.do_create(store, id, dict.new())
  let rename = fn(from, to) {
    let assert Ok(_) =
      pog.query(
        "ALTER TABLE \"" <> schema <> "\"." <> from <> " RENAME TO " <> to,
      )
      |> pog.execute(connection)
    Nil
  }
  rename("saga_records", "saga_records_away")
  let assert Error(storage.Unavailable(_)) = storage.do_claim(store, id)
  rename("saga_records_away", "saga_records")
  let assert Ok(#(claim, stored)) = storage.do_claim(store, id)
  storage.generation(stored) |> should.equal(storage.claim_generation(claim))
  storage.claim_generation(claim) |> should.equal(1)
  support.drop(connection, schema)
}
