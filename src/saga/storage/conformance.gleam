/// Reusable contract checks for third-party storage adapters. Every fixture
/// must address a fresh execution and allow concurrent callers in one VM.
/// These checks exercise the storage protocol; they do not certify cross-node
/// fencing, media durability, or power-loss behavior.
import gleam/erlang/process
import gleam/list
import gleam/result
import saga/internal/ffi
import saga/storage.{type Record, type Storage}

pub type Fixture {
  Fixture(storage: Storage, cleanup: fn() -> Nil)
}

pub type Failure {
  SetupFailed(String)
  UnexpectedResult(check: String)
  AdapterCrashed(String)
  TimedOut
  InvalidTimeout
}

/// Runs each scenario on a fresh fixture, with a bounded worker lifetime.
/// The factory and cleanup run in the caller; adapter operations run in test
/// workers. Choose a timeout suitable for the backend. Cleanup always runs
/// after the scenario worker exits, including on failure or timeout.
pub fn run(
  fresh: fn() -> Result(Fixture, String),
  timeout_ms: Int,
) -> Result(Nil, Failure) {
  use _ <- result.try(expect(timeout_ms > 0, True, InvalidTimeout))
  list.try_each(
    [lifecycle, concurrent_create, cancellation_race, owner_loss],
    fn(check) {
      use fixture <- result.try(fresh() |> result.map_error(SetupFailed))
      let reply = process.new_subject()
      let pid =
        process.spawn_unlinked(fn() {
          let outcome = case ffi.rescue(fn() { check(fixture.storage) }) {
            ffi.Rescued(outcome) -> outcome
            ffi.Raised(_, reason) -> Error(AdapterCrashed(reason))
          }
          process.send(reply, outcome)
          // Keep links alive until the caller stops this worker and its helpers.
          process.sleep_forever()
        })
      let outcome = case process.receive(reply, timeout_ms) {
        Ok(outcome) -> outcome
        Error(_) -> Error(TimedOut)
      }
      stop(pid)
      fixture.cleanup()
      outcome
    },
  )
}

fn expect(actual: a, expected: a, failure: Failure) -> Result(Nil, Failure) {
  case actual == expected {
    True -> Ok(Nil)
    False -> Error(failure)
  }
}

fn equal(actual: a, expected: a, label: String) -> Result(Nil, Failure) {
  expect(actual, expected, UnexpectedResult(label))
}

fn record(
  value: Result(Record, storage.Error),
  label: String,
) -> Result(Record, Failure) {
  result.map_error(value, fn(_) { UnexpectedResult(label) })
}

fn stop(pid: process.Pid) -> Nil {
  let monitor = process.monitor(pid)
  process.kill(pid)
  process.new_selector()
  |> process.select_specific_monitor(monitor, fn(_) { Nil })
  |> process.selector_receive_forever
}

// Linked helpers cannot outlive a failed or timed-out scenario worker.
fn elsewhere(body: fn() -> a) -> a {
  let reply = process.new_subject()
  process.spawn(fn() { process.send(reply, body()) })
  process.receive_forever(reply)
}

fn lifecycle(s: Storage) -> Result(Nil, Failure) {
  use _ <- result.try(equal(s.load(), Error(storage.NotFound), "load missing"))
  use _ <- result.try(equal(s.claim(), Error(storage.NotFound), "claim missing"))
  use _ <- result.try(equal(
    s.cancel(),
    Error(storage.NotFound),
    "cancel missing",
  ))
  use initial <- result.try(record(s.create(<<"initial":utf8>>), "create"))
  use _ <- result.try(equal(
    initial,
    storage.Record(0, 0, False, <<"initial":utf8>>),
    "initial record",
  ))
  use _ <- result.try(equal(
    s.create(<<"replacement":utf8>>),
    Error(storage.AlreadyExists),
    "duplicate create",
  ))
  use _ <- result.try(equal(
    s.load(),
    Ok(initial),
    "duplicate create preserves data",
  ))
  use owner <- result.try(record(s.claim(), "claim"))
  use _ <- result.try(equal(
    owner.generation > initial.generation,
    True,
    "claim advances generation",
  ))
  use _ <- result.try(equal(
    storage.Record(..owner, generation: initial.generation),
    initial,
    "claim preserves record",
  ))
  use _ <- result.try(equal(
    s.claim(),
    Error(storage.Busy),
    "same owner cannot claim twice",
  ))
  use _ <- result.try(equal(
    elsewhere(fn() { s.claim() }),
    Error(storage.Busy),
    "competing claim",
  ))
  use _ <- result.try(equal(
    elsewhere(fn() { s.commit(owner.generation, 0, False, <<>>) }),
    Error(storage.StaleOwner),
    "token alone grants no ownership",
  ))
  use _ <- result.try(equal(
    elsewhere(fn() { s.release(owner.generation) }),
    Error(storage.StaleOwner),
    "non-owner release",
  ))
  use _ <- result.try(equal(
    s.commit(owner.generation + 1, 0, False, <<>>),
    Error(storage.StaleOwner),
    "wrong generation",
  ))
  use _ <- result.try(equal(
    s.commit(owner.generation, 99, False, <<>>),
    Error(storage.Conflict),
    "wrong revision",
  ))
  use _ <- result.try(equal(
    s.release(owner.generation + 1),
    Error(storage.StaleOwner),
    "wrong release generation",
  ))
  use _ <- result.try(equal(
    s.load(),
    Ok(owner),
    "refused mutations preserve record",
  ))
  use committed <- result.try(record(
    s.commit(owner.generation, owner.revision, False, <<"next":utf8>>),
    "commit",
  ))
  use _ <- result.try(equal(
    committed,
    storage.Record(..owner, revision: 1, data: <<"next":utf8>>),
    "commit advances revision",
  ))
  use _ <- result.try(equal(
    s.commit(owner.generation, 0, False, <<>>),
    Error(storage.Conflict),
    "old revision",
  ))
  use _ <- result.try(equal(
    elsewhere(fn() { s.cancel() }),
    Ok(Nil),
    "cancel while owned",
  ))
  use _ <- result.try(equal(s.cancel(), Ok(Nil), "cancel idempotence"))
  use _ <- result.try(equal(
    s.load(),
    Ok(storage.Record(..committed, cancelled: True)),
    "cancel preserves progress",
  ))
  use _ <- result.try(equal(
    s.commit(owner.generation, 1, False, <<>>),
    Error(storage.CancellationChanged),
    "unobserved cancellation",
  ))
  use cancelled <- result.try(record(
    s.commit(owner.generation, 1, True, <<"cancelled":utf8>>),
    "commit observed cancellation",
  ))
  use _ <- result.try(equal(
    cancelled,
    storage.Record(2, owner.generation, True, <<"cancelled":utf8>>),
    "commit preserves cancellation",
  ))
  use _ <- result.try(equal(s.release(owner.generation), Ok(Nil), "release"))
  use _ <- result.try(equal(
    s.commit(owner.generation, 2, True, <<>>),
    Error(storage.StaleOwner),
    "released writer fenced",
  ))
  use next <- result.try(record(s.claim(), "reclaim"))
  use _ <- result.try(equal(
    next.generation > owner.generation,
    True,
    "reclaim advances generation",
  ))
  use _ <- result.try(equal(
    s.commit(owner.generation, 2, True, <<>>),
    Error(storage.StaleOwner),
    "previous generation fenced",
  ))
  use _ <- result.try(equal(
    s.release(owner.generation),
    Error(storage.StaleOwner),
    "old release cannot unlock successor",
  ))
  use _ <- result.try(equal(
    elsewhere(fn() { s.claim() }),
    Error(storage.Busy),
    "successor remains exclusive",
  ))
  use _ <- result.try(equal(
    s.load(),
    Ok(next),
    "stale owner cannot overwrite successor",
  ))
  equal(s.release(next.generation), Ok(Nil), "release successor")
}

fn concurrent_create(s: Storage) -> Result(Nil, Failure) {
  let ready = process.new_subject()
  let replies = process.new_subject()
  list.each([<<"a":utf8>>, <<"b":utf8>>], fn(bytes) {
    process.spawn(fn() {
      let go = process.new_subject()
      process.send(ready, go)
      process.receive_forever(go)
      process.send(replies, s.create(bytes))
    })
  })
  let a = process.receive_forever(ready)
  let b = process.receive_forever(ready)
  process.send(a, Nil)
  process.send(b, Nil)
  let results = [
    process.receive_forever(replies),
    process.receive_forever(replies),
  ]
  let winners = list.filter_map(results, fn(value) { value })
  use _ <- result.try(equal(list.length(winners), 1, "exactly one create wins"))
  use _ <- result.try(equal(
    list.count(results, fn(value) { value == Error(storage.AlreadyExists) }),
    1,
    "losing create reports AlreadyExists",
  ))
  let assert [winner] = winners
  equal(s.load(), Ok(winner), "winning create data survives")
}

fn cancellation_race(s: Storage) -> Result(Nil, Failure) {
  use _ <- result.try(record(s.create(<<"before":utf8>>), "race create"))
  use owner <- result.try(record(s.claim(), "race claim"))
  let ready = process.new_subject()
  let reply = process.new_subject()
  process.spawn(fn() {
    let go = process.new_subject()
    process.send(ready, go)
    process.receive_forever(go)
    process.send(reply, s.cancel())
  })
  process.send(process.receive_forever(ready), Nil)
  let committed = s.commit(owner.generation, 0, False, <<"after":utf8>>)
  use _ <- result.try(equal(
    process.receive_forever(reply),
    Ok(Nil),
    "racing cancel succeeds",
  ))
  let expected = case committed {
    Ok(_) -> Ok(storage.Record(1, owner.generation, True, <<"after":utf8>>))
    Error(storage.CancellationChanged) ->
      Ok(storage.Record(0, owner.generation, True, <<"before":utf8>>))
    _ -> Error(UnexpectedResult("cancel/commit must linearize"))
  }
  use expected <- result.try(expected)
  use _ <- result.try(equal(
    s.load(),
    Ok(expected),
    "cancel/commit preserves winner and intent",
  ))
  equal(s.release(owner.generation), Ok(Nil), "race release")
}

fn owner_loss(s: Storage) -> Result(Nil, Failure) {
  use _ <- result.try(record(s.create(<<"survives":utf8>>), "owner-loss create"))
  let reply = process.new_subject()
  let owner =
    process.spawn(fn() {
      process.send(reply, s.claim())
      process.sleep_forever()
    })
  use before <- result.try(record(
    process.receive_forever(reply),
    "dying owner claim",
  ))
  // Unlink before killing so the scenario remains alive.
  process.unlink(owner)
  stop(owner)
  use after <- result.try(record(reclaim(s, 100), "reclaim after owner death"))
  use _ <- result.try(equal(
    after.generation > before.generation,
    True,
    "owner loss advances generation",
  ))
  use _ <- result.try(equal(
    storage.Record(..after, generation: before.generation),
    before,
    "owner loss preserves progress",
  ))
  use _ <- result.try(equal(
    s.commit(before.generation, 0, False, <<>>),
    Error(storage.StaleOwner),
    "dead owner token fenced",
  ))
  equal(s.release(after.generation), Ok(Nil), "owner-loss release")
}

fn reclaim(s: Storage, remaining: Int) -> Result(Record, storage.Error) {
  case s.claim() {
    Error(storage.Busy) if remaining > 0 -> {
      process.sleep(5)
      reclaim(s, remaining - 1)
    }
    result -> result
  }
}
