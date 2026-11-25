//// Checks a storage adapter against the `saga/storage` contract.
////
//// Use this module from an adapter's own tests; it needs no test framework.
//// `run` gives each scenario a fresh fixture and fresh execution ids, and
//// checks atomic creation, unchanged data after refused writes, claims as
//// values (any holder may commit; a rebuilt claim with another token is
//// refused), revision and generation checks, cancellation races, release,
//// that a live owner keeps its claim, that a lost owner's claim ends within
//// `owner_loss_within`, that a commit replaces only the records it names,
//// the paged `list` by phase, and that a
//// `saga/durable` drive whose runner is killed frees the execution at once
//// for the next drive, which resumes from the checkpoint. One
//// fixture may serve several executions, as a database pool does. The
//// memory, file and PostgreSQL adapters pass the same checks.
////
//// A pass covers the storage protocol from one VM; it does not certify
//// cross-node fencing, media durability or power-loss behavior.
////
//// ```gleam
//// import gleam/time/duration
//// import saga/storage/conformance
////
//// let result =
////   conformance.run(
////     fn() {
////       let assert Ok(store) = memory.start()
////       Ok(conformance.fixture(memory.storage(store), cleanup: fn() {
////         memory.stop(store)
////       }))
////     },
////     timeout: duration.seconds(5),
////     owner_loss_within: duration.milliseconds(500),
////   )
//// ```
////
//// `owner_loss_within` is the adapter's declared window for noticing that
//// an owner is gone: near zero for the memory adapter, which watches the
//// claiming process, and the lease duration for a lease-based adapter.

import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/internal/ffi
import saga/internal/timer
import saga/storage.{type Claim, type Storage, type Stored}

/// A storage under test and how to clean it up after one scenario.
pub opaque type Fixture {
  Fixture(storage: Storage, cleanup: fn() -> Nil)
}

/// Builds a fixture.
pub fn fixture(storage: Storage, cleanup cleanup: fn() -> Nil) -> Fixture {
  Fixture(storage:, cleanup:)
}

/// Why the suite failed. The union is closed.
pub type Failure {
  /// The fixture factory returned this error.
  SetupFailed(String)
  /// The named check saw an unexpected result.
  UnexpectedResult(check: String)
  /// An adapter operation raised; the formatted exception, for logs.
  AdapterCrashed(String)
  /// A scenario did not finish in time.
  TimedOut
  /// `timeout` or `owner_loss_within` was below 1 millisecond, or
  /// `timeout + 4 * owner_loss_within` exceeds 4_294_967_295 milliseconds.
  InvalidTimeout
}

/// Runs each scenario on a fresh fixture. `timeout` bounds each scenario's
/// storage work, and scenarios that wait for an owner to be lost get
/// `owner_loss_within` on top. The factory and cleanup run in the caller;
/// adapter operations run in test workers. Cleanup runs after the scenario
/// worker exits, including on failure or timeout.
pub fn run(
  fresh: fn() -> Result(Fixture, String),
  timeout timeout: Duration,
  owner_loss_within owner_loss_within: Duration,
) -> Result(Nil, Failure) {
  let timeout = duration.to_milliseconds(timeout)
  let owner_loss_within = duration.to_milliseconds(owner_loss_within)
  let budget = timeout + 4 * owner_loss_within
  use _ <- result.try(
    case
      timeout > 0 && owner_loss_within > 0 && budget <= timer.max_milliseconds
    {
      True -> Ok(Nil)
      False -> Error(InvalidTimeout)
    },
  )
  list.try_each(
    [
      lifecycle,
      concurrent_create,
      cancellation_race,
      owner_loss,
      live_owner_keeps_claim,
      records_merge,
      listing,
      killed_runner,
    ],
    fn(check) {
      use fixture <- result.try(fresh() |> result.map_error(SetupFailed))
      let reply = process.new_subject()
      let pid =
        process.spawn_unlinked(fn() {
          let outcome = case
            ffi.rescue(fn() { check(fixture.storage, owner_loss_within) })
          {
            ffi.Rescued(outcome) -> outcome
            ffi.Raised(_, reason) -> Error(AdapterCrashed(reason))
          }
          process.send(reply, outcome)
          // Keep links alive until the caller stops this worker and its helpers.
          process.sleep_forever()
        })
      let outcome = case process.receive(reply, budget) {
        Ok(outcome) -> outcome
        Error(_) -> Error(TimedOut)
      }
      stop(pid)
      fixture.cleanup()
      outcome
    },
  )
}

fn fresh_id() -> String {
  "saga-conformance-" <> int.to_string(ffi.unique_integer())
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

fn ok(value: Result(a, storage.Error), label: String) -> Result(a, Failure) {
  result.map_error(value, fn(_) { UnexpectedResult(label) })
}

/// Records from `#(key, text)` pairs.
fn records(pairs: List(#(String, String))) -> Dict(String, BitArray) {
  list.map(pairs, fn(pair) { #(pair.0, <<pair.1:utf8>>) }) |> dict.from_list
}

/// A checkpoint of one record.
fn one(text: String) -> Dict(String, BitArray) {
  records([#("checkpoint", text)])
}

fn view(stored: Stored) -> #(Int, Int, Bool, Dict(String, BitArray)) {
  #(
    storage.revision(stored),
    storage.generation(stored),
    storage.cancelled(stored),
    storage.records(stored),
  )
}

fn loaded(
  s: Storage,
  id: String,
) -> Result(#(Int, Int, Bool, Dict(String, BitArray)), storage.Error) {
  storage.do_load(s, id) |> result.map(view)
}

/// Commits `records` and, when the commit succeeds, reads the execution
/// back.
fn commit(
  s: Storage,
  claim: Claim,
  revision: Int,
  cancelled: Bool,
  records: Dict(String, BitArray),
) -> Result(#(Int, Int, Bool, Dict(String, BitArray)), storage.Error) {
  storage.do_commit(
    s,
    claim,
    storage.Commit(
      expected_revision: revision,
      observed_cancelled: cancelled,
      phase: storage.Pending,
      records: records,
    ),
  )
  |> result.try(fn(_) { loaded(s, storage.claim_id(claim)) })
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

/// Starts a linked process that claims `id`, keeps the claim (renewing it
/// when the storage declares renewal) and reports it.
fn holder(s: Storage, id: String) -> #(process.Pid, Result(Claim, Failure)) {
  let reply = process.new_subject()
  let pid =
    process.spawn(fn() {
      case storage.do_claim(s, id) {
        Ok(#(claim, _)) -> {
          process.send(reply, Ok(claim))
          case storage.renewal(s) {
            None -> process.sleep_forever()
            Some(#(every, renew)) ->
              renew_forever(claim, duration.to_milliseconds(every), renew)
          }
        }
        Error(_) -> process.send(reply, Error(UnexpectedResult("holder claim")))
      }
    })
  #(pid, process.receive_forever(reply))
}

fn renew_forever(
  claim: Claim,
  every: Int,
  renew: fn(Claim) -> Result(Nil, storage.Error),
) -> Nil {
  process.sleep(int.clamp(every, 1, timer.max_milliseconds))
  let _ = renew(claim)
  renew_forever(claim, every, renew)
}

fn lose(pid: process.Pid) -> Nil {
  // Unlink before killing so the scenario remains alive.
  process.unlink(pid)
  stop(pid)
}

fn lifecycle(s: Storage, _within: Int) -> Result(Nil, Failure) {
  let id = fresh_id()
  use _ <- result.try(equal(
    storage.do_load(s, id),
    Error(storage.NotFound),
    "load missing",
  ))
  use _ <- result.try(equal(
    storage.do_claim(s, id),
    Error(storage.NotFound),
    "claim missing",
  ))
  use _ <- result.try(equal(
    storage.do_cancel(s, id),
    Error(storage.NotFound),
    "cancel missing",
  ))
  use _ <- result.try(ok(storage.do_create(s, id, one("initial")), "create"))
  use _ <- result.try(equal(
    loaded(s, id),
    Ok(#(0, 0, False, one("initial"))),
    "initial record",
  ))
  use _ <- result.try(equal(
    storage.do_create(s, id, one("replacement")),
    Error(storage.AlreadyExists),
    "duplicate create",
  ))
  use _ <- result.try(equal(
    loaded(s, id),
    Ok(#(0, 0, False, one("initial"))),
    "duplicate create preserves records",
  ))
  use #(owner, claimed) <- result.try(ok(storage.do_claim(s, id), "claim"))
  let generation = storage.generation(claimed)
  use _ <- result.try(equal(generation > 0, True, "claim advances generation"))
  use _ <- result.try(equal(
    #(storage.claim_id(owner), storage.claim_generation(owner), view(claimed)),
    #(id, generation, #(0, generation, False, one("initial"))),
    "claim preserves record",
  ))
  use _ <- result.try(equal(
    storage.do_claim(s, id) |> result.replace(Nil),
    Error(storage.Busy),
    "same caller cannot claim twice",
  ))
  use _ <- result.try(equal(
    elsewhere(fn() { storage.do_claim(s, id) |> result.replace(Nil) }),
    Error(storage.Busy),
    "competing claim",
  ))
  let forged =
    storage.claim(
      id: id,
      generation: generation,
      token: storage.claim_token(owner) <> "-forged",
    )
  use _ <- result.try(equal(
    commit(s, forged, 0, False, one("")),
    Error(storage.StaleOwner),
    "a generation alone grants no ownership",
  ))
  use _ <- result.try(equal(
    storage.do_release(s, forged),
    Error(storage.StaleOwner),
    "forged release",
  ))
  let later =
    storage.claim(
      id: id,
      generation: generation + 1,
      token: storage.claim_token(owner),
    )
  use _ <- result.try(equal(
    commit(s, later, 0, False, one("")),
    Error(storage.StaleOwner),
    "wrong generation",
  ))
  use _ <- result.try(equal(
    commit(s, owner, 99, False, one("")),
    Error(storage.Conflict),
    "wrong revision",
  ))
  use _ <- result.try(equal(
    storage.do_release(s, later),
    Error(storage.StaleOwner),
    "wrong release generation",
  ))
  use _ <- result.try(equal(
    loaded(s, id),
    Ok(view(claimed)),
    "refused mutations preserve record",
  ))
  use _ <- result.try(equal(
    elsewhere(fn() { commit(s, owner, 0, False, one("next")) }),
    Ok(#(1, generation, False, one("next"))),
    "a claim is a value: another process commits with it",
  ))
  use _ <- result.try(equal(
    commit(s, owner, 0, False, one("")),
    Error(storage.Conflict),
    "old revision",
  ))
  use _ <- result.try(equal(
    elsewhere(fn() { storage.do_cancel(s, id) }),
    Ok(Nil),
    "cancel while owned",
  ))
  use _ <- result.try(equal(storage.do_cancel(s, id), Ok(Nil), "cancel twice"))
  use _ <- result.try(equal(
    loaded(s, id),
    Ok(#(1, generation, True, one("next"))),
    "cancel preserves progress",
  ))
  use _ <- result.try(equal(
    commit(s, owner, 1, False, one("")),
    Error(storage.CancellationChanged),
    "unobserved cancellation",
  ))
  use _ <- result.try(equal(
    commit(s, owner, 99, False, one("")),
    Error(storage.CancellationChanged),
    "an unobserved cancellation takes precedence over a wrong revision",
  ))
  use _ <- result.try(equal(
    commit(s, forged, 99, False, one("")),
    Error(storage.StaleOwner),
    "a stale claim takes precedence over every other refusal",
  ))
  use _ <- result.try(equal(
    commit(s, owner, 1, True, one("cancelled")),
    Ok(#(2, generation, True, one("cancelled"))),
    "commit observed cancellation",
  ))
  use _ <- result.try(equal(storage.do_release(s, owner), Ok(Nil), "release"))
  use _ <- result.try(equal(
    commit(s, owner, 2, True, one("")),
    Error(storage.StaleOwner),
    "released claim fenced",
  ))
  use #(next, _) <- result.try(ok(storage.do_claim(s, id), "reclaim"))
  use _ <- result.try(equal(
    storage.claim_generation(next) > generation,
    True,
    "reclaim advances generation",
  ))
  use _ <- result.try(equal(
    commit(s, owner, 2, True, one("")),
    Error(storage.StaleOwner),
    "previous generation fenced",
  ))
  use _ <- result.try(equal(
    storage.do_release(s, owner),
    Error(storage.StaleOwner),
    "old release cannot unlock successor",
  ))
  use _ <- result.try(equal(
    elsewhere(fn() { storage.do_claim(s, id) |> result.replace(Nil) }),
    Error(storage.Busy),
    "successor remains exclusive",
  ))
  use _ <- result.try(equal(
    loaded(s, id),
    Ok(#(2, storage.claim_generation(next), True, one("cancelled"))),
    "stale owner cannot overwrite successor",
  ))
  equal(storage.do_release(s, next), Ok(Nil), "release successor")
}

fn concurrent_create(s: Storage, _within: Int) -> Result(Nil, Failure) {
  let id = fresh_id()
  let ready = process.new_subject()
  let replies = process.new_subject()
  list.each([one("a"), one("b")], fn(created) {
    process.spawn(fn() {
      let go = process.new_subject()
      process.send(ready, go)
      process.receive_forever(go)
      process.send(
        replies,
        storage.do_create(s, id, created)
          |> result.map(fn(_) { #(0, 0, False, created) }),
      )
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
  equal(loaded(s, id), Ok(winner), "winning create records survive")
}

fn cancellation_race(s: Storage, _within: Int) -> Result(Nil, Failure) {
  let id = fresh_id()
  use _ <- result.try(ok(storage.do_create(s, id, one("before")), "race create"))
  use #(owner, _) <- result.try(ok(storage.do_claim(s, id), "race claim"))
  let generation = storage.claim_generation(owner)
  let ready = process.new_subject()
  let reply = process.new_subject()
  process.spawn(fn() {
    let go = process.new_subject()
    process.send(ready, go)
    process.receive_forever(go)
    process.send(reply, storage.do_cancel(s, id))
  })
  process.send(process.receive_forever(ready), Nil)
  let committed = commit(s, owner, 0, False, one("after"))
  use _ <- result.try(equal(
    process.receive_forever(reply),
    Ok(Nil),
    "racing cancel succeeds",
  ))
  use expected <- result.try(case committed {
    Ok(_) -> Ok(#(1, generation, True, one("after")))
    Error(storage.CancellationChanged) ->
      Ok(#(0, generation, True, one("before")))
    _ -> Error(UnexpectedResult("cancel/commit must linearize"))
  })
  use _ <- result.try(equal(
    loaded(s, id),
    Ok(expected),
    "cancel/commit preserves winner and intent",
  ))
  equal(storage.do_release(s, owner), Ok(Nil), "race release")
}

fn owner_loss(s: Storage, within: Int) -> Result(Nil, Failure) {
  let id = fresh_id()
  use _ <- result.try(ok(
    storage.do_create(s, id, one("survives")),
    "owner-loss create",
  ))
  let reply = process.new_subject()
  let owner =
    process.spawn(fn() {
      process.send(reply, storage.do_claim(s, id))
      process.sleep_forever()
    })
  use #(lost, before) <- result.try(ok(
    process.receive_forever(reply),
    "dying owner claim",
  ))
  lose(owner)
  let deadline = ffi.monotonic_time() + within
  use #(successor, after) <- result.try(ok(
    reclaim(s, id, deadline, poll_interval(within)),
    "reclaim within owner_loss_within after owner loss",
  ))
  use _ <- result.try(equal(
    storage.generation(after) > storage.generation(before),
    True,
    "owner loss advances generation",
  ))
  use _ <- result.try(equal(
    #(storage.revision(after), storage.cancelled(after), storage.records(after)),
    #(
      storage.revision(before),
      storage.cancelled(before),
      storage.records(before),
    ),
    "owner loss preserves progress",
  ))
  use _ <- result.try(equal(
    commit(s, lost, 0, False, one("")),
    Error(storage.StaleOwner),
    "lost owner's claim fenced",
  ))
  equal(storage.do_release(s, successor), Ok(Nil), "owner-loss release")
}

fn live_owner_keeps_claim(s: Storage, within: Int) -> Result(Nil, Failure) {
  let id = fresh_id()
  use _ <- result.try(ok(storage.do_create(s, id, one("")), "live-owner create"))
  let #(owner, claimed) = holder(s, id)
  use _ <- result.try(claimed)
  let until = ffi.monotonic_time() + 2 * within
  use _ <- result.try(stays_busy(s, id, until, poll_interval(within) * 4))
  lose(owner)
  let deadline = ffi.monotonic_time() + within
  use #(successor, _) <- result.try(ok(
    reclaim(s, id, deadline, poll_interval(within)),
    "reclaim after the live owner is lost",
  ))
  equal(storage.do_release(s, successor), Ok(Nil), "live-owner release")
}

/// A commit replaces the records it names and keeps every other record; a
/// refused commit writes none of its records.
fn records_merge(s: Storage, _within: Int) -> Result(Nil, Failure) {
  let id = fresh_id()
  use _ <- result.try(ok(
    storage.do_create(s, id, records([#("a", "1"), #("b", "2")])),
    "merge create",
  ))
  use #(owner, _) <- result.try(ok(storage.do_claim(s, id), "merge claim"))
  let generation = storage.claim_generation(owner)
  use _ <- result.try(equal(
    commit(s, owner, 0, False, records([#("b", "3"), #("c", "4")])),
    Ok(#(1, generation, False, records([#("a", "1"), #("b", "3"), #("c", "4")]))),
    "a commit replaces only the records it names",
  ))
  use _ <- result.try(equal(
    commit(s, owner, 0, False, records([#("a", "lost"), #("d", "lost")])),
    Error(storage.Conflict),
    "merge conflict",
  ))
  use _ <- result.try(equal(
    loaded(s, id),
    Ok(#(1, generation, False, records([#("a", "1"), #("b", "3"), #("c", "4")]))),
    "a refused commit writes none of its records",
  ))
  use _ <- result.try(equal(
    commit(s, owner, 1, False, dict.new()),
    Ok(#(2, generation, False, records([#("a", "1"), #("b", "3"), #("c", "4")]))),
    "a commit with no records advances the revision",
  ))
  equal(storage.do_release(s, owner), Ok(Nil), "merge release")
}

/// `list` returns unowned executions of one phase in id order, page by
/// page.
fn listing(s: Storage, within: Int) -> Result(Nil, Failure) {
  // One prefix per run keeps ids in order and apart from other scenarios.
  let prefix = fresh_id() <> "-"
  let id = fn(name) { prefix <> name }
  use _ <- result.try(
    list.try_each(
      ["a", "b", "c", "d", "held", "finished", "suspended"],
      fn(name) {
        ok(storage.do_create(s, id(name), one("")), "listing create")
        |> result.replace(Nil)
      },
    ),
  )
  let #(holder_pid, claimed) = holder(s, id("held"))
  use _ <- result.try(claimed)
  use _ <- result.try(
    list.try_each(
      [
        #(id("finished"), storage.Finished),
        #(id("suspended"), storage.Suspended),
      ],
      fn(pair) {
        use #(claim, _) <- result.try(ok(
          storage.do_claim(s, pair.0),
          "listing claim",
        ))
        use _ <- result.try(ok(
          storage.do_commit(
            s,
            claim,
            storage.Commit(
              expected_revision: 0,
              observed_cancelled: False,
              phase: pair.1,
              records: dict.new(),
            ),
          ),
          "listing commit",
        ))
        ok(storage.do_release(s, claim), "listing release")
      },
    ),
  )
  let mine = fn(listed: List(String)) {
    list.filter(listed, fn(listed) { string.starts_with(listed, prefix) })
  }
  let page = fn(phase, after, limit) {
    storage.do_list(s, storage.Listing(phase:, after:, limit:))
    |> result.map(mine)
  }
  use _ <- result.try(equal(
    page(storage.Pending, Some(prefix), 2),
    Ok([id("a"), id("b")]),
    "list returns the first page of pending executions in id order",
  ))
  use _ <- result.try(equal(
    page(storage.Pending, Some(id("b")), 2),
    Ok([id("c"), id("d")]),
    "list continues after the given id",
  ))
  use _ <- result.try(equal(
    page(storage.Pending, Some(id("d")), 10),
    Ok([]),
    "list skips an execution that a live claim holds",
  ))
  use _ <- result.try(equal(
    page(storage.Suspended, Some(prefix), 10),
    Ok([id("suspended")]),
    "list filters by phase: suspended",
  ))
  use _ <- result.try(equal(
    page(storage.Finished, Some(prefix), 10),
    Ok([id("finished")]),
    "list filters by phase: finished",
  ))
  use all <- result.try(ok(
    storage.do_list(s, storage.Listing(storage.Pending, None, 100_000)),
    "list from the start",
  ))
  use _ <- result.try(equal(
    mine(all),
    [id("a"), id("b"), id("c"), id("d")],
    "list without a cursor starts at the first id",
  ))
  lose(holder_pid)
  let deadline = ffi.monotonic_time() + within
  equal(
    listed_by(deadline, poll_interval(within), fn() {
      page(storage.Pending, Some(id("d")), 10)
    }),
    Ok([id("held")]),
    "list includes an execution once its claim is lost",
  )
}

/// Lists until the listing is not empty or `deadline` passes.
fn listed_by(
  deadline: Int,
  every: Int,
  list: fn() -> Result(List(String), storage.Error),
) -> Result(List(String), storage.Error) {
  case list() {
    Ok([]) ->
      case ffi.monotonic_time() < deadline {
        True -> {
          process.sleep(every)
          listed_by(deadline, every, list)
        }
        False -> Ok([])
      }
    result -> result
  }
}

fn poll_interval(within: Int) -> Int {
  int.clamp(within / 20, min: 1, max: 25)
}

fn reclaim(
  s: Storage,
  id: String,
  deadline: Int,
  every: Int,
) -> Result(#(Claim, Stored), storage.Error) {
  case storage.do_claim(s, id) {
    Error(storage.Busy) ->
      case ffi.monotonic_time() < deadline {
        True -> {
          process.sleep(every)
          reclaim(s, id, deadline, every)
        }
        False -> Error(storage.Busy)
      }
    result -> result
  }
}

fn stays_busy(
  s: Storage,
  id: String,
  until: Int,
  every: Int,
) -> Result(Nil, Failure) {
  case ffi.monotonic_time() >= until {
    True -> Ok(Nil)
    False -> {
      use _ <- result.try(equal(
        storage.do_claim(s, id) |> result.replace(Nil),
        Error(storage.Busy),
        "a live owner keeps its claim past owner_loss_within",
      ))
      process.sleep(every)
      stays_busy(s, id, until, every)
    }
  }
}

/// A drive whose runner is killed while its caller lives returns
/// `RunnerLost` and frees the execution: the next drive claims it at once,
/// well inside `owner_loss_within`, and resumes from the checkpoint.
fn killed_runner(s: Storage, _within: Int) -> Result(Nil, Failure) {
  let id = fresh_id()
  let runners = process.new_subject()
  let entered = process.new_subject()
  let watched = reporting_claims(s, runners)
  let assert Ok(run) =
    durable.start_or_reconnect(
      blocking(entered, False),
      watched,
      id: id,
      input: "x",
    )
  let lost = process.new_subject()
  process.spawn(fn() {
    process.send(lost, durable.drive(run, timeout: duration.seconds(5)))
  })
  use runner <- result.try(
    process.receive(runners, 5000)
    |> result.replace_error(UnexpectedResult("killed-runner claim")),
  )
  use _ <- result.try(
    process.receive(entered, 5000)
    |> result.replace_error(UnexpectedResult("killed-runner attempt")),
  )
  process.kill(runner)
  use _ <- result.try(equal(
    process.receive(lost, 5000),
    Ok(Error(durable.RunnerLost)),
    "a killed runner's drive returns RunnerLost",
  ))
  use resumed <- result.try(
    durable.reconnect(blocking(entered, True), s, id: id)
    |> result.replace_error(UnexpectedResult("killed-runner reconnect")),
  )
  equal(
    durable.drive(resumed, timeout: duration.seconds(5)),
    Ok(execution.Completed("x recovered")),
    "the next drive after a killed runner claims at once and resumes",
  )
}

/// One durable step that blocks until its runner dies; after a restart
/// (`recovered`), its resolver reports the attempt completed.
fn blocking(
  entered: process.Subject(Nil),
  recovered: Bool,
) -> durable.Persistence(String, String, String, String) {
  let text = codec.text()
  let workflow =
    saga.define("saga-conformance-killed-runner", fn(input) {
      saga.perform(
        input,
        saga.step("block", fn(value) {
          process.send(entered, Nil)
          process.sleep_forever()
          Ok(value)
        })
          |> durable.recoverable(
            version: "1",
            input: text,
            output: text,
            resolve: fn(value, _) {
              case recovered {
                True -> durable.Completed(value <> " recovered")
                False -> durable.MaybeSent
              }
            },
          ),
      )
    })
  durable.new(
    workflow,
    input: text,
    output: text,
    error: text,
    undo_error: text,
  )
  |> durable.with_config(
    execution.config() |> execution.with_step_timeout(execution.Infinity),
  )
}

/// The storage under test, reporting each process that claims through it
/// (a durable runner) to `runners`. It keeps the storage's renewal and call
/// timeout.
fn reporting_claims(
  s: Storage,
  runners: process.Subject(process.Pid),
) -> Storage {
  let rebuilt =
    storage.new(
      create: fn(id, records) { storage.do_create(s, id, records) },
      load: fn(id) { storage.do_load(s, id) },
      claim: fn(id) {
        let claimed = storage.do_claim(s, id)
        case claimed {
          Ok(_) -> process.send(runners, process.self())
          Error(_) -> Nil
        }
        claimed
      },
      commit: fn(claim, change) { storage.do_commit(s, claim, change) },
      release: fn(claim) { storage.do_release(s, claim) },
      cancel: fn(id) { storage.do_cancel(s, id) },
      list: fn(listing) { storage.do_list(s, listing) },
    )
    |> storage.with_call_timeout(storage.call_timeout(s))
  case storage.renewal(s) {
    Some(#(every, renew)) -> storage.with_renewal(rebuilt, every:, renew:)
    None -> rebuilt
  }
}
