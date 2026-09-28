//// `execution.start_reporting`: a run that delivers its outcome to a
//// caller-supplied `Subject` instead of to the process that started it.
////
//// The owner process in most tests is spawned unlinked and killed, the way
//// a consumer's task is killed when its own run is cancelled; the test
//// process holds the report subject and outlives it. Every
//// synchronization point is a message exchange, never a sleep.

import gleam/erlang/process.{type Pid, type Subject}
import gleeunit/should
import saga
import saga/execution.{type Execution}
import saga/testing
import support/probe

/// What the `reserve` step's undo does.
pub type Release {
  Releases
  Refuses
  Hangs
}

/// `reserve` completes at once; `charge` blocks on `gate`. Each undo sends
/// its name to `log` before acting.
fn trip(
  gate: probe.Gate,
  log: Subject(String),
) -> saga.Workflow(Release, String, String, String) {
  let reserve =
    saga.step("reserve", fn(release: Release) { Ok(release) })
    |> saga.undo(fn(_input, release) {
      process.send(log, "release")
      case release {
        Releases -> Ok(Nil)
        Refuses -> Error("refused")
        Hangs -> {
          process.sleep_forever()
          Ok(Nil)
        }
      }
    })
  let charge =
    saga.step("charge", fn(_release: Release) {
      probe.enter(gate)
      Ok("charged")
    })
    |> saga.undo(fn(_input, _output) {
      process.send(log, "refund")
      Ok(Nil)
    })
  let assert Ok(workflow) =
    saga.define("trip", fn(input) {
      input |> saga.perform(reserve) |> saga.perform(charge)
    })
  workflow
}

fn at(name: String) -> saga.StepAddress {
  saga.StepAddress([], name, 1)
}

fn settlement(
  undone undone: List(String),
  undo_failures undo_failures: List(execution.UndoFailure(String)),
  interrupted interrupted: List(String),
) -> execution.Settlement(String, String) {
  execution.Settlement(
    undone: list_map(undone, at),
    undo_failures: undo_failures,
    not_undoable: [],
    held: [],
    interrupted: list_map(interrupted, at),
    compensation_failures: [],
    sibling_failures: [],
  )
}

fn list_map(items: List(a), f: fn(a) -> b) -> List(b) {
  case items {
    [] -> []
    [first, ..rest] -> [f(first), ..list_map(rest, f)]
  }
}

/// Starts `workflow` from a fresh, unlinked owner process that reports to
/// `report` and then idles until it is killed.
fn start_owned_elsewhere(
  workflow: saga.Workflow(i, o, e, u),
  input: i,
  config: execution.Config,
  report: Subject(execution.Outcome(o, e, u)),
) -> #(Pid, Execution(o, e, u)) {
  let handed = process.new_subject()
  let owner =
    process.spawn_unlinked(fn() {
      let assert Ok(run) =
        execution.start_reporting(workflow, input, config, to: report)
      process.send(handed, run)
      process.sleep_forever()
    })
  let assert Ok(run) = process.receive(handed, 5000)
  #(owner, run)
}

/// Kills `owner` once `charge` is blocked, and waits until the run has
/// begun settling because of it.
fn kill_owner_mid_run(
  owner: Pid,
  run: Execution(o, e, u),
  gate: probe.Gate,
) -> Nil {
  let assert Ok(_) = probe.wait_entered(gate, 5000)
  process.kill(owner)
  let assert Ok(_) =
    testing.wait_until(
      run,
      matching: fn(progress) { progress.phase != execution.Running },
      within: 5000,
    )
  Nil
}

/// The report was the only message: once the coordinator has exited
/// (its `Down` is ordered after anything it sent), `report` is empty.
fn assert_reported_once(
  run: Execution(o, e, u),
  report: Subject(execution.Outcome(o, e, u)),
) -> Nil {
  let monitor = process.monitor(execution.pid(run))
  let assert Ok(_) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(5000)
  let assert Error(Nil) = process.receive(report, 0)
  Nil
}

fn logged(log: Subject(String)) -> List(String) {
  case process.receive(log, 0) {
    Ok(entry) -> [entry, ..logged(log)]
    Error(Nil) -> []
  }
}

// ---------------------------------------------------------------------------
// The owner dies mid-run
// ---------------------------------------------------------------------------

/// The owner's death cancels the run; the steps that completed (including
/// the one that finished inside the settle window) are undone, and the
/// surviving holder of `report` learns exactly that, once.
pub fn owner_death_reports_a_completed_compensation_test() {
  let gate = probe.new_gate()
  let log = process.new_subject()
  let report = process.new_subject()
  let #(owner, run) =
    start_owned_elsewhere(trip(gate, log), Releases, execution.config(), report)

  kill_owner_mid_run(owner, run, gate)
  probe.open(gate)

  process.receive(report, 5000)
  |> should.equal(
    Ok(execution.Cancelled(
      execution.OwnerExited,
      settlement(
        undone: ["charge", "reserve"],
        undo_failures: [],
        interrupted: [],
      ),
    )),
  )
  assert_reported_once(run, report)
  logged(log) |> should.equal(["refund", "release"])
}

/// An undo that fails after the owner died is reported, not hidden.
pub fn owner_death_reports_a_failed_compensation_test() {
  let gate = probe.new_gate()
  let log = process.new_subject()
  let report = process.new_subject()
  let #(owner, run) =
    start_owned_elsewhere(trip(gate, log), Refuses, execution.config(), report)

  kill_owner_mid_run(owner, run, gate)
  probe.open(gate)

  process.receive(report, 5000)
  |> should.equal(
    Ok(execution.Cancelled(
      execution.OwnerExited,
      settlement(
        undone: ["charge"],
        undo_failures: [execution.UndoFailed(at("reserve"), "refused")],
        interrupted: [],
      ),
    )),
  )
  assert_reported_once(run, report)
}

/// A step still running when the settle window closes is killed and
/// reported `interrupted` (its effect unknown, never undone); an undo that
/// outlives `cleanup_timeout` is reported `UndoTimedOut`. Both still reach
/// the report.
pub fn owner_death_reports_an_interrupted_compensation_test() {
  let gate = probe.new_gate()
  let log = process.new_subject()
  let report = process.new_subject()
  let config =
    execution.Config(
      ..execution.config(),
      settle_timeout: 0,
      cleanup_timeout: 50,
    )
  let #(owner, run) =
    start_owned_elsewhere(trip(gate, log), Hangs, config, report)

  kill_owner_mid_run(owner, run, gate)

  process.receive(report, 5000)
  |> should.equal(
    Ok(execution.Cancelled(
      execution.OwnerExited,
      settlement(
        undone: [],
        undo_failures: [execution.UndoTimedOut(at("reserve"))],
        interrupted: ["charge"],
      ),
    )),
  )
  assert_reported_once(run, report)
}

// ---------------------------------------------------------------------------
// The owner stays alive
// ---------------------------------------------------------------------------

/// The owner reports to a subject of its own and receives the outcome in
/// its own selector, next to its other messages. `await` is not available
/// for a reporting run, and nothing else is left in the owner's mailbox.
pub fn a_report_joins_the_owners_own_selector_test() {
  probe.flush_mailbox()
  let gate = probe.new_gate()
  let log = process.new_subject()
  let report = process.new_subject()
  let other = process.new_subject()
  let assert Ok(run) =
    execution.start_reporting(
      trip(gate, log),
      Releases,
      execution.config(),
      to: report,
    )
  execution.await(run, 0) |> should.equal(Error(execution.NotOwner))

  let selector =
    process.new_selector()
    |> process.select_map(report, Ok)
    |> process.select_map(other, Error)
  process.send(other, "hello")
  process.selector_receive(selector, 5000)
  |> should.equal(Ok(Error("hello")))

  let assert Ok(_) = probe.wait_entered(gate, 5000)
  probe.open(gate)
  process.selector_receive(selector, 5000)
  |> should.equal(Ok(Ok(execution.Completed("charged"))))

  assert_reported_once(run, report)
  probe.mailbox_length() |> should.equal(0)
}

/// A process registered under a name receives the report sent to that
/// name's subject.
pub fn a_named_process_receives_the_report_test() {
  let name = process.new_name("saga_report_test")
  let forwarded = process.new_subject()
  let registered = process.new_subject()
  let receiver =
    process.spawn_unlinked(fn() {
      let assert Ok(Nil) = process.register(process.self(), name)
      process.send(registered, Nil)
      let assert Ok(outcome) =
        process.receive(process.named_subject(name), 5000)
      process.send(forwarded, outcome)
    })
  let assert Ok(Nil) = process.receive(registered, 5000)

  let gate = probe.new_gate()
  probe.open(gate)
  let log = process.new_subject()
  let assert Ok(_run) =
    execution.start_reporting(
      trip(gate, log),
      Releases,
      execution.config(),
      to: process.named_subject(name),
    )

  process.receive(forwarded, 5000)
  |> should.equal(Ok(execution.Completed("charged")))
  process.kill(receiver)
}

/// A report that cannot be delivered (no process holds the name) is
/// dropped; the coordinator still exits normally.
pub fn an_undeliverable_report_is_dropped_test() {
  let name = process.new_name("saga_report_nobody")
  let gate = probe.new_gate()
  probe.open(gate)
  let log = process.new_subject()
  let assert Ok(run) =
    execution.start_reporting(
      trip(gate, log),
      Releases,
      execution.config(),
      to: process.named_subject(name),
    )
  let monitor = process.monitor(execution.pid(run))
  let assert Ok(process.ProcessDown(_, _, process.Normal)) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(5000)
  Nil
}

// ---------------------------------------------------------------------------
// Loss and refusal
// ---------------------------------------------------------------------------

/// A coordinator killed from outside sends no report. The receiver learns
/// of the loss from its own monitor on `execution.pid`: a `Down` with no
/// report before it.
pub fn a_lost_run_is_seen_by_the_receivers_monitor_test() {
  let gate = probe.new_gate()
  let log = process.new_subject()
  let report = process.new_subject()
  let assert Ok(run) =
    execution.start_reporting(
      trip(gate, log),
      Releases,
      execution.config(),
      to: report,
    )
  let monitor = process.monitor(execution.pid(run))
  let assert Ok(_) = probe.wait_entered(gate, 5000)

  process.kill(execution.pid(run))

  process.new_selector()
  |> process.select_map(report, fn(outcome) { Ok(outcome) })
  |> process.select_specific_monitor(monitor, fn(down) {
    case down {
      process.ProcessDown(_, _, reason) -> Error(reason)
      process.PortDown(_, _, reason) -> Error(reason)
    }
  })
  |> process.selector_receive(5000)
  |> should.equal(Ok(Error(process.Killed)))
}

pub fn start_reporting_refuses_an_invalid_config_test() {
  let gate = probe.new_gate()
  let log = process.new_subject()
  let report = process.new_subject()
  let assert Error(execution.InvalidConfig([
    execution.MaxConcurrencyNotPositive(0),
  ])) =
    execution.start_reporting(
      trip(gate, log),
      Releases,
      execution.Config(..execution.config(), max_concurrency: 0),
      to: report,
    )
  let assert Error(Nil) = process.receive(report, 0)
  Nil
}
