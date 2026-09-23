// SPDX-FileCopyrightText: 2026 gleam-dream contributors
//
// SPDX-License-Identifier: Apache-2.0
//
/// Gleam side of the Reactor 1.0.6 differential oracle. Each `d*_test`
/// here reruns one of `oracle/reactor/scenarios/d*.exs`'s scenarios
/// against Saga and asserts either the same outcome as the recorded
/// Reactor trace (`oracle/reactor/expected/d*.txt`) or the documented,
/// deliberate difference. See `PROVENANCE.md` for the full table and
/// `scripts/oracle.sh` for how the two sides are run together.
import gleam/erlang/process.{type Subject}
import gleam/list
import gleeunit/should
import saga
import saga/execution

pub type DemoError {
  Boom
}

pub type DemoUndoError {
  UndoBoom(step: String)
}

// ---------------------------------------------------------------------------
// A tiny ordered event recorder, self-contained so this module does not
// reach into test/support/probe.gleam (owned by parallel work on the
// consumer example). Mirrors oracle/reactor/lib/oracle/trace.ex: each
// scenario appends small, orderable terms and reads them back in order.
// ---------------------------------------------------------------------------

type RecorderMessage(a) {
  Record(a)
  Snapshot(reply: Subject(List(a)))
}

fn new_recorder() -> Subject(RecorderMessage(a)) {
  let ready = process.new_subject()
  process.spawn(fn() {
    let subject = process.new_subject()
    process.send(ready, subject)
    recorder_loop(subject, [])
  })
  let assert Ok(subject) = process.receive(ready, 1000)
  subject
}

fn recorder_loop(subject: Subject(RecorderMessage(a)), events: List(a)) -> Nil {
  case process.receive_forever(subject) {
    Record(event) -> recorder_loop(subject, [event, ..events])
    Snapshot(reply) -> {
      process.send(reply, list.reverse(events))
      recorder_loop(subject, events)
    }
  }
}

fn record(subject: Subject(RecorderMessage(a)), event: a) -> Nil {
  process.send(subject, Record(event))
}

fn snapshot(subject: Subject(RecorderMessage(a))) -> List(a) {
  let reply = process.new_subject()
  process.send(subject, Snapshot(reply))
  let assert Ok(events) = process.receive(reply, 1000)
  events
}

// ---------------------------------------------------------------------------
// D1: dependency ordering. Reactor: [run: a, run: b, run: c]. Saga matches.
// ---------------------------------------------------------------------------

pub fn oracle_d1_sequential_dependency_test() {
  let events = new_recorder()
  let assert Ok(workflow) =
    saga.define("d1", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("a", fn(x: Int) {
            record(events, #("run", "a"))
            Ok(x)
          }),
        )
      let b =
        a
        |> saga.perform(
          saga.step("b", fn(x: Int) {
            record(events, #("run", "b"))
            Ok(x)
          }),
        )
      b
      |> saga.perform(
        saga.step("c", fn(x: Int) {
          record(events, #("run", "c"))
          Ok(x)
        }),
      )
    })

  let assert Ok(execution.Completed(_)) =
    execution.run(workflow, 0, execution.config())

  // Matches Reactor's d1.trace exactly.
  snapshot(events)
  |> should.equal([#("run", "a"), #("run", "b"), #("run", "c")])
}

// ---------------------------------------------------------------------------
// D2: undo order and multiple undo failures. Reactor undoes forward
// (e1, e2, e3) and retains 3 error classes. Saga is a DELIBERATE
// DIFFERENCE: reverse completion order (e3, e2, e1), and only the 2 undo
// failures are retained in `settlement.undo_failures` (the triggering run
// failure is the `Failed` cause, not an undo failure).
// ---------------------------------------------------------------------------

pub fn oracle_d2_undo_order_and_failures_test() {
  let events = new_recorder()
  let assert Ok(workflow) =
    saga.define("d2", fn(input) {
      let e1 =
        input
        |> saga.perform(
          saga.step("e1", fn(x: Int) {
            record(events, #("run", "e1"))
            Ok(x)
          })
          |> saga.undo(fn(_i, _o) {
            record(events, #("undo", "e1"))
            Ok(Nil)
          }),
        )
      let e2 =
        e1
        |> saga.perform(
          saga.step("e2", fn(x: Int) {
            record(events, #("run", "e2"))
            Ok(x)
          })
          |> saga.undo(fn(_i, _o) {
            record(events, #("undo", "e2"))
            Error(UndoBoom("e2"))
          }),
        )
      let e3 =
        e2
        |> saga.perform(
          saga.step("e3", fn(x: Int) {
            record(events, #("run", "e3"))
            Ok(x)
          })
          |> saga.undo(fn(_i, _o) {
            record(events, #("undo", "e3"))
            Error(UndoBoom("e3"))
          }),
        )
      e3 |> saga.perform(saga.step("e4", fn(_x: Int) { Error(Boom) }))
    })

  let assert Ok(execution.Failed(cause, settlement)) =
    execution.run(workflow, 0, execution.config())

  case cause {
    execution.StepFailed(step, Boom) -> step.name |> should.equal("e4")
    _ -> panic as "expected StepFailed(e4, Boom)"
  }

  // Deliberate difference from Reactor's forward order (P1 / D2): Saga
  // undoes in REVERSE completion order.
  snapshot(events)
  |> should.equal([
    #("run", "e1"),
    #("run", "e2"),
    #("run", "e3"),
    #("undo", "e3"),
    #("undo", "e2"),
    #("undo", "e1"),
  ])

  // Both undo failures are retained (matches Reactor retaining all
  // failures rather than stopping at the first).
  settlement.undo_failures
  |> list.map(fn(f) {
    case f {
      execution.UndoFailed(step, UndoBoom(name)) -> #(step.name, name)
      _ -> panic as "expected UndoFailed"
    }
  })
  |> should.equal([#("e3", "e3"), #("e2", "e2")])

  settlement.undone |> list.map(fn(a) { a.name }) |> should.equal(["e1"])
}

// ---------------------------------------------------------------------------
// D3: failure with an active sibling. Reactor returns while `slow` is
// still running and never undoes it (an orphaned effect: P2). Saga is a
// DELIBERATE DIFFERENCE: it settles `slow` before returning, so it is
// either undone (if it finishes within settle_timeout) or reported
// `interrupted` (if killed). This scenario uses a gate instead of a sleep
// so the settlement is deterministic rather than timing-dependent.
// ---------------------------------------------------------------------------

type GateMessage {
  Release
  AwaitRelease(reply: Subject(Nil))
}

fn new_manual_gate() -> Subject(GateMessage) {
  let ready = process.new_subject()
  process.spawn(fn() {
    let subject = process.new_subject()
    process.send(ready, subject)
    manual_gate_loop(subject, False, [])
  })
  let assert Ok(subject) = process.receive(ready, 1000)
  subject
}

fn manual_gate_loop(
  subject: Subject(GateMessage),
  released: Bool,
  waiters: List(Subject(Nil)),
) -> Nil {
  case process.receive_forever(subject) {
    Release -> {
      list.each(waiters, fn(w) { process.send(w, Nil) })
      manual_gate_loop(subject, True, [])
    }
    AwaitRelease(reply) ->
      case released {
        True -> {
          process.send(reply, Nil)
          manual_gate_loop(subject, released, waiters)
        }
        False -> manual_gate_loop(subject, released, [reply, ..waiters])
      }
  }
}

fn wait_for_gate(gate: Subject(GateMessage), timeout_ms: Int) -> Nil {
  let reply = process.new_subject()
  process.send(gate, AwaitRelease(reply))
  let assert Ok(_) = process.receive(reply, timeout_ms)
  Nil
}

pub fn oracle_d3_failure_with_active_sibling_settles_test() {
  let events = new_recorder()
  let slow_gate = new_manual_gate()

  let assert Ok(workflow) =
    saga.define("d3", fn(input) {
      let slow =
        input
        |> saga.perform(
          saga.step("slow", fn(x: Int) {
            record(events, #("start", "slow"))
            wait_for_gate(slow_gate, 2000)
            record(events, #("done", "slow"))
            Ok(x)
          })
          |> saga.undo(fn(_i, _o) {
            record(events, #("undo", "slow"))
            Ok(Nil)
          }),
        )
      let fast_fail =
        input
        |> saga.perform(
          saga.step("fast_fail", fn(_x: Int) {
            record(events, #("start", "fast_fail"))
            Error(Boom)
          })
          |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
            record(events, #("compensate", "fast_fail"))
            saga.Abort(Boom)
          }),
        )
      let quick =
        input
        |> saga.perform(
          saga.step("quick", fn(x: Int) {
            record(events, #("start", "quick"))
            Ok(x)
          })
          |> saga.undo(fn(_i, _o) {
            record(events, #("undo", "quick"))
            Ok(Nil)
          }),
        )
      saga.all([slow, fast_fail, quick])
    })

  // Release `slow` shortly after the run starts, well within the default
  // settle_timeout (5s), so the coordinator's settlement — not a race with
  // a real sleep — determines whether `slow` gets undone.
  process.spawn(fn() {
    process.sleep(50)
    process.send(slow_gate, Release)
  })

  let assert Ok(execution.Failed(_cause, settlement)) =
    execution.run(workflow, 0, execution.config())

  let recorded = snapshot(events)

  // fast_fail's compensate ran, quick was undone: matches Reactor.
  { list.contains(recorded, #("compensate", "fast_fail")) }
  |> should.be_true
  { list.contains(recorded, #("undo", "quick")) } |> should.be_true

  // Deliberate difference from Reactor (P2 / D3): `slow` is SETTLED, not
  // orphaned. Because it finished within settle_timeout, it is undone.
  { list.contains(recorded, #("done", "slow")) } |> should.be_true
  { list.contains(recorded, #("undo", "slow")) } |> should.be_true
  settlement.undone
  |> list.map(fn(a) { a.name })
  |> list.contains("slow")
  |> should.be_true
}

// ---------------------------------------------------------------------------
// D4: retry limits. Reactor: max_retries: 2 -> 3 total attempts, ok.
// Saga's equivalent is max_attempts: 3 (Saga counts total attempts, not
// retries after the first, per PROVENANCE D4/R7).
// ---------------------------------------------------------------------------

pub fn oracle_d4_retry_then_success_test() {
  let events = new_recorder()
  let assert Ok(workflow) =
    saga.define("d4", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(_x: Int) {
          record(events, #("attempt", ""))
          case
            list.length(
              list.filter(snapshot(events), fn(e) { e.0 == "attempt" }),
            )
            < 3
          {
            True -> Error(Boom)
            False -> Ok(Nil)
          }
        })
        |> saga.compensate(max_attempts: 3, with: fn(_i, _f, _a) {
          record(events, #("retry_decision", ""))
          saga.Retry
        }),
      )
    })

  let assert Ok(execution.Completed(Nil)) =
    execution.run(workflow, 0, execution.config())

  let attempts =
    snapshot(events) |> list.filter(fn(e) { e.0 == "attempt" }) |> list.length
  attempts |> should.equal(3)
}

// ---------------------------------------------------------------------------
// D5: compensate `{:continue, v}` in Reactor maps to Saga's
// `Continue(output, undo)`. Matches: the run completes with the
// replacement value.
// ---------------------------------------------------------------------------

pub fn oracle_d5_compensate_continue_test() {
  let events = new_recorder()
  let assert Ok(workflow) =
    saga.define("d5", fn(input) {
      input
      |> saga.perform(
        saga.step("flaky", fn(_x: Int) {
          record(events, #("run", ""))
          Error(Boom)
        })
        |> saga.compensate(max_attempts: 1, with: fn(_i, _f, _a) {
          record(events, #("compensate", ""))
          saga.Continue("replacement", saga.NoUndo)
        }),
      )
    })

  let assert Ok(execution.Completed(output)) =
    execution.run(workflow, 0, execution.config())
  output |> should.equal("replacement")
  snapshot(events) |> should.equal([#("run", ""), #("compensate", "")])
}

// ---------------------------------------------------------------------------
// D6: max_concurrency bound. Reactor: peak == 3 with max_concurrency: 3.
// Saga matches via Config.max_concurrency. Uses the gate-based counter
// pattern already exercised by scheduling_test.gleam's
// max_concurrency_bounds_running_attempts_test, restated here for the
// oracle's own trace so the comparison is self-contained.
// ---------------------------------------------------------------------------

type PeakMessage {
  PeakEnter(reply: Subject(Nil))
  PeakLeave
  PeakSnapshot(reply: Subject(Int))
}

fn new_peak_tracker() -> Subject(PeakMessage) {
  let ready = process.new_subject()
  process.spawn(fn() {
    let subject = process.new_subject()
    process.send(ready, subject)
    peak_tracker_loop(subject, 0, 0)
  })
  let assert Ok(subject) = process.receive(ready, 1000)
  subject
}

fn peak_tracker_loop(
  subject: Subject(PeakMessage),
  current: Int,
  peak: Int,
) -> Nil {
  case process.receive_forever(subject) {
    PeakEnter(reply) -> {
      let next_current = current + 1
      process.send(reply, Nil)
      peak_tracker_loop(subject, next_current, int_max(next_current, peak))
    }
    PeakLeave -> peak_tracker_loop(subject, current - 1, peak)
    PeakSnapshot(reply) -> {
      process.send(reply, peak)
      peak_tracker_loop(subject, current, peak)
    }
  }
}

fn int_max(a: Int, b: Int) -> Int {
  case a > b {
    True -> a
    False -> b
  }
}

fn peak_enter(subject: Subject(PeakMessage)) -> Nil {
  let reply = process.new_subject()
  process.send(subject, PeakEnter(reply))
  let assert Ok(_) = process.receive(reply, 1000)
  Nil
}

fn peak_leave(subject: Subject(PeakMessage)) -> Nil {
  process.send(subject, PeakLeave)
}

fn peak_snapshot(subject: Subject(PeakMessage)) -> Int {
  let reply = process.new_subject()
  process.send(subject, PeakSnapshot(reply))
  let assert Ok(peak) = process.receive(reply, 1000)
  peak
}

pub fn oracle_d6_max_concurrency_bound_test() {
  let tracker = new_peak_tracker()

  let make_step = fn(name: String) {
    saga.step(name, fn(x: Int) {
      peak_enter(tracker)
      process.sleep(30)
      peak_leave(tracker)
      Ok(x)
    })
  }

  let assert Ok(workflow) =
    saga.define("d6", fn(input) {
      ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"]
      |> list.map(fn(name) { input |> saga.perform(make_step(name)) })
      |> saga.all
    })

  let assert Ok(execution.Completed(_)) =
    execution.run(
      workflow,
      0,
      execution.Config(..execution.config(), max_concurrency: 3),
    )

  // Matches Reactor's d6.peak_concurrency: 3.
  peak_snapshot(tracker) |> should.equal(3)
}

// ---------------------------------------------------------------------------
// D7: a shared dependency executes once. Native Saga scenario (no single
// upstream test isolates this; memoization is implicit in Reactor's DAG).
// Compared differentially: both sides run the shared producer exactly
// once.
// ---------------------------------------------------------------------------

pub fn oracle_d7_shared_step_once_test() {
  let events = new_recorder()
  let assert Ok(workflow) =
    saga.define("d7", fn(input) {
      let producer =
        input
        |> saga.perform(
          saga.step("producer", fn(_x: Int) {
            record(events, #("produce", ""))
            Ok("shared_value")
          }),
        )
      let left =
        producer
        |> saga.perform(saga.step("left", fn(v: String) { Ok(#("left", v)) }))
      let right =
        producer
        |> saga.perform(saga.step("right", fn(v: String) { Ok(#("right", v)) }))
      saga.both(left, right)
    })

  let assert Ok(execution.Completed(#(left, right))) =
    execution.run(workflow, 0, execution.config())

  left |> should.equal(#("left", "shared_value"))
  right |> should.equal(#("right", "shared_value"))
  snapshot(events) |> list.length |> should.equal(1)
}
