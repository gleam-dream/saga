// SPDX-FileCopyrightText: 2026 gleam-dream contributors
//
// SPDX-License-Identifier: Apache-2.0
//
/// Gleam side of the Reactor 1.0.6 differential oracle.
///
/// Two entry points read the SAME scenario functions below, so there is
/// exactly one implementation of each D1-D7 scenario, never a hand
/// duplicated "test version" and "trace version" that could silently
/// drift apart:
///
///   - `oracle_dN_..._test()` runs under `gleam test` and asserts the
///     scenario's outcome (used by the wider test suite and CI).
///   - `main()` runs under `gleam run -m oracle_test` and prints the same
///     scenarios' results as normalized `normalized.dN.*` lines to
///     stdout, in the exact text format
///     `oracle/reactor/lib/oracle/trace.ex`'s `print_normalized/2` emits
///     on the Reactor side (see that module's moduledoc for the format).
///     `scripts/oracle.sh` runs both sides and does a real `diff` between
///     them: same-shaped output is asserted equal by the script; a
///     documented, deliberate difference (D2's undo order, D3's sibling
///     settlement) must match a checked-in expected diff under
///     `oracle/differences/*.diff`, not an echoed summary.
///
/// See `PROVENANCE.md` for the full upstream-test-to-Saga-test mapping
/// and exactly what "differential comparison" means for each scenario.
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/io
import gleam/list
import gleam/string
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
// Normalized printing, mirroring oracle/reactor/lib/oracle/trace.ex's
// `print_normalized/2` byte-for-byte: an event tuple #("kind", "name")
// becomes "kind:name", a bare event becomes its own text, a list becomes
// "[e1, e2, e3]", and booleans/ints print literally.
// ---------------------------------------------------------------------------

fn print_normalized_trace(
  label: String,
  events: List(#(String, String)),
) -> Nil {
  let body =
    events
    |> list.map(fn(event) {
      let #(kind, name) = event
      case name {
        "" -> kind
        _ -> kind <> ":" <> name
      }
    })
    |> string.join(", ")
  io.println(label <> ": [" <> body <> "]")
}

fn print_normalized_int(label: String, value: Int) -> Nil {
  io.println(label <> ": " <> int.to_string(value))
}

fn print_normalized_bool(label: String, value: Bool) -> Nil {
  io.println(
    label
    <> ": "
    <> case value {
      True -> "true"
      False -> "false"
    },
  )
}

fn print_normalized_atom(label: String, value: String) -> Nil {
  io.println(label <> ": " <> value)
}

// ---------------------------------------------------------------------------
// D1: dependency ordering. Reactor: [run: a, run: b, run: c]. Saga matches.
// ---------------------------------------------------------------------------

fn run_d1() -> #(
  execution.Outcome(Int, DemoError, DemoUndoError),
  List(#(String, String)),
) {
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

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  #(outcome, snapshot(events))
}

pub fn oracle_d1_sequential_dependency_test() {
  let #(outcome, events) = run_d1()
  let assert execution.Completed(_) = outcome

  // Matches Reactor's d1.trace exactly.
  events
  |> should.equal([#("run", "a"), #("run", "b"), #("run", "c")])
}

fn trace_d1() -> Nil {
  let #(outcome, events) = run_d1()
  let assert execution.Completed(_) = outcome
  print_normalized_trace("normalized.d1.trace", events)
  print_normalized_atom("normalized.d1.result", "ok")
}

// ---------------------------------------------------------------------------
// D2: undo order and multiple undo failures. Reactor undoes forward
// (e1, e2, e3) and retains 3 error classes. Saga is a DELIBERATE
// DIFFERENCE: reverse completion order (e3, e2, e1), and only the 2 undo
// failures are retained in `settlement.undo_failures` (the triggering run
// failure is the `Failed` cause, not an undo failure). See
// `oracle/differences/d2.diff` for the checked, exact expected diff.
// ---------------------------------------------------------------------------

fn run_d2() -> #(
  execution.Outcome(Nil, DemoError, DemoUndoError),
  List(#(String, String)),
) {
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

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  #(outcome, snapshot(events))
}

pub fn oracle_d2_undo_order_and_failures_test() {
  let #(outcome, events) = run_d2()
  let assert execution.Failed(cause, settlement) = outcome

  case cause {
    execution.StepFailed(step, Boom) -> step.name |> should.equal("e4")
    _ -> panic as "expected StepFailed(e4, Boom)"
  }

  // Deliberate difference from Reactor's forward order (P1 / D2): Saga
  // undoes in REVERSE completion order.
  events
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

fn trace_d2() -> Nil {
  let #(outcome, events) = run_d2()
  let assert execution.Failed(_cause, settlement) = outcome
  print_normalized_trace("normalized.d2.trace", events)
  print_normalized_int(
    "normalized.d2.undo_failure_count",
    list.length(settlement.undo_failures),
  )
  print_normalized_atom("normalized.d2.result", "error")
}

// ---------------------------------------------------------------------------
// D3: failure with an active sibling. Reactor returns while `slow` is
// still running and never undoes it (an orphaned effect: P2). Saga is a
// DELIBERATE DIFFERENCE: it settles `slow` before returning, so it is
// either undone (if it finishes within settle_timeout) or reported
// `interrupted` (if killed). See `oracle/differences/d3.diff`.
//
// `slow` blocks on a manual gate rather than sleeping, and the gate is
// released by `fast_fail` itself, from inside its own step body,
// immediately before it returns `Error(Boom)`. This guarantees — by
// construction, not by timing — that `slow` is still blocked on the gate
// (has not reached `record(events, #("done", "slow"))`) at the exact
// instant the failure occurs: releasing the gate and failing happen in
// one sequential step body, so there is no window in which an
// independent timer could fire early or late relative to the failure.
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

fn run_d3() -> #(
  execution.Outcome(List(Int), DemoError, DemoUndoError),
  List(#(String, String)),
) {
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
            // Open the gate for `slow` from inside the failing step's own
            // body, immediately before failing. `slow` is therefore
            // guaranteed to still be waiting on the gate (not yet past its
            // own `wait_for_gate` call) at the moment this failure is
            // observed by the coordinator: the release and the failure are
            // two statements in one sequential function, not two
            // independently-timed processes.
            process.send(slow_gate, Release)
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
      saga.all(slow, [fast_fail, quick])
    })

  // `slow`, `fast_fail`, and `quick` must all be able to attempt
  // concurrently for D3's interleaving (fast_fail fails while slow is still
  // waiting) to happen at all — `max_concurrency` is set explicitly (rather
  // than relying on `config()`'s scheduler-count default) so this passes
  // under a single-scheduler `+S 1:1` run too.
  let config = execution.Config(..execution.config(), max_concurrency: 3)
  let assert Ok(outcome) = execution.run(workflow, 0, config)
  #(outcome, snapshot(events))
}

pub fn oracle_d3_failure_with_active_sibling_settles_test() {
  let #(outcome, recorded) = run_d3()
  let assert execution.Failed(_cause, settlement) = outcome

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

fn trace_d3() -> Nil {
  let #(outcome, recorded) = run_d3()
  let assert execution.Failed(_cause, settlement) = outcome
  print_normalized_bool(
    "normalized.d3.fast_fail_compensated",
    list.contains(recorded, #("compensate", "fast_fail")),
  )
  print_normalized_bool(
    "normalized.d3.quick_undone",
    list.contains(recorded, #("undo", "quick")),
  )
  print_normalized_bool(
    "normalized.d3.slow_done",
    list.contains(recorded, #("done", "slow")),
  )
  print_normalized_bool(
    "normalized.d3.slow_undone",
    settlement.undone |> list.map(fn(a) { a.name }) |> list.contains("slow"),
  )
  print_normalized_atom("normalized.d3.result", "error")
}

// ---------------------------------------------------------------------------
// D4: retry limits. Reactor: max_retries: 2 -> 3 total attempts, ok.
// Saga's equivalent is max_attempts: 3 (Saga counts total attempts, not
// retries after the first, per PROVENANCE D4/R7).
// ---------------------------------------------------------------------------

fn run_d4() -> #(execution.Outcome(Nil, DemoError, DemoUndoError), Int) {
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

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  let attempts =
    snapshot(events) |> list.filter(fn(e) { e.0 == "attempt" }) |> list.length
  #(outcome, attempts)
}

pub fn oracle_d4_retry_then_success_test() {
  let #(outcome, attempts) = run_d4()
  let assert execution.Completed(Nil) = outcome
  attempts |> should.equal(3)
}

fn trace_d4() -> Nil {
  let #(outcome, attempts) = run_d4()
  let assert execution.Completed(Nil) = outcome
  print_normalized_int("normalized.d4.attempts", attempts)
  print_normalized_atom("normalized.d4.result", "ok")
}

// ---------------------------------------------------------------------------
// D5: compensate `{:continue, v}` in Reactor maps to Saga's
// `Continue(output, undo)`. Matches: the run completes with the
// replacement value.
// ---------------------------------------------------------------------------

fn run_d5() -> #(
  execution.Outcome(String, DemoError, DemoUndoError),
  List(#(String, String)),
) {
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

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  #(outcome, snapshot(events))
}

pub fn oracle_d5_compensate_continue_test() {
  let #(outcome, events) = run_d5()
  let assert execution.Completed(output) = outcome
  output |> should.equal("replacement")
  events |> should.equal([#("run", ""), #("compensate", "")])
}

fn trace_d5() -> Nil {
  let #(outcome, events) = run_d5()
  let assert execution.Completed(output) = outcome
  print_normalized_trace("normalized.d5.trace", events)
  print_normalized_atom("normalized.d5.result", "ok:" <> output)
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

fn run_d6() -> #(execution.Outcome(List(Int), DemoError, DemoUndoError), Int) {
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
      let assert [first, ..rest] =
        ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"]
        |> list.map(fn(name) { input |> saga.perform(make_step(name)) })
      saga.all(first, rest)
    })

  let assert Ok(outcome) =
    execution.run(
      workflow,
      0,
      execution.Config(..execution.config(), max_concurrency: 3),
    )
  #(outcome, peak_snapshot(tracker))
}

pub fn oracle_d6_max_concurrency_bound_test() {
  let #(outcome, peak) = run_d6()
  let assert execution.Completed(_) = outcome
  // Matches Reactor's d6.peak_concurrency: 3.
  peak |> should.equal(3)
}

fn trace_d6() -> Nil {
  let #(outcome, peak) = run_d6()
  let assert execution.Completed(_) = outcome
  print_normalized_int("normalized.d6.peak_concurrency", peak)
  print_normalized_atom("normalized.d6.result", "ok")
}

// ---------------------------------------------------------------------------
// D7: a shared dependency executes once. Native Saga scenario (no single
// upstream test isolates this; memoization is implicit in Reactor's DAG).
// Compared differentially: both sides run the shared producer exactly
// once.
// ---------------------------------------------------------------------------

fn run_d7() -> #(
  execution.Outcome(
    #(#(String, String), #(String, String)),
    DemoError,
    DemoUndoError,
  ),
  Int,
) {
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

  let assert Ok(outcome) = execution.run(workflow, 0, execution.config())
  #(outcome, snapshot(events) |> list.length)
}

pub fn oracle_d7_shared_step_once_test() {
  let #(outcome, produce_count) = run_d7()
  let assert execution.Completed(#(left, right)) = outcome
  left |> should.equal(#("left", "shared_value"))
  right |> should.equal(#("right", "shared_value"))
  produce_count |> should.equal(1)
}

fn trace_d7() -> Nil {
  let #(outcome, produce_count) = run_d7()
  let assert execution.Completed(_) = outcome
  print_normalized_int("normalized.d7.produce_count", produce_count)
  print_normalized_atom("normalized.d7.result", "ok")
}

// ---------------------------------------------------------------------------
// Entry point for `gleam run -m oracle_test`, invoked by scripts/oracle.sh.
// Prints exactly the `normalized.dN.*` lines this file's moduledoc
// documents, nothing else, so the script's diff against the Reactor
// side's own `normalized.dN.*` lines is a clean comparison.
// ---------------------------------------------------------------------------

pub fn main() -> Nil {
  trace_d1()
  trace_d2()
  trace_d3()
  trace_d4()
  trace_d5()
  trace_d6()
  trace_d7()
}
