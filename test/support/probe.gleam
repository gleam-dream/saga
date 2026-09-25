/// Test-only synchronization helpers: gates a step can block on, a counter
/// process for execution/concurrency assertions, and a `with_run` wrapper
/// that guarantees cleanup even when an assertion panics mid-test. No
/// helper here uses `process.sleep`; every synchronization point is a
/// message exchange.
import gleam/erlang/process.{type Pid, type Subject}
import saga
import saga/execution.{type Execution}

// ---------------------------------------------------------------------------
// Gates
// ---------------------------------------------------------------------------

/// A gate a step can block on until the test releases it. `enter` announces
/// arrival (so the test can synchronize on "the step has started") and then
/// blocks until `open` is called.
///
/// A `Subject` may only be received on by the process that created it, and
/// `enter` is called from a fresh task process on every attempt, so the
/// gate cannot be "one shared release subject" the way a single-owner
/// primitive would work. Instead the gate is a small broker process:
/// `enter` registers its *own* (self-owned) reply subject with the broker
/// and then receives on that; `open` asks the broker to release the oldest
/// registered waiter, queuing the release if nobody has registered yet.
pub opaque type Gate {
  Gate(broker: Subject(GateMessage), arrived: Subject(Pid))
}

type GateMessage {
  Register(reply: Subject(Nil))
  Release
}

pub fn new_gate() -> Gate {
  let ready = process.new_subject()
  process.spawn(fn() {
    let broker = process.new_subject()
    process.send(ready, broker)
    gate_loop(broker, [], 0)
  })
  let assert Ok(broker) = process.receive(ready, 1000)
  Gate(broker: broker, arrived: process.new_subject())
}

fn gate_loop(
  broker: Subject(GateMessage),
  waiters: List(Subject(Nil)),
  pending_releases: Int,
) -> Nil {
  let message = process.receive_forever(broker)
  case message {
    Register(reply) ->
      case pending_releases > 0 {
        True -> {
          process.send(reply, Nil)
          gate_loop(broker, waiters, pending_releases - 1)
        }
        False -> gate_loop(broker, [reply, ..waiters], pending_releases)
      }
    Release ->
      case list_reverse(waiters) {
        [] -> gate_loop(broker, [], pending_releases + 1)
        [oldest, ..rest] -> {
          process.send(oldest, Nil)
          gate_loop(broker, list_reverse(rest), pending_releases)
        }
      }
  }
}

fn list_reverse(items: List(a)) -> List(a) {
  list_reverse_acc(items, [])
}

fn list_reverse_acc(items: List(a), acc: List(a)) -> List(a) {
  case items {
    [] -> acc
    [first, ..rest] -> list_reverse_acc(rest, [first, ..acc])
  }
}

/// Blocks the calling (task) process until the gate opens. Announces
/// arrival on `arrived` first, so `wait_entered` can synchronize on it.
pub fn enter(gate: Gate) -> Nil {
  process.send(gate.arrived, process.self())
  let my_reply = process.new_subject()
  process.send(gate.broker, Register(my_reply))
  process.receive_forever(my_reply)
}

/// Opens the gate once, releasing exactly one blocked `enter` call (the
/// oldest still waiting), or queuing the release for the next `enter` if
/// nobody is currently blocked.
pub fn open(gate: Gate) -> Nil {
  process.send(gate.broker, Release)
}

/// Blocks until a task has announced arrival at the gate, bounded by
/// `timeout_ms`. Returns the arrived task's pid.
pub fn wait_entered(gate: Gate, timeout_ms: Int) -> Result(Pid, Nil) {
  process.receive(gate.arrived, timeout_ms)
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

/// A small process tracking how many times a step body has run and the
/// high-water mark of concurrently-entered bodies.
pub opaque type Counter {
  Counter(subject: Subject(CounterMessage))
}

type CounterMessage {
  Enter(reply: Subject(Nil))
  Leave(reply: Subject(Nil))
  Snapshot(reply: Subject(CounterState))
  AwaitHighWater(target: Int, reply: Subject(Nil))
  AwaitTotalEntries(target: Int, reply: Subject(Nil))
}

type CounterState {
  CounterState(
    total_entries: Int,
    current: Int,
    high_water: Int,
    high_water_waiters: List(#(Int, Subject(Nil))),
    total_entries_waiters: List(#(Int, Subject(Nil))),
  )
}

pub fn new_counter() -> Counter {
  // A `Subject` may only be received on by the process that created it
  // (`process.new_subject`'s owner), so the counter process must create its
  // own subject and hand it back through a short-lived bootstrap subject
  // rather than receiving on one created by the caller.
  let ready = process.new_subject()
  process.spawn(fn() {
    let subject = process.new_subject()
    process.send(ready, subject)
    counter_loop(
      subject,
      CounterState(
        total_entries: 0,
        current: 0,
        high_water: 0,
        high_water_waiters: [],
        total_entries_waiters: [],
      ),
    )
  })
  let assert Ok(subject) = process.receive(ready, 1000)
  Counter(subject)
}

fn counter_loop(subject: Subject(CounterMessage), state: CounterState) -> Nil {
  let message = process.receive_forever(subject)
  case message {
    Enter(reply) -> {
      let current = state.current + 1
      let high_water = case current > state.high_water {
        True -> current
        False -> state.high_water
      }
      process.send(reply, Nil)
      let state =
        CounterState(
          ..state,
          total_entries: state.total_entries + 1,
          current: current,
          high_water: high_water,
        )
      counter_loop(subject, notify_waiters(state))
    }
    Leave(reply) -> {
      process.send(reply, Nil)
      counter_loop(subject, CounterState(..state, current: state.current - 1))
    }
    Snapshot(reply) -> {
      process.send(reply, state)
      counter_loop(subject, state)
    }
    AwaitHighWater(target, reply) ->
      case state.high_water >= target {
        True -> {
          process.send(reply, Nil)
          counter_loop(subject, state)
        }
        False ->
          counter_loop(
            subject,
            CounterState(..state, high_water_waiters: [
              #(target, reply),
              ..state.high_water_waiters
            ]),
          )
      }
    AwaitTotalEntries(target, reply) ->
      case state.total_entries >= target {
        True -> {
          process.send(reply, Nil)
          counter_loop(subject, state)
        }
        False ->
          counter_loop(
            subject,
            CounterState(..state, total_entries_waiters: [
              #(target, reply),
              ..state.total_entries_waiters
            ]),
          )
      }
  }
}

fn notify_waiters(state: CounterState) -> CounterState {
  let #(ready_hw, pending_hw) =
    split_ready(state.high_water_waiters, state.high_water)
  let #(ready_te, pending_te) =
    split_ready(state.total_entries_waiters, state.total_entries)
  notify_all(ready_hw)
  notify_all(ready_te)
  CounterState(
    ..state,
    high_water_waiters: pending_hw,
    total_entries_waiters: pending_te,
  )
}

fn split_ready(
  waiters: List(#(Int, Subject(Nil))),
  current: Int,
) -> #(List(#(Int, Subject(Nil))), List(#(Int, Subject(Nil)))) {
  case waiters {
    [] -> #([], [])
    [#(target, reply), ..rest] -> {
      let #(ready, pending) = split_ready(rest, current)
      case current >= target {
        True -> #([#(target, reply), ..ready], pending)
        False -> #(ready, [#(target, reply), ..pending])
      }
    }
  }
}

fn notify_all(waiters: List(#(Int, Subject(Nil)))) -> Nil {
  case waiters {
    [] -> Nil
    [#(_target, reply), ..rest] -> {
      process.send(reply, Nil)
      notify_all(rest)
    }
  }
}

/// Records one entry into a tracked region. Call `counter_leave` when the
/// region is exited.
pub fn counter_enter(counter: Counter) -> Nil {
  process.call(counter.subject, 1000, Enter)
}

pub fn counter_leave(counter: Counter) -> Nil {
  process.call(counter.subject, 1000, Leave)
}

pub fn total_entries(counter: Counter) -> Int {
  process.call(counter.subject, 1000, Snapshot).total_entries
}

pub fn high_water(counter: Counter) -> Int {
  process.call(counter.subject, 1000, Snapshot).high_water
}

/// Blocks the calling process until the counter's high-water mark reaches
/// `target`, bounded by `timeout_ms` (panics on timeout, which fails the
/// test). No polling: the counter process replies as soon as the threshold
/// is crossed.
pub fn await_high_water(counter: Counter, target: Int, timeout_ms: Int) -> Nil {
  process.call(counter.subject, timeout_ms, AwaitHighWater(target, _))
}

/// Blocks the calling process until the counter's total entry count reaches
/// `target`, bounded by `timeout_ms` (panics on timeout).
pub fn await_total_entries(
  counter: Counter,
  target: Int,
  timeout_ms: Int,
) -> Nil {
  process.call(counter.subject, timeout_ms, AwaitTotalEntries(target, _))
}

// ---------------------------------------------------------------------------
// with_run: guaranteed cleanup
// ---------------------------------------------------------------------------

/// Starts `workflow` with `input` and `config`, runs `use_execution` against
/// the resulting `Execution`, then always cancels and awaits (bounded) the
/// run before returning — even if `use_execution` panics (a failing
/// assertion). This is what keeps a blocked run from leaking between tests.
pub fn with_run(
  workflow: saga.Workflow(i, o, e, u),
  input: i,
  config: execution.Config,
  use_execution: fn(Execution(o, e, u)) -> a,
) -> a {
  let assert Ok(execution) = execution.start(workflow, input, config)
  ensure(fn() { use_execution(execution) }, fn() {
    execution.cancel(execution)
    let _ = execution.await(execution, 2000)
    Nil
  })
}

@external(erlang, "probe_ffi", "ensure")
fn ensure(body: fn() -> a, after: fn() -> Nil) -> a

// ---------------------------------------------------------------------------
// Mailbox and native-exception probes
// ---------------------------------------------------------------------------

/// The calling process's own mailbox length. Used to assert that
/// `run`/`await` never leak a coordinator monitor's `Down` message into the
/// caller's mailbox.
@external(erlang, "probe_ffi", "mailbox_length")
pub fn mailbox_length() -> Int

/// Drains every message currently queued in the calling process's mailbox.
@external(erlang, "probe_ffi", "flush_mailbox")
pub fn flush_mailbox() -> Nil

/// The number of entries in the calling process's own process dictionary.
/// Used to assert that starting and awaiting runs never grows it.
@external(erlang, "probe_ffi", "dictionary_size")
pub fn dictionary_size() -> Int

/// Raises a native `throw` (not `error`/`exit`), for asserting a step
/// body's crash class is reported as `ThrowClass` rather than folded into
/// `ErrorClass`.
@external(erlang, "probe_ffi", "native_throw")
pub fn native_throw() -> a
// Progress polling used to live here as `wait_until_progress`. It is now
// `saga/testing.wait_until` — a public, dogfooded helper built on the same
// `execution.progress` polling loop — so every call site imports
// `saga/testing` directly instead.
