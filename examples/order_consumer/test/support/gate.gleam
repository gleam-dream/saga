/// A small broker-process gate a workflow step can block on until the test
/// releases it, independent of saga's own test-support module (this package
/// only uses saga's public API). A `Subject` may only be received on by the
/// process that created it, and a step's body runs inside a fresh task
/// process on every attempt, so the release channel cannot be a subject the
/// test process alone owns: `enter` registers the calling process's own
/// reply subject with the broker and blocks on it; `open` releases the
/// oldest still-waiting registration.
import gleam/erlang/process.{type Pid, type Subject}

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
/// `timeout_ms`.
pub fn wait_entered(gate: Gate, timeout_ms: Int) -> Result(Pid, Nil) {
  process.receive(gate.arrived, timeout_ms)
}
