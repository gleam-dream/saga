//// The reporting receiver's bounded readiness handshake.

import gleam/erlang/process.{type Monitor, type Pid, type Subject}

pub type Error {
  ReceiverExited
  TimedOut
}

/// Keep the receiver monitor after readiness; execution uses it to detect a
/// lost outcome. On failure no workflow has started, so killing the receiver
/// needs no compensation. Closing the alias prevents future startup messages.
pub fn await(
  receiver: Pid,
  monitor: Monitor,
  ready: Subject(a),
  close_ready: fn() -> Nil,
  within: Int,
) -> Result(a, Error) {
  let received =
    process.new_selector()
    |> process.select_map(ready, Ok)
    |> process.select_specific_monitor(monitor, fn(_) { Error(ReceiverExited) })
    |> process.selector_receive(within)
  close_ready()
  discard_ready(ready)
  let outcome = case received {
    Ok(result) -> result
    Error(Nil) -> Error(TimedOut)
  }
  case outcome {
    Ok(value) -> Ok(value)
    Error(error) -> {
      process.kill(receiver)
      process.demonitor_process(monitor)
      Error(error)
    }
  }
}

// A reply can arrive after the receive deadline but before alias deactivation.
// Only this startup channel is drained; other caller messages remain intact.
fn discard_ready(ready: Subject(a)) -> Nil {
  case process.receive(ready, 0) {
    Ok(_) -> discard_ready(ready)
    Error(Nil) -> Nil
  }
}
