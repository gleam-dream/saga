import gleam/erlang/process
import gleeunit/should
import saga/internal/ffi
import saga/internal/reporting_startup as startup

pub fn receiver_death_is_a_result_and_preserves_unrelated_messages_test() {
  let ready = process.new_subject()
  let #(send_ready, close_ready) = ffi.aliased_sender(ready)
  let unrelated = process.new_subject()
  process.send(unrelated, "keep")
  let receiver = process.spawn_unlinked(fn() { Nil })
  let exited = process.monitor(receiver)
  process.new_selector()
  |> process.select_specific_monitor(exited, fn(_) { Nil })
  |> process.selector_receive(1000)
  |> should.equal(Ok(Nil))
  process.demonitor_process(exited)
  let monitor = process.monitor(receiver)
  startup.await(receiver, monitor, ready, close_ready, 1000)
  |> should.equal(Error(startup.ReceiverExited))
  process.receive(unrelated, 0) |> should.equal(Ok("keep"))
  send_ready(42)
  process.receive(ready, 0) |> should.equal(Error(Nil))
  no_monitor_message()
}

pub fn readiness_timeout_stops_receiver_and_drops_late_ready_test() {
  let ready = process.new_subject()
  let #(send_ready, close_ready) = ffi.aliased_sender(ready)
  let receiver =
    process.spawn_unlinked(fn() {
      process.receive_forever(process.new_subject())
    })
  let monitor = process.monitor(receiver)
  startup.await(receiver, monitor, ready, close_ready, 0)
  |> should.equal(Error(startup.TimedOut))
  let terminated = process.monitor(receiver)
  process.new_selector()
  |> process.select_specific_monitor(terminated, fn(_) { Nil })
  |> process.selector_receive(1000)
  |> should.equal(Ok(Nil))
  process.demonitor_process(terminated)
  send_ready(42)
  process.receive(ready, 0) |> should.equal(Error(Nil))
  no_monitor_message()
}

pub fn ready_receiver_retains_its_monitor_and_closes_startup_channel_test() {
  let ready = process.new_subject()
  let #(send_ready, close_ready) = ffi.aliased_sender(ready)
  let receiver =
    process.spawn_unlinked(fn() {
      send_ready(42)
      process.receive_forever(process.new_subject())
    })
  let monitor = process.monitor(receiver)
  startup.await(receiver, monitor, ready, close_ready, 1000)
  |> should.equal(Ok(42))
  process.is_alive(receiver) |> should.be_true
  send_ready(43)
  process.receive(ready, 0) |> should.equal(Error(Nil))
  process.kill(receiver)
  process.new_selector()
  |> process.select_specific_monitor(monitor, fn(_) { Nil })
  |> process.selector_receive(1000)
  |> should.equal(Ok(Nil))
  process.demonitor_process(monitor)
  no_monitor_message()
}

fn no_monitor_message() -> Nil {
  process.new_selector()
  |> process.select_monitors(fn(_) { Nil })
  |> process.selector_receive(0)
  |> should.equal(Error(Nil))
}
