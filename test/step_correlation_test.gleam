//// A step reads its run's correlation from the `EffectKey` it already
//// receives, in `effect`, `undo`, `compensate` and the durable resolvers.

import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/time/duration
import gleeunit/should
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage
import saga/storage/memory
import saga/telemetry
import sinal
import sinal/correlation.{type Correlation}
import support/stores

fn order() -> Correlation {
  let assert Ok(order) = correlation.from_string("order-42")
  order
}

fn receive(
  subject: process.Subject(Option(Correlation)),
) -> Result(Option(Correlation), Nil) {
  process.receive(subject, 1000)
}

/// `a` succeeds and is undone; `b` fails once and its decider aborts. Every
/// callback reports the correlation of its key.
fn reporting_workflow(
  seen: process.Subject(#(String, Option(Correlation))),
) -> saga.Workflow(Int, Int, String, String) {
  saga.define("reporting", fn(input) {
    input
    |> saga.perform(
      saga.effect("a", fn(x: Int, key) {
        process.send(seen, #("effect a", key.correlation))
        Ok(x)
      })
      |> saga.undo(fn(undo) {
        process.send(seen, #("undo a", undo.key.correlation))
        Ok(Nil)
      }),
    )
    |> saga.perform(
      saga.effect("b", fn(_x: Int, key) {
        process.send(seen, #("effect b", key.correlation))
        Error("boom")
      })
      |> saga.compensate(max_attempts: 2, with: fn(failed) {
        process.send(seen, #("decide b", failed.key.correlation))
        saga.Abort("boom")
      }),
    )
  })
}

fn drain(
  seen: process.Subject(#(String, Option(Correlation))),
  acc: List(#(String, Option(Correlation))),
) -> List(#(String, Option(Correlation))) {
  case process.receive(seen, 100) {
    Ok(event) -> drain(seen, [event, ..acc])
    Error(Nil) -> list.reverse(acc)
  }
}

pub fn local_callbacks_read_the_configured_correlation_test() {
  let seen = process.new_subject()
  let config = execution.config() |> execution.with_correlation(order())
  let assert Ok(execution.Failed(..)) =
    execution.run(reporting_workflow(seen), 1, config)
  drain(seen, [])
  |> should.equal([
    #("effect a", Some(order())),
    #("effect b", Some(order())),
    #("decide b", Some(order())),
    #("undo a", Some(order())),
  ])
}

pub fn local_callbacks_see_none_without_a_correlation_test() {
  let seen = process.new_subject()
  let assert Ok(execution.Failed(..)) =
    execution.run(reporting_workflow(seen), 1, execution.config())
  drain(seen, [])
  |> should.equal([
    #("effect a", None),
    #("effect b", None),
    #("decide b", None),
    #("undo a", None),
  ])
}

/// The correlation a step reads is the one its run's events carry.
pub fn step_and_events_share_the_correlation_test() {
  let step_saw = process.new_subject()
  let event_saw = process.new_subject()
  let workflow =
    saga.define("shared", fn(input) {
      saga.perform(
        input,
        saga.effect("only", fn(x: Int, key) {
          process.send(step_saw, key.correlation)
          Ok(x)
        }),
      )
    })
  let plan =
    sinal.subscriptions([
      sinal.subscription(telemetry.step_started(), fn(_m, d) {
        process.send(event_saw, d.correlation)
      }),
    ])
  let config = execution.config() |> execution.with_correlation(order())
  let assert Ok(sinal.SubscriptionCompletion(Ok(execution.Completed(1)), [])) =
    sinal.with_subscriptions(plan, fn() { execution.run(workflow, 1, config) })
  receive(step_saw) |> should.equal(Ok(Some(order())))
  receive(event_saw) |> should.equal(Ok(Some(order())))
}

fn text_persistence(
  workflow: saga.Workflow(String, String, String, String),
) -> durable.Persistence(String, String, String, String) {
  let text = codec.text()
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

fn recoverable(
  step: saga.Step(String, String, String, String),
  resolve: fn(String, saga.EffectKey) -> durable.Evidence(String, String),
) {
  durable.recoverable(
    step,
    version: "1",
    input: codec.text(),
    output: codec.text(),
    resolve: resolve,
  )
}

fn start(
  persistence: durable.Persistence(String, String, String, String),
  store: storage.Storage,
  id: String,
) -> durable.Run(String, String, String, String) {
  let assert Ok(run) =
    durable.start_or_reconnect(persistence, store, id: id, input: "x")
  run
}

fn reconnect(
  persistence: durable.Persistence(String, String, String, String),
  store: storage.Storage,
  id: String,
) -> durable.Run(String, String, String, String) {
  let assert Ok(run) = durable.reconnect(persistence, store, id: id)
  run
}

fn drive(run) {
  durable.drive(run, timeout: duration.seconds(10))
}

fn kill_and_wait(pid: process.Pid) -> Nil {
  let monitor = process.monitor(pid)
  process.kill(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
  process.selector_receive_forever(selector)
}

fn echo_persistence(
  seen: process.Subject(Option(Correlation)),
) -> durable.Persistence(String, String, String, String) {
  saga.define("echo", fn(input) {
    saga.perform(
      input,
      saga.effect("echo", fn(value, key) {
        process.send(seen, key.correlation)
        Ok(value <> "!")
      })
        |> recoverable(fn(_, _) { durable.MaybeSent }),
    )
  })
  |> text_persistence
}

/// A durable run without `with_correlation` is correlated by its execution
/// id, in its steps and in its events; the handle's own correlation wins.
pub fn durable_run_defaults_to_its_execution_id_test() {
  let step_saw = process.new_subject()
  let event_saw = process.new_subject()
  let assert Ok(store) = memory.start()
  let persistence = echo_persistence(step_saw)
  let plan =
    sinal.subscriptions([
      sinal.subscription(telemetry.run_stopped(), fn(_m, d) {
        process.send(event_saw, d.correlation)
      }),
    ])
  let by_id = correlation.from_key("checkout:7")
  let assert Ok(sinal.SubscriptionCompletion(Ok(execution.Completed("x!")), [])) =
    sinal.with_subscriptions(plan, fn() {
      drive(start(persistence, memory.storage(store), "checkout:7"))
    })
  receive(step_saw) |> should.equal(Ok(Some(by_id)))
  receive(event_saw) |> should.equal(Ok(Some(by_id)))

  let handle =
    start(persistence, memory.storage(store), "checkout:8")
    |> durable.with_correlation(order())
  let assert Ok(execution.Completed("x!")) = drive(handle)
  receive(step_saw) |> should.equal(Ok(Some(order())))
  memory.stop(store)
}

/// A local run has no id to derive one from.
pub fn local_run_without_a_correlation_stays_uncorrelated_test() {
  execution.correlation_of(execution.config(), None) |> should.equal(None)
}

/// After a restart, the resolver, the key in `RecoveryRequired`, and an undo
/// replayed from the checkpoint all carry the correlation of the new handle.
pub fn restart_paths_carry_the_correlation_test() {
  let resolved = process.new_subject()
  let undone = process.new_subject()
  let owner = process.new_subject()
  let build = fn(recovering) {
    saga.define("restart", fn(input) {
      input
      |> saga.perform(
        saga.step("resource", fn(value) { Ok(value) })
        |> saga.undo(fn(undo) {
          process.send(undone, undo.key.correlation)
          case recovering {
            True -> Ok(Nil)
            False -> {
              process.sleep_forever()
              Ok(Nil)
            }
          }
        })
        |> durable.resolve_undo(fn(undo) {
          process.send(resolved, undo.key.correlation)
          durable.NotSent
        })
        |> recoverable(fn(_, _) { durable.MaybeSent }),
      )
      |> saga.perform(
        saga.step("fail", fn(_: String) { Error("boom") })
        |> recoverable(fn(_, key) {
          process.send(resolved, key.correlation)
          durable.MaybeSent
        }),
      )
    })
    |> text_persistence
  }
  let assert Ok(store) = memory.start()
  let backend = stores.watched(memory.storage(store), owner)
  let by_id = Some(correlation.from_key("restart-1"))

  let result = process.new_subject()
  process.spawn_unlinked(fn() {
    process.send(result, drive(start(build(False), backend, "restart-1")))
  })
  let assert Ok(pid) = process.receive(owner, 1000)
  receive(undone) |> should.equal(Ok(by_id))
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)

  // The undo's resolver says NotSent, so the undo replays from the
  // checkpoint under the reconnecting handle's correlation.
  let handle = reconnect(build(True), backend, "restart-1")
  let assert Ok(execution.Failed(..)) = drive(handle)
  receive(resolved) |> should.equal(Ok(by_id))
  receive(undone) |> should.equal(Ok(by_id))

  // The same paths with an explicit correlation on the new handle.
  let assert Ok(execution.Failed(..)) =
    drive(
      start(build(True), backend, "restart-2")
      |> durable.with_correlation(order()),
    )
  receive(undone) |> should.equal(Ok(Some(order())))
  memory.stop(store)
}

/// `RecoveryRequired` names the effect with the handle's correlation.
pub fn recovery_required_key_carries_the_correlation_test() {
  let owner = process.new_subject()
  let build = fn() {
    saga.define("uncertain", fn(input) {
      saga.perform(
        input,
        saga.effect("effect", fn(value, _) {
          process.sleep_forever()
          Ok(value)
        })
          |> recoverable(fn(_, _) { durable.MaybeSent }),
      )
    })
    |> text_persistence
  }
  let assert Ok(store) = memory.start()
  let backend = stores.watched(memory.storage(store), owner)
  let result = process.new_subject()
  process.spawn_unlinked(fn() {
    process.send(result, drive(start(build(), backend, "uncertain-1")))
  })
  let assert Ok(pid) = process.receive(owner, 1000)
  process.sleep(100)
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  let assert Error(durable.RecoveryRequired(durable.Required(_, _, key))) =
    drive(reconnect(build(), backend, "uncertain-1"))
  key.correlation |> should.equal(Some(correlation.from_key("uncertain-1")))
  let assert Error(durable.RecoveryRequired(durable.Required(_, _, key))) =
    drive(
      reconnect(build(), backend, "uncertain-1")
      |> durable.with_correlation(order()),
    )
  key.correlation |> should.equal(Some(order()))
  memory.stop(store)
}

/// An undo restored from the checkpoint of an earlier drive runs under the
/// correlation of the handle that rolls back.
pub fn restored_undo_carries_the_correlation_test() {
  let undone = process.new_subject()
  let owner = process.new_subject()
  let build = fn(recovering) {
    saga.define("restored", fn(input) {
      input
      |> saga.perform(
        saga.step("resource", fn(value) { Ok(value) })
        |> saga.undo(fn(undo) {
          process.send(undone, undo.key.correlation)
          Ok(Nil)
        })
        |> recoverable(fn(_, _) { durable.MaybeSent }),
      )
      |> saga.perform(
        saga.step("gate", fn(value: String) {
          case recovering {
            True -> Error("boom")
            False -> {
              process.sleep_forever()
              Ok(value)
            }
          }
        })
        |> recoverable(fn(_, _) { durable.NotSent }),
      )
    })
    |> text_persistence
  }
  let assert Ok(store) = memory.start()
  let backend = stores.watched(memory.storage(store), owner)
  let result = process.new_subject()
  process.spawn_unlinked(fn() {
    process.send(result, drive(start(build(False), backend, "restored-1")))
  })
  let assert Ok(pid) = process.receive(owner, 1000)
  process.sleep(200)
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  let assert Ok(execution.Failed(..)) =
    drive(
      reconnect(build(True), backend, "restored-1")
      |> durable.with_correlation(order()),
    )
  receive(undone) |> should.equal(Ok(Some(order())))
  memory.stop(store)
}
