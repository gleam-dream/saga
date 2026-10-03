import gleam/bit_array
import gleam/erlang/process
import gleam/list
import gleam/option.{None, Some}
import gleam/string
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
import sinal/correlation
import support/probe
import support/stores

fn text_step(name: String, run: fn(String) -> Result(String, String)) {
  saga.step(name, run)
  |> durable.recoverable(
    version: "1",
    input: codec.text(),
    output: codec.text(),
    resolve: fn(_, _) { durable.MaybeSent },
  )
}

fn echo_workflow() -> saga.Workflow(String, String, String, String) {
  let workflow =
    saga.define("echo", fn(input) {
      saga.perform(input, text_step("echo", fn(value) { Ok(value <> "!") }))
    })
  workflow
}

fn prepare(
  workflow: saga.Workflow(String, String, String, String),
) -> durable.Persistence(String, String, String, String) {
  let text = codec.text()
  let persistence =
    durable.new(
      workflow,
      input: text,
      output: text,
      error: text,
      undo_error: text,
    )
  persistence
  |> durable.with_config(
    execution.config() |> execution.with_step_timeout(execution.Infinity),
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

fn drive(
  run: durable.Run(String, String, String, String),
) -> Result(execution.Outcome(String, String, String), durable.Error) {
  durable.drive(run, timeout: duration.seconds(10))
}

fn drive_later(
  run: durable.Run(String, String, String, String),
) -> process.Subject(
  Result(execution.Outcome(String, String, String), durable.Error),
) {
  let result = process.new_subject()
  process.spawn_unlinked(fn() { process.send(result, drive(run)) })
  result
}

fn kill_and_wait(pid: process.Pid) -> Nil {
  let monitor = process.monitor(pid)
  process.kill(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
  process.selector_receive_forever(selector)
}

fn stops_within(pid: process.Pid, milliseconds: Int) -> Nil {
  let monitor = process.monitor(pid)
  let assert Ok(Nil) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
    |> process.selector_receive(milliseconds)
  Nil
}

fn start_memory() -> memory.Memory {
  let assert Ok(store) = memory.start()
  store
}

fn at(name: String) -> saga.StepAddress {
  saga.StepAddress([], name, 1)
}

pub fn same_definition_local_and_persistent_test() {
  let workflow = echo_workflow()
  execution.run(workflow, "hello", execution.config())
  |> should.equal(Ok(execution.Completed("hello!")))
  let store = start_memory()
  let backend = memory.storage(store)
  let persistence = prepare(workflow)
  let assert Ok(run) =
    durable.start_or_reconnect(persistence, backend, id: "one", input: "hello")
  durable.id(run) |> should.equal("one")
  drive(run) |> should.equal(Ok(execution.Completed("hello!")))
  durable.read(run)
  |> should.equal(Ok(durable.Finished(execution.Completed("hello!"))))
  let assert Ok(again) =
    durable.start_or_reconnect(persistence, backend, id: "one", input: "hello")
  drive(again) |> should.equal(Ok(execution.Completed("hello!")))
  durable.start_or_reconnect(persistence, backend, id: "one", input: "other")
  |> should.equal(Error(durable.InputMismatch))
  memory.stop(store)
}

/// One storage serves a pool: two executions under different ids share it
/// and finish independently (R2; a second id used to fail with
/// `ReferenceMismatch`).
pub fn one_storage_serves_many_executions_test() {
  let store = start_memory()
  let backend = memory.storage(store)
  let persistence = prepare(echo_workflow())
  let assert Ok(first) =
    durable.start_or_reconnect(persistence, backend, id: "a", input: "one")
  let assert Ok(second) =
    durable.start_or_reconnect(persistence, backend, id: "b", input: "two")
  drive(second) |> should.equal(Ok(execution.Completed("two!")))
  drive(first) |> should.equal(Ok(execution.Completed("one!")))
  durable.read(first)
  |> should.equal(Ok(durable.Finished(execution.Completed("one!"))))
  memory.stop(store)
}

pub fn reconnect_requires_a_saved_execution_test() {
  let store = start_memory()
  let backend = memory.storage(store)
  let persistence = prepare(echo_workflow())
  let assert Error(durable.StorageFailure(storage.NotFound)) =
    durable.reconnect(persistence, backend, id: "missing")
  let _ = start(persistence, backend, "saved")
  let run = reconnect(persistence, backend, "saved")
  drive(run) |> should.equal(Ok(execution.Completed("x!")))
  memory.stop(store)
}

pub fn persistent_concurrent_shared_dependency_test() {
  let ready = process.new_subject()
  let branch = fn(name) {
    text_step(name, fn(value) {
      let release = process.new_subject()
      process.send(ready, release)
      process.receive_forever(release)
      Ok(value <> name)
    })
  }
  let workflow =
    saga.define("parallel", fn(input) {
      let shared =
        saga.perform(input, text_step("shared", fn(value) { Ok(value <> "!") }))
      saga.both(
        saga.perform(shared, branch("a")),
        saga.perform(shared, branch("b")),
      )
      |> saga.map(fn(pair) { pair.0 <> pair.1 })
    })
  let store = start_memory()
  let persistence =
    prepare(workflow)
    |> durable.with_config(
      execution.config()
      |> execution.with_max_concurrency(2)
      |> execution.with_step_timeout(execution.Infinity),
    )
  let result = drive_later(start(persistence, memory.storage(store), "p"))
  let assert Ok(a) = process.receive(ready, 1000)
  let assert Ok(b) = process.receive(ready, 1000)
  process.send(a, Nil)
  process.send(b, Nil)
  process.receive(result, 2000)
  |> should.equal(Ok(Ok(execution.Completed("x!ax!b"))))
  memory.stop(store)
}

pub fn saved_success_and_uncertain_effect_resume_test() {
  let events = process.new_subject()
  let owner = process.new_subject()
  let build = fn(recovering) {
    let workflow =
      saga.define("recover", fn(input) {
        input
        |> saga.perform(
          text_step("saved", fn(value) {
            process.send(events, "saved ran")
            Ok(value <> "!")
          }),
        )
        |> saga.perform(
          saga.effect("uncertain", fn(value, _) {
            process.send(events, "effect ran")
            process.sleep_forever()
            Ok(value)
          })
          |> durable.recoverable(
            version: "1",
            input: codec.text(),
            output: codec.text(),
            resolve: fn(value, _) {
              case recovering {
                True -> durable.Completed(value <> "recovered")
                False -> durable.MaybeSent
              }
            },
          ),
        )
      })
    prepare(workflow)
  }
  let store = start_memory()
  let backend = stores.watched(memory.storage(store), owner)
  let result = drive_later(start(build(False), backend, "recover"))
  let assert Ok(pid) = process.receive(owner, 1000)
  process.receive(events, 1000) |> should.equal(Ok("saved ran"))
  process.receive(events, 1000) |> should.equal(Ok("effect ran"))
  kill_and_wait(pid)
  process.receive(result, 1000) |> should.equal(Ok(Error(durable.RunnerLost)))
  let unrecovered = reconnect(build(False), backend, "recover")
  let assert Error(durable.RecoveryRequired(durable.Required(
    step,
    execution.StepAttempt(1),
    key,
  ))) = drive(unrecovered)
  step |> should.equal(at("uncertain"))
  saga.attempt_number(key) |> should.equal(1)
  let assert Ok(durable.Suspended(durable.RecoveryRequired(_))) =
    durable.read(unrecovered)
  drive(reconnect(build(True), backend, "recover"))
  |> should.equal(Ok(execution.Completed("x!recovered")))
  process.receive(events, 0) |> should.equal(Error(Nil))
  memory.stop(store)
}

pub fn rollback_resumes_uncertain_undo_test() {
  let events = process.new_subject()
  let owner = process.new_subject()
  let build = fn(recovering) {
    let workflow =
      saga.define("undo", fn(input) {
        input
        |> saga.perform(
          saga.step("resource", fn(value) { Ok(value) })
          |> saga.undo(fn(_undo) {
            process.send(events, "undo")
            process.sleep_forever()
            Ok(Nil)
          })
          |> durable.resolve_undo(fn(_undo) {
            case recovering {
              True -> durable.Completed(Nil)
              False -> durable.MaybeSent
            }
          })
          |> durable.recoverable(
            version: "1",
            input: codec.text(),
            output: codec.text(),
            resolve: fn(_, _) { durable.MaybeSent },
          ),
        )
        |> saga.perform(text_step("fail", fn(_) { Error("boom") }))
      })
    prepare(workflow)
  }
  let store = start_memory()
  let backend = stores.watched(memory.storage(store), owner)
  let result = drive_later(start(build(False), backend, "undo"))
  let assert Ok(pid) = process.receive(owner, 1000)
  process.receive(events, 1000) |> should.equal(Ok("undo"))
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  let assert Error(durable.RecoveryRequired(durable.Required(
    _,
    execution.StepUndo,
    _,
  ))) = drive(reconnect(build(False), backend, "undo"))
  let assert Ok(execution.Failed(execution.StepFailed(_, "boom"), settlement)) =
    drive(reconnect(build(True), backend, "undo"))
  settlement.undone |> should.equal([at("resource")])
  process.receive(events, 0) |> should.equal(Error(Nil))
  memory.stop(store)
}

/// `NotSent` from an undo resolver replays the restored undo.
pub fn undo_resolver_not_sent_replays_the_undo_test() {
  let events = process.new_subject()
  let owner = process.new_subject()
  let build = fn(recovering) {
    let workflow =
      saga.define("undo-replay", fn(input) {
        input
        |> saga.perform(
          saga.step("resource", fn(value) { Ok(value) })
          |> saga.undo(fn(undo) {
            process.send(events, "undo " <> undo.output)
            case recovering {
              True -> Ok(Nil)
              False -> {
                process.sleep_forever()
                Ok(Nil)
              }
            }
          })
          |> durable.resolve_undo(fn(_undo) { durable.NotSent })
          |> durable.recoverable(
            version: "1",
            input: codec.text(),
            output: codec.text(),
            resolve: fn(_, _) { durable.MaybeSent },
          ),
        )
        |> saga.perform(text_step("fail", fn(_) { Error("boom") }))
      })
    prepare(workflow)
  }
  let store = start_memory()
  let backend = stores.watched(memory.storage(store), owner)
  let result = drive_later(start(build(False), backend, "undo-replay"))
  let assert Ok(pid) = process.receive(owner, 1000)
  process.receive(events, 1000) |> should.equal(Ok("undo x"))
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  let assert Ok(execution.Failed(_, settlement)) =
    drive(reconnect(build(True), backend, "undo-replay"))
  settlement.undone |> should.equal([at("resource")])
  process.receive(events, 1000) |> should.equal(Ok("undo x"))
  memory.stop(store)
}

pub fn cancellation_before_admission_test() {
  let store = start_memory()
  let run = start(prepare(echo_workflow()), memory.storage(store), "cancel")
  durable.cancel(run) |> should.equal(Ok(Nil))
  let assert Ok(execution.Cancelled(_, _)) = drive(run)
  let assert Ok(durable.Finished(execution.Cancelled(_, _))) = durable.read(run)
  durable.cancel(run) |> should.equal(Ok(Nil))
  memory.stop(store)
}

pub fn persisted_choice_survives_fresh_definition_test() {
  let entered = process.new_subject()
  let owner = process.new_subject()
  let build = fn(recovering) {
    let workflow =
      saga.define("choice-recovery", fn(input) {
        saga.choose(
          input,
          "route",
          saga.map(input, fn(_) { !recovering }),
          fn(port) {
            saga.perform(
              port,
              saga.step("selected", fn(value) {
                process.send(entered, Nil)
                process.sleep_forever()
                Ok(value)
              })
                |> durable.recoverable(
                  version: "1",
                  input: codec.text(),
                  output: codec.text(),
                  resolve: fn(value, _) {
                    durable.Completed(value <> "-selected")
                  },
                ),
            )
          },
          fn(port) {
            saga.perform(
              port,
              text_step("unchosen", fn(_) { Error("unchosen branch ran") }),
            )
          },
        )
      })
    prepare(workflow)
  }
  let store = start_memory()
  let backend = stores.watched(memory.storage(store), owner)
  let result = drive_later(start(build(False), backend, "choice"))
  let assert Ok(pid) = process.receive(owner, 1000)
  let assert Ok(Nil) = process.receive(entered, 1000)
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  // Deliberately change this pure callback to prove the saved selection is
  // authoritative. Production callback changes require a version change.
  drive(reconnect(build(True), backend, "choice"))
  |> should.equal(Ok(execution.Completed("x-selected")))
  memory.stop(store)
}

/// The resolver receives the same `EffectKey` the interrupted attempt had:
/// its `idempotency` names the execution and step, so it survives a restart.
pub fn restart_recovers_two_concurrent_admissions_test() {
  let entered = process.new_subject()
  let owner = process.new_subject()
  let build = fn(recovering) {
    let branch = fn(name) {
      saga.effect(name, fn(value, key) {
        process.send(entered, #(name, key))
        process.sleep_forever()
        Ok(value)
      })
      |> durable.recoverable(
        version: "1",
        input: codec.text(),
        output: codec.text(),
        resolve: fn(value, key) {
          process.send(entered, #(name, key))
          case recovering {
            True -> durable.Completed(value <> name)
            False -> durable.MaybeSent
          }
        },
      )
    }
    let workflow =
      saga.define("two", fn(input) {
        saga.both(
          saga.perform(input, branch("a")),
          saga.perform(input, branch("b")),
        )
        |> saga.map(fn(pair) { pair.0 <> pair.1 })
      })
    prepare(workflow)
    |> durable.with_config(
      execution.config()
      |> execution.with_max_concurrency(2)
      |> execution.with_step_timeout(execution.Infinity),
    )
  }
  let store = start_memory()
  let backend = stores.watched(memory.storage(store), owner)
  let result = drive_later(start(build(False), backend, "two"))
  let assert Ok(pid) = process.receive(owner, 1000)
  let assert Ok(first) = process.receive(entered, 1000)
  let assert Ok(second) = process.receive(entered, 1000)
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  drive(reconnect(build(True), backend, "two"))
  |> should.equal(Ok(execution.Completed("xaxb")))
  let assert Ok(resolved_first) = process.receive(entered, 1000)
  let assert Ok(resolved_second) = process.receive(entered, 1000)
  let by_name = fn(a: #(String, saga.EffectKey), b: #(String, saga.EffectKey)) {
    string.compare(a.0, b.0)
  }
  let original = list.sort([first, second], by_name)
  original
  |> should.equal(list.sort([resolved_first, resolved_second], by_name))
  let assert [#("a", a_key), #("b", b_key)] = original
  { saga.idempotency_key(a_key) != saga.idempotency_key(b_key) }
  |> should.be_true
  string.contains(saga.idempotency_key(a_key), "two") |> should.be_true
  memory.stop(store)
}

pub fn retry_and_continue_share_local_semantics_test() {
  let attempts = process.new_subject()
  let step =
    saga.step("retry", fn(value) {
      process.send(attempts, value)
      Error("retry")
    })
    |> saga.compensate(max_attempts: 2, with: fn(failed) {
      case failed.attempt {
        1 -> saga.RetryAfter(duration.milliseconds(1))
        _ -> saga.Continue(failed.input <> "-continued", saga.NoUndo)
      }
    })
    |> durable.restore_undo(fn(_undo) { saga.NoUndo })
    |> durable.recoverable(
      version: "1",
      input: codec.text(),
      output: codec.text(),
      resolve: fn(_, _) { durable.MaybeSent },
    )
  let workflow = saga.define("retry", fn(input) { saga.perform(input, step) })
  let store = start_memory()
  drive(start(prepare(workflow), memory.storage(store), "retry"))
  |> should.equal(Ok(execution.Completed("x-continued")))
  process.receive(attempts, 100) |> should.equal(Ok("x"))
  process.receive(attempts, 100) |> should.equal(Ok("x"))
  process.receive(attempts, 0) |> should.equal(Error(Nil))
  memory.stop(store)
}

pub fn incompatible_definition_refused_before_callbacks_test() {
  let events = process.new_subject()
  let store = start_memory()
  let backend = memory.storage(store)
  let _ = start(prepare(echo_workflow()), backend, "versioned")
  let text = codec.text()
  let changed_codec =
    codec.new("different", fn(value) { Ok(value) }, fn(value) {
      process.send(events, Nil)
      Ok(value)
    })
  let changed =
    durable.new(
      echo_workflow(),
      input: text,
      output: changed_codec,
      error: text,
      undo_error: text,
    )
  durable.reconnect(changed, backend, id: "versioned")
  |> should.equal(Error(durable.IncompatibleDefinition))
  // A new workflow version alone is refused too.
  let bumped = prepare(echo_workflow()) |> durable.with_version("2")
  durable.reconnect(bumped, backend, id: "versioned")
  |> should.equal(Error(durable.IncompatibleDefinition))
  process.receive(events, 0) |> should.equal(Error(Nil))
  memory.stop(store)
}

pub fn cancellation_racing_completion_wins_test() {
  let store = start_memory()
  let entered = process.new_subject()
  let root_codec =
    codec.new(
      "root",
      fn(value) {
        let release = process.new_subject()
        process.send(entered, release)
        process.receive_forever(release)
        Ok(value)
      },
      Ok,
    )
  let text = codec.text()
  let persistence =
    durable.new(
      echo_workflow(),
      input: text,
      output: root_codec,
      error: text,
      undo_error: text,
    )
  let run = start(persistence, memory.storage(store), "race")
  let result = drive_later(run)
  let assert Ok(release) = process.receive(entered, 1000)
  durable.cancel(run) |> should.equal(Ok(Nil))
  process.send(release, Nil)
  let assert Ok(Ok(execution.Cancelled(_, _))) = process.receive(result, 2000)
  memory.stop(store)
}

pub fn cancellation_reconciles_admitted_absence_without_dispatch_test() {
  let entered = process.new_subject()
  let owner = process.new_subject()
  let workflow =
    saga.define("cancel-admitted", fn(input) {
      saga.perform(
        input,
        saga.step("effect", fn(value) {
          process.send(entered, Nil)
          process.sleep_forever()
          Ok(value)
        })
          |> durable.recoverable(
            version: "1",
            input: codec.text(),
            output: codec.text(),
            resolve: fn(_, _) { durable.NotSent },
          ),
      )
    })
  let store = start_memory()
  let backend = stores.watched(memory.storage(store), owner)
  let run = start(prepare(workflow), backend, "cancel-admitted")
  let result = drive_later(run)
  let assert Ok(pid) = process.receive(owner, 1000)
  let assert Ok(Nil) = process.receive(entered, 1000)
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  durable.cancel(run) |> should.equal(Ok(Nil))
  let assert Ok(execution.Cancelled(_, _)) = drive(run)
  process.receive(entered, 0) |> should.equal(Error(Nil))
  memory.stop(store)
}

pub fn bad_input_codec_prevents_effect_test() {
  let effects = process.new_subject()
  let bad = codec.new("bad", fn(_) { Error("cannot encode") }, Ok)
  let workflow =
    saga.define("bad-codec", fn(input) {
      saga.perform(
        input,
        saga.step("effect", fn(value) {
          process.send(effects, Nil)
          Ok(value)
        })
          |> durable.recoverable(
            version: "1",
            input: bad,
            output: codec.text(),
            resolve: fn(_, _) { durable.MaybeSent },
          ),
      )
    })
  // Codec configuration has no cost or admission requirement for local use.
  execution.run(workflow, "x", execution.config())
  |> should.equal(Ok(execution.Completed("x")))
  let assert Ok(Nil) = process.receive(effects, 100)
  let store = start_memory()
  let run = start(prepare(workflow), memory.storage(store), "bad")
  let expected =
    durable.CodecFailure(
      durable.StepInput(at("effect")),
      codec.EncodeFailed("cannot encode"),
    )
  drive(run) |> should.equal(Error(expected))
  durable.read(run) |> should.equal(Ok(durable.Suspended(expected)))
  durable.error_kind(expected) |> should.equal(durable.Defect)
  durable.describe_error(expected)
  |> should.equal(
    "the codec of the input of step effect failed: the encoder refused the value: cannot encode",
  )
  process.receive(effects, 0) |> should.equal(Error(Nil))
  memory.stop(store)
}

pub fn failed_commit_prevents_dispatch_test() {
  let effects = process.new_subject()
  let workflow =
    saga.define("refused", fn(input) {
      saga.perform(
        input,
        text_step("effect", fn(value) {
          process.send(effects, Nil)
          Ok(value)
        }),
      )
    })
  let persistence = prepare(workflow)
  let store = start_memory()
  let backend = memory.storage(store)
  let refusing =
    stores.with_commit(backend, fn(_, _) { Error(storage.Conflict) })
  let assert Error(durable.StorageFailure(storage.Conflict)) =
    drive(start(persistence, refusing, "refused"))
  process.receive(effects, 0) |> should.equal(Error(Nil))
  // The committed state still has no admitted effect, so a working adapter
  // can proceed without asking the unknown resolver to authorize a replay.
  drive(reconnect(persistence, backend, "refused"))
  |> should.equal(Ok(execution.Completed("x")))
  process.receive(effects, 100) |> should.equal(Ok(Nil))
  memory.stop(store)
}

pub fn parallel_rollback_covers_both_completed_branches_test() {
  let entered = process.new_subject()
  let undone = process.new_subject()
  let branch = fn(name) {
    saga.step(name, fn(value) {
      let release = process.new_subject()
      process.send(entered, #(name, release))
      process.receive_forever(release)
      Ok(value)
    })
    |> saga.undo(fn(_undo) {
      process.send(undone, name)
      Ok(Nil)
    })
    |> durable.recoverable(
      version: "1",
      input: codec.text(),
      output: codec.text(),
      resolve: fn(_, _) { durable.MaybeSent },
    )
  }
  let workflow =
    saga.define("rollback-order", fn(input) {
      let a = saga.perform(input, branch("a"))
      let b = saga.perform(input, branch("b"))
      saga.both(a, b)
      |> saga.map(fn(pair) { pair.0 <> pair.1 })
      |> saga.perform(text_step("fail", fn(_) { Error("boom") }))
    })
  let store = start_memory()
  let persistence =
    prepare(workflow)
    |> durable.with_config(
      execution.config()
      |> execution.with_max_concurrency(2)
      |> execution.with_step_timeout(execution.Infinity),
    )
  let result = drive_later(start(persistence, memory.storage(store), "order"))
  let assert Ok(first) = process.receive(entered, 1000)
  let assert Ok(second) = process.receive(entered, 1000)
  // Each branch's return is admitted independently; release order is not a
  // promise of commit order, so verify dependency-safe rollback coverage.
  process.send(first.1, Nil)
  process.send(second.1, Nil)
  let assert Ok(Ok(execution.Failed(_, settlement))) =
    process.receive(result, 2000)
  list.length(settlement.undone) |> should.equal(2)
  let assert Ok(a) = process.receive(undone, 1000)
  let assert Ok(b) = process.receive(undone, 1000)
  list.sort([a, b], string.compare) |> should.equal(["a", "b"])
  memory.stop(store)
}

@external(erlang, "saga_test_panic", "message")
fn panic_message(body: fn() -> a) -> Result(String, Nil)

/// `new` panics naming every problem at once: they are source bugs.
pub fn new_names_every_persistence_problem_test() {
  let step =
    saga.step("compensate", fn(value) { Error(value) })
    |> saga.compensate(1, fn(failed) {
      saga.Continue(failed.input, saga.UndoWith(fn() { Ok(Nil) }))
    })
    |> durable.recoverable(
      version: "",
      input: codec.text(),
      output: codec.text(),
      resolve: fn(_, _) { durable.MaybeSent },
    )
  let workflow =
    saga.define("eligibility", fn(input) {
      input
      |> saga.perform(step)
      |> saga.perform(saga.step("plain", fn(value) { Ok(value) }))
    })
  let text = codec.text()
  let empty = codec.new("", Ok, Ok)
  let assert Ok(message) =
    panic_message(fn() {
      durable.new(
        workflow,
        input: text,
        output: empty,
        error: text,
        undo_error: text,
      )
    })
  [
    "workflow \"eligibility\" cannot be persisted",
    "step compensate has an empty version",
    "step compensate compensates but has no durable.restore_undo",
    "step plain has no durable.recoverable",
    "the codec of the workflow output has an empty version",
  ]
  |> list.each(fn(part) { string.contains(message, part) |> should.be_true })
  let assert Ok(message) =
    panic_message(fn() { prepare(echo_workflow()) |> durable.with_version("") })
  string.contains(message, "needs a non-empty version") |> should.be_true
}

pub fn interrupted_compensation_requires_explicit_resolution_test() {
  let entered = process.new_subject()
  let owner = process.new_subject()
  let make = fn(resolve) {
    let step =
      saga.step("compensate", fn(_) { Error("failed") })
      |> saga.compensate(max_attempts: 2, with: fn(failed) {
        process.send(entered, failed.key)
        process.receive_forever(process.new_subject())
      })
      |> durable.restore_undo(fn(_undo) { saga.NoUndo })
      |> durable.resolve_compensation(resolve)
      |> durable.recoverable(
        version: "1",
        input: codec.text(),
        output: codec.text(),
        resolve: fn(_, _) { durable.MaybeSent },
      )
    let workflow =
      saga.define("compensation-recovery", fn(input) {
        saga.perform(input, step)
      })
    prepare(workflow)
  }
  let store = start_memory()
  let backend = memory.storage(store)
  let persistence = make(fn(_, _) { None })
  let result =
    drive_later(start(persistence, stores.watched(backend, owner), "comp"))
  let assert Ok(pid) = process.receive(owner, 1000)
  let assert Ok(key) = process.receive(entered, 1000)
  kill_and_wait(pid)
  let assert Ok(Error(durable.RunnerLost)) = process.receive(result, 1000)
  let expected =
    durable.RecoveryRequired(durable.Required(
      at("compensate"),
      execution.StepCompensation(1),
      key,
    ))
  let run = reconnect(persistence, backend, "comp")
  drive(run) |> should.equal(Error(expected))
  durable.read(run) |> should.equal(Ok(durable.Suspended(expected)))
  durable.error_kind(expected) |> should.equal(durable.NeedsReconciliation)
  let recovered =
    make(fn(input, saved_key) {
      saved_key |> should.equal(key)
      Some(saga.Continue(input <> "!", saga.NoUndo))
    })
  drive(reconnect(recovered, backend, "comp"))
  |> should.equal(Ok(execution.Completed("x!")))
  process.receive(entered, 0) |> should.equal(Error(Nil))
  memory.stop(store)
}

fn resolve_interrupted(
  decision: saga.Recovery(String, String, String),
  budget: Int,
  cancel: Bool,
) -> execution.Outcome(String, String, String) {
  let entered = process.new_subject()
  let effects = process.new_subject()
  let owner = process.new_subject()
  let make = fn(recovering) {
    let step =
      saga.step("decision", fn(input) {
        process.send(effects, Nil)
        case recovering {
          True -> Ok(input <> " retried")
          False -> Error("original")
        }
      })
      |> saga.compensate(max_attempts: budget, with: fn(_failed) {
        process.send(entered, Nil)
        process.receive_forever(process.new_subject())
      })
      |> durable.restore_undo(fn(_undo) { saga.NoUndo })
      |> durable.resolve_compensation(fn(_, key) {
        saga.attempt_number(key) |> should.equal(1)
        Some(decision)
      })
      |> saga.map_step_errors(fn(e) { "mapped " <> e }, fn(u) { "mapped " <> u })
      |> durable.recoverable(
        version: "1",
        input: codec.text(),
        output: codec.text(),
        resolve: fn(_, _) { durable.MaybeSent },
      )
      // Reattaching codecs must preserve both reconciliation callbacks.
      |> durable.recoverable(
        version: "1",
        input: codec.text(),
        output: codec.text(),
        resolve: fn(_, _) { durable.MaybeSent },
      )
    let workflow =
      saga.define("decision", fn(input) { saga.perform(input, step) })
    prepare(workflow)
  }
  let store = start_memory()
  let backend = memory.storage(store)
  let first = start(make(False), stores.watched(backend, owner), "decision")
  let pending = drive_later(first)
  let assert Ok(pid) = process.receive(owner, 1000)
  let assert Ok(Nil) = process.receive(entered, 1000)
  let assert Ok(Nil) = process.receive(effects, 1000)
  kill_and_wait(pid)
  let assert Ok(Error(durable.RunnerLost)) = process.receive(pending, 1000)
  case cancel {
    True -> durable.cancel(first) |> should.equal(Ok(Nil))
    False -> Nil
  }
  let recovered = reconnect(make(True), backend, "decision")
  let assert Ok(outcome) = drive(recovered)
  durable.read(recovered) |> should.equal(Ok(durable.Finished(outcome)))
  process.receive(entered, 0) |> should.equal(Error(Nil))
  case outcome {
    execution.Completed("x retried") ->
      process.receive(effects, 0) |> should.equal(Ok(Nil))
    _ -> process.receive(effects, 0) |> should.equal(Error(Nil))
  }
  memory.stop(store)
  outcome
}

pub fn compensation_recovery_retry_test() {
  resolve_interrupted(saga.Retry, 2, False)
  |> should.equal(execution.Completed("x retried"))
  resolve_interrupted(saga.RetryAfter(duration.milliseconds(5)), 2, False)
  |> should.equal(execution.Completed("x retried"))
}

pub fn compensation_recovery_preserves_budget_test() {
  let assert execution.Failed(
    execution.RetryLimitReached(_, saga.Returned("mapped original")),
    _,
  ) = resolve_interrupted(saga.Retry, 1, False)
}

pub fn compensation_recovery_preserves_abort_and_cleanup_test() {
  let assert execution.Failed(execution.StepFailed(_, "mapped stop"), _) =
    resolve_interrupted(saga.Abort("stop"), 2, False)
  let assert execution.Failed(
    execution.StepFailed(_, "mapped stop"),
    settlement,
  ) =
    resolve_interrupted(
      saga.AbortAfterCleanupFailure("stop", "cleanup"),
      2,
      False,
    )
  let assert [execution.CleanupFailed(_, "mapped cleanup")] =
    settlement.compensation_failures
}

pub fn compensation_recovery_preserves_hold_test() {
  let assert execution.Unresolved(_, "mapped evidence", _) =
    resolve_interrupted(saga.Hold("evidence"), 2, False)
}

pub fn compensation_recovery_cancellation_supersedes_retry_test() {
  let assert execution.Cancelled(_, settlement) =
    resolve_interrupted(saga.Retry, 2, True)
  let assert [execution.RetrySuperseded(_, saga.Returned("mapped original"))] =
    settlement.sibling_failures
}

pub fn compensation_continue_restores_undo_after_second_restart_test() {
  let entered = process.new_subject()
  let owner = process.new_subject()
  let undos = process.new_subject()
  let make = fn(stage) {
    let first =
      saga.step("first", fn(_) { Error("failed") })
      |> saga.compensate(1, fn(_failed) {
        process.send(entered, "compensation")
        process.receive_forever(process.new_subject())
      })
      |> durable.restore_undo(fn(undo) {
        saga.UndoWith(fn() {
          process.send(undos, undo.input <> undo.output)
          Ok(Nil)
        })
      })
      |> durable.resolve_compensation(fn(input, _) {
        Some(saga.Continue(
          input <> "!",
          saga.UndoWith(fn() {
            panic as "original closure must not survive restart"
          }),
        ))
      })
      |> durable.recoverable(
        version: "1",
        input: codec.text(),
        output: codec.text(),
        resolve: fn(_, _) { durable.MaybeSent },
      )
    let second =
      saga.step("second", fn(_) {
        process.send(entered, "next effect")
        process.receive_forever(process.new_subject())
      })
      |> durable.recoverable(
        version: "1",
        input: codec.text(),
        output: codec.text(),
        resolve: fn(_, _) {
          case stage {
            2 -> durable.Failed("stop")
            _ -> durable.MaybeSent
          }
        },
      )
    let workflow =
      saga.define("restore-continue", fn(input) {
        input |> saga.perform(first) |> saga.perform(second)
      })
    prepare(workflow)
  }
  let store = start_memory()
  let backend = stores.watched(memory.storage(store), owner)
  let pending = drive_later(start(make(0), backend, "continue"))
  let assert Ok(pid) = process.receive(owner, 1000)
  process.receive(entered, 1000) |> should.equal(Ok("compensation"))
  kill_and_wait(pid)
  process.receive(pending, 1000) |> should.equal(Ok(Error(durable.RunnerLost)))
  let pending = drive_later(reconnect(make(1), backend, "continue"))
  let assert Ok(pid) = process.receive(owner, 1000)
  process.receive(entered, 1000) |> should.equal(Ok("next effect"))
  kill_and_wait(pid)
  process.receive(pending, 1000) |> should.equal(Ok(Error(durable.RunnerLost)))
  let assert Ok(execution.Failed(execution.StepFailed(_, "stop"), settlement)) =
    drive(reconnect(make(2), backend, "continue"))
  list.length(settlement.undone) |> should.equal(1)
  process.receive(undos, 1000) |> should.equal(Ok("xx!"))
  process.receive(entered, 0) |> should.equal(Error(Nil))
  memory.stop(store)
}

pub fn checkpoint_error_categories_remain_distinct_test() {
  let persistence = prepare(echo_workflow())
  let store = start_memory()
  let backend = memory.storage(store)
  let run = start(persistence, backend, "errors")
  list.each(
    [storage.Conflict, storage.StaleOwner, storage.Unavailable("disk")],
    fn(error) {
      let refused = stores.with_commit(backend, fn(_, _) { Error(error) })
      drive(reconnect(persistence, refused, "errors"))
      |> should.equal(Error(durable.StorageFailure(error)))
    },
  )
  let assert Ok(#(owner, stored)) = storage.do_claim(backend, "errors")
  let assert Ok(_) =
    storage.do_commit(
      backend,
      owner,
      storage.Commit(
        expected_revision: storage.revision(stored),
        observed_cancelled: storage.cancelled(stored),
        phase: storage.Pending,
        data: <<"not a checkpoint":utf8>>,
      ),
    )
  let assert Ok(Nil) = storage.do_release(backend, owner)
  durable.read(run)
  |> should.equal(Error(durable.InvalidCheckpoint(durable.Malformed)))
  drive(run)
  |> should.equal(Error(durable.InvalidCheckpoint(durable.Malformed)))
  memory.stop(store)
}

type WriteGate {
  RefuseWrites
  CheckWrite(process.Subject(Bool))
}

fn write_gate(messages: process.Subject(WriteGate), allowed: Bool) -> Nil {
  case process.receive_forever(messages) {
    RefuseWrites -> write_gate(messages, False)
    CheckWrite(reply) -> {
      process.send(reply, allowed)
      write_gate(messages, allowed)
    }
  }
}

pub fn suspension_recording_reports_both_failures_test() {
  let ready = process.new_subject()
  let gate_pid =
    process.spawn(fn() {
      let messages = process.new_subject()
      process.send(ready, messages)
      write_gate(messages, True)
    })
  let gate = process.receive_forever(ready)
  let broken =
    codec.new(
      "broken",
      fn(_) {
        process.send(gate, RefuseWrites)
        Error("cannot encode input")
      },
      Ok,
    )
  let step =
    saga.step("checked", fn(value) { Ok(value) })
    |> durable.recoverable(
      version: "1",
      input: broken,
      output: codec.text(),
      resolve: fn(_, _) { durable.MaybeSent },
    )
  let workflow =
    saga.define("recording", fn(input) { saga.perform(input, step) })
  let persistence = prepare(workflow)
  let store = start_memory()
  let backend = memory.storage(store)
  let refusing =
    stores.with_commit(backend, fn(owner, change) {
      let reply = process.new_subject()
      process.send(gate, CheckWrite(reply))
      case process.receive_forever(reply) {
        True -> storage.do_commit(backend, owner, change)
        False -> Error(storage.Unavailable("disk unavailable"))
      }
    })
  let run = start(persistence, refusing, "recording")
  drive(run)
  |> should.equal(
    Error(durable.SuspensionNotSaved(
      durable.CodecFailure(
        durable.StepInput(at("checked")),
        codec.EncodeFailed("cannot encode input"),
      ),
      durable.StorageFailure(storage.Unavailable("disk unavailable")),
    )),
  )
  durable.read(reconnect(persistence, backend, "recording"))
  |> should.equal(Ok(durable.Pending))
  process.unlink(gate_pid)
  kill_and_wait(gate_pid)
  memory.stop(store)
}

pub fn false_undo_declaration_blocks_continue_commit_test() {
  let step =
    saga.step("continue", fn(_) { Error("fail") })
    |> saga.compensate(1, fn(failed) {
      saga.Continue(failed.input, saga.UndoWith(fn() { Ok(Nil) }))
    })
    |> durable.restore_undo(fn(_undo) { saga.NoUndo })
    |> durable.recoverable(
      version: "1",
      input: codec.text(),
      output: codec.text(),
      resolve: fn(_, _) { durable.MaybeSent },
    )
  let workflow =
    saga.define("false-declaration", fn(input) { saga.perform(input, step) })
  let store = start_memory()
  let run = start(prepare(workflow), memory.storage(store), "false")
  let expected =
    durable.InvalidCheckpoint(durable.UndoNotRestorable(at("continue")))
  drive(run) |> should.equal(Error(expected))
  durable.read(run) |> should.equal(Ok(durable.Suspended(expected)))
  memory.stop(store)
}

pub fn compensation_recovery_cancellation_settles_continue_test() {
  let assert execution.Cancelled(_, settlement) =
    resolve_interrupted(saga.Continue("replacement", saga.NoUndo), 2, True)
  list.length(settlement.not_undoable) |> should.equal(1)
}

// ---------------------------------------------------------------------------
// drive bounds (R4) and storage bounds
// ---------------------------------------------------------------------------

fn blocking_workflow(
  entered: process.Subject(process.Pid),
  recovered: Bool,
) -> saga.Workflow(String, String, String, String) {
  let workflow =
    saga.define("blocking", fn(input) {
      saga.perform(
        input,
        saga.step("slow", fn(value) {
          process.send(entered, process.self())
          process.sleep_forever()
          Ok(value)
        })
          |> durable.recoverable(
            version: "1",
            input: codec.text(),
            output: codec.text(),
            resolve: fn(value, _) {
              case recovered {
                True -> durable.Completed(value <> " recovered")
                False -> durable.MaybeSent
              }
            },
          ),
      )
    })
  workflow
}

/// On timeout the runner stops: its attempt is killed, its claim is
/// released at once, and the next drive resumes from the checkpoint.
pub fn drive_timeout_stops_the_runner_and_keeps_the_checkpoint_test() {
  let entered = process.new_subject()
  let store = start_memory()
  let backend = memory.storage(store)
  let run = start(prepare(blocking_workflow(entered, False)), backend, "slow")
  durable.drive(run, timeout: duration.milliseconds(200))
  |> should.equal(Error(durable.DriveTimedOut))
  let assert Ok(attempt) = process.receive(entered, 1000)
  stops_within(attempt, 1000)
  durable.read(run) |> should.equal(Ok(durable.Pending))
  // The claim was released: the next drive is not Busy.
  durable.drive(
    reconnect(prepare(blocking_workflow(entered, True)), backend, "slow"),
    timeout: duration.seconds(5),
  )
  |> should.equal(Ok(execution.Completed("x recovered")))
  durable.error_kind(durable.DriveTimedOut) |> should.equal(durable.Transient)
  memory.stop(store)
}

/// When the process that called `drive` exits, the runner stops by itself
/// instead of carrying on unattended (CHK-6), and does not cancel.
pub fn caller_exit_stops_the_runner_test() {
  let entered = process.new_subject()
  let store = start_memory()
  let backend = memory.storage(store)
  let run = start(prepare(blocking_workflow(entered, False)), backend, "orphan")
  let caller =
    process.spawn_unlinked(fn() {
      durable.drive(run, timeout: duration.seconds(60))
    })
  let assert Ok(attempt) = process.receive(entered, 1000)
  kill_and_wait(caller)
  stops_within(attempt, 1000)
  durable.read(run) |> should.equal(Ok(durable.Pending))
  durable.drive(
    reconnect(prepare(blocking_workflow(entered, True)), backend, "orphan"),
    timeout: duration.seconds(5),
  )
  |> should.equal(Ok(execution.Completed("x recovered")))
  memory.stop(store)
}

pub fn drive_rejects_a_non_positive_timeout_test() {
  let store = start_memory()
  let run = start(prepare(echo_workflow()), memory.storage(store), "zero")
  durable.drive(run, timeout: duration.milliseconds(0))
  |> should.equal(Error(durable.InvalidTimeout(duration.milliseconds(0))))
  memory.stop(store)
}

pub fn drive_reports_an_invalid_config_test() {
  let store = start_memory()
  let persistence =
    prepare(echo_workflow())
    |> durable.with_config(
      execution.config() |> execution.with_max_concurrency(0),
    )
  let run = start(persistence, memory.storage(store), "config")
  durable.drive(run, timeout: duration.seconds(1))
  |> should.equal(
    Error(durable.InvalidConfig([execution.MaxConcurrencyNotPositive(0)])),
  )
  memory.stop(store)
}

/// A commit slower than the storage's call timeout stops the runner with
/// `TimedOut` and keeps the last checkpoint.
pub fn slow_storage_call_times_out_test() {
  let store = start_memory()
  let backend = memory.storage(store)
  let slow =
    stores.with_commit(backend, fn(_, _) {
      process.sleep(60_000)
      Error(storage.Unavailable("never"))
    })
    |> storage.with_call_timeout(duration.milliseconds(100))
  let persistence = prepare(echo_workflow())
  let run = start(persistence, slow, "slow-store")
  durable.drive(run, timeout: duration.seconds(5))
  |> should.equal(Error(durable.StorageFailure(storage.TimedOut)))
  drive(reconnect(persistence, backend, "slow-store"))
  |> should.equal(Ok(execution.Completed("x!")))
  memory.stop(store)
}

/// A checkpoint above the size limit suspends the run before any effect.
pub fn checkpoint_size_is_bounded_test() {
  let effects = process.new_subject()
  let workflow =
    saga.define("large", fn(input) {
      saga.perform(
        input,
        text_step("effect", fn(value) {
          process.send(effects, Nil)
          Ok(value)
        }),
      )
    })
  let store = start_memory()
  let persistence = prepare(workflow) |> durable.with_max_checkpoint_bytes(16)
  let assert Error(durable.CheckpointTooLarge(_, 16)) =
    durable.start_or_reconnect(
      persistence,
      memory.storage(store),
      id: "large",
      input: "x",
    )
  let backend = memory.storage(store)
  let _ = start(prepare(workflow), backend, "roomy")
  let assert Ok(saved) = storage.do_load(backend, "roomy")
  let initial = bit_array.byte_size(storage.data(saved))
  // The first checkpoint fits; the next one, with the admitted input, not.
  let tight = prepare(workflow) |> durable.with_max_checkpoint_bytes(initial)
  let tight_run = reconnect(tight, backend, "roomy")
  let assert Error(durable.CheckpointTooLarge(bytes, limit)) = drive(tight_run)
  limit |> should.equal(initial)
  // The suspension is recorded, with its reason, although it passes the
  // limit by that reason.
  durable.read(tight_run)
  |> should.equal(
    Ok(durable.Suspended(durable.CheckpointTooLarge(bytes, limit))),
  )
  { bytes > initial } |> should.be_true
  process.receive(effects, 0) |> should.equal(Error(Nil))
  memory.stop(store)
}

/// A storage with renewal keeps a long step's claim alive, and a renewal
/// that finds the claim taken over stops the runner.
pub fn renewal_keeps_the_claim_and_detects_a_takeover_test() {
  let renewals = process.new_subject()
  let renewed = probe.new_counter()
  let entered = process.new_subject()
  let store = start_memory()
  let renewing =
    memory.storage(store)
    |> storage.with_renewal(every: duration.milliseconds(20), renew: fn(claim) {
      process.send(renewals, claim)
      // The third renewal finds the claim taken over.
      probe.counter_enter(renewed)
      case probe.total_entries(renewed) >= 3 {
        True -> Error(storage.StaleOwner)
        False -> Ok(Nil)
      }
    })
  let run = start(prepare(blocking_workflow(entered, False)), renewing, "lease")
  durable.drive(run, timeout: duration.seconds(5))
  |> should.equal(Error(durable.StorageFailure(storage.StaleOwner)))
  let assert Ok(attempt) = process.receive(entered, 1000)
  stops_within(attempt, 1000)
  let assert Ok(first) = process.receive(renewals, 1000)
  storage.claim_id(first) |> should.equal("lease")
  memory.stop(store)
}

/// `unfinished` lists executions that wait for a driver; finished ones and
/// ones a live runner owns are left out.
pub fn unfinished_lists_executions_waiting_for_a_driver_test() {
  let entered = process.new_subject()
  let store = start_memory()
  let backend = memory.storage(store)
  let persistence = prepare(echo_workflow())
  let finished = start(persistence, backend, "finished")
  let assert Ok(_) = drive(finished)
  let _ = start(persistence, backend, "waiting")
  let busy = start(prepare(blocking_workflow(entered, False)), backend, "busy")
  let running = drive_later(busy)
  let assert Ok(_) = process.receive(entered, 1000)
  durable.unfinished(backend, limit: 10) |> should.equal(Ok(["waiting"]))
  durable.unfinished(backend, limit: 0) |> should.equal(Ok([]))
  let assert Ok(Nil) = durable.cancel(busy)
  let _ = running
  memory.stop(store)
}

/// A durable run's events carry the execution id, and the handle's
/// correlation.
pub fn durable_events_carry_execution_and_correlation_test() {
  let seen = process.new_subject()
  let assert Ok(order) = correlation.from_string("order-7")
  let store = start_memory()
  let run =
    start(prepare(echo_workflow()), memory.storage(store), "checkout:7")
    |> durable.with_correlation(order)
  let plan =
    sinal.subscriptions([
      sinal.subscription(telemetry.run_stopped(), fn(_m, d) {
        process.send(seen, #(d.execution, d.correlation))
      }),
    ])
  let assert Ok(sinal.SubscriptionCompletion(Ok(execution.Completed("x!")), [])) =
    sinal.with_subscriptions(plan, fn() { drive(run) })
  process.receive(seen, 1000)
  |> should.equal(Ok(#(Some("checkout:7"), Some(order))))
  memory.stop(store)
}

pub fn error_kinds_classify_every_variant_test() {
  durable.error_kind(durable.StorageFailure(storage.Busy))
  |> should.equal(durable.Busy)
  durable.error_kind(durable.StorageFailure(storage.TimedOut))
  |> should.equal(durable.Transient)
  durable.error_kind(durable.RunnerLost) |> should.equal(durable.Transient)
  durable.error_kind(durable.IncompatibleDefinition)
  |> should.equal(durable.Incompatible)
  durable.error_kind(durable.InputMismatch)
  |> should.equal(durable.Incompatible)
  durable.error_kind(durable.SuspensionNotSaved(
    durable.RunnerLost,
    durable.StorageFailure(storage.Corrupt),
  ))
  |> should.equal(durable.Transient)
  durable.error_kind(durable.InvalidCheckpoint(durable.Malformed))
  |> should.equal(durable.Defect)
  durable.describe_error(durable.StorageFailure(storage.Busy))
  |> should.equal("storage: another runner owns the execution")
}

// ---------------------------------------------------------------------------
// unknown_when across a restart (open question 1)
// ---------------------------------------------------------------------------

/// A "maybe sent" error is recorded as unknown when it ends, and the record
/// survives a restart: the in-flight decision goes to the compensation
/// resolver, and the retried success still completes with unknown effects.
pub fn classified_error_survives_a_restart_in_its_decision_test() {
  let entered = process.new_subject()
  let owner = process.new_subject()
  let make = fn(recovering) {
    let step =
      saga.effect("charge", fn(value, key: saga.EffectKey) {
        case saga.attempt_number(key) {
          1 -> Error("maybe charged")
          _ -> Ok(value <> " charged")
        }
      })
      |> saga.unknown_when(fn(error) { error == "maybe charged" })
      |> saga.compensate(max_attempts: 2, with: fn(_failed) {
        process.send(entered, Nil)
        process.receive_forever(process.new_subject())
      })
      |> durable.restore_undo(fn(_undo) { saga.NoUndo })
      |> durable.resolve_compensation(fn(_, _) {
        case recovering {
          True -> Some(saga.Retry)
          False -> None
        }
      })
      |> durable.recoverable(
        version: "1",
        input: codec.text(),
        output: codec.text(),
        resolve: fn(_, _) { durable.MaybeSent },
      )
    let workflow = saga.define("maybe", fn(input) { saga.perform(input, step) })
    prepare(workflow)
  }
  let store = start_memory()
  let backend = stores.watched(memory.storage(store), owner)
  let result = drive_later(start(make(False), backend, "maybe"))
  let assert Ok(pid) = process.receive(owner, 1000)
  let assert Ok(Nil) = process.receive(entered, 1000)
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  let assert Ok(execution.CompletedWithUnknownEffects("x charged", [unknown])) =
    drive(reconnect(make(True), backend, "maybe"))
  unknown.step |> should.equal(at("charge"))
  unknown.action |> should.equal(execution.StepAttempt(1))
  unknown.ending |> should.equal(execution.ActionReturnedUnknown)
  memory.stop(store)
}

/// After a restart, a resolver's `Failed` that `unknown_when` classifies
/// follows `on_unknown`: by default the execution finishes `Unresolved`
/// and keeps the reservation made before the payment; with `RollBack` it
/// fails and releases it.
pub fn classified_resolver_failure_follows_on_unknown_test() {
  let entered = process.new_subject()
  let owner = process.new_subject()
  let released = process.new_subject()
  let make = fn(recovering, policy) {
    let charge =
      saga.step("charge", fn(value) {
        process.send(entered, Nil)
        process.sleep_forever()
        Ok(value)
      })
      |> saga.unknown_when(fn(error) { error == "maybe charged" })
      |> saga.on_unknown(policy)
      |> durable.recoverable(
        version: "1",
        input: codec.text(),
        output: codec.text(),
        resolve: fn(_, _) {
          case recovering {
            True -> durable.Failed("maybe charged")
            False -> durable.MaybeSent
          }
        },
      )
    let reserve =
      saga.step("reserve", fn(value) { Ok(value) })
      |> saga.undo(fn(_) {
        process.send(released, Nil)
        Ok(Nil)
      })
      |> durable.recoverable(
        version: "1",
        input: codec.text(),
        output: codec.text(),
        resolve: fn(_, _) { durable.MaybeSent },
      )
    let workflow =
      saga.define("maybe-resolved", fn(input) {
        input |> saga.perform(reserve) |> saga.perform(charge)
      })
    prepare(workflow)
  }
  let store = start_memory()
  let backend = stores.watched(memory.storage(store), owner)
  let interrupt = fn(id, policy) {
    let result = drive_later(start(make(False, policy), backend, id))
    let assert Ok(pid) = process.receive(owner, 1000)
    let assert Ok(Nil) = process.receive(entered, 1000)
    kill_and_wait(pid)
    process.receive(result, 1000) |> should.equal(Ok(Error(durable.RunnerLost)))
    let resumed = drive(reconnect(make(True, policy), backend, id))
    // Forget the resuming runner.
    let assert Ok(_) = process.receive(owner, 0)
    resumed
  }
  let assert Ok(held) = interrupt("held", saga.Reconcile)
  let assert execution.Unresolved(step, "maybe charged", settlement) = held
  step |> should.equal(at("charge"))
  settlement.held |> should.equal([at("reserve")])
  let assert [unknown] = execution.unknown_effects(held)
  unknown.ending |> should.equal(execution.ActionReturnedUnknown)
  process.receive(released, 50) |> should.equal(Error(Nil))
  // The saved outcome survives a reconnect.
  durable.read(reconnect(make(True, saga.Reconcile), backend, "held"))
  |> should.equal(Ok(durable.Finished(held)))
  let assert Ok(rolled_back) = interrupt("rolled-back", saga.RollBack)
  let assert execution.Failed(
    execution.StepFailed(_, "maybe charged"),
    settlement,
  ) = rolled_back
  settlement.undone |> should.equal([at("reserve")])
  process.receive(released, 1000) |> should.equal(Ok(Nil))
  memory.stop(store)
}
