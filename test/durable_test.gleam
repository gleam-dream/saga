import gleam/erlang/process
import gleeunit/should
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/reconciliation
import saga/storage
import saga/storage/memory

fn echo_workflow() -> saga.Workflow(String, String, String, String) {
  let assert Ok(workflow) =
    saga.define("echo", fn(input) {
      saga.perform(
        input,
        saga.step("echo", fn(value) { Ok(value <> "!") })
          |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
            saga.EffectUnknown
          }),
      )
    })
  workflow
}

fn prepare(
  workflow: saga.Workflow(String, String, String, String),
) -> durable.Persistence(String, String, String, String) {
  let text = codec.text()
  let assert Ok(persistence) =
    durable.prepare(workflow, "1", text, text, text, text)
  persistence
}

pub fn same_definition_local_and_persistent_test() {
  let workflow = echo_workflow()
  execution.run(workflow, "hello", execution.config())
  |> should.equal(Ok(execution.Completed("hello!")))
  let memory = memory.new()
  let storage = memory.storage(memory)
  let persistence = prepare(workflow)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "one", persistence, "hello")
  durable.drive(storage, reference, persistence, execution.config())
  |> should.equal(Ok(execution.Completed("hello!")))
  durable.read(storage, reference, persistence)
  |> should.equal(Ok(durable.Finished(execution.Completed("hello!"))))
  durable.start_or_reconnect(storage, "one", persistence, "hello")
  |> should.equal(Ok(reference))
  durable.start_or_reconnect(storage, "one", persistence, "other")
  |> should.equal(Error(durable.InputMismatch))
  durable.start_or_reconnect(storage, "two", persistence, "hello")
  |> should.equal(Error(durable.ReferenceMismatch))
  memory.close(memory)
}

pub fn memory_conflicts_and_ownership_test() {
  let memory = memory.new()
  let storage = memory.storage(memory)
  let assert Ok(_) = storage.create(<<"initial":utf8>>)
  let assert Ok(owner) = storage.claim()
  storage.commit(owner.generation, 99, False, <<>>)
  |> should.equal(Error(storage.Conflict))
  storage.commit(owner.generation + 1, 0, False, <<>>)
  |> should.equal(Error(storage.StaleOwner))
  let response = process.new_subject()
  process.spawn_unlinked(fn() { process.send(response, storage.claim()) })
  process.receive(response, 1000) |> should.equal(Ok(Error(storage.Busy)))
  let assert Ok(_) = storage.cancel()
  storage.commit(owner.generation, 0, False, <<>>)
  |> should.equal(Error(storage.CancellationChanged))
  let assert Ok(_) = storage.release(owner.generation)
  memory.close(memory)
}

pub fn persistent_concurrent_shared_dependency_test() {
  let ready = process.new_subject()
  let branch = fn(name) {
    saga.step(name, fn(value) {
      let release = process.new_subject()
      process.send(ready, release)
      process.receive_forever(release)
      Ok(value <> name)
    })
    |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
      saga.EffectUnknown
    })
  }
  let assert Ok(workflow) =
    saga.define("parallel", fn(input) {
      let shared =
        saga.perform(
          input,
          saga.step("shared", fn(value) { Ok(value <> "!") })
            |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
              saga.EffectUnknown
            }),
        )
      saga.both(
        saga.perform(shared, branch("a")),
        saga.perform(shared, branch("b")),
      )
      |> saga.map(fn(pair) { pair.0 <> pair.1 })
    })
  let memory = memory.new()
  let storage = memory.storage(memory)
  let persistence = prepare(workflow)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "parallel", persistence, "x")
  let result = process.new_subject()
  process.spawn_unlinked(fn() {
    process.send(
      result,
      durable.drive(
        storage,
        reference,
        persistence,
        execution.config()
          |> execution.with_max_concurrency(2)
          |> execution.without_step_timeout(),
      ),
    )
  })
  let assert Ok(a) = process.receive(ready, 1000)
  let assert Ok(b) = process.receive(ready, 1000)
  process.send(a, Nil)
  process.send(b, Nil)
  process.receive(result, 2000)
  |> should.equal(Ok(Ok(execution.Completed("x!ax!b"))))
  memory.close(memory)
}

fn watched(
  storage: storage.Storage,
  owner: process.Subject(process.Pid),
) -> storage.Storage {
  storage.Storage(..storage, claim: fn() {
    let result = storage.claim()
    case result {
      Ok(_) -> process.send(owner, process.self())
      Error(_) -> Nil
    }
    result
  })
}

fn drive_later(
  storage: storage.Storage,
  reference: durable.Reference,
  persistence: durable.Persistence(String, String, String, String),
) -> process.Subject(
  Result(execution.Outcome(String, String, String), durable.Error),
) {
  let result = process.new_subject()
  process.spawn_unlinked(fn() {
    process.send(
      result,
      durable.drive(
        storage,
        reference,
        persistence,
        execution.config() |> execution.without_step_timeout(),
      ),
    )
  })
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

pub fn saved_success_and_uncertain_effect_resume_test() {
  let events = process.new_subject()
  let owner = process.new_subject()
  let build = fn(recovering) {
    let assert Ok(workflow) =
      saga.define("recover", fn(input) {
        input
        |> saga.perform(
          saga.step("saved", fn(value) {
            process.send(events, "saved ran")
            Ok(value <> "!")
          })
          |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
            saga.EffectUnknown
          }),
        )
        |> saga.perform(
          saga.effect("uncertain", fn(value, _) {
            process.send(events, "effect ran")
            process.sleep_forever()
            Ok(value)
          })
          |> saga.recoverable("1", codec.text(), codec.text(), fn(value, _) {
            case recovering {
              True -> saga.EffectCompleted(value <> "recovered")
              False -> saga.EffectUnknown
            }
          }),
        )
      })
    prepare(workflow)
  }
  let memory = memory.new()
  let storage = watched(memory.storage(memory), owner)
  let initial = build(False)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "recover", initial, "x")
  let result = drive_later(storage, reference, initial)
  let assert Ok(pid) = process.receive(owner, 1000)
  process.receive(events, 1000) |> should.equal(Ok("saved ran"))
  process.receive(events, 1000) |> should.equal(Ok("effect ran"))
  kill_and_wait(pid)
  process.receive(result, 1000) |> should.equal(Ok(Error(durable.RunnerLost)))
  let assert Error(durable.RecoveryRequired(_)) =
    durable.drive(storage, reference, build(False), execution.config())
  let assert Ok(durable.Suspended(_)) =
    durable.read(storage, reference, build(False))
  durable.drive(storage, reference, build(True), execution.config())
  |> should.equal(Ok(execution.Completed("x!recovered")))
  process.receive(events, 0) |> should.equal(Error(Nil))
  memory.close(memory)
}

pub fn rollback_resumes_uncertain_undo_test() {
  let events = process.new_subject()
  let owner = process.new_subject()
  let build = fn(recovering) {
    let assert Ok(workflow) =
      saga.define("undo", fn(input) {
        input
        |> saga.perform(
          saga.step("resource", fn(value) { Ok(value) })
          |> saga.undo(fn(_, _) {
            process.send(events, "undo")
            process.sleep_forever()
            Ok(Nil)
          })
          |> saga.reconcile_undo(fn(_, _, _) {
            case recovering {
              True -> saga.UndoCompleted
              False -> saga.UndoUnknown
            }
          })
          |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
            saga.EffectUnknown
          }),
        )
        |> saga.perform(
          saga.step("fail", fn(_) { Error("boom") })
          |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
            saga.EffectUnknown
          }),
        )
      })
    prepare(workflow)
  }
  let memory = memory.new()
  let storage = watched(memory.storage(memory), owner)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "undo", build(False), "x")
  let result = drive_later(storage, reference, build(False))
  let assert Ok(pid) = process.receive(owner, 1000)
  process.receive(events, 1000) |> should.equal(Ok("undo"))
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  let assert Ok(execution.Failed(execution.StepFailed(_, "boom"), settlement)) =
    durable.drive(storage, reference, build(True), execution.config())
  settlement.undone |> should.equal([saga.StepAddress([], "resource", 1)])
  process.receive(events, 0) |> should.equal(Error(Nil))
  memory.close(memory)
}

pub fn cancellation_before_admission_test() {
  let memory = memory.new()
  let storage = memory.storage(memory)
  let persistence = prepare(echo_workflow())
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "cancel", persistence, "x")
  durable.cancel(storage, reference, persistence) |> should.equal(Ok(Nil))
  let assert Ok(execution.Cancelled(_, _)) =
    durable.drive(storage, reference, persistence, execution.config())
  let assert Ok(durable.Finished(execution.Cancelled(_, _))) =
    durable.read(storage, reference, persistence)
  memory.close(memory)
}

pub fn persisted_choice_survives_fresh_definition_test() {
  let entered = process.new_subject()
  let owner = process.new_subject()
  let build = fn(recovering) {
    let assert Ok(workflow) =
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
                |> saga.recoverable(
                  "1",
                  codec.text(),
                  codec.text(),
                  fn(value, _) { saga.EffectCompleted(value <> "-selected") },
                ),
            )
          },
          fn(port) {
            saga.perform(
              port,
              saga.step("unchosen", fn(_) { Error("unchosen branch ran") })
                |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
                  saga.EffectUnknown
                }),
            )
          },
        )
      })
    prepare(workflow)
  }
  let memory = memory.new()
  let storage = watched(memory.storage(memory), owner)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "choice", build(False), "x")
  let result = drive_later(storage, reference, build(False))
  let assert Ok(pid) = process.receive(owner, 1000)
  let assert Ok(Nil) = process.receive(entered, 1000)
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  // Deliberately change this pure callback to prove the saved selection is
  // authoritative. Production callback changes require a version change.
  durable.drive(storage, reference, build(True), execution.config())
  |> should.equal(Ok(execution.Completed("x-selected")))
  memory.close(memory)
}

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
      |> saga.recoverable("1", codec.text(), codec.text(), fn(value, key) {
        process.send(entered, #(name, key))
        case recovering {
          True -> saga.EffectCompleted(value <> name)
          False -> saga.EffectUnknown
        }
      })
    }
    let assert Ok(workflow) =
      saga.define("two", fn(input) {
        saga.both(
          saga.perform(input, branch("a")),
          saga.perform(input, branch("b")),
        )
        |> saga.map(fn(pair) { pair.0 <> pair.1 })
      })
    prepare(workflow)
  }
  let memory = memory.new()
  let storage = watched(memory.storage(memory), owner)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "two", build(False), "x")
  let result = drive_later(storage, reference, build(False))
  let assert Ok(pid) = process.receive(owner, 1000)
  let assert Ok(first) = process.receive(entered, 1000)
  let assert Ok(second) = process.receive(entered, 1000)
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  durable.drive(storage, reference, build(True), execution.config())
  |> should.equal(Ok(execution.Completed("xaxb")))
  let assert Ok(resolved_first) = process.receive(entered, 1000)
  let assert Ok(resolved_second) = process.receive(entered, 1000)
  [first, second]
  |> gleam_list.sort(fn(a, b) { gleam_string.compare(a.0, b.0) })
  |> should.equal(
    gleam_list.sort([resolved_first, resolved_second], fn(a, b) {
      gleam_string.compare(a.0, b.0)
    }),
  )
  memory.close(memory)
}

pub fn retry_and_continue_share_local_semantics_test() {
  let attempts = process.new_subject()
  let step =
    saga.step("retry", fn(value) {
      process.send(attempts, value)
      Error("retry")
    })
    |> saga.compensate(max_attempts: 2, with: fn(value, _, attempt) {
      case attempt.number {
        1 -> saga.RetryAfter(1)
        _ -> saga.Continue(value <> "-continued", saga.NoUndo)
      }
    })
    |> saga.restore_undo(fn(_, _, _) { saga.NoUndo })
    |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
      saga.EffectUnknown
    })
  let assert Ok(workflow) =
    saga.define("retry", fn(input) { saga.perform(input, step) })
  let memory = memory.new()
  let storage = memory.storage(memory)
  let persistence = prepare(workflow)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "retry", persistence, "x")
  durable.drive(storage, reference, persistence, execution.config())
  |> should.equal(Ok(execution.Completed("x-continued")))
  process.receive(attempts, 100) |> should.equal(Ok("x"))
  process.receive(attempts, 100) |> should.equal(Ok("x"))
  process.receive(attempts, 0) |> should.equal(Error(Nil))
  memory.close(memory)
}

pub fn incompatible_definition_refused_before_callbacks_test() {
  let events = process.new_subject()
  let memory = memory.new()
  let storage = memory.storage(memory)
  let initial = prepare(echo_workflow())
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "versioned", initial, "x")
  let text = codec.text()
  let changed_codec =
    codec.new("different", fn(value) { Ok(value) }, fn(value) {
      process.send(events, Nil)
      Ok(value)
    })
  let assert Ok(changed) =
    durable.prepare(echo_workflow(), "2", text, changed_codec, text, text)
  durable.drive(storage, reference, changed, execution.config())
  |> should.equal(Error(durable.IncompatibleDefinition))
  process.receive(events, 0) |> should.equal(Error(Nil))
  memory.close(memory)
}

pub fn cancellation_racing_completion_wins_test() {
  let memory = memory.new()
  let backend = memory.storage(memory)
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
  let assert Ok(persistence) =
    durable.prepare(echo_workflow(), "1", text, root_codec, text, text)
  let assert Ok(reference) =
    durable.start_or_reconnect(backend, "race", persistence, "x")
  let result = drive_later(backend, reference, persistence)
  let assert Ok(release) = process.receive(entered, 1000)
  durable.cancel(backend, reference, persistence) |> should.equal(Ok(Nil))
  process.send(release, Nil)
  let assert Ok(Ok(execution.Cancelled(_, _))) = process.receive(result, 2000)
  memory.close(memory)
}

pub fn cancellation_reconciles_admitted_absence_without_dispatch_test() {
  let entered = process.new_subject()
  let owner = process.new_subject()
  let assert Ok(workflow) =
    saga.define("cancel-admitted", fn(input) {
      saga.perform(
        input,
        saga.step("effect", fn(value) {
          process.send(entered, Nil)
          process.sleep_forever()
          Ok(value)
        })
          |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
            saga.EffectAbsent
          }),
      )
    })
  let persistence = prepare(workflow)
  let memory = memory.new()
  let storage = watched(memory.storage(memory), owner)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "cancel-admitted", persistence, "x")
  let result = drive_later(storage, reference, persistence)
  let assert Ok(pid) = process.receive(owner, 1000)
  let assert Ok(Nil) = process.receive(entered, 1000)
  kill_and_wait(pid)
  let _ = process.receive(result, 1000)
  durable.cancel(storage, reference, persistence) |> should.equal(Ok(Nil))
  let assert Ok(execution.Cancelled(_, _)) =
    durable.drive(storage, reference, persistence, execution.config())
  process.receive(entered, 0) |> should.equal(Error(Nil))
  memory.close(memory)
}

import gleam/list as gleam_list
import gleam/string as gleam_string

pub fn bad_input_codec_prevents_effect_test() {
  let effects = process.new_subject()
  let bad = codec.new("bad", fn(_) { Error("cannot encode") }, Ok)
  let assert Ok(workflow) =
    saga.define("bad-codec", fn(input) {
      saga.perform(
        input,
        saga.step("effect", fn(value) {
          process.send(effects, Nil)
          Ok(value)
        })
          |> saga.recoverable("1", bad, codec.text(), fn(_, _) {
            saga.EffectUnknown
          }),
      )
    })
  // Codec configuration has no cost or admission requirement for local use.
  execution.run(workflow, "x", execution.config())
  |> should.equal(Ok(execution.Completed("x")))
  let assert Ok(Nil) = process.receive(effects, 100)
  let persistence = prepare(workflow)
  let memory = memory.new()
  let storage = memory.storage(memory)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "bad", persistence, "x")
  let assert Error(durable.CodecFailure("cannot encode")) =
    durable.drive(storage, reference, persistence, execution.config())
  process.receive(effects, 0) |> should.equal(Error(Nil))
  memory.close(memory)
}

pub fn failed_commit_prevents_dispatch_test() {
  let effects = process.new_subject()
  let assert Ok(workflow) =
    saga.define("refused", fn(input) {
      saga.perform(
        input,
        saga.step("effect", fn(value) {
          process.send(effects, Nil)
          Ok(value)
        })
          |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
            saga.EffectUnknown
          }),
      )
    })
  let persistence = prepare(workflow)
  let memory = memory.new()
  let backend = memory.storage(memory)
  let storage =
    storage.Storage(..backend, commit: fn(_, _, _, _) {
      Error(storage.Conflict)
    })
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "refused", persistence, "x")
  let assert Error(durable.StorageError(storage.Conflict)) =
    durable.drive(storage, reference, persistence, execution.config())
  process.receive(effects, 0) |> should.equal(Error(Nil))
  // The committed state still has no admitted effect, so a working adapter
  // can proceed without asking the unknown resolver to authorize a replay.
  durable.drive(backend, reference, persistence, execution.config())
  |> should.equal(Ok(execution.Completed("x")))
  process.receive(effects, 100) |> should.equal(Ok(Nil))
  memory.close(memory)
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
    |> saga.undo(fn(_, _) {
      process.send(undone, name)
      Ok(Nil)
    })
    |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
      saga.EffectUnknown
    })
  }
  let assert Ok(workflow) =
    saga.define("rollback-order", fn(input) {
      let a = saga.perform(input, branch("a"))
      let b = saga.perform(input, branch("b"))
      saga.both(a, b)
      |> saga.map(fn(pair) { pair.0 <> pair.1 })
      |> saga.perform(
        saga.step("fail", fn(_) { Error("boom") })
        |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
          saga.EffectUnknown
        }),
      )
    })
  let persistence = prepare(workflow)
  let memory = memory.new()
  let backend = memory.storage(memory)
  let assert Ok(reference) =
    durable.start_or_reconnect(backend, "order", persistence, "x")
  let result = drive_later(backend, reference, persistence)
  let assert Ok(first) = process.receive(entered, 1000)
  let assert Ok(second) = process.receive(entered, 1000)
  // Each branch's return is admitted independently; release order is not a
  // promise of commit order, so verify dependency-safe rollback coverage.
  process.send(first.1, Nil)
  process.send(second.1, Nil)
  let assert Ok(Ok(execution.Failed(_, settlement))) =
    process.receive(result, 2000)
  gleam_list.length(settlement.undone) |> should.equal(2)
  let assert Ok(a) = process.receive(undone, 1000)
  let assert Ok(b) = process.receive(undone, 1000)
  gleam_list.sort([a, b], gleam_string.compare) |> should.equal(["a", "b"])
  memory.close(memory)
}

pub fn persistent_compensation_requires_undo_contract_before_effects_test() {
  let effects = process.new_subject()
  let step =
    saga.step("compensate", fn(value) {
      process.send(effects, value)
      Error("failed")
    })
    |> saga.compensate(1, fn(value, _, _) {
      saga.Continue(value, saga.UndoWith(fn() { Ok(Nil) }))
    })
    |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
      saga.EffectUnknown
    })
  let assert Ok(workflow) =
    saga.define("eligibility", fn(input) { saga.perform(input, step) })
  let text = codec.text()
  let assert Error(durable.InvalidDefinition(_)) =
    durable.prepare(workflow, "1", text, text, text, text)
  process.receive(effects, 0) |> should.equal(Error(Nil))
}

pub fn interrupted_compensation_requires_explicit_resolution_test() {
  let entered = process.new_subject()
  let owner = process.new_subject()
  let make = fn(resolve) {
    let step =
      saga.step("compensate", fn(_) { Error("failed") })
      |> saga.compensate_with_key(2, fn(_, _, attempt, key) {
        process.send(entered, #(attempt, key))
        process.receive_forever(process.new_subject())
      })
      |> saga.restore_undo(fn(_, _, _) { saga.NoUndo })
      |> saga.reconcile_compensation(resolve)
      |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
        saga.EffectUnknown
      })
    let assert Ok(workflow) =
      saga.define("compensation-recovery", fn(input) {
        saga.perform(input, step)
      })
    prepare(workflow)
  }
  let memory = memory.new()
  let backend = memory.storage(memory)
  let persistence = make(fn(_, _, _) { saga.CompensationUnknown })
  let assert Ok(reference) =
    durable.start_or_reconnect(backend, "compensation", persistence, "x")
  let result = drive_later(watched(backend, owner), reference, persistence)
  let assert Ok(pid) = process.receive(owner, 1000)
  let assert Ok(#(attempt, key)) = process.receive(entered, 1000)
  kill_and_wait(pid)
  let assert Ok(Error(durable.RunnerLost)) = process.receive(result, 1000)
  let expected =
    durable.RecoveryRequired(reconciliation.Required(
      "compensate",
      reconciliation.Compensation,
      key,
    ))
  durable.drive(backend, reference, persistence, execution.config())
  |> should.equal(Error(expected))
  durable.read(backend, reference, persistence)
  |> should.equal(Ok(durable.Suspended(expected)))
  let recovered =
    make(fn(input, saved_attempt, saved_key) {
      saved_attempt |> should.equal(attempt)
      saved_key |> should.equal(key)
      saga.CompensationResolved(saga.Continue(input <> "!", saga.NoUndo))
    })
  durable.drive(backend, reference, recovered, execution.config())
  |> should.equal(Ok(execution.Completed("x!")))
  process.receive(entered, 0) |> should.equal(Error(Nil))
  memory.close(memory)
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
      |> saga.compensate_with_key(budget, fn(_, _, _, _) {
        process.send(entered, Nil)
        process.receive_forever(process.new_subject())
      })
      |> saga.restore_undo(fn(_, _, _) { saga.NoUndo })
      |> saga.reconcile_compensation(fn(_, attempt, _) {
        attempt.number |> should.equal(1)
        attempt.remaining |> should.equal(budget - 1)
        saga.CompensationResolved(decision)
      })
      |> saga.map_step_errors(fn(e) { "mapped " <> e }, fn(u) { "mapped " <> u })
      |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
        saga.EffectUnknown
      })
      // Reattaching codecs must preserve both reconciliation callbacks.
      |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
        saga.EffectUnknown
      })
    let assert Ok(workflow) =
      saga.define("decision", fn(input) { saga.perform(input, step) })
    prepare(workflow)
  }
  let memory = memory.new()
  let backend = memory.storage(memory)
  let first = make(False)
  let assert Ok(reference) =
    durable.start_or_reconnect(backend, "decision", first, "x")
  let pending = drive_later(watched(backend, owner), reference, first)
  let assert Ok(pid) = process.receive(owner, 1000)
  let assert Ok(Nil) = process.receive(entered, 1000)
  let assert Ok(Nil) = process.receive(effects, 1000)
  kill_and_wait(pid)
  let assert Ok(Error(durable.RunnerLost)) = process.receive(pending, 1000)
  case cancel {
    True -> durable.cancel(backend, reference, first) |> should.equal(Ok(Nil))
    False -> Nil
  }
  let assert Ok(outcome) =
    durable.drive(backend, reference, make(True), execution.config())
  durable.read(backend, reference, make(True))
  |> should.equal(Ok(durable.Finished(outcome)))
  process.receive(entered, 0) |> should.equal(Error(Nil))
  case outcome {
    execution.Completed("x retried") ->
      process.receive(effects, 0) |> should.equal(Ok(Nil))
    _ -> process.receive(effects, 0) |> should.equal(Error(Nil))
  }
  memory.close(memory)
  outcome
}

pub fn compensation_recovery_retry_test() {
  resolve_interrupted(saga.Retry, 2, False)
  |> should.equal(execution.Completed("x retried"))
  resolve_interrupted(saga.RetryAfter(5), 2, False)
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
      |> saga.compensate(1, fn(_, _, _) {
        process.send(entered, "compensation")
        process.receive_forever(process.new_subject())
      })
      |> saga.restore_undo(fn(input, output, _) {
        saga.UndoWith(fn() {
          process.send(undos, input <> output)
          Ok(Nil)
        })
      })
      |> saga.reconcile_compensation(fn(input, _, _) {
        saga.CompensationResolved(saga.Continue(
          input <> "!",
          saga.UndoWith(fn() {
            panic as "original closure must not survive restart"
          }),
        ))
      })
      |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
        saga.EffectUnknown
      })
    let second =
      saga.step("second", fn(_) {
        process.send(entered, "next effect")
        process.receive_forever(process.new_subject())
      })
      |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
        case stage {
          2 -> saga.EffectFailed("stop")
          _ -> saga.EffectUnknown
        }
      })
    let assert Ok(workflow) =
      saga.define("restore-continue", fn(input) {
        input |> saga.perform(first) |> saga.perform(second)
      })
    prepare(workflow)
  }
  let memory = memory.new()
  let backend = memory.storage(memory)
  let assert Ok(reference) =
    durable.start_or_reconnect(backend, "continue", make(0), "x")
  let pending = drive_later(watched(backend, owner), reference, make(0))
  let assert Ok(pid) = process.receive(owner, 1000)
  process.receive(entered, 1000) |> should.equal(Ok("compensation"))
  kill_and_wait(pid)
  process.receive(pending, 1000) |> should.equal(Ok(Error(durable.RunnerLost)))
  let pending = drive_later(watched(backend, owner), reference, make(1))
  let assert Ok(pid) = process.receive(owner, 1000)
  process.receive(entered, 1000) |> should.equal(Ok("next effect"))
  kill_and_wait(pid)
  process.receive(pending, 1000) |> should.equal(Ok(Error(durable.RunnerLost)))
  let assert Ok(execution.Failed(execution.StepFailed(_, "stop"), settlement)) =
    durable.drive(backend, reference, make(2), execution.config())
  gleam_list.length(settlement.undone) |> should.equal(1)
  process.receive(undos, 1000) |> should.equal(Ok("xx!"))
  process.receive(entered, 0) |> should.equal(Error(Nil))
  memory.close(memory)
}

pub fn checkpoint_error_categories_remain_distinct_test() {
  let persistence = prepare(echo_workflow())
  let memory = memory.new()
  let backend = memory.storage(memory)
  let assert Ok(reference) =
    durable.start_or_reconnect(backend, "errors", persistence, "x")
  gleam_list.each(
    [storage.Conflict, storage.StaleOwner, storage.Io("disk unavailable")],
    fn(error) {
      let refused =
        storage.Storage(..backend, commit: fn(_, _, _, _) { Error(error) })
      durable.drive(refused, reference, persistence, execution.config())
      |> should.equal(Error(durable.StorageError(error)))
    },
  )
  let assert Ok(owner) = backend.claim()
  let assert Ok(_) =
    backend.commit(owner.generation, owner.revision, owner.cancelled, <<
      "not a checkpoint":utf8,
    >>)
  let assert Ok(Nil) = backend.release(owner.generation)
  let assert Error(durable.InvalidCheckpoint(_)) =
    durable.read(backend, reference, persistence)
  let assert Error(durable.InvalidCheckpoint(_)) =
    durable.drive(backend, reference, persistence, execution.config())
  memory.close(memory)
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
    |> saga.recoverable("1", broken, codec.text(), fn(_, _) {
      saga.EffectUnknown
    })
  let assert Ok(workflow) =
    saga.define("recording", fn(input) { saga.perform(input, step) })
  let persistence = prepare(workflow)
  let memory = memory.new()
  let backend = memory.storage(memory)
  let refusing =
    storage.Storage(
      ..backend,
      commit: fn(generation, revision, cancelled, bytes) {
        let reply = process.new_subject()
        process.send(gate, CheckWrite(reply))
        case process.receive_forever(reply) {
          True -> backend.commit(generation, revision, cancelled, bytes)
          False -> Error(storage.Io("disk unavailable"))
        }
      },
    )
  let assert Ok(reference) =
    durable.start_or_reconnect(refusing, "recording", persistence, "x")
  durable.drive(refusing, reference, persistence, execution.config())
  |> should.equal(
    Error(durable.SuspensionNotSaved(
      durable.CodecFailure("cannot encode input"),
      durable.StorageError(storage.Io("disk unavailable")),
    )),
  )
  durable.read(backend, reference, persistence)
  |> should.equal(Ok(durable.Pending))
  process.unlink(gate_pid)
  kill_and_wait(gate_pid)
  memory.close(memory)
}

pub fn false_undo_declaration_blocks_continue_commit_test() {
  let step =
    saga.step("continue", fn(_) { Error("fail") })
    |> saga.compensate(1, fn(input, _, _) {
      saga.Continue(input, saga.UndoWith(fn() { Ok(Nil) }))
    })
    |> saga.restore_undo(fn(_, _, _) { saga.NoUndo })
    |> saga.recoverable("1", codec.text(), codec.text(), fn(_, _) {
      saga.EffectUnknown
    })
  let assert Ok(workflow) =
    saga.define("false-declaration", fn(input) { saga.perform(input, step) })
  let persistence = prepare(workflow)
  let memory = memory.new()
  let backend = memory.storage(memory)
  let assert Ok(reference) =
    durable.start_or_reconnect(backend, "false-declaration", persistence, "x")
  let assert Error(durable.CodecFailure(reason)) =
    durable.drive(backend, reference, persistence, execution.config())
  gleam_string.contains(reason, "undo reconstruction") |> should.equal(True)
  durable.read(backend, reference, persistence)
  |> should.equal(Ok(durable.Suspended(durable.CodecFailure(reason))))
  memory.close(memory)
}

pub fn compensation_recovery_cancellation_settles_continue_test() {
  let assert execution.Cancelled(_, settlement) =
    resolve_interrupted(saga.Continue("replacement", saga.NoUndo), 2, True)
  gleam_list.length(settlement.not_undoable) |> should.equal(1)
}
