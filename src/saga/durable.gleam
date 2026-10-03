//// Runs a `saga.Workflow` with saved checkpoints, so that a run survives
//// the loss of its runner and can be recovered, read or cancelled later.
////
//// Use this module when a run must outlive the process or VM that started
//// it. It runs the same workflow and the same runner as `saga/execution`
//// and returns the same `execution.Outcome`; local runs need none of it.
//// `prepare` attaches `saga/codec` codecs and a compatibility stamp to an
//// existing workflow. A `saga/storage.Storage` value saves one execution's
//// checkpoint: `saga/storage/memory` and `saga/storage/file` are the
//// included adapters, and `saga/storage/conformance` checks a third-party
//// one. Waking or scheduling a runner after a restart is left to the
//// caller. A step that may leave an unknown effect is made recoverable with
//// `saga.recoverable`; recovery that cannot decide its effect suspends the
//// run with `RecoveryRequired(saga/reconciliation.Required)`.
////
//// ```gleam
//// import saga/codec
//// import saga/durable
//// import saga/execution
//// import saga/storage/memory
////
//// let text = codec.text()
//// let assert Ok(persistence) =
////   durable.prepare(workflow, "1", text, text, text, text)
//// let memory = memory.new()
//// let storage = memory.storage(memory)
//// let assert Ok(reference) =
////   durable.start_or_reconnect(storage, "checkout-123", persistence, "order-123")
//// let outcome = durable.drive(storage, reference, persistence, execution.config())
//// let status = durable.read(storage, reference, persistence)
//// memory.close(memory)
//// ```
////
//// `drive` waits until the run ends or suspends, with no timeout. The
//// caller's exit does not cancel the run; only `cancel` does. See
//// DURABILITY.md for the storage contract and recovery rules.

import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import saga.{type Workflow}
import saga/codec.{type Codec}
import saga/execution
import saga/internal/checkpoint
import saga/internal/coordinator
import saga/internal/ffi
import saga/internal/node
import saga/reconciliation
import saga/storage.{type Storage}

/// Why a durable operation failed or a run suspended.
pub type Error {
  StorageError(storage.Error)
  InvalidDefinition(String)
  IncompatibleDefinition
  ReferenceMismatch
  InputMismatch
  CodecFailure(String)
  RecoveryRequired(reconciliation.Required)
  InvalidCheckpoint(String)
  SuspensionNotSaved(cause: Error, recording: Error)
  InvalidConfig(List(execution.ConfigError))
  RunnerLost
}

/// What a recovery resolver established about an action that was
/// interrupted before saga saved its result:
///
/// - `Completed(value)`: it happened, with this result;
/// - `Failed(error)`: it happened and failed with this error;
/// - `NotSent`: it provably never happened, which authorizes saga to run it
///   again under the same key;
/// - `MaybeSent`: its effect is unknown, so the execution suspends with
///   `RecoveryRequired` until a later `drive` can establish it.
///
/// The union is closed.
pub type Evidence(o, e) {
  Completed(o)
  Failed(e)
  NotSent
  MaybeSent
}

fn evidence_to_node(evidence: Evidence(o, e)) -> node.Evidence(o, e) {
  case evidence {
    Completed(value) -> node.EvidenceCompleted(value)
    Failed(error) -> node.EvidenceFailed(error)
    NotSent -> node.EvidenceNotSent
    MaybeSent -> node.EvidenceMaybeSent
  }
}

/// Makes a step recoverable after a restart: `version` names the step's
/// behavior (change it when the step's callbacks change meaning),
/// `input` and `output` save its values, and `resolve` establishes the
/// effect of an attempt that was admitted but whose result was not saved.
/// `resolve` receives the attempt's input and `EffectKey`, must be safe to
/// call repeatedly, and is called instead of repeating the attempt. Every
/// step of a durable workflow needs this capability; `new` names each step
/// that lacks it. The order relative to `saga.compensate`,
/// `saga.unknown_when` and `saga.map_step_errors` does not matter, except
/// that a `Failed` answer from a `recoverable` added after
/// `map_step_errors` needs a `compensate` decider written in the mapped
/// vocabulary (see `saga.map_step_errors`).
pub fn recoverable(
  step: saga.Step(i, o, e, u),
  version version: String,
  input input: Codec(i),
  output output: Codec(o),
  resolve resolve: fn(i, saga.EffectKey) -> Evidence(o, e),
) -> saga.Step(i, o, e, u) {
  saga.set_recoverable(step, version, input, output, fn(value, key) {
    evidence_to_node(resolve(value, key))
  })
}

/// Declares how to rebuild a compensating step's undo from its saved input
/// and output, after a restart or after a `Continue` decision. Every
/// persistent step with `saga.compensate` needs it, even when the answer is
/// `saga.NoUndo`. The factory must be pure: saga may call it while saving a
/// checkpoint to check that the undo can be rebuilt.
pub fn restore_undo(
  step: saga.Step(i, o, e, u),
  restore: fn(saga.UndoRequest(i, o)) -> saga.Undo(u),
) -> saga.Step(i, o, e, u) {
  saga.set_restore_undo(step, restore)
}

/// Establishes the effect of an undo that was interrupted: `Completed(Nil)`
/// when the undo happened, `Failed(error)` when it failed, `NotSent` to run
/// the saved undo again, and `MaybeSent` to suspend. Without a resolver an
/// interrupted undo suspends the execution.
pub fn resolve_undo(
  step: saga.Step(i, o, e, u),
  resolve: fn(saga.UndoRequest(i, o)) -> Evidence(Nil, u),
) -> saga.Step(i, o, e, u) {
  saga.set_resolve_undo(step, fn(request) { evidence_to_node(resolve(request)) })
}

/// Establishes the decision of a `saga.compensate` decider that was
/// interrupted, from the failed attempt's input and `EffectKey`:
/// `Some(decision)` applies that decision under the original attempt
/// budget, and `None` keeps the execution suspended. Saga never repeats
/// the decider itself after a restart, so a decider with an external
/// effect records its decision under `key.attempt_key`.
pub fn resolve_compensation(
  step: saga.Step(i, o, e, u),
  resolve: fn(i, saga.EffectKey) -> Option(saga.Recovery(o, e, u)),
) -> saga.Step(i, o, e, u) {
  saga.set_resolve_compensation(step, resolve)
}

/// A checked persistence capability for an existing workflow. It adds codecs
/// and compatibility information; it does not construct another graph.
pub opaque type Persistence(i, o, e, u) {
  Persistence(
    workflow: Workflow(i, o, e, u),
    stamp: String,
    input: Codec(i),
    output: Codec(o),
    error: Codec(e),
    undo_error: Codec(u),
  )
}

/// Identifies one saved execution by the id given to `start_or_reconnect`.
pub opaque type Reference {
  Reference(id: String)
}

/// Returns the execution id of a reference.
pub fn reference_id(reference: Reference) -> String {
  reference.id
}

/// A saved execution's state: not finished, suspended with a saved reason,
/// or finished with its outcome.
pub type Status(o, e, u) {
  Pending
  Suspended(reason: Error)
  Finished(outcome: execution.Outcome(o, e, u))
}

type Envelope(o, e, u) {
  Envelope(
    format: Int,
    reference: String,
    stamp: String,
    input: String,
    snapshot: Option(coordinator.Snapshot(e, u)),
    outcome: Option(coordinator.Outcome(o, e, u)),
    issue: Option(checkpoint.Failure),
  )
}

/// Checks that every step of `workflow` can be restored and attaches the
/// root codecs and a compatibility stamp built from `version`, the graph
/// and every codec version. Does not build a second graph.
pub fn prepare(
  workflow: Workflow(i, o, e, u),
  version: String,
  input: Codec(i),
  output: Codec(o),
  error: Codec(e),
  undo_error: Codec(u),
) -> Result(Persistence(i, o, e, u), Error) {
  use stamp <- result.try(
    saga.persistence_stamp(workflow, version)
    |> result.map_error(InvalidDefinition),
  )
  let versions = [
    codec.version(input),
    codec.version(output),
    codec.version(error),
    codec.version(undo_error),
  ]
  use _ <- result.try(case list.contains(versions, "") {
    True -> Error(InvalidDefinition("empty codec version"))
    False -> Ok(Nil)
  })
  Ok(Persistence(
    workflow,
    frame([stamp, ..versions]),
    input,
    output,
    error,
    undo_error,
  ))
}

/// Saves a new execution with `input` under `id`, or reconnects to the one
/// already saved there. Reconnecting succeeds only for the same id,
/// compatible definition and encoded input; a different input returns
/// `InputMismatch`. Does not run anything; call `drive`.
pub fn start_or_reconnect(
  storage: Storage,
  id: String,
  persistence: Persistence(i, o, e, u),
  input: i,
) -> Result(Reference, Error) {
  use encoded <- result.try(
    codec.encode(persistence.input, input) |> result.map_error(CodecFailure),
  )
  let envelope = Envelope(1, id, persistence.stamp, encoded, None, None, None)
  use bytes <- result.try(encode(envelope, persistence))
  case storage.create(bytes) {
    Ok(_) -> Ok(Reference(id))
    Error(storage.AlreadyExists) -> {
      use existing <- result.try(load(storage, Reference(id), persistence))
      case existing.input == encoded {
        True -> Ok(Reference(id))
        False -> Error(InputMismatch)
      }
    }
    Error(error) -> Error(StorageError(error))
  }
}

/// Reads the saved state of an execution without a live runner.
pub fn read(
  storage: Storage,
  reference: Reference,
  persistence: Persistence(i, o, e, u),
) -> Result(Status(o, e, u), Error) {
  use envelope <- result.try(load(storage, reference, persistence))
  Ok(case envelope.outcome, envelope.issue {
    Some(outcome), _ -> Finished(execution.from_coordinator(outcome))
    None, Some(reason) -> Suspended(from_checkpoint(reason))
    None, None -> Pending
  })
}

/// Records cancellation for an unfinished execution. A running `drive`
/// observes it at its next checkpoint and rolls back. Does nothing for a
/// finished execution.
pub fn cancel(
  storage: Storage,
  reference: Reference,
  persistence: Persistence(i, o, e, u),
) -> Result(Nil, Error) {
  use envelope <- result.try(load(storage, reference, persistence))
  case envelope.outcome {
    Some(_) -> Ok(Nil)
    None -> storage.cancel() |> result.map_error(StorageError)
  }
}

/// Blocks until the runner finishes or needs reconciliation. Process loss of
/// the caller is detachment, never cancellation. Multiple callers contend
/// through the adapter's execution ownership contract.
pub fn drive(
  storage: Storage,
  reference: Reference,
  persistence: Persistence(i, o, e, u),
  config: execution.Config,
) -> Result(execution.Outcome(o, e, u), Error) {
  use settings <- result.try(
    execution.settings(config, Some(reference.id))
    |> result.map_error(InvalidConfig),
  )
  let reply = process.new_subject()
  let pid =
    process.spawn_unlinked(fn() {
      case
        ffi.rescue(fn() {
          drive_owned(storage, reference, persistence, settings)
        })
      {
        ffi.Rescued(result) -> process.send(reply, result)
        ffi.Raised(_, _) -> process.send(reply, Error(RunnerLost))
      }
    })
  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select(reply)
    |> process.select_specific_monitor(monitor, fn(_) { Error(RunnerLost) })
  let result = process.selector_receive_forever(selector)
  process.demonitor_process(monitor)
  result
}

fn drive_owned(
  storage: Storage,
  reference: Reference,
  persistence: Persistence(i, o, e, u),
  config: coordinator.Settings,
) -> Result(execution.Outcome(o, e, u), Error) {
  use record <- result.try(storage.claim() |> result.map_error(StorageError))
  let result = drive_claimed(storage, record, reference, persistence, config)
  let released = storage.release(record.generation)
  case result, released {
    Ok(_), Error(error) -> Error(StorageError(error))
    _, _ -> result
  }
}

fn drive_claimed(
  storage: Storage,
  record: storage.Record,
  reference: Reference,
  persistence: Persistence(i, o, e, u),
  config: coordinator.Settings,
) -> Result(execution.Outcome(o, e, u), Error) {
  use _ <- result.try(check_header(record.data, reference, persistence))
  use envelope <- result.try(decode(record.data, persistence))
  use _ <- result.try(check(envelope, reference, persistence))
  case envelope.outcome {
    Some(outcome) -> Ok(execution.from_coordinator(outcome))
    None -> {
      use input <- result.try(
        codec.decode(persistence.input, envelope.input)
        |> result.map_error(CodecFailure),
      )
      let result_out = process.new_subject()
      let session =
        coordinator.Session(
          execution_id: reference.id,
          snapshot: envelope.snapshot,
          cancelled: record.cancelled,
          save: fn(snapshot) {
            save(
              storage,
              record.generation,
              record.cancelled,
              Envelope(..envelope, snapshot: Some(snapshot), issue: None),
              persistence,
            )
            |> result.map_error(to_checkpoint)
          },
          failed: fn(reason) {
            // Preserve the last committed checkpoint. Uncommitted admissions
            // were never dispatched and must not become recovery facts.
            let recorded = {
              use current <- result.try(load(storage, reference, persistence))
              save(
                storage,
                record.generation,
                record.cancelled,
                Envelope(..current, issue: Some(reason)),
                persistence,
              )
            }
            let failure = case reason, recorded {
              _, Ok(Nil) -> from_checkpoint(reason)
              _, Error(recording) ->
                case from_checkpoint(reason) == recording {
                  True -> recording
                  False ->
                    SuspensionNotSaved(from_checkpoint(reason), recording)
                }
            }
            process.send(result_out, Error(failure))
          },
        )
      coordinator.execute_saved(
        saga.name(persistence.workflow),
        config,
        fn() { saga.for_run(persistence.workflow, input) },
        session,
        fn(outcome) {
          let saved = {
            use current <- result.try(load(storage, reference, persistence))
            save(
              storage,
              record.generation,
              record.cancelled,
              Envelope(..current, outcome: Some(outcome), issue: None),
              persistence,
            )
          }
          process.send(
            result_out,
            result.map(saved, fn(_) { execution.from_coordinator(outcome) }),
          )
        },
      )
      let outcome = process.receive_forever(result_out)
      // A cancellation racing a checkpoint wins before dispatch. Restart
      // from the saved state with the new cancellation observation.
      case storage.load() {
        Ok(current) if current.cancelled != record.cancelled ->
          drive_claimed(storage, current, reference, persistence, config)
        _ -> outcome
      }
    }
  }
}

fn save(
  storage: Storage,
  token: Int,
  cancelled: Bool,
  envelope: Envelope(o, e, u),
  persistence: Persistence(i, o, e, u),
) -> Result(Nil, Error) {
  use bytes <- result.try(encode(envelope, persistence))
  use record <- result.try(storage.load() |> result.map_error(StorageError))
  storage.commit(token, record.revision, cancelled, bytes)
  |> result.map(fn(_) { Nil })
  |> result.map_error(StorageError)
}

fn load(
  storage: Storage,
  reference: Reference,
  persistence: Persistence(i, o, e, u),
) -> Result(Envelope(o, e, u), Error) {
  use record <- result.try(storage.load() |> result.map_error(StorageError))
  use _ <- result.try(check_header(record.data, reference, persistence))
  use envelope <- result.try(decode(record.data, persistence))
  use _ <- result.try(check(envelope, reference, persistence))
  Ok(envelope)
}

fn check(
  envelope: Envelope(o, e, u),
  reference: Reference,
  persistence: Persistence(i, o, e, u),
) -> Result(Nil, Error) {
  case envelope.reference == reference.id, envelope.stamp == persistence.stamp {
    False, _ -> Error(ReferenceMismatch)
    _, False -> Error(IncompatibleDefinition)
    True, True -> Ok(Nil)
  }
}

fn encode(
  envelope: Envelope(o, e, u),
  p: Persistence(i, o, e, u),
) -> Result(BitArray, Error) {
  encode_envelope(
    envelope,
    fn(value) { codec.encode(p.output, value) },
    fn(value) { codec.encode(p.error, value) },
    fn(value) { codec.encode(p.undo_error, value) },
  )
  |> result.map_error(from_checkpoint)
}

fn decode(
  bytes: BitArray,
  p: Persistence(i, o, e, u),
) -> Result(Envelope(o, e, u), Error) {
  decode_envelope(
    bytes,
    fn(value) { codec.decode(p.output, value) },
    fn(value) { codec.decode(p.error, value) },
    fn(value) { codec.decode(p.undo_error, value) },
  )
  |> result.map_error(from_checkpoint)
}

@external(erlang, "saga_checkpoint", "encode")
fn encode_envelope(
  envelope: Envelope(o, e, u),
  output: fn(o) -> Result(String, String),
  error: fn(e) -> Result(String, String),
  undo: fn(u) -> Result(String, String),
) -> Result(BitArray, checkpoint.Failure)

@external(erlang, "saga_checkpoint", "decode")
fn decode_envelope(
  bytes: BitArray,
  output: fn(String) -> Result(o, String),
  error: fn(String) -> Result(e, String),
  undo: fn(String) -> Result(u, String),
) -> Result(Envelope(o, e, u), checkpoint.Failure)

fn from_checkpoint(failure: checkpoint.Failure) -> Error {
  case failure {
    checkpoint.StorageFailure(error) -> StorageError(error)
    checkpoint.CodecFailure(reason) -> CodecFailure(reason)
    checkpoint.InvalidState(reason) -> InvalidCheckpoint(reason)
    checkpoint.Uncertain(required) -> RecoveryRequired(required)
  }
}

fn to_checkpoint(error: Error) -> checkpoint.Failure {
  case error {
    StorageError(error) -> checkpoint.StorageFailure(error)
    CodecFailure(reason) -> checkpoint.CodecFailure(reason)
    InvalidCheckpoint(reason) -> checkpoint.InvalidState(reason)
    RecoveryRequired(required) -> checkpoint.Uncertain(required)
    InvalidDefinition(reason) -> checkpoint.InvalidState(reason)
    IncompatibleDefinition -> checkpoint.InvalidState("incompatible definition")
    ReferenceMismatch -> checkpoint.InvalidState("reference mismatch")
    InputMismatch -> checkpoint.InvalidState("input mismatch")
    InvalidConfig(_) -> checkpoint.InvalidState("invalid runner configuration")
    RunnerLost -> checkpoint.InvalidState("runner lost")
    SuspensionNotSaved(_, _) ->
      checkpoint.InvalidState("suspension recording failed")
  }
}

import gleam/int
import gleam/string

fn frame(parts: List(String)) -> String {
  list.map(parts, fn(part) {
    int.to_string(string.byte_size(part)) <> ":" <> part
  })
  |> string.concat
}

fn check_header(
  bytes: BitArray,
  reference: Reference,
  persistence: Persistence(i, o, e, u),
) -> Result(Nil, Error) {
  use header <- result.try(header(bytes) |> result.map_error(InvalidCheckpoint))
  case header.0 == reference.id, header.1 == persistence.stamp {
    False, _ -> Error(ReferenceMismatch)
    _, False -> Error(IncompatibleDefinition)
    True, True -> Ok(Nil)
  }
}

@external(erlang, "saga_checkpoint", "header")
fn header(bytes: BitArray) -> Result(#(String, String), String)
