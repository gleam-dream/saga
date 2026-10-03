//// Runs a `saga.Workflow` with saved checkpoints, so that a run survives
//// the loss of its runner, its caller or its VM, and can be recovered, read
//// or cancelled later.
////
//// Use this module when a run must outlive the process or VM that started
//// it. It runs the same workflow and the same runner as `saga/execution`
//// and returns the same `execution.Outcome`; local runs need none of it.
////
//// 1. Make every step recoverable: `recoverable` gives a step its codecs
////    and the resolver that establishes an interrupted attempt's effect;
////    `restore_undo`, `resolve_undo` and `resolve_compensation` cover undo
////    and compensation.
//// 2. `new` checks the workflow once and attaches root codecs and a
////    compatibility stamp; `with_version` changes the workflow version
////    (default `"1"`) and `with_config` sets the run configuration.
//// 3. `start_or_reconnect` saves an execution under a caller-chosen id, or
////    reconnects to the one saved there, and returns its `Run` handle.
//// 4. `drive` runs it until it finishes or suspends, within a timeout;
////    `read` and `cancel` act on it without a runner.
////
//// One `saga/storage.Storage` serves every execution of a store:
//// `saga/storage/memory`, `saga/storage/file` and the `saga_postgres`
//// package are adapters, and `saga/storage/conformance` checks another.
////
//// ```gleam
//// import gleam/dynamic/decode
//// import gleam/json
//// import saga
//// import saga/codec
//// import saga/durable
//// import saga/storage/memory
////
//// let order = codec.json("order-1", fn(id) { Ok(json.string(id)) }, decode.string)
//// let text = codec.text()
//// let charge =
////   saga.effect("charge", fn(order, key) { charge(order, key.idempotency) })
////   |> saga.undo(fn(undo) { refund(undo.output, undo.key.idempotency) })
////   |> durable.recoverable(version: "1", input: order, output: text, resolve: lookup_charge)
////   |> durable.resolve_undo(lookup_refund)
//// let workflow = saga.define("checkout", saga.perform(_, charge))
//// let persistence =
////   durable.new(workflow, input: order, output: text, error: text, undo_error: text)
//// let assert Ok(store) = memory.start()
//// let assert Ok(run) =
////   durable.start_or_reconnect(persistence, memory.storage(store), id: "checkout-123", input: "order-123")
//// let outcome = durable.drive(run, timeout: duration.seconds(30))
//// ```
////
//// **Who stops what.** `drive` runs the execution in a runner process and
//// waits at most `timeout`. On timeout, when the process that
//// called `drive` exits, and when the runner is killed or crashes, the run
//// stops: in-flight attempts are killed, the claim is released, and the
//// last checkpoint stays the recovery authority, so the next `drive`
//// resumes. Only the loss of the runner's VM leaves the claim to the
//// storage's own expiry, such as a lease. Only `cancel` cancels.
//// Waking a runner after a restart is the caller's job: `unfinished` lists
//// the executions that wait for one.
////
//// See DURABILITY.md for the storage contract and recovery rules.

import gleam/bit_array
import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import saga.{type Workflow}
import saga/codec.{type Codec}
import saga/execution
import saga/internal/checkpoint
import saga/internal/coordinator
import saga/internal/ffi
import saga/internal/node
import saga/storage.{type Storage}
import sinal/correlation.{type Correlation}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why a durable operation failed or an execution suspended. Later releases
/// may add variants: branch on `error_kind` and log `describe_error`.
pub type Error {
  /// The storage refused or failed an operation.
  StorageFailure(error: storage.Error)
  /// The saved execution belongs to another workflow, version or codec set.
  IncompatibleDefinition
  /// `start_or_reconnect` found the id saved with a different input.
  InputMismatch
  /// A codec could not save or restore a value at `boundary`.
  CodecFailure(boundary: Boundary, error: codec.CodecError)
  /// A resolver could not establish an interrupted action's effect: find
  /// out what happened to `required.key`, then `drive` again.
  RecoveryRequired(required: Required)
  /// The saved checkpoint cannot be restored by this definition.
  InvalidCheckpoint(problem: CheckpointProblem)
  /// A checkpoint grew past the limit (`with_max_checkpoint_bytes`).
  CheckpointTooLarge(bytes: Int, limit: Int)
  /// The execution suspended with `cause`, and saving that suspension
  /// failed with `recording`; the previous checkpoint stays.
  SuspensionNotSaved(cause: Error, recording: Error)
  /// The run configuration is invalid.
  InvalidConfig(errors: List(execution.ConfigError))
  /// `drive` was given a timeout below 1 millisecond.
  InvalidTimeout(timeout: Duration)
  /// The runner process was killed or crashed before reporting, or the
  /// caller's exit stopped it. Its claim was released; `drive` again.
  RunnerLost
  /// `drive` reached its timeout; the runner stopped and the execution
  /// resumes from its last checkpoint at the next `drive`.
  DriveTimedOut
}

/// The closed classification of an `Error`, for deciding what to do:
///
/// - `Busy`: another runner owns the execution; try again later.
/// - `Transient`: the runner or the store failed; `drive` again.
/// - `NeedsReconciliation`: an effect is unknown; resolve it first.
/// - `Incompatible`: the saved execution belongs to another definition or
///   input.
/// - `Defect`: a bug in the definition, configuration, codecs or adapter.
pub type ErrorKind {
  Busy
  Transient
  NeedsReconciliation
  Incompatible
  Defect
}

/// Which saved value a codec failed on. The union is closed.
pub type Boundary {
  RunInput
  RunOutput
  RunError
  RunUndoError
  StepInput(step: saga.StepAddress)
  StepOutput(step: saga.StepAddress)
}

/// Why a saved checkpoint cannot be restored. Later releases may add
/// variants.
pub type CheckpointProblem {
  /// The bytes are not a checkpoint this version of saga wrote.
  Malformed
  /// The storage returned the checkpoint of execution `saved`.
  ForeignExecution(saved: String)
  /// The saved graph has a different number of steps.
  GraphMismatch
  /// More attempts were in flight when it was saved than the configured
  /// `max_concurrency` allows now.
  ConcurrencyBelowInFlight(in_flight: Int, max_concurrency: Int)
  /// A saved undo cannot be rebuilt: `restore_undo` returned `NoUndo`.
  UndoNotRestorable(step: saga.StepAddress)
  /// A compensation decision was saved before its input was.
  CompensationInputMissing(step: saga.StepAddress)
  /// A resolver added after `saga.map_step_errors` answered `Failed`, and
  /// the step has no `compensate` decider in the mapped vocabulary.
  DeciderMissingAfterMapping(step: saga.StepAddress)
}

/// The action whose effect must be established before the execution can
/// continue: its step, which action (`StepAttempt(n)`,
/// `StepCompensation(n)` or `StepUndo`), and its `EffectKey`.
pub type Required {
  Required(
    step: saga.StepAddress,
    action: execution.Action,
    key: saga.EffectKey,
  )
}

/// Classifies an error; see `ErrorKind`.
pub fn error_kind(error: Error) -> ErrorKind {
  case error {
    StorageFailure(storage.Busy) | StorageFailure(storage.StaleOwner) -> Busy
    StorageFailure(storage.Conflict)
    | StorageFailure(storage.CancellationChanged)
    | StorageFailure(storage.Unavailable(_))
    | StorageFailure(storage.TimedOut)
    | RunnerLost
    | DriveTimedOut -> Transient
    StorageFailure(storage.NotFound)
    | StorageFailure(storage.AlreadyExists)
    | StorageFailure(storage.Corrupt) -> Defect
    RecoveryRequired(_) -> NeedsReconciliation
    IncompatibleDefinition | InputMismatch -> Incompatible
    CodecFailure(..)
    | InvalidCheckpoint(_)
    | CheckpointTooLarge(..)
    | InvalidConfig(_)
    | InvalidTimeout(_) -> Defect
    SuspensionNotSaved(cause, _) -> error_kind(cause)
  }
}

/// Describes an error for logs.
pub fn describe_error(error: Error) -> String {
  case error {
    StorageFailure(error) -> "storage: " <> storage.describe_error(error)
    IncompatibleDefinition ->
      "the execution was saved by another workflow definition or version"
    InputMismatch -> "the execution was saved with a different input"
    CodecFailure(boundary, error) ->
      "the codec of "
      <> describe_boundary(boundary)
      <> " failed: "
      <> codec.describe_error(error)
    RecoveryRequired(Required(step, action, key)) ->
      "the effect of "
      <> describe_action(action)
      <> " of step "
      <> saga.address_to_string(step)
      <> " is unknown; establish it for key "
      <> key.attempt_key
    InvalidCheckpoint(problem) ->
      "the checkpoint cannot be restored: " <> describe_checkpoint(problem)
    CheckpointTooLarge(bytes, limit) ->
      "the checkpoint has "
      <> int.to_string(bytes)
      <> " bytes, more than the limit of "
      <> int.to_string(limit)
      <> " (with_max_checkpoint_bytes)"
    SuspensionNotSaved(cause, recording) ->
      describe_error(cause)
      <> "; saving the suspension failed: "
      <> describe_error(recording)
    InvalidConfig(errors) ->
      "invalid configuration: "
      <> string.join(list.map(errors, execution.describe_config_error), "; ")
    InvalidTimeout(timeout) ->
      "the drive timeout must be at least 1 ms, got "
      <> int.to_string(duration.to_milliseconds(timeout))
      <> " ms"
    RunnerLost -> "the runner process died"
    DriveTimedOut ->
      "drive reached its timeout; the execution resumes at the next drive"
  }
}

fn describe_problem(problem: checkpoint.DefinitionProblem) -> String {
  case problem {
    checkpoint.EmptyWorkflowVersion -> "the workflow version is empty"
    checkpoint.EmptyStepCodecVersion(boundary) ->
      "the codec of "
      <> describe_boundary(boundary_from(boundary))
      <> " has an empty version"
    checkpoint.MissingRecoverable(step) ->
      "step "
      <> saga.address_to_string(address_from(step))
      <> " has no durable.recoverable"
    checkpoint.EmptyStepVersion(step) ->
      "step "
      <> saga.address_to_string(address_from(step))
      <> " has an empty version"
    checkpoint.MissingRestoreUndo(step) ->
      "step "
      <> saga.address_to_string(address_from(step))
      <> " compensates but has no durable.restore_undo"
  }
}

fn describe_boundary(boundary: Boundary) -> String {
  case boundary {
    RunInput -> "the workflow input"
    RunOutput -> "the workflow output"
    RunError -> "the workflow error"
    RunUndoError -> "the workflow undo error"
    StepInput(step) -> "the input of step " <> saga.address_to_string(step)
    StepOutput(step) -> "the output of step " <> saga.address_to_string(step)
  }
}

fn describe_checkpoint(problem: CheckpointProblem) -> String {
  case problem {
    Malformed -> "it is malformed"
    ForeignExecution(saved) -> "it belongs to execution " <> saved
    GraphMismatch -> "it has a different number of steps"
    ConcurrencyBelowInFlight(in_flight, max_concurrency) ->
      int.to_string(in_flight)
      <> " actions were in flight, more than max_concurrency "
      <> int.to_string(max_concurrency)
    UndoNotRestorable(step) ->
      "the undo of step "
      <> saga.address_to_string(step)
      <> " cannot be rebuilt"
    CompensationInputMissing(step) ->
      "the compensation of step "
      <> saga.address_to_string(step)
      <> " has no saved input"
    DeciderMissingAfterMapping(step) ->
      "step "
      <> saga.address_to_string(step)
      <> " needs a compensate decider after map_step_errors"
  }
}

fn describe_action(action: execution.Action) -> String {
  case action {
    execution.StepAttempt(n) -> "attempt " <> int.to_string(n)
    execution.StepCompensation(n) ->
      "the compensation decision about attempt " <> int.to_string(n)
    execution.StepUndo -> "the undo"
  }
}

// ---------------------------------------------------------------------------
// Persistence modifiers
// ---------------------------------------------------------------------------

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
/// behavior (change it when the step's callbacks change meaning), `input`
/// and `output` save its values, and `resolve` establishes the effect of an
/// attempt that was admitted but whose result was not saved. `resolve`
/// receives the attempt's input and `EffectKey`, must be safe to call
/// repeatedly, and is called instead of repeating the attempt. Every step
/// of a durable workflow needs this capability; `new` panics naming each
/// step that lacks it. The order relative to `saga.compensate`, `saga.unknown_when`
/// and `saga.map_step_errors` does not matter, except that a `Failed`
/// answer from a `recoverable` added after `map_step_errors` needs a
/// `compensate` decider written in the mapped vocabulary (see
/// `saga.map_step_errors`).
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
/// checkpoint, to check that the undo can be rebuilt.
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
/// budget, and `None` keeps the execution suspended. Saga never repeats the
/// decider itself after a restart, so a decider with an external effect
/// records its decision under `key.attempt_key`.
pub fn resolve_compensation(
  step: saga.Step(i, o, e, u),
  resolve: fn(i, saga.EffectKey) -> Option(saga.Recovery(o, e, u)),
) -> saga.Step(i, o, e, u) {
  saga.set_resolve_compensation(step, resolve)
}

// ---------------------------------------------------------------------------
// Persistence and runs
// ---------------------------------------------------------------------------

/// A checked persistence capability for an existing workflow: root codecs,
/// a compatibility stamp and the run configuration. It does not construct
/// another graph.
pub opaque type Persistence(i, o, e, u) {
  Persistence(
    workflow: Workflow(i, o, e, u),
    stamp: String,
    input: Codec(i),
    output: Codec(o),
    error: Codec(e),
    undo_error: Codec(u),
    config: execution.Config,
    max_checkpoint_bytes: Int,
  )
}

/// One saved execution: its persistence, its storage and its id.
pub opaque type Run(i, o, e, u) {
  Run(
    persistence: Persistence(i, o, e, u),
    storage: Storage,
    id: String,
    config: execution.Config,
  )
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

/// The default checkpoint size limit: 16 MiB.
const default_max_checkpoint_bytes = 16_777_216

/// The workflow version a new persistence starts with.
const default_version = "1"

/// Checks that every step of `workflow` can be restored and attaches the
/// root codecs and a compatibility stamp built from the workflow version
/// (`"1"` until `with_version` changes it), the graph and every codec
/// version.
///
/// A workflow that cannot be persisted is a bug in the source: a step
/// without `recoverable`, a compensating step without `restore_undo`, or an
/// empty step or codec version. `new` then panics with a message naming
/// every offending step and codec.
pub fn new(
  workflow: Workflow(i, o, e, u),
  input input: Codec(i),
  output output: Codec(o),
  error error: Codec(e),
  undo_error undo_error: Codec(u),
) -> Persistence(i, o, e, u) {
  let roots = [
    #(RunInput, codec.version(input)),
    #(RunOutput, codec.version(output)),
    #(RunError, codec.version(error)),
    #(RunUndoError, codec.version(undo_error)),
  ]
  let root_problems =
    list.filter_map(roots, fn(root) {
      case root.1 {
        "" ->
          Ok(
            "the codec of "
            <> describe_boundary(root.0)
            <> " has an empty version",
          )
        _ -> Error(Nil)
      }
    })
  let problems = case saga.persistence_stamp(workflow, default_version) {
    Ok(_) -> root_problems
    Error(problems) ->
      list.append(list.map(problems, describe_problem), root_problems)
  }
  case problems {
    [] -> Nil
    _ ->
      panic as {
        "saga/durable.new: workflow \""
        <> saga.name(workflow)
        <> "\" cannot be persisted: "
        <> string.join(problems, "; ")
      }
  }
  Persistence(
    workflow:,
    stamp: stamp(workflow, default_version, roots),
    input:,
    output:,
    error:,
    undo_error:,
    config: execution.config(),
    max_checkpoint_bytes: default_max_checkpoint_bytes,
  )
}

/// Sets the workflow version (default `"1"`). Change it whenever the
/// workflow's behavior changes: a saved execution with another version is
/// refused with `IncompatibleDefinition` instead of being misread. Panics
/// on an empty version.
pub fn with_version(
  persistence: Persistence(i, o, e, u),
  version: String,
) -> Persistence(i, o, e, u) {
  case version {
    "" ->
      panic as {
        "saga/durable.with_version: workflow \""
        <> saga.name(persistence.workflow)
        <> "\" needs a non-empty version"
      }
    _ -> Nil
  }
  let roots = [
    #(RunInput, codec.version(persistence.input)),
    #(RunOutput, codec.version(persistence.output)),
    #(RunError, codec.version(persistence.error)),
    #(RunUndoError, codec.version(persistence.undo_error)),
  ]
  Persistence(..persistence, stamp: stamp(persistence.workflow, version, roots))
}

fn stamp(
  workflow: Workflow(i, o, e, u),
  version: String,
  roots: List(#(Boundary, String)),
) -> String {
  let assert Ok(graph) = saga.persistence_stamp(workflow, version)
  frame([graph, ..list.map(roots, fn(root) { root.1 })])
}

/// Sets the configuration every `drive` of this persistence runs with
/// (default `execution.config()`).
pub fn with_config(
  persistence: Persistence(i, o, e, u),
  config: execution.Config,
) -> Persistence(i, o, e, u) {
  Persistence(..persistence, config: config)
}

/// Bounds the size of one saved checkpoint (default 16 MiB). Every commit
/// rewrites the whole checkpoint; a larger one suspends the execution with
/// `CheckpointTooLarge`.
pub fn with_max_checkpoint_bytes(
  persistence: Persistence(i, o, e, u),
  bytes: Int,
) -> Persistence(i, o, e, u) {
  Persistence(..persistence, max_checkpoint_bytes: bytes)
}

/// Saves a new execution with `input` under `id`, or reconnects to the one
/// already saved there, and returns its handle. Reconnecting succeeds only
/// for a compatible definition and the same encoded input; a different
/// input returns `InputMismatch`. Does not run anything; call `drive`.
/// Choose ids that name the workflow (`"checkout:" <> order_id`) when one
/// store serves several workflows.
pub fn start_or_reconnect(
  persistence: Persistence(i, o, e, u),
  storage: Storage,
  id id: String,
  input input: i,
) -> Result(Run(i, o, e, u), Error) {
  use encoded <- result.try(
    codec.encode(persistence.input, input)
    |> result.map_error(CodecFailure(RunInput, _)),
  )
  let run = Run(persistence, storage, id, persistence.config)
  let envelope = Envelope(1, id, persistence.stamp, encoded, None, None, None)
  use bytes <- result.try(
    encode(envelope, persistence) |> result.map_error(from_checkpoint(run, _)),
  )
  case call(storage, fn() { storage.do_create(storage, id, bytes) }) {
    Ok(_) -> Ok(run)
    Error(storage.AlreadyExists) -> {
      use existing <- result.try(load(run))
      case existing.input == encoded {
        True -> Ok(run)
        False -> Error(InputMismatch)
      }
    }
    Error(error) -> Error(StorageFailure(error))
  }
}

/// Reconnects to the execution saved under `id`, for example one that
/// `unfinished` listed, without knowing its input. Fails with
/// `StorageFailure(NotFound)` when there is none.
pub fn reconnect(
  persistence: Persistence(i, o, e, u),
  storage: Storage,
  id id: String,
) -> Result(Run(i, o, e, u), Error) {
  let run = Run(persistence, storage, id, persistence.config)
  use _ <- result.try(load(run))
  Ok(run)
}

/// Carries `correlation` in every `saga/telemetry` event of this handle's
/// drives, and in the `saga.EffectKey` of every step callback and resolver
/// that runs under it. The correlation is not saved: set it on every handle
/// that drives. A handle without one carries `correlation.from_key` of its
/// execution id, so a forgotten call still joins the execution's events.
pub fn with_correlation(
  run: Run(i, o, e, u),
  correlation: Correlation,
) -> Run(i, o, e, u) {
  Run(..run, config: execution.with_correlation(run.config, correlation))
}

/// The execution id given to `start_or_reconnect`.
pub fn id(run: Run(i, o, e, u)) -> String {
  run.id
}

/// Reads the saved state of an execution without a live runner.
pub fn read(run: Run(i, o, e, u)) -> Result(Status(o, e, u), Error) {
  use envelope <- result.try(load(run))
  Ok(case envelope.outcome, envelope.issue {
    Some(outcome), _ -> Finished(execution.from_coordinator(outcome))
    None, Some(reason) -> Suspended(from_checkpoint(run, reason))
    None, None -> Pending
  })
}

/// Records cancellation for an unfinished execution. A running `drive`
/// observes it at its next checkpoint and rolls back. Does nothing for a
/// finished execution.
pub fn cancel(run: Run(i, o, e, u)) -> Result(Nil, Error) {
  use envelope <- result.try(load(run))
  case envelope.outcome {
    Some(_) -> Ok(Nil)
    None ->
      call(run.storage, fn() { storage.do_cancel(run.storage, run.id) })
      |> result.map_error(StorageFailure)
  }
}

/// The ids of up to `limit` executions that are pending or suspended and
/// that no live runner owns: the executions that wait for a `drive`.
/// Reconnect to each with `reconnect`. Saga itself never drives them.
pub fn unfinished(
  storage: Storage,
  limit limit: Int,
) -> Result(List(String), Error) {
  case limit > 0 {
    False -> Ok([])
    True ->
      call(storage, fn() { storage.do_unfinished(storage, limit) })
      |> result.map_error(StorageFailure)
  }
}

// ---------------------------------------------------------------------------
// drive
// ---------------------------------------------------------------------------

type RunnerMessage(o, e, u) {
  RunnerClaimed(claim: storage.Claim)
  RunnerFinished(result: Result(execution.Outcome(o, e, u), Error))
  RunnerDown(exit: RunnerExit)
}

/// How a runner ended, as its monitor reports it.
type RunnerExit {
  /// It returned after releasing its own claim.
  ExitedNormally
  /// It was killed or crashed, so its claim may still be held.
  ExitedAbnormally
  /// No exit was seen within the drain window.
  StillRunning
}

/// Runs the execution until it finishes, suspends or `timeout` passes, and returns its outcome once it is saved. A finished execution
/// returns its saved outcome at once.
///
/// The run happens in a runner process. When `timeout` passes, `drive`
/// stops the runner and returns `DriveTimedOut`; when the calling process
/// exits, the runner stops by itself; when the runner itself is killed or
/// crashes while the caller lives, `drive` returns `RunnerLost`. In every
/// case the runner's in-flight attempts are killed, its claim is released
/// at once, and the last checkpoint stays: this is not cancellation, and
/// the next `drive` resumes, asking each interrupted attempt's resolver
/// what happened. Only when the runner's whole VM is lost does the claim
/// stay until the storage notices, such as a lease that expires.
/// Concurrent `drive`s of one execution contend through the storage: all
/// but one return `StorageFailure(Busy)`, which callers retry after at
/// least the storage's owner-loss window (a lease-based adapter's lease).
pub fn drive(
  run: Run(i, o, e, u),
  timeout timeout: Duration,
) -> Result(execution.Outcome(o, e, u), Error) {
  let timeout_ms = duration.to_milliseconds(timeout)
  use _ <- result.try(case timeout_ms > 0 {
    True -> Ok(Nil)
    False -> Error(InvalidTimeout(timeout))
  })
  use settings <- result.try(
    execution.settings(run.config, Some(run.id))
    |> result.map_error(InvalidConfig),
  )
  let caller = process.self()
  let reply = process.new_subject()
  let runner =
    process.spawn_unlinked(fn() {
      let runner = process.self()
      let guard =
        start_guard(runner, call_timeout_ms(run.storage), fn() {
          process.send(
            reply,
            RunnerFinished(Error(StorageFailure(storage.TimedOut))),
          )
        })
      let result =
        ffi.rescue(fn() { drive_owned(run, settings, caller, guard, reply) })
      stop_linked(guard.pid)
      case result {
        ffi.Rescued(result) -> process.send(reply, RunnerFinished(result))
        // The claim, its renewal and any in-flight attempt outlived the
        // raise: exit abnormally, so that linked helpers stop and `drive`
        // releases the claim.
        ffi.Raised(_, _) -> process.kill(runner)
      }
    })
  let monitor = process.monitor(runner)
  let selector =
    process.new_selector()
    |> process.select(reply)
    |> process.select_specific_monitor(monitor, fn(down) {
      case down {
        process.ProcessDown(reason: process.Normal, ..)
        | process.PortDown(reason: process.Normal, ..) ->
          RunnerDown(ExitedNormally)
        _ -> RunnerDown(ExitedAbnormally)
      }
    })
  let deadline = ffi.monotonic_time() + timeout_ms
  await_runner(run, runner, monitor, selector, deadline, None)
}

fn await_runner(
  run: Run(i, o, e, u),
  runner: Pid,
  monitor: process.Monitor,
  selector: process.Selector(RunnerMessage(o, e, u)),
  deadline: Int,
  claim: Option(storage.Claim),
) -> Result(execution.Outcome(o, e, u), Error) {
  let remaining = int.max(0, deadline - ffi.monotonic_time())
  case process.selector_receive(selector, remaining) {
    Ok(RunnerClaimed(claim)) ->
      await_runner(run, runner, monitor, selector, deadline, Some(claim))
    Ok(RunnerFinished(result)) -> {
      // Return once the runner is gone, so its claim is released and its
      // workers are stopped.
      let #(_, claim, exit) = drain_runner(selector, None, claim)
      process.demonitor_process(monitor)
      release_after(run, claim, exit)
      result
    }
    Ok(RunnerDown(exit)) -> {
      release_after(run, claim, exit)
      Error(RunnerLost)
    }
    Error(Nil) -> {
      // Stop the runner, then prefer a result it reported just before.
      process.kill(runner)
      let #(reported, claim, exit) = drain_runner(selector, None, claim)
      process.demonitor_process(monitor)
      release_after(run, claim, exit)
      option.unwrap(reported, Error(DriveTimedOut))
    }
  }
}

/// Reads what the runner sent until its exit arrives.
fn drain_runner(
  selector: process.Selector(RunnerMessage(o, e, u)),
  reported: Option(Result(execution.Outcome(o, e, u), Error)),
  claim: Option(storage.Claim),
) -> #(
  Option(Result(execution.Outcome(o, e, u), Error)),
  Option(storage.Claim),
  RunnerExit,
) {
  case process.selector_receive(selector, 5000) {
    Ok(RunnerFinished(result)) -> drain_runner(selector, Some(result), claim)
    Ok(RunnerClaimed(claim)) -> drain_runner(selector, reported, Some(claim))
    Ok(RunnerDown(exit)) -> #(reported, claim, exit)
    Error(Nil) -> #(reported, claim, StillRunning)
  }
}

/// Releases the claim of a runner that was killed or crashed, so that the
/// next `drive` need not wait for the storage to notice. A runner that
/// returned released its own claim.
fn release_after(
  run: Run(i, o, e, u),
  claim: Option(storage.Claim),
  exit: RunnerExit,
) -> Nil {
  case claim, exit {
    Some(claim), ExitedAbnormally -> {
      let _ = call(run.storage, fn() { storage.do_release(run.storage, claim) })
      Nil
    }
    _, _ -> Nil
  }
}

fn drive_owned(
  run: Run(i, o, e, u),
  settings: coordinator.Settings,
  caller: Pid,
  guard: Guard,
  reply: Subject(RunnerMessage(o, e, u)),
) -> Result(execution.Outcome(o, e, u), Error) {
  let store = run.storage
  use #(claim, stored) <- result.try(
    guarded(guard, fn() { storage.do_claim(store, run.id) })
    |> result.map_error(StorageFailure),
  )
  process.send(reply, RunnerClaimed(claim))
  let heartbeat = case storage.renewal(store) {
    None -> None
    Some(#(every, renew)) ->
      Some(
        start_heartbeat(store, process.self(), every, claim, renew, fn() {
          process.send(
            reply,
            RunnerFinished(Error(StorageFailure(storage.StaleOwner))),
          )
        }),
      )
  }
  let result = drive_claimed(run, settings, caller, guard, claim, stored)
  option.map(heartbeat, stop_linked)
  // A finished run's outcome is saved; a failed release only delays the
  // next claim until the adapter notices this runner is gone.
  let _ = guarded(guard, fn() { storage.do_release(store, claim) })
  result
}

fn drive_claimed(
  run: Run(i, o, e, u),
  settings: coordinator.Settings,
  caller: Pid,
  guard: Guard,
  claim: storage.Claim,
  stored: storage.Stored,
) -> Result(execution.Outcome(o, e, u), Error) {
  let persistence = run.persistence
  use envelope <- result.try(open(run, storage.data(stored)))
  case envelope.outcome {
    Some(outcome) -> Ok(execution.from_coordinator(outcome))
    None -> {
      use input <- result.try(
        codec.decode(persistence.input, envelope.input)
        |> result.map_error(CodecFailure(RunInput, _)),
      )
      let cancelled = storage.cancelled(stored)
      // The last committed revision and envelope, owned by this runner.
      let committed = process.new_subject()
      process.send(committed, #(storage.revision(stored), envelope))
      let commit = fn(
        change: fn(Envelope(o, e, u)) -> Envelope(o, e, u),
        phase: storage.Phase,
      ) {
        let assert Ok(#(revision, current)) = process.receive(committed, 0)
        let next = change(current)
        let written = {
          // A suspension record may pass the size limit by its reason, so
          // that `read` can still report why the execution stopped.
          let limit = case phase {
            storage.Suspended -> None
            storage.Pending | storage.Finished ->
              Some(persistence.max_checkpoint_bytes)
          }
          use bytes <- result.try(encode_within(next, persistence, limit))
          guarded(guard, fn() {
            storage.do_commit(
              run.storage,
              claim,
              storage.Commit(
                expected_revision: revision,
                observed_cancelled: cancelled,
                phase: phase,
                data: bytes,
              ),
            )
          })
          |> result.map_error(checkpoint.StorageFailure)
        }
        case written {
          Ok(saved) -> {
            process.send(committed, #(storage.revision(saved), next))
            Ok(Nil)
          }
          Error(failure) -> {
            process.send(committed, #(revision, current))
            Error(failure)
          }
        }
      }
      let result_out = process.new_subject()
      let session =
        coordinator.Session(
          execution_id: run.id,
          snapshot: envelope.snapshot,
          cancelled: cancelled,
          save: fn(snapshot) {
            commit(
              fn(current) {
                Envelope(..current, snapshot: Some(snapshot), issue: None)
              },
              storage.Pending,
            )
          },
          failed: fn(reason) {
            // Keep the last committed checkpoint: admissions that were not
            // committed were never dispatched and are not recovery facts.
            let failure = case
              commit(
                fn(current) { Envelope(..current, issue: Some(reason)) },
                storage.Suspended,
              )
            {
              Ok(Nil) -> from_checkpoint(run, reason)
              Error(recording) if recording == reason ->
                from_checkpoint(run, reason)
              Error(recording) ->
                SuspensionNotSaved(
                  from_checkpoint(run, reason),
                  from_checkpoint(run, recording),
                )
            }
            process.send(result_out, Error(failure))
          },
          stopped: fn() { process.send(result_out, Error(RunnerLost)) },
        )
      coordinator.execute_saved(
        saga.name(persistence.workflow),
        caller,
        settings,
        fn() { saga.for_run(persistence.workflow, input) },
        session,
        fn(outcome) {
          let saved =
            commit(
              fn(current) {
                Envelope(..current, outcome: Some(outcome), issue: None)
              },
              storage.Finished,
            )
          process.send(
            result_out,
            saved
              |> result.map(fn(_) { execution.from_coordinator(outcome) })
              |> result.map_error(from_checkpoint(run, _)),
          )
        },
      )
      let outcome = process.receive_forever(result_out)
      // A cancellation racing a checkpoint wins before dispatch: restart
      // from the saved state with the new observation.
      case guarded(guard, fn() { storage.do_load(run.storage, run.id) }) {
        Ok(current) ->
          case storage.cancelled(current) == cancelled {
            True -> outcome
            False -> drive_claimed(run, settings, caller, guard, claim, current)
          }
        Error(_) -> outcome
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Storage calls
// ---------------------------------------------------------------------------

/// Runs one storage operation from the caller's process in a helper
/// process, bounded by the storage's call timeout.
fn call(
  store: Storage,
  operation: fn() -> Result(a, storage.Error),
) -> Result(a, storage.Error) {
  let reply = process.new_subject()
  let helper =
    process.spawn_unlinked(fn() { process.send(reply, rescued(operation)) })
  let monitor = process.monitor(helper)
  let selector =
    process.new_selector()
    |> process.select(reply)
    |> process.select_specific_monitor(monitor, fn(_) {
      Error(storage.Unavailable("the storage operation exited"))
    })
  case process.selector_receive(selector, call_timeout_ms(store)) {
    Ok(result) -> {
      process.demonitor_process(monitor)
      result
    }
    Error(Nil) -> {
      process.kill(helper)
      process.demonitor_process(monitor)
      Error(storage.TimedOut)
    }
  }
}

fn rescued(
  operation: fn() -> Result(a, storage.Error),
) -> Result(a, storage.Error) {
  case ffi.rescue(operation) {
    ffi.Rescued(result) -> result
    ffi.Raised(_, reason) ->
      Error(storage.Unavailable("the storage operation raised: " <> reason))
  }
}

/// A runner's watchdog: runner storage operations run in the runner itself,
/// so an adapter sees the runner as the claiming process, and the watchdog
/// stops a runner whose operation outlives the call timeout.
type Guard {
  Guard(pid: Pid, subject: Subject(GuardMessage))
}

type GuardMessage {
  Begin
  End
}

fn start_guard(runner: Pid, timeout: Int, on_timeout: fn() -> Nil) -> Guard {
  let ready = process.new_subject()
  let pid =
    process.spawn(fn() {
      let subject = process.new_subject()
      process.send(ready, subject)
      guard_idle(subject, runner, timeout, on_timeout)
    })
  let assert Ok(subject) = process.receive(ready, 5000)
  Guard(pid, subject)
}

fn guard_idle(
  subject: Subject(GuardMessage),
  runner: Pid,
  timeout: Int,
  on_timeout: fn() -> Nil,
) -> Nil {
  case process.receive_forever(subject) {
    Begin ->
      case process.receive(subject, timeout) {
        Ok(_) -> guard_idle(subject, runner, timeout, on_timeout)
        Error(Nil) -> {
          on_timeout()
          process.kill(runner)
        }
      }
    End -> guard_idle(subject, runner, timeout, on_timeout)
  }
}

/// The storage's call timeout in milliseconds, for the process timers.
fn call_timeout_ms(store: Storage) -> Int {
  duration.to_milliseconds(storage.call_timeout(store))
}

fn guarded(
  guard: Guard,
  operation: fn() -> Result(a, storage.Error),
) -> Result(a, storage.Error) {
  process.send(guard.subject, Begin)
  let result = rescued(operation)
  process.send(guard.subject, End)
  result
}

/// Renews a claim every `every` while the runner lives; when the claim was
/// taken over, reports it and stops the runner.
fn start_heartbeat(
  store: Storage,
  runner: Pid,
  every: Duration,
  claim: storage.Claim,
  renew: fn(storage.Claim) -> Result(Nil, storage.Error),
  on_lost: fn() -> Nil,
) -> Pid {
  let every_ms = duration.to_milliseconds(every)
  process.spawn(fn() {
    heartbeat(store, runner, every_ms, claim, renew, on_lost)
  })
}

fn heartbeat(
  store: Storage,
  runner: Pid,
  every: Int,
  claim: storage.Claim,
  renew: fn(storage.Claim) -> Result(Nil, storage.Error),
  on_lost: fn() -> Nil,
) -> Nil {
  process.sleep(int.max(1, every))
  // The storage's call timeout bounds a renewal too. A renewal that fails
  // with `Unavailable` or `TimedOut` is tried again; commits stay fenced by
  // the claim if the lease expires meanwhile.
  case call(store, fn() { renew(claim) }) {
    Error(storage.StaleOwner) | Error(storage.NotFound) -> {
      on_lost()
      process.kill(runner)
    }
    _ -> heartbeat(store, runner, every, claim, renew, on_lost)
  }
}

/// Stops a helper linked to the calling process without taking the caller
/// down.
fn stop_linked(pid: Pid) -> Nil {
  process.unlink(pid)
  process.kill(pid)
}

// ---------------------------------------------------------------------------
// Checkpoint envelope
// ---------------------------------------------------------------------------

fn load(run: Run(i, o, e, u)) -> Result(Envelope(o, e, u), Error) {
  use stored <- result.try(
    call(run.storage, fn() { storage.do_load(run.storage, run.id) })
    |> result.map_error(StorageFailure),
  )
  open(run, storage.data(stored))
}

fn open(
  run: Run(i, o, e, u),
  bytes: BitArray,
) -> Result(Envelope(o, e, u), Error) {
  let persistence = run.persistence
  use #(saved_id, saved_stamp) <- result.try(
    header(bytes) |> result.replace_error(InvalidCheckpoint(Malformed)),
  )
  use _ <- result.try(
    case saved_id == run.id, saved_stamp == persistence.stamp {
      False, _ -> Error(InvalidCheckpoint(ForeignExecution(saved_id)))
      _, False -> Error(IncompatibleDefinition)
      True, True -> Ok(Nil)
    },
  )
  decode_envelope(
    bytes,
    fn(value) { codec.decode(persistence.output, value) },
    fn(value) { codec.decode(persistence.error, value) },
    fn(value) { codec.decode(persistence.undo_error, value) },
  )
  |> result.map_error(from_checkpoint(run, _))
}

fn encode(
  envelope: Envelope(o, e, u),
  persistence: Persistence(i, o, e, u),
) -> Result(BitArray, checkpoint.Failure) {
  encode_within(envelope, persistence, Some(persistence.max_checkpoint_bytes))
}

fn encode_within(
  envelope: Envelope(o, e, u),
  persistence: Persistence(i, o, e, u),
  limit: Option(Int),
) -> Result(BitArray, checkpoint.Failure) {
  use bytes <- result.try(
    encode_envelope(
      envelope,
      fn(value) { codec.encode(persistence.output, value) },
      fn(value) { codec.encode(persistence.error, value) },
      fn(value) { codec.encode(persistence.undo_error, value) },
    ),
  )
  let size = bit_array.byte_size(bytes)
  case limit {
    Some(limit) if size > limit -> Error(checkpoint.TooLarge(size, limit))
    _ -> Ok(bytes)
  }
}

@external(erlang, "saga_checkpoint", "encode")
fn encode_envelope(
  envelope: Envelope(o, e, u),
  output: fn(o) -> Result(String, codec.CodecError),
  error: fn(e) -> Result(String, codec.CodecError),
  undo: fn(u) -> Result(String, codec.CodecError),
) -> Result(BitArray, checkpoint.Failure)

@external(erlang, "saga_checkpoint", "decode")
fn decode_envelope(
  bytes: BitArray,
  output: fn(String) -> Result(o, codec.CodecError),
  error: fn(String) -> Result(e, codec.CodecError),
  undo: fn(String) -> Result(u, codec.CodecError),
) -> Result(Envelope(o, e, u), checkpoint.Failure)

@external(erlang, "saga_checkpoint", "header")
fn header(bytes: BitArray) -> Result(#(String, String), Nil)

fn frame(parts: List(String)) -> String {
  list.map(parts, fn(part) {
    int.to_string(string.byte_size(part)) <> ":" <> part
  })
  |> string.concat
}

// ---------------------------------------------------------------------------
// Internal to public vocabulary
// ---------------------------------------------------------------------------

fn from_checkpoint(run: Run(i, o, e, u), failure: checkpoint.Failure) -> Error {
  case failure {
    checkpoint.StorageFailure(error) -> StorageFailure(error)
    checkpoint.CodecFailure(boundary, error) ->
      CodecFailure(boundary_from(boundary), error)
    checkpoint.InvalidState(problem) ->
      InvalidCheckpoint(checkpoint_problem_from(problem))
    checkpoint.Uncertain(checkpoint.Required(step, action, key)) ->
      RecoveryRequired(Required(
        step: address_from(step),
        action: case action {
          checkpoint.AttemptAction(n) -> execution.StepAttempt(n)
          checkpoint.CompensationAction(n) -> execution.StepCompensation(n)
          checkpoint.UndoAction -> execution.StepUndo
        },
        key: saga.key_from_saved(
          key,
          execution.correlation_of(run.config, Some(run.id)),
        ),
      ))
    checkpoint.TooLarge(bytes, limit) -> CheckpointTooLarge(bytes, limit)
  }
}

fn address_from(address: checkpoint.Address) -> saga.StepAddress {
  saga.StepAddress(address.scope, address.name, address.occurrence)
}

fn boundary_from(boundary: checkpoint.Boundary) -> Boundary {
  case boundary {
    checkpoint.RunInput -> RunInput
    checkpoint.RunOutput -> RunOutput
    checkpoint.RunError -> RunError
    checkpoint.RunUndoError -> RunUndoError
    checkpoint.StepInput(step) -> StepInput(address_from(step))
    checkpoint.StepOutput(step) -> StepOutput(address_from(step))
  }
}

fn checkpoint_problem_from(problem: checkpoint.Problem) -> CheckpointProblem {
  case problem {
    checkpoint.Malformed -> Malformed
    checkpoint.ForeignExecution(saved) -> ForeignExecution(saved)
    checkpoint.GraphMismatch -> GraphMismatch
    checkpoint.ConcurrencyBelowInFlight(in_flight, max_concurrency) ->
      ConcurrencyBelowInFlight(in_flight, max_concurrency)
    checkpoint.UndoNotRestorable(step) -> UndoNotRestorable(address_from(step))
    checkpoint.CompensationInputMissing(step) ->
      CompensationInputMissing(address_from(step))
    checkpoint.DeciderMissingAfterMapping(step) ->
      DeciderMissingAfterMapping(address_from(step))
  }
}
