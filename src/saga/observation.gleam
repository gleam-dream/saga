//// Defines the Sinal events a run emits: run start and stop, step start and
//// stop, compensation decisions, and undo outcomes.
////
//// Use this module to observe runs started with `saga/execution` or
//// `saga/durable`. Each function returns a typed `sinal.Event` descriptor;
//// attach a handler to it with `sinal.observe` or `sinal.attach`. This
//// module owns the events, not handler registration.
////
//// A run's coordinator emits each event after the state change it
//// describes, with `sinal.emit`. Handlers therefore run synchronously in
//// the coordinator process: a slow handler delays the run. An emit error is
//// ignored, because observations never control a run.
////
//// ```gleam
//// import saga/observation
//// import sinal
////
//// let assert Ok(id) = sinal.handler_id("checkout-run-stopped")
//// let assert Ok(attachment) =
////   sinal.observe(id, observation.run_stopped(), fn(_measurements, metadata) {
////     log_outcome(metadata.workflow, metadata.outcome)
////   })
//// ```

import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/atom
import sinal.{type Event}
import sinal/fields

/// `[saga, run, start]` measurements: native monotonic system time.
pub type RunStartMeasurements {
  RunStartMeasurements(system_time: Int)
}

/// `[saga, run, start]` metadata: the workflow's name and this run's id.
pub type RunMetadata {
  RunMetadata(workflow: String, run: Int)
}

/// `[saga, run, stop]` measurements: total duration and settlement counts.
pub type RunStopMeasurements {
  RunStopMeasurements(
    duration: Int,
    undone: Int,
    undo_failures: Int,
    interrupted: Int,
  )
}

/// The closed set of ways a run can end, for `[saga, run, stop]` metadata.
pub type OutcomeKind {
  OutcomeCompleted
  OutcomeFailed
  OutcomeCancelled
  OutcomeUnresolved
}

/// `[saga, run, stop]` metadata: the workflow, the run, and its outcome kind.
pub type RunStopMetadata {
  RunStopMetadata(workflow: String, run: Int, outcome: OutcomeKind)
}

/// `[saga, step, start]` measurements: native monotonic system time.
pub type StepStartMeasurements {
  StepStartMeasurements(system_time: Int)
}

/// Shared metadata shape for a step's start: workflow, run, step address
/// (rendered with `saga.address_to_string`), and 1-based attempt number.
pub type StepMetadata {
  StepMetadata(workflow: String, run: Int, step: String, attempt: Int)
}

/// `[saga, step, stop]`, `[saga, step, compensate, stop]`, and
/// `[saga, step, undo, stop]` share this measurement shape: the duration of
/// the attempt, compensation decision, or undo action.
pub type StepStopMeasurements {
  StepStopMeasurements(duration: Int)
}

/// The closed set of ways one attempt can end, for `[saga, step, stop]`.
pub type AttemptKind {
  AttemptSucceeded
  AttemptFailed
  AttemptCrashed
  AttemptTimedOut
  AttemptInterrupted
}

/// `[saga, step, stop]` metadata.
pub type StepStopMetadata {
  StepStopMetadata(
    workflow: String,
    run: Int,
    step: String,
    attempt: Int,
    result: AttemptKind,
  )
}

/// The closed set of recovery decisions, for
/// `[saga, step, compensate, stop]`.
pub type DecisionKind {
  DecisionRetry
  DecisionContinue
  DecisionAbort
  DecisionHold
  DecisionCrashed
  DecisionTimedOut
}

/// `[saga, step, compensate, stop]` metadata.
pub type CompensationMetadata {
  CompensationMetadata(
    workflow: String,
    run: Int,
    step: String,
    attempt: Int,
    decision: DecisionKind,
  )
}

/// The closed set of ways one undo action can end, for
/// `[saga, step, undo, stop]`.
pub type UndoKind {
  UndoUndone
  UndoFailedKind
  UndoCrashedKind
  UndoTimedOutKind
}

/// `[saga, step, undo, stop]` metadata.
pub type UndoMetadata {
  UndoMetadata(workflow: String, run: Int, step: String, result: UndoKind)
}

fn outcome_kind_to_string(kind: OutcomeKind) -> String {
  case kind {
    OutcomeCompleted -> "completed"
    OutcomeFailed -> "failed"
    OutcomeCancelled -> "cancelled"
    OutcomeUnresolved -> "unresolved"
  }
}

fn outcome_kind_from_string(raw: String) -> Result(OutcomeKind, Nil) {
  case raw {
    "completed" -> Ok(OutcomeCompleted)
    "failed" -> Ok(OutcomeFailed)
    "cancelled" -> Ok(OutcomeCancelled)
    "unresolved" -> Ok(OutcomeUnresolved)
    _ -> Error(Nil)
  }
}

fn attempt_kind_to_string(kind: AttemptKind) -> String {
  case kind {
    AttemptSucceeded -> "succeeded"
    AttemptFailed -> "failed"
    AttemptCrashed -> "crashed"
    AttemptTimedOut -> "timed_out"
    AttemptInterrupted -> "interrupted"
  }
}

fn attempt_kind_from_string(raw: String) -> Result(AttemptKind, Nil) {
  case raw {
    "succeeded" -> Ok(AttemptSucceeded)
    "failed" -> Ok(AttemptFailed)
    "crashed" -> Ok(AttemptCrashed)
    "timed_out" -> Ok(AttemptTimedOut)
    "interrupted" -> Ok(AttemptInterrupted)
    _ -> Error(Nil)
  }
}

fn decision_kind_to_string(kind: DecisionKind) -> String {
  case kind {
    DecisionRetry -> "retry"
    DecisionContinue -> "continue"
    DecisionAbort -> "abort"
    DecisionHold -> "hold"
    DecisionCrashed -> "crashed"
    DecisionTimedOut -> "timed_out"
  }
}

fn decision_kind_from_string(raw: String) -> Result(DecisionKind, Nil) {
  case raw {
    "retry" -> Ok(DecisionRetry)
    "continue" -> Ok(DecisionContinue)
    "abort" -> Ok(DecisionAbort)
    "hold" -> Ok(DecisionHold)
    "crashed" -> Ok(DecisionCrashed)
    "timed_out" -> Ok(DecisionTimedOut)
    _ -> Error(Nil)
  }
}

fn undo_kind_to_string(kind: UndoKind) -> String {
  case kind {
    UndoUndone -> "undone"
    UndoFailedKind -> "failed"
    UndoCrashedKind -> "crashed"
    UndoTimedOutKind -> "timed_out"
  }
}

fn undo_kind_from_string(raw: String) -> Result(UndoKind, Nil) {
  case raw {
    "undone" -> Ok(UndoUndone)
    "failed" -> Ok(UndoFailedKind)
    "crashed" -> Ok(UndoCrashedKind)
    "timed_out" -> Ok(UndoTimedOutKind)
    _ -> Error(Nil)
  }
}

/// Declares a field whose wire representation is a native string but whose
/// Gleam representation is a closed enum, via an explicit total
/// `to_string`/partial `from_string` pair. Built directly on
/// `sinal/fields.field` (same construction `fields.string` itself uses),
/// so this stays inside Sinal's public field API with no extra FFI.
fn closed_string_field(
  key: String,
  to_string: fn(a) -> String,
  from_string: fn(String) -> Result(a, Nil),
) -> fields.Fields(a) {
  fields.field(
    atom.create(key),
    fn(value) { Ok(dynamic.string(to_string(value))) },
    fn(raw) {
      case decode.run(raw, decode.string) {
        Error(_) ->
          Error(fields.FieldDecodeError("Expected a native BEAM string"))
        Ok(str) ->
          case from_string(str) {
            Ok(value) -> Ok(value)
            Error(Nil) ->
              Error(fields.FieldDecodeError(
                "Unrecognized " <> key <> " kind: " <> str,
              ))
          }
      }
    },
  )
}

/// The `[saga, run, start]` event descriptor.
pub fn run_started() -> Event(RunStartMeasurements, RunMetadata) {
  let name = [atom.create("saga"), atom.create("run"), atom.create("start")]
  let measurements =
    fields.imap(
      fields.int(atom.create("system_time")),
      RunStartMeasurements,
      fn(m) { m.system_time },
    )
  let assert Ok(meta_pair) =
    fields.pair(
      fields.string(atom.create("workflow")),
      fields.int(atom.create("run")),
    )
  let metadata =
    fields.imap(meta_pair, fn(pair) { RunMetadata(pair.0, pair.1) }, fn(m) {
      #(m.workflow, m.run)
    })
  let assert Ok(event) = sinal.event(name, measurements, metadata)
  event
}

/// The `[saga, run, stop]` event descriptor.
pub fn run_stopped() -> Event(RunStopMeasurements, RunStopMetadata) {
  let name = [atom.create("saga"), atom.create("run"), atom.create("stop")]
  let assert Ok(meas_pair1) =
    fields.pair(
      fields.int(atom.create("duration")),
      fields.int(atom.create("undone")),
    )
  let assert Ok(meas_pair2) =
    fields.pair(
      fields.int(atom.create("undo_failures")),
      fields.int(atom.create("interrupted")),
    )
  let assert Ok(meas_all) = fields.pair(meas_pair1, meas_pair2)
  let measurements =
    fields.imap(
      meas_all,
      fn(p) {
        let #(#(duration, undone), #(undo_failures, interrupted)) = p
        RunStopMeasurements(duration, undone, undo_failures, interrupted)
      },
      fn(m) { #(#(m.duration, m.undone), #(m.undo_failures, m.interrupted)) },
    )
  let assert Ok(meta_pair1) =
    fields.pair(
      fields.string(atom.create("workflow")),
      fields.int(atom.create("run")),
    )
  let assert Ok(meta_all) =
    fields.pair(
      meta_pair1,
      closed_string_field(
        "outcome",
        outcome_kind_to_string,
        outcome_kind_from_string,
      ),
    )
  let metadata =
    fields.imap(
      meta_all,
      fn(p) {
        let #(#(workflow, run), outcome) = p
        RunStopMetadata(workflow, run, outcome)
      },
      fn(m) { #(#(m.workflow, m.run), m.outcome) },
    )
  let assert Ok(event) = sinal.event(name, measurements, metadata)
  event
}

/// The `[saga, step, start]` event descriptor.
pub fn step_started() -> Event(StepStartMeasurements, StepMetadata) {
  let name = [atom.create("saga"), atom.create("step"), atom.create("start")]
  let measurements =
    fields.imap(
      fields.int(atom.create("system_time")),
      StepStartMeasurements,
      fn(m) { m.system_time },
    )
  let metadata = step_metadata_fields()
  let assert Ok(event) = sinal.event(name, measurements, metadata)
  event
}

fn step_metadata_fields() -> fields.Fields(StepMetadata) {
  let assert Ok(p1) =
    fields.pair(
      fields.string(atom.create("workflow")),
      fields.int(atom.create("run")),
    )
  let assert Ok(p2) = fields.pair(p1, fields.string(atom.create("step")))
  let assert Ok(p3) = fields.pair(p2, fields.int(atom.create("attempt")))
  fields.imap(
    p3,
    fn(p) {
      let #(#(#(workflow, run), step), attempt) = p
      StepMetadata(workflow, run, step, attempt)
    },
    fn(m) { #(#(#(m.workflow, m.run), m.step), m.attempt) },
  )
}

/// The `[saga, step, stop]` event descriptor.
pub fn step_stopped() -> Event(StepStopMeasurements, StepStopMetadata) {
  let name = [atom.create("saga"), atom.create("step"), atom.create("stop")]
  let measurements =
    fields.imap(
      fields.int(atom.create("duration")),
      StepStopMeasurements,
      fn(m) { m.duration },
    )
  let assert Ok(p1) =
    fields.pair(
      fields.string(atom.create("workflow")),
      fields.int(atom.create("run")),
    )
  let assert Ok(p2) = fields.pair(p1, fields.string(atom.create("step")))
  let assert Ok(p3) = fields.pair(p2, fields.int(atom.create("attempt")))
  let assert Ok(p4) =
    fields.pair(
      p3,
      closed_string_field(
        "result",
        attempt_kind_to_string,
        attempt_kind_from_string,
      ),
    )
  let metadata =
    fields.imap(
      p4,
      fn(p) {
        let #(#(#(#(workflow, run), step), attempt), result) = p
        StepStopMetadata(workflow, run, step, attempt, result)
      },
      fn(m) { #(#(#(#(m.workflow, m.run), m.step), m.attempt), m.result) },
    )
  let assert Ok(event) = sinal.event(name, measurements, metadata)
  event
}

/// The `[saga, step, compensate, stop]` event descriptor.
pub fn compensation_stopped() -> Event(
  StepStopMeasurements,
  CompensationMetadata,
) {
  let name = [
    atom.create("saga"),
    atom.create("step"),
    atom.create("compensate"),
    atom.create("stop"),
  ]
  let measurements =
    fields.imap(
      fields.int(atom.create("duration")),
      StepStopMeasurements,
      fn(m) { m.duration },
    )
  let assert Ok(p1) =
    fields.pair(
      fields.string(atom.create("workflow")),
      fields.int(atom.create("run")),
    )
  let assert Ok(p2) = fields.pair(p1, fields.string(atom.create("step")))
  let assert Ok(p3) = fields.pair(p2, fields.int(atom.create("attempt")))
  let assert Ok(p4) =
    fields.pair(
      p3,
      closed_string_field(
        "decision",
        decision_kind_to_string,
        decision_kind_from_string,
      ),
    )
  let metadata =
    fields.imap(
      p4,
      fn(p) {
        let #(#(#(#(workflow, run), step), attempt), decision) = p
        CompensationMetadata(workflow, run, step, attempt, decision)
      },
      fn(m) { #(#(#(#(m.workflow, m.run), m.step), m.attempt), m.decision) },
    )
  let assert Ok(event) = sinal.event(name, measurements, metadata)
  event
}

/// The `[saga, step, undo, stop]` event descriptor.
pub fn undo_stopped() -> Event(StepStopMeasurements, UndoMetadata) {
  let name = [
    atom.create("saga"),
    atom.create("step"),
    atom.create("undo"),
    atom.create("stop"),
  ]
  let measurements =
    fields.imap(
      fields.int(atom.create("duration")),
      StepStopMeasurements,
      fn(m) { m.duration },
    )
  let assert Ok(p1) =
    fields.pair(
      fields.string(atom.create("workflow")),
      fields.int(atom.create("run")),
    )
  let assert Ok(p2) = fields.pair(p1, fields.string(atom.create("step")))
  let assert Ok(p3) =
    fields.pair(
      p2,
      closed_string_field("result", undo_kind_to_string, undo_kind_from_string),
    )
  let metadata =
    fields.imap(
      p3,
      fn(p) {
        let #(#(workflow, run), step) = p.0
        UndoMetadata(workflow, run, step, p.1)
      },
      fn(m) { #(#(#(m.workflow, m.run), m.step), m.result) },
    )
  let assert Ok(event) = sinal.event(name, measurements, metadata)
  event
}
