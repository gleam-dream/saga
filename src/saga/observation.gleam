//// Defines the Sinal events a run emits: run start and stop, step start and
//// stop, compensation decisions, and undo outcomes.
////
//// Use this module to observe runs started with `saga/execution` or
//// `saga/durable`. Each function returns a typed `sinal.Event` descriptor;
//// attach a handler to it with `sinal.observe` or `sinal.attach`. This
//// module owns the events, not handler registration.
////
//// A run's coordinator emits each event after the state change it
//// describes, with `sinal.emit`. Handlers run synchronously in the
//// coordinator process unless the application routes the `saga` prefix to
//// a `sinal/forwarder`: a slow inline handler delays the run. Observations
//// never control a run.
////
//// ```gleam
//// import saga/observation
//// import sinal
////
//// let attachment =
////   sinal.observe(observation.run_stopped(), fn(_measurements, metadata) {
////     log_outcome(metadata.workflow, metadata.outcome)
////   })
//// ```

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

fn attempt_kind_to_string(kind: AttemptKind) -> String {
  case kind {
    AttemptSucceeded -> "succeeded"
    AttemptFailed -> "failed"
    AttemptCrashed -> "crashed"
    AttemptTimedOut -> "timed_out"
    AttemptInterrupted -> "interrupted"
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

fn undo_kind_to_string(kind: UndoKind) -> String {
  case kind {
    UndoUndone -> "undone"
    UndoFailedKind -> "failed"
    UndoCrashedKind -> "crashed"
    UndoTimedOutKind -> "timed_out"
  }
}

/// The `[saga, run, start]` event descriptor.
pub fn run_started() -> Event(RunStartMeasurements, RunMetadata) {
  let measurements =
    fields.record({
      use system_time <- fields.parameter
      RunStartMeasurements(system_time:)
    })
    |> fields.and(fields.int("system_time"), fn(m: RunStartMeasurements) {
      m.system_time
    })
    |> fields.build
  let metadata =
    fields.record({
      use workflow <- fields.parameter
      use run <- fields.parameter
      RunMetadata(workflow:, run:)
    })
    |> fields.and(fields.string("workflow"), fn(m: RunMetadata) { m.workflow })
    |> fields.and(fields.int("run"), fn(m) { m.run })
    |> fields.build
  sinal.event(["saga", "run", "start"], measurements, metadata)
}

/// The `[saga, run, stop]` event descriptor.
pub fn run_stopped() -> Event(RunStopMeasurements, RunStopMetadata) {
  let measurements =
    fields.record({
      use duration <- fields.parameter
      use undone <- fields.parameter
      use undo_failures <- fields.parameter
      use interrupted <- fields.parameter
      RunStopMeasurements(duration:, undone:, undo_failures:, interrupted:)
    })
    |> fields.and(fields.int("duration"), fn(m: RunStopMeasurements) {
      m.duration
    })
    |> fields.and(fields.int("undone"), fn(m) { m.undone })
    |> fields.and(fields.int("undo_failures"), fn(m) { m.undo_failures })
    |> fields.and(fields.int("interrupted"), fn(m) { m.interrupted })
    |> fields.build
  let metadata =
    fields.record({
      use workflow <- fields.parameter
      use run <- fields.parameter
      use outcome <- fields.parameter
      RunStopMetadata(workflow:, run:, outcome:)
    })
    |> fields.and(fields.string("workflow"), fn(m: RunStopMetadata) {
      m.workflow
    })
    |> fields.and(fields.int("run"), fn(m) { m.run })
    |> fields.and(
      fields.enum(
        "outcome",
        [OutcomeCompleted, OutcomeFailed, OutcomeCancelled, OutcomeUnresolved],
        outcome_kind_to_string,
      ),
      fn(m) { m.outcome },
    )
    |> fields.build
  sinal.event(["saga", "run", "stop"], measurements, metadata)
}

/// The `[saga, step, start]` event descriptor.
pub fn step_started() -> Event(StepStartMeasurements, StepMetadata) {
  let measurements =
    fields.record({
      use system_time <- fields.parameter
      StepStartMeasurements(system_time:)
    })
    |> fields.and(fields.int("system_time"), fn(m: StepStartMeasurements) {
      m.system_time
    })
    |> fields.build
  let metadata =
    fields.record({
      use workflow <- fields.parameter
      use run <- fields.parameter
      use step <- fields.parameter
      use attempt <- fields.parameter
      StepMetadata(workflow:, run:, step:, attempt:)
    })
    |> fields.and(fields.string("workflow"), fn(m: StepMetadata) { m.workflow })
    |> fields.and(fields.int("run"), fn(m) { m.run })
    |> fields.and(fields.string("step"), fn(m) { m.step })
    |> fields.and(fields.int("attempt"), fn(m) { m.attempt })
    |> fields.build
  sinal.event(["saga", "step", "start"], measurements, metadata)
}

/// The `[saga, step, stop]` event descriptor.
pub fn step_stopped() -> Event(StepStopMeasurements, StepStopMetadata) {
  let metadata =
    fields.record({
      use workflow <- fields.parameter
      use run <- fields.parameter
      use step <- fields.parameter
      use attempt <- fields.parameter
      use result <- fields.parameter
      StepStopMetadata(workflow:, run:, step:, attempt:, result:)
    })
    |> fields.and(fields.string("workflow"), fn(m: StepStopMetadata) {
      m.workflow
    })
    |> fields.and(fields.int("run"), fn(m) { m.run })
    |> fields.and(fields.string("step"), fn(m) { m.step })
    |> fields.and(fields.int("attempt"), fn(m) { m.attempt })
    |> fields.and(
      fields.enum(
        "result",
        [
          AttemptSucceeded,
          AttemptFailed,
          AttemptCrashed,
          AttemptTimedOut,
          AttemptInterrupted,
        ],
        attempt_kind_to_string,
      ),
      fn(m) { m.result },
    )
    |> fields.build
  sinal.event(["saga", "step", "stop"], stop_measurements(), metadata)
}

/// The `[saga, step, compensate, stop]` event descriptor.
pub fn compensation_stopped() -> Event(
  StepStopMeasurements,
  CompensationMetadata,
) {
  let metadata =
    fields.record({
      use workflow <- fields.parameter
      use run <- fields.parameter
      use step <- fields.parameter
      use attempt <- fields.parameter
      use decision <- fields.parameter
      CompensationMetadata(workflow:, run:, step:, attempt:, decision:)
    })
    |> fields.and(fields.string("workflow"), fn(m: CompensationMetadata) {
      m.workflow
    })
    |> fields.and(fields.int("run"), fn(m) { m.run })
    |> fields.and(fields.string("step"), fn(m) { m.step })
    |> fields.and(fields.int("attempt"), fn(m) { m.attempt })
    |> fields.and(
      fields.enum(
        "decision",
        [
          DecisionRetry,
          DecisionContinue,
          DecisionAbort,
          DecisionHold,
          DecisionCrashed,
          DecisionTimedOut,
        ],
        decision_kind_to_string,
      ),
      fn(m) { m.decision },
    )
    |> fields.build
  sinal.event(
    ["saga", "step", "compensate", "stop"],
    stop_measurements(),
    metadata,
  )
}

/// The `[saga, step, undo, stop]` event descriptor.
pub fn undo_stopped() -> Event(StepStopMeasurements, UndoMetadata) {
  let metadata =
    fields.record({
      use workflow <- fields.parameter
      use run <- fields.parameter
      use step <- fields.parameter
      use result <- fields.parameter
      UndoMetadata(workflow:, run:, step:, result:)
    })
    |> fields.and(fields.string("workflow"), fn(m: UndoMetadata) { m.workflow })
    |> fields.and(fields.int("run"), fn(m) { m.run })
    |> fields.and(fields.string("step"), fn(m) { m.step })
    |> fields.and(
      fields.enum(
        "result",
        [UndoUndone, UndoFailedKind, UndoCrashedKind, UndoTimedOutKind],
        undo_kind_to_string,
      ),
      fn(m) { m.result },
    )
    |> fields.build
  sinal.event(["saga", "step", "undo", "stop"], stop_measurements(), metadata)
}

fn stop_measurements() -> fields.Fields(StepStopMeasurements) {
  fields.record({
    use duration <- fields.parameter
    StepStopMeasurements(duration:)
  })
  |> fields.and(fields.int("duration"), fn(m: StepStopMeasurements) {
    m.duration
  })
  |> fields.build
}
