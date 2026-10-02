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
  let measurements = {
    use system_time <- fields.include(fields.int("system_time"), get: fn(m) {
      m.system_time
    })
    fields.success(RunStartMeasurements(system_time:))
  }
  let metadata = {
    use workflow <- fields.include(fields.string("workflow"), get: fn(m) {
      m.workflow
    })
    use run <- fields.include(fields.int("run"), get: fn(m) { m.run })
    fields.success(RunMetadata(workflow:, run:))
  }
  sinal.event(["saga", "run", "start"], measurements, metadata)
}

/// The `[saga, run, stop]` event descriptor.
pub fn run_stopped() -> Event(RunStopMeasurements, RunStopMetadata) {
  let measurements = {
    use duration <- fields.include(fields.int("duration"), get: fn(m) {
      m.duration
    })
    use undone <- fields.include(fields.int("undone"), get: fn(m) { m.undone })
    use undo_failures <- fields.include(fields.int("undo_failures"), get: fn(m) {
      m.undo_failures
    })
    use interrupted <- fields.include(fields.int("interrupted"), get: fn(m) {
      m.interrupted
    })
    fields.success(RunStopMeasurements(
      duration:,
      undone:,
      undo_failures:,
      interrupted:,
    ))
  }
  let metadata = {
    use workflow <- fields.include(fields.string("workflow"), get: fn(m) {
      m.workflow
    })
    use run <- fields.include(fields.int("run"), get: fn(m) { m.run })
    use outcome <- fields.include(
      fields.enum(
        "outcome",
        [OutcomeCompleted, OutcomeFailed, OutcomeCancelled, OutcomeUnresolved],
        outcome_kind_to_string,
      ),
      get: fn(m) { m.outcome },
    )
    fields.success(RunStopMetadata(workflow:, run:, outcome:))
  }
  sinal.event(["saga", "run", "stop"], measurements, metadata)
}

/// The `[saga, step, start]` event descriptor.
pub fn step_started() -> Event(StepStartMeasurements, StepMetadata) {
  let measurements = {
    use system_time <- fields.include(fields.int("system_time"), get: fn(m) {
      m.system_time
    })
    fields.success(StepStartMeasurements(system_time:))
  }
  let metadata = {
    use workflow <- fields.include(fields.string("workflow"), get: fn(m) {
      m.workflow
    })
    use run <- fields.include(fields.int("run"), get: fn(m) { m.run })
    use step <- fields.include(fields.string("step"), get: fn(m) { m.step })
    use attempt <- fields.include(fields.int("attempt"), get: fn(m) {
      m.attempt
    })
    fields.success(StepMetadata(workflow:, run:, step:, attempt:))
  }
  sinal.event(["saga", "step", "start"], measurements, metadata)
}

/// The `[saga, step, stop]` event descriptor.
pub fn step_stopped() -> Event(StepStopMeasurements, StepStopMetadata) {
  let metadata = {
    use workflow <- fields.include(fields.string("workflow"), get: fn(m) {
      m.workflow
    })
    use run <- fields.include(fields.int("run"), get: fn(m) { m.run })
    use step <- fields.include(fields.string("step"), get: fn(m) { m.step })
    use attempt <- fields.include(fields.int("attempt"), get: fn(m) {
      m.attempt
    })
    use result <- fields.include(
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
      get: fn(m) { m.result },
    )
    fields.success(StepStopMetadata(workflow:, run:, step:, attempt:, result:))
  }
  sinal.event(["saga", "step", "stop"], stop_measurements(), metadata)
}

/// The `[saga, step, compensate, stop]` event descriptor.
pub fn compensation_stopped() -> Event(
  StepStopMeasurements,
  CompensationMetadata,
) {
  let metadata = {
    use workflow <- fields.include(fields.string("workflow"), get: fn(m) {
      m.workflow
    })
    use run <- fields.include(fields.int("run"), get: fn(m) { m.run })
    use step <- fields.include(fields.string("step"), get: fn(m) { m.step })
    use attempt <- fields.include(fields.int("attempt"), get: fn(m) {
      m.attempt
    })
    use decision <- fields.include(
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
      get: fn(m) { m.decision },
    )
    fields.success(CompensationMetadata(
      workflow:,
      run:,
      step:,
      attempt:,
      decision:,
    ))
  }
  sinal.event(
    ["saga", "step", "compensate", "stop"],
    stop_measurements(),
    metadata,
  )
}

/// The `[saga, step, undo, stop]` event descriptor.
pub fn undo_stopped() -> Event(StepStopMeasurements, UndoMetadata) {
  let metadata = {
    use workflow <- fields.include(fields.string("workflow"), get: fn(m) {
      m.workflow
    })
    use run <- fields.include(fields.int("run"), get: fn(m) { m.run })
    use step <- fields.include(fields.string("step"), get: fn(m) { m.step })
    use result <- fields.include(
      fields.enum(
        "result",
        [UndoUndone, UndoFailedKind, UndoCrashedKind, UndoTimedOutKind],
        undo_kind_to_string,
      ),
      get: fn(m) { m.result },
    )
    fields.success(UndoMetadata(workflow:, run:, step:, result:))
  }
  sinal.event(["saga", "step", "undo", "stop"], stop_measurements(), metadata)
}

fn stop_measurements() -> fields.Fields(StepStopMeasurements) {
  use duration <- fields.include(fields.int("duration"), get: fn(m) {
    m.duration
  })
  fields.success(StepStopMeasurements(duration:))
}
