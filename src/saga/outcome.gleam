//// What a Saga outcome proves about the workflow's effects.
////
//// A result is definite only when Saga's report proves that every effect
//// of the run is known and none is left in place. Saga records every step
//// attempt, recovery decision and undo that ended without a result (it
//// crashed or its process exited, it was killed at its time bound, or it
//// was killed when the settle window closed) as an `UnknownEffect`, when
//// it ended, whatever was decided afterwards. It records an attempt that
//// returned a typed error its step marks with `saga.unknown_when` the same
//// way (`ActionReturnedUnknown`): the error says the effect may have
//// happened, such as a refund the provider may have taken, so the call is
//// uncertain, never a definite failure. `execution.unknown_effects` is `[]`
//// exactly when every action returned `Ok` or an unmarked typed error. A
//// typed error of an undo or of a recovery decision's cleanup, a step with
//// no undo, and a held step are known effects left in place, reported by
//// the settlement.
////
//// | Saga outcome | Result |
//// | --- | --- |
//// | `Completed(output)` | the output: every action returned |
//// | `Failed(StepFailed(_, error))` or `Failed(RetryLimitReached(_, Returned(error)))`, no unknown effect, nothing left in place | definite: `explain(error)` |
//// | `Failed(DeadlineExceeded)`, with the same conditions | definite: the workflow missed its deadline |
//// | `Failed(OutputCrashed(_))`, with the same conditions | definite: the workflow could not compute its output |
//// | `Cancelled`, with the same conditions | definite: cancelled, every completed step undone |
//// | `CompletedWithUnknownEffects`, `Unresolved`, a `Failed` or `Cancelled` with an unknown effect or an effect left in place, a cause that is itself a crash or timeout | uncertain, with a summary of Saga's report |
////
//// `OutputCrashed` is definite when nothing else is uncertain: the output
//// transform (`saga.map`) is not an action and has no effect of its own,
//// and Saga rolls back every completed step after it crashed. A cause that
//// is a crash or timeout (`StepCrashed`, `StepTimedOut`, a retry cause
//// whose last attempt crashed or timed out) names an action that Saga also
//// lists as an unknown effect; it is uncertain on its own too, since it
//// carries no typed error to explain. `RetrySuperseded(_, Returned(error))`
//// is judged like `RetryLimitReached`.
////
//// `Unresolved` carries the error the step held its effects on (the error
//// `saga.unknown_when` marked, or a recovery decision's `Hold(evidence)`).
//// The evidence renders it with `explain`, the application's own wording,
//// after the held step: `the workflow held the effects of step refund
//// unresolved: <explain(error)>`, so the person who reconciles the call
//// reads what the step reported.
////
//// Saga's report (`summary`) names outcome kinds, actions and step
//// addresses only: never a step's typed error, output, or crash reason,
//// which may carry application data. `explain` renders the held error
//// alone; no other typed error reaches the evidence.

import gleam/int
import gleam/list
import gleam/string
import saga.{type StepAddress}
import saga/execution

/// Why the workflow did not produce its output.
pub type Failure {
  /// Nothing the workflow did is left in place; a caller may retry deliberately.
  Definitely(message: String)
  /// An effect may be left in place or be of unknown status.
  Unknown(evidence: String)
}

/// The caller result that `outcome` proves.
pub fn classify(
  outcome: execution.Outcome(output, error, undo_error),
  explain: fn(error) -> String,
) -> Result(output, Failure) {
  let report = summary(outcome)
  let effects = unknown_effects(execution.unknown_effects(outcome))
  case outcome {
    execution.Completed(output) -> Ok(output)
    execution.CompletedWithUnknownEffects(..) -> Error(unknown(effects, report))
    execution.Unresolved(step, evidence, settlement) ->
      Error(unknown(
        [
          "the workflow held the effects of step "
            <> saga.address_to_string(step)
            <> " unresolved: "
            <> explain(evidence),
          ..list.append(effects, left_in_place(settlement))
        ],
        report,
      ))
    execution.Failed(cause, settlement) ->
      stopped(failure(cause, explain), effects, settlement, report)
    execution.Cancelled(_, settlement) ->
      stopped(
        Ok("the workflow was cancelled; every completed step was undone"),
        effects,
        settlement,
        report,
      )
  }
}

/// A stopped run is definite when its cause has a `message` and nothing is
/// unknown or left in place.
fn stopped(
  message: Result(String, Nil),
  effects: List(String),
  settlement: execution.Settlement(error, undo_error),
  report: String,
) -> Result(output, Failure) {
  case message, list.append(effects, left_in_place(settlement)) {
    Ok(message), [] -> Error(Definitely(message))
    _, reasons -> Error(unknown(reasons, report))
  }
}

/// What the caller is told a known cause was; `Error(Nil)` for a cause that
/// is itself a crash or timeout.
fn failure(
  cause: execution.Cause(error),
  explain: fn(error) -> String,
) -> Result(String, Nil) {
  case cause {
    execution.StepFailed(_, error)
    | execution.RetryLimitReached(_, saga.Returned(error))
    | execution.RetrySuperseded(_, saga.Returned(error)) -> Ok(explain(error))
    execution.DeadlineExceeded -> Ok("the workflow missed its deadline")
    execution.OutputCrashed(_) ->
      Ok("the workflow could not compute its output")
    execution.StepCrashed(..)
    | execution.StepTimedOut(_)
    | execution.RetryLimitReached(_, saga.Crashed(_))
    | execution.RetryLimitReached(_, saga.TimedOut)
    | execution.RetrySuperseded(_, saga.Crashed(_))
    | execution.RetrySuperseded(_, saga.TimedOut) -> Error(Nil)
  }
}

fn unknown(reasons: List(String), report: String) -> Failure {
  Unknown(
    "the workflow's effects are not known: "
    <> string.join(reasons, "; ")
    <> " (Saga reported "
    <> report
    <> ")",
  )
}

/// Each action that ended without a result, as one reason; `[]` when every
/// action returned.
fn unknown_effects(effects: List(execution.UnknownEffect)) -> List(String) {
  case effects {
    [] -> []
    effects -> [
      "these actions ended without a result, so their effect is unknown: "
      <> string.join(list.map(effects, describe_effect), ", "),
    ]
  }
}

fn describe_effect(effect: execution.UnknownEffect) -> String {
  let step = saga.address_to_string(effect.step)
  let action = case effect.action {
    execution.StepAttempt(n) ->
      "attempt " <> int.to_string(n) <> " of step " <> step
    execution.StepCompensation(n) ->
      "the recovery decision on attempt "
      <> int.to_string(n)
      <> " of step "
      <> step
    execution.StepUndo -> "the undo of step " <> step
  }
  action
  <> case effect.ending {
    execution.ActionCrashed(_) -> " crashed"
    execution.ActionTimedOut -> " timed out"
    execution.ActionInterrupted -> " was interrupted"
    execution.ActionReturnedUnknown ->
      " returned an error after which its effect is unknown"
  }
}

/// The known effects the settlement left in place.
fn left_in_place(
  settlement: execution.Settlement(error, undo_error),
) -> List(String) {
  let not_undone =
    list.filter_map(settlement.undo_failures, fn(failure) {
      case failure {
        execution.UndoFailed(step, _) -> Ok(step)
        // Unknown effects, named by `unknown_effects`.
        execution.UndoCrashed(..) | execution.UndoTimedOut(_) -> Error(Nil)
      }
    })
  let not_cleaned_up =
    list.filter_map(settlement.compensation_failures, fn(failure) {
      case failure {
        execution.CleanupFailed(step, _) -> Ok(step)
        execution.CompensationCrashed(..) | execution.CompensationTimedOut(_) ->
          Error(Nil)
      }
    })
  [
    #("not undone", not_undone),
    #("not cleaned up", not_cleaned_up),
    #("held", settlement.held),
    #("without an undo", settlement.not_undoable),
  ]
  |> list.filter_map(fn(entry) {
    case entry.1 {
      [] -> Error(Nil)
      steps -> Ok(entry.0 <> " " <> addresses(steps))
    }
  })
}

/// Saga's report without application data: the outcome's kind, its cause's
/// kind and step, and the settlement's steps.
pub fn summary(
  outcome: execution.Outcome(output, error, undo_error),
) -> String {
  case outcome {
    execution.Completed(_) -> "completed"
    execution.CompletedWithUnknownEffects(_, effects) ->
      "completed with unknown effects: "
      <> string.join(list.map(effects, describe_effect), ", ")
    execution.Failed(cause, settlement) ->
      "failed: " <> describe_cause(cause) <> settled(settlement)
    execution.Cancelled(reason, settlement) ->
      "cancelled ("
      <> case reason {
        execution.CancelRequested -> "requested"
        execution.OwnerExited -> "owner exited"
      }
      <> ")"
      <> settled(settlement)
    execution.Unresolved(step, _, settlement) ->
      "unresolved at " <> saga.address_to_string(step) <> settled(settlement)
  }
}

fn describe_cause(cause: execution.Cause(error)) -> String {
  case cause {
    execution.StepFailed(step, _) ->
      "typed error of " <> saga.address_to_string(step)
    execution.StepCrashed(step, _) ->
      "crash of " <> saga.address_to_string(step)
    execution.StepTimedOut(step) ->
      "timeout of " <> saga.address_to_string(step)
    execution.RetryLimitReached(step, _) ->
      "retry limit of " <> saga.address_to_string(step)
    execution.RetrySuperseded(step, _) ->
      "retry superseded of " <> saga.address_to_string(step)
    execution.OutputCrashed(_) -> "output crash"
    execution.DeadlineExceeded -> "deadline exceeded"
  }
}

fn settled(settlement: execution.Settlement(error, undo_error)) -> String {
  [
    #("undone", settlement.undone),
    #(
      "sibling failures",
      list.filter_map(settlement.sibling_failures, fn(cause) {
        case cause {
          execution.StepFailed(step, _)
          | execution.StepCrashed(step, _)
          | execution.StepTimedOut(step)
          | execution.RetryLimitReached(step, _)
          | execution.RetrySuperseded(step, _) -> Ok(step)
          execution.OutputCrashed(_) | execution.DeadlineExceeded -> Error(Nil)
        }
      }),
    ),
  ]
  |> list.filter_map(fn(entry) {
    case entry.1 {
      [] -> Error(Nil)
      steps -> Ok("; " <> entry.0 <> " " <> addresses(steps))
    }
  })
  |> string.concat
}

fn addresses(steps: List(StepAddress)) -> String {
  string.join(list.map(steps, saga.address_to_string), ", ")
}

/// Stable classification of the evidence an execution retained.
pub type Kind {
  Completed
  Compensated
  Unresolved
}

/// Completion is successful only without unknown effects; compensation means
/// that every effect is known and no effect remains in place.
pub fn kind(outcome: execution.Outcome(o, e, u)) -> Kind {
  case classify(outcome, fn(_) { "" }) {
    Ok(_) -> Completed
    Error(Definitely(_)) -> Compensated
    Error(Unknown(_)) -> Unresolved
  }
}

/// Stable classification of a stopped execution.
pub fn failure_kind(failure: Failure) -> Kind {
  case failure {
    Definitely(_) -> Compensated
    Unknown(_) -> Unresolved
  }
}

pub fn describe_failure(failure: Failure) -> String {
  case failure {
    Definitely(message) -> message
    Unknown(evidence) -> evidence
  }
}

/// Steps whose effects are explicitly held, in the execution's own addresses.
/// Other unresolved evidence is retained by `execution.unknown_effects` and
/// the settlement; `summary` describes every category without application data.
pub fn held_steps(outcome: execution.Outcome(o, e, u)) -> List(StepAddress) {
  case outcome {
    execution.Unresolved(step, _, settlement) ->
      list.unique([step, ..settlement.held])
    execution.Failed(_, settlement) | execution.Cancelled(_, settlement) ->
      settlement.held
    execution.Completed(_) | execution.CompletedWithUnknownEffects(..) -> []
  }
}
