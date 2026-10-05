//// The mapping from Saga's report to a definite or uncertain tool result
//// (`saga/outcome`), one row of the module's table per
//// test, over reports built the way Saga builds them.

import gleam/string
import gleeunit/should
import saga
import saga/execution
import saga/outcome as verdict

fn at(name: String) -> saga.StepAddress {
  saga.StepAddress([], name, 1)
}

fn crash(reason: String) -> execution.UnknownEnding {
  execution.ActionCrashed(saga.Crash(saga.ErrorClass, reason))
}

fn effect(
  step: String,
  action: execution.Action,
  ending: execution.UnknownEnding,
) -> execution.UnknownEffect {
  execution.UnknownEffect(at(step), action, ending)
}

/// Every completed step undone, every action returned.
fn clean() -> execution.Settlement(String, String) {
  execution.Settlement(
    undone: [at("reserve")],
    undo_failures: [],
    not_undoable: [],
    held: [],
    interrupted: [],
    compensation_failures: [],
    sibling_failures: [],
    unknown_effects: [],
  )
}

fn with_unknown(
  effects: List(execution.UnknownEffect),
) -> execution.Settlement(String, String) {
  execution.Settlement(..clean(), unknown_effects: effects)
}

fn explain(error: String) -> String {
  "explained " <> error
}

fn failed(
  cause: execution.Cause(String),
  settlement: execution.Settlement(String, String),
) -> Result(String, verdict.Failure) {
  verdict.classify(execution.Failed(cause, settlement), explain)
}

fn uncertain(result: Result(a, verdict.Failure), mentions: String) -> Nil {
  let assert Error(verdict.Unknown(evidence)) = result
  string.contains(evidence, mentions) |> should.be_true
}

// --- Completed ----------------------------------------------------------------

/// `Completed` proves every action returned, whether or not a step had a
/// recovery decider.
pub fn a_completed_workflow_is_its_output_test() {
  verdict.classify(execution.Completed("out"), explain)
  |> should.equal(Ok("out"))
}

pub fn a_crash_retried_to_success_is_uncertain_test() {
  verdict.classify(
    execution.CompletedWithUnknownEffects("out", [
      effect("charge", execution.StepAttempt(1), crash("boom")),
    ]),
    explain,
  )
  |> uncertain("attempt 1 of step charge crashed")
}

// --- Failed -------------------------------------------------------------------

pub fn a_typed_error_with_a_clean_rollback_is_definite_test() {
  failed(execution.StepFailed(at("charge"), "declined"), clean())
  |> should.equal(Error(verdict.Definitely("explained declined")))
}

pub fn a_sibling_typed_error_is_definite_test() {
  let settlement =
    execution.Settlement(..clean(), sibling_failures: [
      execution.StepFailed(at("note"), "also"),
    ])
  failed(execution.StepFailed(at("reserve"), "full"), settlement)
  |> should.equal(Error(verdict.Definitely("explained full")))
}

pub fn typed_errors_after_retries_with_a_clean_rollback_are_definite_test() {
  failed(
    execution.RetryLimitReached(at("charge"), saga.Returned("declined")),
    clean(),
  )
  |> should.equal(Error(verdict.Definitely("explained declined")))
}

pub fn a_missed_deadline_with_everything_undone_is_definite_test() {
  failed(execution.DeadlineExceeded, clean())
  |> should.equal(Error(verdict.Definitely("the workflow missed its deadline")))
}

/// The output transform is not an action: it has no effect of its own, so
/// a crash of it with every step undone is definite.
pub fn an_output_crash_with_everything_undone_is_definite_test() {
  failed(execution.OutputCrashed(saga.Crash(saga.ErrorClass, "boom")), clean())
  |> should.equal(
    Error(verdict.Definitely("the workflow could not compute its output")),
  )
}

/// A decider that aborts after a crash is reported as `StepFailed`; the
/// crashed attempt is in the evidence.
pub fn a_crash_followed_by_abort_is_uncertain_test() {
  failed(
    execution.StepFailed(at("charge"), "declined"),
    with_unknown([effect("charge", execution.StepAttempt(1), crash("boom"))]),
  )
  |> uncertain("attempt 1 of step charge crashed")
}

/// A typed error that the step marks with `saga.unknown_when` says the
/// effect may have happened: Saga names the attempt with
/// `ActionReturnedUnknown`, so the typed error is not explained to the
/// model as a definite failure (SD-1: a refund the provider may have taken).
pub fn a_typed_error_marked_unknown_is_uncertain_test() {
  let result =
    failed(
      execution.StepFailed(at("refund"), "payment outcome unknown"),
      with_unknown([
        effect(
          "refund",
          execution.StepAttempt(1),
          execution.ActionReturnedUnknown,
        ),
      ]),
    )
  result
  |> uncertain(
    "attempt 1 of step refund returned an error after which its effect is unknown",
  )
  let assert Error(verdict.Unknown(evidence)) = result
  string.contains(evidence, "payment outcome unknown") |> should.be_false
}

pub fn a_marked_error_retried_to_success_is_uncertain_test() {
  verdict.classify(
    execution.CompletedWithUnknownEffects("out", [
      effect(
        "refund",
        execution.StepAttempt(1),
        execution.ActionReturnedUnknown,
      ),
    ]),
    explain,
  )
  |> uncertain(
    "attempt 1 of step refund returned an error after which its effect is unknown",
  )
}

pub fn a_sibling_crash_in_the_settle_window_is_uncertain_test() {
  let settlement =
    execution.Settlement(
      ..with_unknown([effect("note", execution.StepAttempt(1), crash("boom"))]),
      sibling_failures: [
        execution.StepCrashed(at("note"), saga.Crash(saga.ErrorClass, "boom")),
      ],
    )
  failed(execution.StepFailed(at("reserve"), "full"), settlement)
  |> uncertain("attempt 1 of step note crashed")
}

pub fn a_crashed_decision_is_uncertain_test() {
  failed(
    execution.StepFailed(at("charge"), "declined"),
    with_unknown([
      effect("charge", execution.StepCompensation(2), crash("boom")),
    ]),
  )
  |> uncertain("the recovery decision on attempt 2 of step charge crashed")
}

pub fn a_timed_out_decision_is_uncertain_test() {
  failed(
    execution.StepFailed(at("charge"), "declined"),
    with_unknown([
      effect("charge", execution.StepCompensation(1), execution.ActionTimedOut),
    ]),
  )
  |> uncertain("the recovery decision on attempt 1 of step charge timed out")
}

pub fn a_crashed_undo_is_uncertain_test() {
  failed(
    execution.StepFailed(at("charge"), "declined"),
    with_unknown([effect("reserve", execution.StepUndo, crash("boom"))]),
  )
  |> uncertain("the undo of step reserve crashed")
}

pub fn a_timed_out_undo_is_uncertain_test() {
  failed(
    execution.StepFailed(at("charge"), "declined"),
    with_unknown([
      effect("reserve", execution.StepUndo, execution.ActionTimedOut),
    ]),
  )
  |> uncertain("the undo of step reserve timed out")
}

pub fn a_typed_undo_error_is_uncertain_test() {
  let settlement =
    execution.Settlement(..clean(), undone: [], undo_failures: [
      execution.UndoFailed(at("reserve"), "refused"),
    ])
  failed(execution.StepFailed(at("charge"), "declined"), settlement)
  |> uncertain("not undone reserve")
}

pub fn a_typed_cleanup_error_is_uncertain_test() {
  let settlement =
    execution.Settlement(..clean(), compensation_failures: [
      execution.CleanupFailed(at("charge"), "refused"),
    ])
  failed(execution.StepFailed(at("charge"), "declined"), settlement)
  |> uncertain("not cleaned up charge")
}

pub fn a_step_without_an_undo_is_uncertain_test() {
  let settlement = execution.Settlement(..clean(), not_undoable: [at("note")])
  failed(execution.StepFailed(at("charge"), "declined"), settlement)
  |> uncertain("without an undo note")
}

// --- Cancelled ----------------------------------------------------------------

pub fn a_cancellation_that_undid_everything_is_definite_test() {
  verdict.classify(
    execution.Cancelled(execution.CancelRequested, clean()),
    explain,
  )
  |> should.equal(
    Error(verdict.Definitely(
      "the workflow was cancelled; every completed step was undone",
    )),
  )
}

pub fn an_interrupted_step_is_uncertain_test() {
  let settlement =
    execution.Settlement(
      ..with_unknown([
        effect("note", execution.StepAttempt(1), execution.ActionInterrupted),
      ]),
      interrupted: [at("note")],
    )
  verdict.classify(
    execution.Cancelled(execution.CancelRequested, settlement),
    explain,
  )
  |> uncertain("attempt 1 of step note was interrupted")
}

pub fn a_cancellation_that_left_a_step_held_is_uncertain_test() {
  let settlement = execution.Settlement(..clean(), held: [at("charge")])
  verdict.classify(
    execution.Cancelled(execution.OwnerExited, settlement),
    explain,
  )
  |> uncertain("held charge")
}

// --- Unresolved ---------------------------------------------------------------

/// The evidence names the held step and renders the error it held its
/// effects on with `explain`; Saga's report stays free of it.
pub fn an_unresolved_workflow_is_uncertain_test() {
  let settlement =
    execution.Settlement(..clean(), undone: [], held: [at("charge")])
  let assert Error(verdict.Unknown(evidence)) =
    verdict.classify(
      execution.Unresolved(at("charge"), "hold", settlement),
      explain,
    )
  evidence
  |> should.equal(
    "the workflow's effects are not known: the workflow held the effects of step charge unresolved: explained hold; held charge (Saga reported unresolved at charge)",
  )
}

/// A recovery decision's `Hold(evidence)` ends the run `Unresolved` with
/// that evidence, which `explain` renders after the unknown effects.
pub fn a_held_decision_renders_its_evidence_test() {
  let settlement =
    execution.Settlement(
      ..with_unknown([
        effect(
          "refund",
          execution.StepAttempt(1),
          execution.ActionReturnedUnknown,
        ),
      ]),
      undone: [],
      held: [at("refund")],
    )
  let assert Error(verdict.Unknown(evidence)) =
    verdict.classify(
      execution.Unresolved(at("refund"), "provider timeout", settlement),
      explain,
    )
  string.contains(
    evidence,
    "held the effects of step refund unresolved: explained provider timeout",
  )
  |> should.be_true
  string.contains(
    evidence,
    "attempt 1 of step refund returned an error after which its effect is unknown",
  )
  |> should.be_true
  verdict.summary(execution.Unresolved(
    at("refund"),
    "provider timeout",
    settlement,
  ))
  |> string.contains("provider timeout")
  |> should.be_false
}

// --- evidence -----------------------------------------------------------------

/// The evidence names kinds, actions and steps only: a typed error or a
/// crash reason may carry application data.
pub fn the_evidence_carries_no_application_data_test() {
  let settlement =
    execution.Settlement(
      ..with_unknown([
        effect("note", execution.StepAttempt(1), crash("secret")),
        effect("reserve", execution.StepUndo, crash("secret")),
      ]),
      undo_failures: [
        execution.UndoCrashed(
          at("reserve"),
          saga.Crash(saga.ErrorClass, "secret"),
        ),
      ],
      sibling_failures: [
        execution.StepCrashed(at("note"), saga.Crash(saga.ErrorClass, "secret")),
      ],
    )
  let assert Error(verdict.Unknown(evidence)) =
    failed(execution.StepFailed(at("charge"), "secret"), settlement)
  string.contains(evidence, "secret") |> should.be_false
  let assert Error(verdict.Unknown(evidence)) =
    verdict.classify(
      execution.CompletedWithUnknownEffects("secret", [
        effect("charge", execution.StepAttempt(1), crash("secret")),
      ]),
      explain,
    )
  string.contains(evidence, "secret") |> should.be_false
}

pub fn outcome_kind_and_held_steps_are_actionable_without_rendering_errors_test() {
  verdict.kind(execution.Completed("answer")) |> should.equal(verdict.Completed)
  verdict.kind(execution.Failed(
    execution.StepFailed(at("pay"), "declined"),
    clean(),
  ))
  |> should.equal(verdict.Compensated)
  let unresolved =
    execution.Unresolved(
      at("pay"),
      "private error",
      execution.Settlement(..clean(), held: [at("pay")]),
    )
  verdict.kind(unresolved) |> should.equal(verdict.Unresolved)
  verdict.held_steps(unresolved) |> should.equal([at("pay")])
  let assert Error(error) = verdict.classify(unresolved, explain)
  verdict.failure_kind(error) |> should.equal(verdict.Unresolved)
  verdict.describe_failure(error)
  |> string.contains("explained private error")
  |> should.be_true
}
