/// Run lifecycle for a `saga.Workflow`: config and its validation, starting
/// a run, waiting for or inspecting its outcome, and cancellation.
///
/// Increment 1 scope: `run`/`start`/`await`/`progress` are implemented with
/// bounded concurrency, retry/compensation, and reverse-order undo.
/// `cancel` is accepted (idempotent, returns immediately) but does not yet
/// interrupt a running attempt — cancellation settlement of active siblings
/// is increment 2, along with deadlines, per-step timeouts, and Sinal
/// observations. `Config`'s `deadline`/`settle_timeout`/`cleanup_timeout`
/// fields exist now for API stability and are validated, but are not yet
/// enforced by the coordinator.
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import saga.{type Workflow}
import saga/internal/coordinator

/// Bounds and pacing for one run.
pub type Config {
  Config(
    max_concurrency: Int,
    deadline: Option(Int),
    settle_timeout: Int,
    cleanup_timeout: Int,
  )
}

/// Sensible defaults: one attempt/compensation task per scheduler, no run
/// deadline, a 5 second settle window, and a 5 second cleanup bound.
pub fn config() -> Config {
  Config(
    max_concurrency: schedulers_online(),
    deadline: None,
    settle_timeout: 5000,
    cleanup_timeout: 5000,
  )
}

@external(erlang, "saga_ffi", "schedulers_online")
fn schedulers_online() -> Int

/// A single violated `Config` invariant. `validate` collects every one that
/// applies, not just the first.
pub type ConfigError {
  MaxConcurrencyNotPositive(value: Int)
  DeadlineNotPositive(value: Int)
  SettleTimeoutNegative(value: Int)
  CleanupTimeoutNotPositive(value: Int)
}

/// Validates a complete `Config` value before any process is started.
/// Collects every violation, not just the first.
pub fn validate(config: Config) -> Result(Config, List(ConfigError)) {
  let errors =
    list.flatten([
      case config.max_concurrency > 0 {
        True -> []
        False -> [MaxConcurrencyNotPositive(config.max_concurrency)]
      },
      case config.deadline {
        None -> []
        Some(ms) if ms > 0 -> []
        Some(ms) -> [DeadlineNotPositive(ms)]
      },
      case config.settle_timeout >= 0 {
        True -> []
        False -> [SettleTimeoutNegative(config.settle_timeout)]
      },
      case config.cleanup_timeout > 0 {
        True -> []
        False -> [CleanupTimeoutNotPositive(config.cleanup_timeout)]
      },
    ])
  case errors {
    [] -> Ok(config)
    _ -> Error(errors)
  }
}

/// Why `run`/`start` never began a run at all.
pub type RunError {
  InvalidConfig(errors: List(ConfigError))
  ExecutionLost(crash: Crash)
}

pub type Crash =
  saga.Crash

pub type StepAddress =
  saga.StepAddress

/// The run-ending cause behind a `Failed` or a sibling failure recorded in
/// a `Settlement`. Keeps an application failure (`StepFailed`), an
/// execution failure (`StepCrashed`/`StepTimedOut`), retry exhaustion, an
/// output-transform crash, and a nondeterministic builder as distinct
/// variants, so no report can conflate them.
pub type Cause(e) {
  StepFailed(step: StepAddress, error: e)
  StepCrashed(step: StepAddress, crash: Crash)
  StepTimedOut(step: StepAddress)
  RetryLimitReached(step: StepAddress, last: saga.AttemptFailure(e))
  OutputCrashed(crash: Crash)
  DefinitionChanged
}

/// One completed step's undo did not succeed, during rollback.
pub type UndoFailure(u) {
  UndoFailed(step: StepAddress, error: u)
  UndoCrashed(step: StepAddress, crash: Crash)
}

/// A compensation decision itself failed to produce a clean outcome.
pub type CompensationFailure(u) {
  CleanupFailed(step: StepAddress, error: u)
  CompensationCrashed(step: StepAddress, crash: Crash)
}

/// What happened to every step once a run stopped admitting new work:
/// which were undone (in reverse completion order), which undo actions
/// failed (all retained, not just the first), which had no undo configured,
/// which were held by an unresolved `Hold`, and which sibling failures
/// settled after the primary cause.
pub type Settlement(e, u) {
  Settlement(
    undone: List(StepAddress),
    undo_failures: List(UndoFailure(u)),
    not_undoable: List(StepAddress),
    held: List(StepAddress),
    compensation_failures: List(CompensationFailure(u)),
    sibling_failures: List(Cause(e)),
  )
}

/// A run's terminal result: success, a failure with its settlement, or an
/// unresolved `Hold` that left completed effects untouched.
pub type Outcome(o, e, u) {
  Completed(output: o)
  Failed(cause: Cause(e), settlement: Settlement(e, u))
  Unresolved(step: StepAddress, evidence: e, settlement: Settlement(e, u))
}

/// The run's current admission phase.
pub type Phase {
  Running
  RollingBack
}

/// One step's current state, as reported by `progress`.
pub type StepState {
  Waiting
  Attempting(attempt: Int)
  Compensating(attempt: Int)
  RetryScheduled(next_attempt: Int)
  Succeeded
  FailedStep
  Undoing
  Undone
  UndoFailedStep
  Skipped
}

pub type StepProgress {
  StepProgress(address: StepAddress, state: StepState)
}

/// A read-only snapshot of a run: its phase and every step's state. Never
/// exposes values or executable closures.
pub type Progress {
  Progress(run_id: Int, phase: Phase, steps: List(StepProgress))
}

pub type ProgressError {
  ProgressTimedOut
  ExecutionEnded
}

/// Why `await` did not return an `Outcome`.
pub type AwaitError {
  AwaitTimedOut
  NotOwner
  AlreadyAwaited
  Lost(crash: Crash)
}

/// A started run. Only the process that called `start` may `await` it.
///
/// Increment 1: `AwaitError.AlreadyAwaited` is part of the public shape but
/// is not yet distinguished from `AwaitTimedOut` after the outcome message
/// has already been consumed — tracking that explicitly is increment 2.
pub opaque type Execution(o, e, u) {
  Execution(
    pid: Pid,
    run_id: Int,
    owner: Pid,
    control: Subject(coordinator.Control(o, e, u)),
    result: Subject(coordinator.Outcome(o, e, u)),
  )
}

/// Runs `workflow` with `input` to completion, validating `config` first.
/// This is the ordinary path: it blocks until the run finishes.
pub fn run(
  workflow: Workflow(i, o, e, u),
  input: i,
  config: Config,
) -> Result(Outcome(o, e, u), RunError) {
  case start(workflow, input, config) {
    Error(errors) -> Error(InvalidConfig(errors))
    Ok(execution) ->
      case await_forever(execution) {
        Ok(outcome) -> Ok(outcome)
        Error(Lost(crash)) -> Error(ExecutionLost(crash))
        Error(_other) ->
          // await_forever never produces AwaitTimedOut (no timeout was
          // given), NotOwner (the same process that started also awaits),
          // or AlreadyAwaited (this is the first and only await).
          panic as "saga: unreachable await error from the owning process"
      }
  }
}

/// Starts a run without blocking, returning an `Execution` handle. Only the
/// calling process may `await` it.
pub fn start(
  workflow: Workflow(i, o, e, u),
  input: i,
  config: Config,
) -> Result(Execution(o, e, u), List(ConfigError)) {
  use validated <- result_try(validate(config))
  let owner = process.self()
  let result_subject = process.new_subject()
  let control_subject_out = process.new_subject()
  let #(pid, run_id) =
    coordinator.start(
      max_concurrency: validated.max_concurrency,
      build_graph: fn() { saga.for_run(workflow, input) },
      result_subject: result_subject,
      control_subject_out: control_subject_out,
    )
  let assert Ok(control) = process.receive(control_subject_out, 5000)
  Ok(Execution(
    pid: pid,
    run_id: run_id,
    owner: owner,
    control: control,
    result: result_subject,
  ))
}

fn result_try(
  result: Result(a, err),
  f: fn(a) -> Result(b, err),
) -> Result(b, err) {
  case result {
    Ok(value) -> f(value)
    Error(error) -> Error(error)
  }
}

/// Waits up to `milliseconds` for the run's outcome. Returns
/// `Error(AwaitTimedOut)` on timeout — the run continues, and `await` may
/// be called again.
pub fn await(
  execution: Execution(o, e, u),
  timeout milliseconds: Int,
) -> Result(Outcome(o, e, u), AwaitError) {
  case process.self() == execution.owner {
    False -> Error(NotOwner)
    True ->
      case process.receive(execution.result, milliseconds) {
        Ok(outcome) -> Ok(to_public_outcome(outcome))
        Error(_) -> Error(AwaitTimedOut)
      }
  }
}

fn await_forever(
  execution: Execution(o, e, u),
) -> Result(Outcome(o, e, u), AwaitError) {
  case process.self() == execution.owner {
    False -> Error(NotOwner)
    True -> Ok(to_public_outcome(process.receive_forever(execution.result)))
  }
}

/// Requests cancellation. Returns immediately; the request is idempotent.
/// Increment 1: accepted but not yet enforced (no in-flight attempt is
/// interrupted). Increment 2 adds settlement of active siblings.
pub fn cancel(execution: Execution(o, e, u)) -> Nil {
  process.send(execution.control, coordinator.CancelRequest)
}

/// Synchronously inspects the run's current phase and per-step states.
pub fn progress(
  execution: Execution(o, e, u),
  timeout milliseconds: Int,
) -> Result(Progress, ProgressError) {
  let reply = process.new_subject()
  process.send(execution.control, coordinator.ProgressRequest(reply))
  case process.receive(reply, milliseconds) {
    Ok(progress) -> Ok(to_public_progress(progress))
    Error(_) -> Error(ProgressTimedOut)
  }
}

/// The coordinator's pid, for monitoring.
pub fn pid(execution: Execution(o, e, u)) -> Pid {
  execution.pid
}

/// The run's id, matching what Sinal observation metadata will use.
pub fn run_id(execution: Execution(o, e, u)) -> Int {
  execution.run_id
}

// ---------------------------------------------------------------------------
// Internal <-> public type translation
// ---------------------------------------------------------------------------

fn to_public_outcome(
  outcome: coordinator.Outcome(o, e, u),
) -> Outcome(o, e, u) {
  case outcome {
    coordinator.Completed(output) -> Completed(output)
    coordinator.Failed(cause, settlement) ->
      Failed(to_public_cause(cause), to_public_settlement(settlement))
    coordinator.Unresolved(step, evidence, settlement) ->
      Unresolved(
        saga.address_from_node(step),
        evidence,
        to_public_settlement(settlement),
      )
  }
}

fn to_public_cause(cause: coordinator.Cause(e)) -> Cause(e) {
  case cause {
    coordinator.StepFailed(step, error) ->
      StepFailed(saga.address_from_node(step), error)
    coordinator.StepCrashed(step, crash) ->
      StepCrashed(saga.address_from_node(step), saga.crash_from_node(crash))
    coordinator.StepTimedOut(step) -> StepTimedOut(saga.address_from_node(step))
    coordinator.RetryLimitReached(step, last) ->
      RetryLimitReached(
        saga.address_from_node(step),
        saga.failure_from_node(last),
      )
    coordinator.OutputCrashed(crash) ->
      OutputCrashed(saga.crash_from_node(crash))
    coordinator.DefinitionChanged -> DefinitionChanged
  }
}

fn to_public_settlement(
  settlement: coordinator.Settlement(e, u),
) -> Settlement(e, u) {
  Settlement(
    undone: list.map(settlement.undone, saga.address_from_node),
    undo_failures: list.map(settlement.undo_failures, to_public_undo_failure),
    not_undoable: list.map(settlement.not_undoable, saga.address_from_node),
    held: list.map(settlement.held, saga.address_from_node),
    compensation_failures: list.map(
      settlement.compensation_failures,
      to_public_compensation_failure,
    ),
    sibling_failures: list.map(settlement.sibling_failures, to_public_cause),
  )
}

fn to_public_undo_failure(
  failure: coordinator.UndoFailure(u),
) -> UndoFailure(u) {
  case failure {
    coordinator.UndoFailed(step, error) ->
      UndoFailed(saga.address_from_node(step), error)
    coordinator.UndoCrashed(step, crash) ->
      UndoCrashed(saga.address_from_node(step), saga.crash_from_node(crash))
  }
}

fn to_public_compensation_failure(
  failure: coordinator.CompensationFailure(u),
) -> CompensationFailure(u) {
  case failure {
    coordinator.CleanupFailed(step, error) ->
      CleanupFailed(saga.address_from_node(step), error)
    coordinator.CompensationCrashed(step, crash) ->
      CompensationCrashed(
        saga.address_from_node(step),
        saga.crash_from_node(crash),
      )
  }
}

fn to_public_progress(progress: coordinator.Progress) -> Progress {
  Progress(
    run_id: progress.run_id,
    phase: case progress.phase {
      coordinator.Running -> Running
      coordinator.RollingBack -> RollingBack
    },
    steps: list.map(progress.steps, fn(sp) {
      StepProgress(
        address: saga.address_from_node(sp.address),
        state: to_public_step_state(sp.state),
      )
    }),
  )
}

fn to_public_step_state(state: coordinator.StepState) -> StepState {
  case state {
    coordinator.Waiting -> Waiting
    coordinator.Attempting(attempt) -> Attempting(attempt)
    coordinator.Compensating(attempt) -> Compensating(attempt)
    coordinator.RetryScheduled(next_attempt) -> RetryScheduled(next_attempt)
    coordinator.Succeeded -> Succeeded
    coordinator.FailedStep -> FailedStep
    coordinator.Undoing -> Undoing
    coordinator.Undone -> Undone
    coordinator.UndoFailedStep -> UndoFailedStep
    coordinator.Skipped -> Skipped
  }
}
