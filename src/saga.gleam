//// Defines typed saga workflows: steps, their undo and compensation, the
//// typed ports that connect them, and validation of the whole graph.
////
//// Use this module to describe a workflow once, then run it with
//// `saga/execution` (in memory) or `saga/durable` (with saved checkpoints).
//// A `Workflow(input, output, error, undo_error)` is a pure description:
//// `define` evaluates its builder exactly once, validates step names,
//// attempt budgets, timeouts and port ownership, and records the static
//// step graph. No run evaluates the builder again, so one `Workflow` value
//// serves every run, each with its own input.
////
//// Dependencies are typed `Port` values, not names: `perform` schedules a
//// step on a port and returns a port for its output, and `map`, `both`,
//// `all` and `choose` combine ports. The compiler checks the wiring.
////
//// A step returns `Ok(output)` or a typed `Error(error)`. `effect` gives a
//// step a stable `EffectKey` to send downstream as an idempotency key.
//// `undo` registers the action that reverses a completed step when a later
//// step fails; `compensate` decides, after a failed attempt, whether to
//// retry, continue with a substitute output, abort, or hold; `unknown_when`
//// names the returned errors after which the step's effect is unknown, and
//// such an error holds the run for reconciliation unless `on_unknown`
//// opts into rollback.
//// `embed` and `map_errors` compose one workflow into another.
//// `saga/durable` adds what a step needs to recover after a restart.
////
//// ```gleam
//// import saga
//// import saga/execution
////
//// pub fn checkout() {
////   saga.define("checkout", fn(order) {
////     order
////     |> saga.perform(
////       saga.step("reserve_inventory", reserve)
////       |> saga.undo(fn(undo) { release(undo.output) }),
////     )
////     |> saga.perform(
////       saga.effect("charge_payment", fn(reservation, key) {
////         charge(reservation, idempotency_key: key.idempotency)
////       })
////       |> saga.unknown_when(is_maybe_sent),
////     )
////   })
//// }
////
//// pub fn run_checkout(workflow, order: Order) {
////   execution.run(workflow, order, execution.config())
//// }
//// ```

import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set.{type Set}
import gleam/string
import gleam/time/duration.{type Duration}
import saga/codec.{type Codec}
import saga/internal/cell
import saga/internal/checkpoint
import saga/internal/ffi
import saga/internal/node.{
  type ErasedRecovery, type Evidence, type Node, AttemptSucceeded,
  EvidenceCompleted, EvidenceFailed, EvidenceMaybeSent, EvidenceNotSent, Node,
}
import saga/internal/store.{type Store}
import sinal/correlation.{type Correlation}

/// A step's recorded location: nested scope (from `embed` and `choose`), a
/// name, and the 1-based occurrence rank among steps sharing the same scope
/// and name.
pub type StepAddress {
  StepAddress(scope: List(String), name: String, occurrence: Int)
}

/// The context of one action of a step, for external systems: its keys and
/// the correlation of its run.
///
/// - `idempotency` is the same for every attempt of the step within one run
///   of a local workflow, or one durable execution across restarts. Send it
///   as the downstream idempotency key, so a retry after an unknown outcome
///   cannot repeat the effect.
/// - `attempt` is the 1-based attempt number.
/// - `attempt_key` is unique to this attempt; use it to record or look up
///   one attempt.
/// - `correlation` is the correlation of the run that performs the action:
///   the one set with `execution.with_correlation` or
///   `durable.with_correlation`, which is also the `correlation` of the run's
///   `saga/telemetry` events. A durable execution without one carries
///   `correlation.from_key(id)` of its execution id, so it is never `None`
///   there. A local run without one has `None`. Pass it on to the clients the
///   step calls, for example `http_gun.with_correlation`, so their events
///   join the run's.
///
/// A local run derives `idempotency` from its run id, which is new for every
/// `execution.run`; a durable execution derives it from the id given to
/// `durable.start_or_reconnect`, so it survives restarts. The text of a key
/// is opaque: compare and store it, never parse it.
///
/// Saga builds this record; read its fields by label. `saga.step` receives
/// only the step's input: use `saga.effect` for a step that needs the
/// context. `undo` and `compensate` receive it as `key`.
pub type EffectKey {
  EffectKey(
    idempotency: String,
    attempt: Int,
    attempt_key: String,
    correlation: Option(Correlation),
  )
}

/// What an undo action receives: the step's input, the output that
/// succeeded, and the undo action's own `EffectKey`.
pub type UndoRequest(i, o) {
  UndoRequest(input: i, output: o, key: EffectKey)
}

/// What a `compensate` decider receives about a failed attempt: the step's
/// input, why the attempt failed, its 1-based number, how many further
/// attempts the budget allows, and the failed attempt's `EffectKey`.
pub type FailedAttempt(i, e) {
  FailedAttempt(
    input: i,
    failure: AttemptFailure(e),
    attempt: Int,
    attempts_left: Int,
    key: EffectKey,
  )
}

/// Why one attempt did not succeed: the step's own `run` returned an
/// application error, the attempt task crashed, or it timed out. The union
/// is closed.
pub type AttemptFailure(e) {
  Returned(error: e)
  Crashed(crash: Crash)
  TimedOut
}

/// A reified native exception: which class was raised, and a formatted
/// reason for logs. Produced only when a task crashes outside its own
/// `Result`.
pub type Crash {
  Crash(class: CrashClass, reason: String)
}

/// The three native exception classes a crashed task can raise.
pub type CrashClass {
  ErrorClass
  ExitClass
  ThrowClass
}

/// Renders `checkout/charge_payment#2` (the occurrence suffix is only shown
/// when there is more than one occurrence at that scope + name).
pub fn address_to_string(address: StepAddress) -> String {
  let StepAddress(scope, name, occurrence) = address
  let path = case scope {
    [] -> name
    _ -> string.join(list.append(scope, [name]), "/")
  }
  case occurrence > 1 {
    True -> path <> "#" <> int.to_string(occurrence)
    False -> path
  }
}

// `node.gleam` cannot import these types from `saga.gleam` (`saga` imports
// `node`), so `node` keeps structurally identical copies and every value
// crosses the boundary through these functions.

fn address_to_node(address: StepAddress) -> node.StepAddress {
  node.StepAddress(address.scope, address.name, address.occurrence)
}

/// Converts a `node`-owned address back to the public vocabulary, for
/// `saga/execution` and `saga/durable`.
@internal
pub fn address_from_node(address: node.StepAddress) -> StepAddress {
  StepAddress(address.scope, address.name, address.occurrence)
}

fn crash_class_to_node(class: CrashClass) -> ffi.CrashClass {
  case class {
    ErrorClass -> ffi.ErrorClass
    ExitClass -> ffi.ExitClass
    ThrowClass -> ffi.ThrowClass
  }
}

fn crash_class_from_node(class: ffi.CrashClass) -> CrashClass {
  case class {
    ffi.ErrorClass -> ErrorClass
    ffi.ExitClass -> ExitClass
    ffi.ThrowClass -> ThrowClass
  }
}

/// Converts a `node`-owned crash back to the public vocabulary.
@internal
pub fn crash_from_node(crash: node.Crash) -> Crash {
  Crash(crash_class_from_node(crash.class), crash.reason)
}

fn crash_to_node(crash: Crash) -> node.Crash {
  node.Crash(crash_class_to_node(crash.class), crash.reason)
}

/// Converts a `node`-owned failure back to the public vocabulary.
@internal
pub fn failure_from_node(failure: node.AttemptFailure(e)) -> AttemptFailure(e) {
  case failure {
    node.Returned(error) -> Returned(error)
    node.Crashed(crash) -> Crashed(crash_from_node(crash))
    node.TimedOut -> TimedOut
  }
}

// ---------------------------------------------------------------------------
// Recovery and undo vocabulary
// ---------------------------------------------------------------------------

/// An explicit decision about a failing attempt, returned from a
/// `compensate` decider. `Retry`/`RetryAfter` request another attempt (if
/// the budget allows; the delay is capped by
/// `execution.with_max_retry_delay`); `Continue` accepts a replacement
/// output with its own undo; `Abort` fails the run and permits rollback of
/// completed steps; `AbortAfterCleanupFailure` additionally records a
/// cleanup error that happened while deciding; `Hold` leaves prior effects
/// unresolved with no rollback authority. The union is closed.
pub type Recovery(o, e, u) {
  Retry
  RetryAfter(delay: Duration)
  Continue(output: o, undo: Undo(u))
  Abort(error: e)
  AbortAfterCleanupFailure(error: e, cleanup_error: u)
  Hold(evidence: e)
}

/// Whether a successful (or `Continue`d) output has an undo action.
pub type Undo(u) {
  NoUndo
  UndoWith(run: fn() -> Result(Nil, u))
}

// ---------------------------------------------------------------------------
// Definition errors and descriptors
// ---------------------------------------------------------------------------

/// A defect in a workflow's authored shape, found by `define` before any
/// runtime resource exists. All errors are collected, not just the first.
/// Later releases may add variants, for example for further graph checks:
/// describe them with `describe_definition_error`.
pub type DefinitionError {
  EmptyWorkflowName
  EmptyStepName(scope: List(String))
  InvalidMaxAttempts(step: StepAddress, value: Int)
  InvalidTimeout(step: StepAddress, value: Duration)
  ForeignPort(step: StepAddress)
  /// A step was created during the builder (via `perform`/`embed`) but its
  /// output port never reaches the workflow's final output, so it would
  /// never run.
  OrphanStep(step: StepAddress)
}

/// Describes a definition error for logs.
pub fn describe_definition_error(error: DefinitionError) -> String {
  case error {
    EmptyWorkflowName -> "the workflow name is empty"
    EmptyStepName([]) -> "a step name is empty"
    EmptyStepName(scope) ->
      "a step name in " <> string.join(scope, "/") <> " is empty"
    InvalidMaxAttempts(step, value) ->
      "step "
      <> address_to_string(step)
      <> " has max_attempts "
      <> int.to_string(value)
      <> "; it must be at least 1"
    InvalidTimeout(step, value) ->
      "step "
      <> address_to_string(step)
      <> " has timeout "
      <> int.to_string(duration.to_milliseconds(value))
      <> " ms; it must be positive"
    ForeignPort(step) ->
      "a port from another workflow definition is used in "
      <> address_to_string(step)
    OrphanStep(step) ->
      "step "
      <> address_to_string(step)
      <> " never reaches the workflow's output, so it would never run"
  }
}

/// A static, read-only description of one step: its address, its
/// dependencies, and which capabilities it was authored with. Never
/// exposes executable closures or values.
pub type StepDescriptor {
  StepDescriptor(
    address: StepAddress,
    depends_on: List(StepAddress),
    undoable: Bool,
    compensates: Bool,
    max_attempts: Int,
    timeout: Option(Duration),
  )
}

// ---------------------------------------------------------------------------
// Step
// ---------------------------------------------------------------------------

/// A pure description of one unit of work: how to run it, and optionally how
/// to undo a completed run or decide on a failed one. `Step` values compose
/// with `perform` inside a `define` builder; they never run until a
/// `Workflow` starts.
///
/// A failed attempt (`Failed` below) carries its recovery decision already
/// bound to the concrete error it produced and to the decider in effect.
/// This keeps `map_step_errors` sound: mapping a step only translates
/// values the step itself produced, so `map_error: e1 -> e2` is only ever
/// called forward, never inverted. `decide_crash` covers `Crashed` and
/// `TimedOut`, which the coordinator detects itself and which carry no
/// error value. `resolve` does the same for a durable restart: it is the
/// recovery resolver with the decider bound in.
pub opaque type Step(i, o, e, u) {
  Step(
    name: String,
    max_attempts: Int,
    timeout: Option(Duration),
    undoable: Bool,
    compensates: Bool,
    attempt: fn(i, EffectKey) -> RunOutcome(o, u, e),
    unknown: fn(e) -> Bool,
    on_unknown: OnUnknown,
    persistence: Option(StepPersistence(i, o)),
    resolve: Option(fn(i, EffectKey) -> Resumed(o, u, e)),
    undo_for: fn(UndoRequest(i, o)) -> Undo(u),
    recovery_undo_declared: Bool,
    resolve_undo: fn(UndoRequest(i, o)) -> Evidence(Nil, u),
    resolve_compensation: fn(i, EffectKey) -> Option(Recovery(o, e, u)),
    decide_returned: Option(fn(FailedAttempt(i, e)) -> Recovery(o, e, u)),
    decide_crash: Option(fn(FailedAttempt(i, e)) -> Recovery(o, e, u)),
  )
}

/// The attempt context a bound decision is completed with.
type Context {
  Context(number: Int, remaining: Int, key: EffectKey)
}

type RunOutcome(o, u, e) {
  Succeeded(output: o, undo: Undo(u))
  Failed(
    failure: AttemptFailure(e),
    recover_returned: Option(fn(Context) -> Recovery(o, e, u)),
    unknown: Bool,
  )
}

/// A recovery resolver's answer with the decider bound in.
type Resumed(o, u, e) {
  ResumedCompleted(output: o)
  ResumedFailed(
    error: e,
    recover_returned: Option(fn(Context) -> Recovery(o, e, u)),
    unknown: Bool,
  )
  ResumedNotSent
  ResumedMaybeSent
}

type StepPersistence(i, o) {
  StepPersistence(version: String, input: Codec(i), output: Codec(o))
}

/// Creates a step from its name and its run function. With no further
/// modifiers, a failure is terminal after one attempt and nothing is undone
/// on rollback. The run function receives only the step's input; use
/// `effect` to read the attempt's keys and the run's correlation.
pub fn step(name: String, run: fn(i) -> Result(o, e)) -> Step(i, o, e, u) {
  effect(name, fn(input, _key) { run(input) })
}

/// Creates a step whose run function also receives the attempt's
/// `EffectKey`. Send `key.idempotency` to the external system as its
/// idempotency key: it stays the same across retries, so a retry after an
/// unknown outcome cannot repeat the effect. `key.correlation` is the run's
/// correlation, for the step's own clients.
pub fn effect(
  name: String,
  run: fn(i, EffectKey) -> Result(o, e),
) -> Step(i, o, e, u) {
  Step(
    name: name,
    max_attempts: 1,
    timeout: None,
    undoable: False,
    compensates: False,
    persistence: None,
    resolve: None,
    unknown: fn(_) { False },
    on_unknown: Reconcile,
    undo_for: fn(_) { NoUndo },
    recovery_undo_declared: False,
    resolve_undo: fn(_) { EvidenceMaybeSent },
    resolve_compensation: fn(_, _) { None },
    decide_returned: None,
    attempt: fn(input, key) {
      case run(input, key) {
        Ok(output) -> Succeeded(output, NoUndo)
        Error(error) -> Failed(Returned(error), None, False)
      }
    },
    decide_crash: None,
  )
}

/// Attaches an undo action, run only if this step's attempt already
/// succeeded and the run later rolls back. It receives the step's input,
/// the output that succeeded and the undo's own `EffectKey`, and runs at
/// most once: a failed, crashed or timed-out undo is reported in the
/// settlement and never retried.
pub fn undo(
  step: Step(i, o, e, u),
  run: fn(UndoRequest(i, o)) -> Result(Nil, u),
) -> Step(i, o, e, u) {
  let attempt = step.attempt
  Step(
    ..step,
    undoable: True,
    undo_for: fn(request) { UndoWith(fn() { run(request) }) },
    attempt: fn(input, key) {
      case attempt(input, key) {
        Succeeded(output, NoUndo) ->
          Succeeded(
            output,
            UndoWith(fn() {
              run(UndoRequest(
                input,
                output,
                undo_key(key.idempotency, key.correlation),
              ))
            }),
          )
        unchanged -> unchanged
      }
    },
  )
}

/// Attaches an explicit recovery decision for a *failing* attempt (an
/// application error, a crash, or a timeout), with a total attempt budget.
/// With no `compensate`, a failure aborts after exactly one attempt.
///
/// **A crash stays visible whatever `decide` returns.** `decide` receives
/// `Crashed`/`TimedOut` for an attempt that never returned, whose effect may
/// or may not have happened. Whatever it returns, that attempt is named in
/// the outcome's `execution.unknown_effects`, as is a returned error that
/// `unknown_when` classified. In particular, `Abort(error)` after a crash
/// ends the run with `execution.StepFailed(step, error)`, the same cause as
/// an aborted typed error: choose an `error` that says the attempt crashed
/// if the caller must tell the two apart from the cause.
pub fn compensate(
  step: Step(i, o, e, u),
  max_attempts max_attempts: Int,
  with decide: fn(FailedAttempt(i, e)) -> Recovery(o, e, u),
) -> Step(i, o, e, u) {
  let attempt = step.attempt
  let bind = fn(input, failure) {
    Some(fn(context: Context) {
      decide(FailedAttempt(
        input: input,
        failure: failure,
        attempt: context.number,
        attempts_left: context.remaining,
        key: context.key,
      ))
    })
  }
  Step(
    ..step,
    max_attempts: max_attempts,
    compensates: True,
    decide_returned: Some(decide),
    decide_crash: Some(decide),
    attempt: fn(input, key) {
      case attempt(input, key) {
        Failed(failure, _, unknown) ->
          Failed(failure, bind(input, failure), unknown)
        unchanged -> unchanged
      }
    },
    resolve: option.map(step.resolve, fn(resolve) {
      fn(input, key) {
        case resolve(input, key) {
          ResumedFailed(error, _, unknown) ->
            ResumedFailed(error, bind(input, Returned(error)), unknown)
          unchanged -> unchanged
        }
      }
    }),
  )
}

/// Marks the returned errors after which the step's effect is unknown: a
/// timeout or connection loss after the request may have been sent, for
/// example. The attempt is named in the outcome's
/// `execution.unknown_effects` with `execution.ActionReturnedUnknown`, so a
/// retried success becomes `CompletedWithUnknownEffects`. Calling it twice
/// marks the errors either classifier matches.
///
/// A matching error still reaches the `compensate` decider as
/// `Returned(error)`, and the decider's decision applies: `Abort` rolls the
/// run back, `Hold` ends it `Unresolved`. When the step has no decider, or
/// its decider asked for a retry the attempt budget no longer allows, the
/// step ends as `on_unknown` says: by default (`Reconcile`) the run ends
/// `execution.Unresolved` with the error as evidence and undoes nothing, so
/// an uncertain payment never releases the stock reserved before it.
/// `on_unknown(RollBack)` fails the run and undoes the completed steps
/// instead. Durable runs apply the same rule to an error that a recovery
/// resolver reports after a restart.
pub fn unknown_when(
  step: Step(i, o, e, u),
  classify: fn(e) -> Bool,
) -> Step(i, o, e, u) {
  let attempt = step.attempt
  let unknown = step.unknown
  Step(
    ..step,
    unknown: fn(error) { unknown(error) || classify(error) },
    attempt: fn(input, key) {
      case attempt(input, key) {
        Failed(Returned(error) as failure, recover, unknown) ->
          Failed(failure, recover, unknown || classify(error))
        unchanged -> unchanged
      }
    },
    resolve: option.map(step.resolve, fn(resolve) {
      fn(input, key) {
        case resolve(input, key) {
          ResumedFailed(error, recover, unknown) ->
            ResumedFailed(error, recover, unknown || classify(error))
          unchanged -> unchanged
        }
      }
    }),
  )
}

/// What a run does when a step ends with an error that `unknown_when`
/// classified and no `compensate` decision settles it. The union is closed.
pub type OnUnknown {
  /// End the run `execution.Unresolved` with the error as evidence and undo
  /// nothing, so the effect can be reconciled first. The default.
  Reconcile
  /// Fail the run with `execution.StepFailed` (or `RetryLimitReached`) and
  /// undo the completed steps, as for a known error.
  RollBack
}

/// Sets what the run does when this step ends with an error that
/// `unknown_when` classified (default `Reconcile`); see `unknown_when`.
/// Crashes and timeouts are not affected.
pub fn on_unknown(
  step: Step(i, o, e, u),
  policy: OnUnknown,
) -> Step(i, o, e, u) {
  Step(..step, on_unknown: policy)
}

/// Bounds one attempt of this step to `limit`, overriding the run's
/// `execution.with_step_timeout` default in either direction. `define`
/// rejects a limit below 1 millisecond.
pub fn timeout(step: Step(i, o, e, u), limit: Duration) -> Step(i, o, e, u) {
  Step(..step, timeout: Some(limit))
}

/// Adapts a step's error and undo-error types into a unified workflow
/// vocabulary. Persistence added by `saga/durable` before the mapping is
/// kept: its resolver's answers are mapped forward like the step's own
/// results. A `compensate` decider written in the old vocabulary keeps
/// deciding the step's failures, including a failure that a resolver added
/// before the mapping establishes after a restart. A resolver added after
/// the mapping answers in the new vocabulary, so a `Failed` answer from it
/// needs a decider in the new vocabulary too: without one the execution
/// suspends with `durable.InvalidCheckpoint`.
pub fn map_step_errors(
  step: Step(i, o, e1, u1),
  error map_error: fn(e1) -> e2,
  undo_error map_undo_error: fn(u1) -> u2,
) -> Step(i, o, e2, u2) {
  let map_bound = fn(recover_returned) {
    option.map(recover_returned, fn(recover_fn) {
      fn(context) {
        recover_fn(context) |> map_recovery(map_error, map_undo_error)
      }
    })
  }
  let map_decider = fn(
    decide: Option(fn(FailedAttempt(i, e1)) -> Recovery(o, e1, u1)),
  ) {
    option.map(decide, fn(decide_fn) {
      fn(failed: FailedAttempt(i, e2)) {
        // Only crashes and timeouts reach this mapped decider, and they
        // carry no error value, so re-tagging touches no `e`.
        let failure = case failed.failure {
          Crashed(crash) -> Crashed(crash)
          TimedOut -> TimedOut
          Returned(_) ->
            panic as "saga: a returned failure reached a mapped crash decider"
        }
        decide_fn(FailedAttempt(..failed, failure: failure))
        |> map_recovery(map_error, map_undo_error)
      }
    })
  }
  Step(
    name: step.name,
    max_attempts: step.max_attempts,
    timeout: step.timeout,
    undoable: step.undoable,
    compensates: step.compensates,
    persistence: step.persistence,
    unknown: fn(_) { False },
    on_unknown: step.on_unknown,
    resolve: option.map(step.resolve, fn(resolve) {
      fn(input, key) {
        case resolve(input, key) {
          ResumedCompleted(output) -> ResumedCompleted(output)
          ResumedNotSent -> ResumedNotSent
          ResumedMaybeSent -> ResumedMaybeSent
          ResumedFailed(error, recover_returned, unknown) ->
            ResumedFailed(
              map_error(error),
              map_bound(recover_returned),
              unknown,
            )
        }
      }
    }),
    recovery_undo_declared: step.recovery_undo_declared,
    resolve_undo: fn(request) {
      case step.resolve_undo(request) {
        EvidenceCompleted(Nil) -> EvidenceCompleted(Nil)
        EvidenceNotSent -> EvidenceNotSent
        EvidenceMaybeSent -> EvidenceMaybeSent
        EvidenceFailed(error) -> EvidenceFailed(map_undo_error(error))
      }
    },
    resolve_compensation: fn(input, key) {
      step.resolve_compensation(input, key)
      |> option.map(map_recovery(_, map_error, map_undo_error))
    },
    decide_returned: None,
    undo_for: fn(request) { map_undo(step.undo_for(request), map_undo_error) },
    attempt: fn(input, key) {
      case step.attempt(input, key) {
        Succeeded(output, undo_choice) ->
          Succeeded(output, map_undo(undo_choice, map_undo_error))
        Failed(failure, recover_returned, unknown) ->
          Failed(
            failure: map_attempt_failure(failure, map_error),
            recover_returned: map_bound(recover_returned),
            unknown: unknown,
          )
      }
    },
    decide_crash: map_decider(step.decide_crash),
  )
}

fn map_attempt_failure(
  failure: AttemptFailure(e1),
  f: fn(e1) -> e2,
) -> AttemptFailure(e2) {
  case failure {
    Returned(error) -> Returned(f(error))
    Crashed(crash) -> Crashed(crash)
    TimedOut -> TimedOut
  }
}

fn map_recovery(
  recovery: Recovery(o, e1, u1),
  map_error: fn(e1) -> e2,
  map_undo_error: fn(u1) -> u2,
) -> Recovery(o, e2, u2) {
  case recovery {
    Retry -> Retry
    RetryAfter(delay) -> RetryAfter(delay)
    Continue(output, undo_choice) ->
      Continue(output, map_undo(undo_choice, map_undo_error))
    Abort(error) -> Abort(map_error(error))
    AbortAfterCleanupFailure(error, cleanup_error) ->
      AbortAfterCleanupFailure(map_error(error), map_undo_error(cleanup_error))
    Hold(evidence) -> Hold(map_error(evidence))
  }
}

fn map_undo(undo_choice: Undo(u1), f: fn(u1) -> u2) -> Undo(u2) {
  case undo_choice {
    NoUndo -> NoUndo
    UndoWith(run) ->
      UndoWith(fn() {
        case run() {
          Ok(Nil) -> Ok(Nil)
          Error(error) -> Error(f(error))
        }
      })
  }
}

// ---------------------------------------------------------------------------
// Persistence capability, set by `saga/durable`
// ---------------------------------------------------------------------------

/// Adds the persistence capability `durable.recoverable` describes: the
/// step's version, its input and output codecs, and the resolver for an
/// interrupted attempt, with the current decider and `unknown_when`
/// classifier bound in.
@internal
pub fn set_recoverable(
  step: Step(i, o, e, u),
  version: String,
  input: Codec(i),
  output: Codec(o),
  resolve: fn(i, EffectKey) -> Evidence(o, e),
) -> Step(i, o, e, u) {
  let decide = step.decide_returned
  let unknown = step.unknown
  Step(
    ..step,
    persistence: Some(StepPersistence(version, input, output)),
    resolve: Some(fn(value, key) {
      case resolve(value, key) {
        EvidenceCompleted(output) -> ResumedCompleted(output)
        EvidenceNotSent -> ResumedNotSent
        EvidenceMaybeSent -> ResumedMaybeSent
        EvidenceFailed(error) ->
          ResumedFailed(
            error,
            option.map(decide, fn(decide_fn) {
              fn(context: Context) {
                decide_fn(FailedAttempt(
                  input: value,
                  failure: Returned(error),
                  attempt: context.number,
                  attempts_left: context.remaining,
                  key: context.key,
                ))
              }
            }),
            unknown(error),
          )
      }
    }),
  )
}

/// Declares how to rebuild the step's undo from saved values.
@internal
pub fn set_restore_undo(
  step: Step(i, o, e, u),
  restore: fn(UndoRequest(i, o)) -> Undo(u),
) -> Step(i, o, e, u) {
  Step(..step, undo_for: restore, recovery_undo_declared: True)
}

/// Sets the resolver for an interrupted undo.
@internal
pub fn set_resolve_undo(
  step: Step(i, o, e, u),
  resolve: fn(UndoRequest(i, o)) -> Evidence(Nil, u),
) -> Step(i, o, e, u) {
  Step(..step, resolve_undo: resolve)
}

/// Sets the resolver for an interrupted compensation decision.
@internal
pub fn set_resolve_compensation(
  step: Step(i, o, e, u),
  resolve: fn(i, EffectKey) -> Option(Recovery(o, e, u)),
) -> Step(i, o, e, u) {
  Step(..step, resolve_compensation: resolve)
}

/// The `EffectKey` of attempt `number` of a step with stable key `base`, in
/// a run with `correlation`.
fn effect_key(
  base: String,
  number: Int,
  correlation: Option(Correlation),
) -> EffectKey {
  EffectKey(
    idempotency: base,
    attempt: number,
    attempt_key: base <> ":attempt:" <> int.to_string(number),
    correlation: correlation,
  )
}

/// The `EffectKey` of the undo of a step with stable key `base`, in a run
/// with `correlation`.
fn undo_key(base: String, correlation: Option(Correlation)) -> EffectKey {
  effect_key(base <> ":undo", 1, correlation)
}

fn saved_key(key: EffectKey) -> checkpoint.Key {
  checkpoint.Key(key.idempotency, key.attempt, key.attempt_key)
}

/// Converts a saved key back to the public vocabulary, in a run with
/// `correlation`.
@internal
pub fn key_from_saved(
  key: checkpoint.Key,
  correlation: Option(Correlation),
) -> EffectKey {
  EffectKey(key.idempotency, key.attempt, key.attempt_key, correlation)
}

fn context_from_node(attempt: node.Attempt) -> Context {
  Context(
    number: attempt.number,
    remaining: attempt.remaining,
    key: effect_key(attempt.base, attempt.number, attempt.correlation),
  )
}

// ---------------------------------------------------------------------------
// Scope tokens (define vs. embed, and foreign-port rejection)
// ---------------------------------------------------------------------------

/// Identifies one workflow's evaluation. A `Port` created under one scope
/// cannot be consumed by `perform`/`map`/`both`/`all` under an unrelated
/// scope; mixing them is rejected as `ForeignPort` at `define` time. `embed`
/// passes the parent scope's `id` through unchanged (only extending `path`
/// for addressing), so an inner builder may legitimately capture outer
/// ports. `registry` accumulates every node `perform` creates anywhere in
/// this one evaluation — including ones a builder later discards rather
/// than threading into its returned port — with an O(1) append per node
/// (see `saga/internal/cell`'s `Registry`), so `define`/`for_run` can both
/// detect steps unreachable from the final output (see `OrphanStep`) and
/// assemble the workflow's node table, without any `Port` ever having to
/// carry or merge a node map of its own. See `Port`'s own doc comment for
/// why threading a node map through every `Port` used to cost O(N) per
/// `both`/`map` combinator call.
type ScopeToken(e, u) {
  ScopeToken(id: Int, path: List(String), registry: cell.Registry(Node(e, u)))
}

fn root_scope() -> ScopeToken(e, u) {
  ScopeToken(id: ffi.unique_integer(), path: [], registry: cell.new_registry())
}

// ---------------------------------------------------------------------------
// Port
// ---------------------------------------------------------------------------

/// A typed reference to a value produced in the workflow graph being
/// built: the workflow's own input, or the output of a `perform`, `map`,
/// `both`, `all`, `choose` or `embed`. Two consumers of the same `Port` value
/// depend on the same node, so that node executes once per run.
///
/// A port reads its value in three stages: the graph is composed once, at
/// `define`; the coordinator reads the dependency values from the run's
/// store for each attempt; and the task that runs the attempt computes any
/// `map` transformation, under `rescue`, so a panicking or slow `map`
/// becomes an attempt crash instead of reaching the coordinator.
pub opaque type Port(a, e, u) {
  Port(
    scope: ScopeToken(e, u),
    deps: Set(Int),
    errors: List(DefinitionError),
    fetch: fn() -> fn(store.Store) -> fn() -> a,
  )
}

fn merge_ports(
  scope: ScopeToken(e, u),
  first: Port(a, e, u),
  second: Port(b, e, u),
  fetch: fn() -> fn(store.Store) -> fn() -> c,
) -> Port(c, e, u) {
  let foreign_errors =
    list.append(
      foreign_error_for(scope, first),
      foreign_error_for(scope, second),
    )
  Port(
    scope: scope,
    deps: set.union(first.deps, second.deps),
    errors: list.flatten([first.errors, second.errors, foreign_errors]),
    fetch: fetch,
  )
}

fn foreign_error_for(
  scope: ScopeToken(e, u),
  port: Port(a, e, u),
) -> List(DefinitionError) {
  case port.scope.id == scope.id {
    True -> []
    False -> [
      ForeignPort(StepAddress(
        scope: port.scope.path,
        name: "<port>",
        occurrence: 1,
      )),
    ]
  }
}

/// Applies a pure transformation to a port's value. `map` is not memoized:
/// it re-runs in every task that consumes the resulting port. `with` is
/// only ever invoked from the innermost (task-level) call of `fetch` — see
/// `Port`'s doc comment — so a panic or a slow computation inside `with`
/// never reaches the coordinator.
pub fn map(port: Port(a, e, u), with: fn(a) -> b) -> Port(b, e, u) {
  Port(..port, fetch: fn() {
    let capture_a = port.fetch()
    fn(store) {
      let produce_a = capture_a(store)
      fn() { with(produce_a()) }
    }
  })
}

/// Combines two ports into one pair, without adding a scheduled node. Both
/// original ports still execute according to their own dependencies.
pub fn both(
  first: Port(a, e, u),
  second: Port(b, e, u),
) -> Port(#(a, b), e, u) {
  merge_ports(first.scope, first, second, fn() {
    let capture_a = first.fetch()
    let capture_b = second.fetch()
    fn(store) {
      let produce_a = capture_a(store)
      let produce_b = capture_b(store)
      fn() { #(produce_a(), produce_b()) }
    }
  })
}

/// Combines `first` and `rest` into one port producing their values, in
/// list order (`first` then `rest`, in `rest`'s own order). Takes `first`
/// as a separate required argument (rather than one `List(Port(..))` that
/// could be empty) so there is no empty case to report or work around at
/// all: a caller with zero ports simply has no `Port` to pass as `first`
/// and cannot call `all` in the first place, which the type system already
/// enforces at the call site — nothing here needs its own placeholder
/// value or definition error for it.
pub fn all(
  first: Port(a, e, u),
  rest: List(Port(a, e, u)),
) -> Port(List(a), e, u) {
  // Builds the combined list newest-first (prepend, not `list.append`,
  // inside the fold) and reverses exactly once at the very end, rather
  // than appending one element at a time across `rest`'s ports: an append
  // per step is O(k) for a list already k long, so appending across k
  // ports would cost O(k^2) total (paid once per run, inside the final
  // `map`'s closure, for every attempt that reads this combined port) —
  // prepending is O(1) per step, O(k) total, with one final O(k) reverse.
  list.fold(rest, map(first, fn(a) { [a] }), fn(acc, port) {
    both(acc, port) |> map(fn(pair) { [pair.1, ..pair.0] })
  })
  |> map(list.reverse)
}

/// Constructs both branches once, and executes only the selected branch.
/// The decision is a scheduled value, shared by all nodes in that branch.
pub fn choose(
  input: Port(i, e, u),
  name: String,
  decision: Port(Bool, e, u),
  when_true: fn(Port(i, e, u)) -> Port(o, e, u),
  when_false: fn(Port(i, e, u)) -> Port(o, e, u),
) -> Port(o, e, u) {
  let selected =
    perform(
      decision,
      step(name, fn(value) { Ok(value) })
        |> set_recoverable("choice-1", codec.bool(), codec.bool(), fn(value, _) {
          EvidenceCompleted(value)
        }),
    )
  let yes = choice_branch(input, selected, name, True, when_true)
  let no = choice_branch(input, selected, name, False, when_false)
  let combined =
    merge_ports(input.scope, yes, no, fn() {
      let capture_decision = selected.fetch()
      let capture_yes = yes.fetch()
      let capture_no = no.fetch()
      fn(run_store) {
        // The decision node is a Bool identity step, so this reads no user map.
        case capture_decision(run_store)() {
          True -> capture_yes(run_store)
          False -> capture_no(run_store)
        }
      }
    })
  Port(
    ..combined,
    deps: set.union(combined.deps, selected.deps),
    errors: list.append(
      combined.errors,
      foreign_error_for(input.scope, decision),
    ),
  )
}

fn choice_branch(
  input: Port(i, e, u),
  selected: Port(Bool, e, u),
  name: String,
  selected_value: Bool,
  build: fn(Port(i, e, u)) -> Port(o, e, u),
) -> Port(o, e, u) {
  let registry = cell.new_registry()
  let branch_name = case selected_value {
    True -> "true"
    False -> "false"
  }
  let scoped =
    Port(
      ..input,
      scope: ScopeToken(
        ..input.scope,
        path: list.append(input.scope.path, [name, branch_name]),
        registry: registry,
      ),
    )
  let output = build(scoped)
  let conditions =
    list.map(set.to_list(selected.deps), fn(id) { #(id, selected_value) })
  cell.all_registered(registry)
  |> list.each(fn(n) {
    cell.register(
      input.scope.registry,
      Node(
        ..n,
        deps: set.union(set.from_list(n.deps), selected.deps) |> set.to_list,
        conditions: list.append(conditions, n.conditions),
      ),
    )
  })
  cell.close(registry)
  Port(
    ..output,
    scope: input.scope,
    errors: list.append(output.errors, foreign_error_for(input.scope, output)),
  )
}

// ---------------------------------------------------------------------------
// perform
// ---------------------------------------------------------------------------

/// Schedules `step` to run once its input port is ready, producing a new
/// port for its output. Reusing the same input `Port` value from two
/// `perform` calls creates two independent nodes (and therefore two
/// occurrences); reusing the *resulting* `Port` value across consumers
/// shares the one node.
pub fn perform(input: Port(i, e, u), step: Step(i, o, e, u)) -> Port(o, e, u) {
  let id = ffi.unique_integer()
  let address =
    StepAddress(scope: input.scope.path, name: step.name, occurrence: 1)
  let deps = set.to_list(input.deps)
  let capture_input = input.fetch()
  let saved_address =
    checkpoint.Address(address.scope, address.name, address.occurrence)
  let input_failure = fn(error) {
    checkpoint.CodecFailure(checkpoint.StepInput(saved_address), error)
  }
  let output_failure = fn(error) {
    checkpoint.CodecFailure(checkpoint.StepOutput(saved_address), error)
  }

  let to_erased_recovery = fn(recovery: Recovery(o, e, u), input: i) -> ErasedRecovery(
    e,
    u,
  ) {
    case recovery {
      Retry -> node.ERetry
      RetryAfter(delay) -> node.ERetryAfter(duration.to_milliseconds(delay))
      Continue(output, undo_choice) ->
        node.EContinue(commit: fn(run_store) {
          #(
            run_store
              |> store.put(id, output)
              |> store.put_record(id, #(input, output, has_undo(undo_choice))),
            undo_option(undo_choice),
          )
        })
      Abort(error) -> node.EAbort(error)
      AbortAfterCleanupFailure(error, cleanup_error) ->
        node.EAbortCleanup(error, cleanup_error)
      Hold(evidence) -> node.EHold(evidence)
    }
  }

  let bind_recovery = fn(
    recover_returned: Option(fn(Context) -> Recovery(o, e, u)),
    value: i,
  ) {
    option.map(recover_returned, fn(recover_fn) {
      fn(node_attempt: node.Attempt) {
        fn() {
          to_erased_recovery(recover_fn(context_from_node(node_attempt)), value)
        }
      }
    })
  }

  let succeeded = fn(value: i, output: o, undo_choice: Undo(u)) {
    AttemptSucceeded(commit: fn(commit_store) {
      #(
        commit_store
          |> store.put(id, output)
          |> store.put_record(id, #(value, output, has_undo(undo_choice))),
        undo_option(undo_choice),
      )
    })
  }

  // `prepare_attempt`/`prepare_crash_recovery` run in the coordinator, which
  // alone owns the run's `Store`: `capture_input(run_store)` performs every
  // dependency read there. The thunk it returns, which may run a slow or
  // panicking `map`, runs only inside the spawned task, under `rescue`.
  let prepare_attempt = fn(node_attempt: node.Attempt, run_store: Store) -> fn() ->
    node.AttemptResult(e, u) {
    let produce_value = capture_input(run_store)
    fn() {
      let prepared = case node_attempt.persistent, step.persistence {
        True, Some(p) -> {
          use value <- result.try(case node_attempt.saved_input {
            Some(encoded) -> codec.decode(p.input, encoded)
            None -> Ok(produce_value())
          })
          use encoded <- result.try(codec.encode(p.input, value))
          node_attempt.admit(encoded)
          Ok(value)
        }
        _, _ -> Ok(produce_value())
      }
      case prepared {
        Error(error) -> node.AttemptBlocked(input_failure(error))
        Ok(value) ->
          case
            step.attempt(
              value,
              effect_key(
                node_attempt.base,
                node_attempt.number,
                node_attempt.correlation,
              ),
            )
          {
            Succeeded(output, undo_choice) ->
              succeeded(value, output, undo_choice)
            Failed(failure, recover_returned, unknown) ->
              node.AttemptFailed(
                failure: failure_to_node(failure),
                recover: bind_recovery(recover_returned, value),
                unknown: unknown,
              )
          }
      }
    }
  }

  // The coordinator's path for a crash or timeout it observed itself: the
  // task never returned, so this asks the decider directly, with the input
  // of this attempt, and never repeats the step's effect.
  let prepare_crash_recovery =
    option.map(step.decide_crash, fn(decide_fn) {
      fn(
        node_failure: node.AttemptFailure(e),
        node_attempt: node.Attempt,
        run_store: Store,
      ) -> fn() -> ErasedRecovery(e, u) {
        let produce_value = capture_input(run_store)
        let failure = case node_failure {
          node.Crashed(crash) -> Crashed(crash_from_node(crash))
          node.TimedOut -> TimedOut
          node.Returned(_) ->
            panic as "saga: prepare_crash_recovery received a Returned failure"
        }
        let context = context_from_node(node_attempt)
        fn() {
          let input = case
            node_attempt.persistent,
            step.persistence,
            node_attempt.saved_input
          {
            True, Some(p), Some(encoded) ->
              codec.decode(p.input, encoded) |> result.map_error(input_failure)
            True, _, None ->
              Error(
                checkpoint.InvalidState(checkpoint.CompensationInputMissing(
                  saved_address,
                )),
              )
            _, _, _ -> Ok(produce_value())
          }
          case input {
            Error(failure) -> node.EBlocked(failure)
            Ok(input) ->
              to_erased_recovery(
                decide_fn(FailedAttempt(
                  input: input,
                  failure: failure,
                  attempt: context.number,
                  attempts_left: context.remaining,
                  key: context.key,
                )),
                input,
              )
          }
        }
      }
    })

  let this_node =
    Node(
      id: id,
      address: address_to_node(address),
      deps: deps,
      conditions: [],
      persistence: option.map(step.persistence, fn(p) {
        node.Persistence(
          version: frame([
            p.version,
            codec.version(p.input),
            codec.version(p.output),
          ]),
          step_version: p.version,
          input_version: codec.version(p.input),
          output_version: codec.version(p.output),
          recovery_undo_declared: step.recovery_undo_declared,
          freeze: fn(run_store, base) {
            use pair <- result.try(case store.get_record(run_store, id) {
              Ok(pair) -> Ok(pair)
              Error(Nil) -> Error(checkpoint.InvalidState(checkpoint.Malformed))
            })
            let #(input, output, undoable) = pair
            let request = UndoRequest(input, output, undo_key(base, None))
            use _ <- result.try(
              case undoable && !has_undo(step.undo_for(request)) {
                True ->
                  Error(
                    checkpoint.InvalidState(checkpoint.UndoNotRestorable(
                      saved_address,
                    )),
                  )
                False -> Ok(Nil)
              },
            )
            use input <- result.try(
              codec.encode(p.input, input) |> result.map_error(input_failure),
            )
            use output <- result.try(
              codec.encode(p.output, output) |> result.map_error(output_failure),
            )
            Ok([
              input,
              output,
              case undoable {
                True -> "undo"
                False -> "none"
              },
            ])
          },
          thaw: fn(saved, _run_store, base, correlation) {
            case saved {
              [input, output, undo_kind] -> {
                use input <- result.try(
                  codec.decode(p.input, input)
                  |> result.map_error(input_failure),
                )
                use output <- result.try(
                  codec.decode(p.output, output)
                  |> result.map_error(output_failure),
                )
                use undo <- result.try(case undo_kind {
                  "none" -> Ok(None)
                  "undo" ->
                    case
                      step.undo_for(UndoRequest(
                        input,
                        output,
                        undo_key(base, correlation),
                      ))
                    {
                      NoUndo ->
                        Error(
                          checkpoint.InvalidState(checkpoint.UndoNotRestorable(
                            saved_address,
                          )),
                        )
                      UndoWith(run) -> Ok(Some(run))
                    }
                  _ -> Error(checkpoint.InvalidState(checkpoint.Malformed))
                })
                Ok(fn(run_store) {
                  #(
                    run_store
                      |> store.put(id, output)
                      |> store.put_record(id, #(
                        input,
                        output,
                        undo_kind == "undo",
                      )),
                    undo,
                  )
                })
              }
              _ -> Error(checkpoint.InvalidState(checkpoint.Malformed))
            }
          },
          resume_compensation: fn(attempt, _run_store) {
            fn() {
              let key =
                effect_key(attempt.base, attempt.number, attempt.correlation)
              case attempt.saved_input {
                None ->
                  node.EBlocked(
                    checkpoint.InvalidState(checkpoint.CompensationInputMissing(
                      saved_address,
                    )),
                  )
                Some(encoded) ->
                  case codec.decode(p.input, encoded) {
                    Error(error) -> node.EBlocked(input_failure(error))
                    Ok(input) ->
                      case step.resolve_compensation(input, key) {
                        None ->
                          node.EBlocked(
                            checkpoint.Uncertain(checkpoint.Required(
                              saved_address,
                              checkpoint.CompensationAction(attempt.number),
                              saved_key(key),
                            )),
                          )
                        Some(recovery) -> to_erased_recovery(recovery, input)
                      }
                  }
              }
            }
          },
          resume_undo: fn(run_store, base, correlation) {
            fn() {
              use pair <- result.try(case store.get_record(run_store, id) {
                Ok(pair) -> Ok(pair)
                Error(Nil) ->
                  Error(checkpoint.InvalidState(checkpoint.Malformed))
              })
              let #(input, output, _) = pair
              let request =
                UndoRequest(input, output, undo_key(base, correlation))
              case step.resolve_undo(request) {
                EvidenceCompleted(Nil) -> Ok(Ok(Nil))
                EvidenceFailed(error) -> Ok(Error(error))
                EvidenceMaybeSent ->
                  Error(
                    checkpoint.Uncertain(checkpoint.Required(
                      saved_address,
                      checkpoint.UndoAction,
                      saved_key(request.key),
                    )),
                  )
                EvidenceNotSent ->
                  case step.undo_for(request) {
                    NoUndo ->
                      Error(
                        checkpoint.InvalidState(checkpoint.UndoNotRestorable(
                          saved_address,
                        )),
                      )
                    UndoWith(run) -> Ok(run())
                  }
              }
            }
          },
          resume: fn(attempt, run_store) {
            let produce = capture_input(run_store)
            fn() {
              let key =
                effect_key(attempt.base, attempt.number, attempt.correlation)
              let input = case attempt.saved_input {
                None -> Ok(produce())
                Some(encoded) ->
                  codec.decode(p.input, encoded)
                  |> result.map_error(input_failure)
              }
              let resumed = case step.resolve {
                Some(resolve) -> resolve
                None -> fn(_, _) { ResumedMaybeSent }
              }
              case input {
                Error(failure) -> node.AttemptBlocked(failure)
                Ok(input) ->
                  case attempt.saved_input {
                    None ->
                      case attempt.cancelled {
                        True -> node.AttemptAbsent
                        False -> prepare_attempt(attempt, run_store)()
                      }
                    Some(_) ->
                      case resumed(input, key) {
                        ResumedMaybeSent ->
                          node.AttemptBlocked(
                            checkpoint.Uncertain(checkpoint.Required(
                              saved_address,
                              checkpoint.AttemptAction(attempt.number),
                              saved_key(key),
                            )),
                          )
                        ResumedNotSent ->
                          case attempt.cancelled {
                            True -> node.AttemptAbsent
                            False -> prepare_attempt(attempt, run_store)()
                          }
                        ResumedCompleted(output) ->
                          succeeded(
                            input,
                            output,
                            step.undo_for(UndoRequest(
                              input,
                              output,
                              undo_key(attempt.base, attempt.correlation),
                            )),
                          )
                        ResumedFailed(error, recover_returned, unknown) ->
                          case recover_returned, step.compensates {
                            None, True ->
                              node.AttemptBlocked(
                                checkpoint.InvalidState(
                                  checkpoint.DeciderMissingAfterMapping(
                                    saved_address,
                                  ),
                                ),
                              )
                            _, _ ->
                              node.AttemptFailed(
                                node.Returned(error),
                                bind_recovery(recover_returned, input),
                                unknown,
                              )
                          }
                      }
                  }
              }
            }
          },
        )
      }),
      max_attempts: step.max_attempts,
      timeout: option.map(step.timeout, duration.to_milliseconds),
      undoable: step.undoable,
      compensates: step.compensates,
      rolls_back_unknown: step.on_unknown == RollBack,
      prepare_attempt: prepare_attempt,
      prepare_crash_recovery: prepare_crash_recovery,
    )
  cell.register(input.scope.registry, this_node)

  let name_error = case step.name {
    "" -> [EmptyStepName(scope: input.scope.path)]
    _ -> []
  }
  let attempts_error = case step.max_attempts >= 1 {
    True -> []
    False -> [InvalidMaxAttempts(step: address, value: step.max_attempts)]
  }
  let timeout_error = case step.timeout {
    None -> []
    Some(limit) ->
      case duration.to_milliseconds(limit) > 0 {
        True -> []
        False -> [InvalidTimeout(step: address, value: limit)]
      }
  }

  Port(
    scope: input.scope,
    deps: set.from_list([id]),
    errors: list.flatten([
      input.errors,
      name_error,
      attempts_error,
      timeout_error,
    ]),
    fetch: fn() {
      fn(run_store) {
        let value = store.get(run_store, id)
        fn() { value }
      }
    },
  )
}

fn failure_to_node(failure: AttemptFailure(e)) -> node.AttemptFailure(e) {
  case failure {
    Returned(error) -> node.Returned(error)
    Crashed(crash) -> node.Crashed(crash_to_node(crash))
    TimedOut -> node.TimedOut
  }
}

// ---------------------------------------------------------------------------
// Workflow
// ---------------------------------------------------------------------------

/// A pure, named workflow description with input, output, business error,
/// and undo-error types.
///
/// A builder function runs exactly once per `define`, in the calling
/// process, to validate the workflow and build its graph; `embed` runs a
/// workflow's builder once more as part of another workflow's `define`. No
/// run evaluates a builder: every run replays the built graph against its
/// own value store, so concurrent runs of one `Workflow` never share values.
pub opaque type Workflow(i, o, e, u) {
  Workflow(
    name: String,
    build: fn(Port(i, e, u)) -> Port(o, e, u),
    root_input_id: Int,
    nodes: Dict(Int, Node(e, u)),
    order: List(Int),
    // The reverse-dependency index (`saga/internal/node.build_dependents`),
    // computed once here since it depends only on `nodes`' own fixed
    // `deps` lists — themselves fixed once the graph is built — and reused
    // unchanged by the coordinator across every run, rather than
    // recomputed per run.
    dependents: Dict(Int, List(Int)),
    fetch_output: fn(Store) -> fn() -> o,
    descriptors: List(StepDescriptor),
  )
}

/// Builds and validates a named workflow written in source code. The
/// builder runs once, in the calling process, before any runtime resource
/// exists: `define` checks step names, attempt budgets, timeouts, that every
/// port used belongs to this evaluation, and that every step reaches the
/// output.
///
/// A defect is a bug in the source, so `define` panics with a message that
/// names the workflow and every offending step; any test that builds the
/// workflow catches it. Use `try_define` when names, budgets or timeouts
/// come from runtime data.
pub fn define(
  name: String,
  build: fn(Port(i, e, u)) -> Port(o, e, u),
) -> Workflow(i, o, e, u) {
  case try_define(name, build) {
    Ok(workflow) -> workflow
    Error(errors) ->
      panic as {
        "saga.define: workflow \""
        <> name
        <> "\" is invalid: "
        <> string.join(list.map(errors, describe_definition_error), "; ")
      }
  }
}

/// Builds and validates a named workflow like `define`, but returns every
/// defect as a `DefinitionError` instead of panicking, for a workflow whose
/// step names, attempt budgets or timeouts come from runtime data.
/// `describe_definition_error` renders one.
pub fn try_define(
  name: String,
  build: fn(Port(i, e, u)) -> Port(o, e, u),
) -> Result(Workflow(i, o, e, u), List(DefinitionError)) {
  let scope = root_scope()
  let root_input_id = ffi.unique_integer()
  let root_input = fresh_root_port(scope, root_input_id)
  let output = build(root_input)

  // `scope.registry`'s every entry, indexed by id once, O(N) total — this
  // is the *only* place this scope's whole node table is ever assembled
  // into a `Dict`; every `perform` call above only ever appended to the
  // registry (O(1) each), never merged one. See `reachable_nodes`'s own doc
  // comment for why walking `deps` from `output` against this table,
  // instead of threading a node map through every `Port`, is what makes
  // this — and every intermediate `both`/`map` call above — no longer O(N)
  // per combinator call.
  let all_nodes =
    cell.all_registered(scope.registry)
    |> list.fold(dict.new(), fn(acc, a_node) {
      dict.insert(acc, a_node.id, a_node)
    })
  let reachable = reachable_nodes(all_nodes, output.deps)

  let name_errors = case name {
    "" -> [EmptyWorkflowName]
    _ -> []
  }
  let root_scope_errors = foreign_error_for(scope, output)
  let orphan_errors = orphan_errors_for(scope, output, reachable)
  // `scope.registry` was only ever needed for `orphan_errors_for`'s check,
  // just above; retiring it now keeps this (typically long-lived) calling
  // process's mailbox from accumulating one stray message per `define`
  // call. See `cell.close`.
  cell.close(scope.registry)
  let all_errors =
    list.flatten([name_errors, output.errors, root_scope_errors, orphan_errors])

  case all_errors {
    [] -> {
      let #(ordered_ids, nodes) = resolved_nodes(reachable)
      // Called once, here, ever: this is the *only* place `output.fetch()`
      // (level 1 — see `Port`'s doc comment) is invoked for this
      // `Workflow`'s final output. The resulting level-2 closure is stored
      // on the `Workflow` and re-invoked fresh (with a new `Store`) by
      // every run in `for_run`.
      let fetch_output = output.fetch()
      Ok(Workflow(
        name: name,
        build: build,
        root_input_id: root_input_id,
        nodes: nodes,
        order: ordered_ids,
        dependents: node.build_dependents(nodes),
        fetch_output: fetch_output,
        descriptors: descriptors_for(ordered_ids, nodes),
      ))
    }
    _ -> Error(all_errors)
  }
}

/// The subset of `all_nodes` transitively reachable from `roots` (a set of
/// immediate dependency ids — `output.deps` at the top-level call), by
/// following each reached node's own `deps` list outward. `all_nodes` is
/// this scope's *entire* registered node table (including any node a
/// builder created but never threaded into its final output), so the
/// result is exactly what `output` actually depends on, directly or
/// indirectly — the same set `define`'s node table and `orphan_errors_for`
/// both need, computed once here by a single O(V+E) graph walk instead of
/// by threading and `dict.merge`-ing a node map through every intermediate
/// `Port` a builder ever constructs (which cost O(N) per `both`/`map` call,
/// O(N^2) total for a builder with N such calls — see `bench/RESULTS.md`'s
/// "define time" section).
fn reachable_nodes(
  all_nodes: Dict(Int, Node(e, u)),
  roots: Set(Int),
) -> Dict(Int, Node(e, u)) {
  walk_reachable(all_nodes, set.to_list(roots), dict.new())
}

fn walk_reachable(
  all_nodes: Dict(Int, Node(e, u)),
  frontier: List(Int),
  acc: Dict(Int, Node(e, u)),
) -> Dict(Int, Node(e, u)) {
  case frontier {
    [] -> acc
    [id, ..rest] ->
      case dict.has_key(acc, id) {
        True -> walk_reachable(all_nodes, rest, acc)
        False ->
          case dict.get(all_nodes, id) {
            Error(Nil) ->
              // Only reachable through a foreign-scoped port, which already
              // reports its own `ForeignPort` error; nothing to add here.
              walk_reachable(all_nodes, rest, acc)
            Ok(a_node) ->
              walk_reachable(
                all_nodes,
                list.append(a_node.deps, rest),
                dict.insert(acc, id, a_node),
              )
          }
      }
  }
}

/// Every node `perform` created anywhere during this scope's one evaluation
/// (via `scope.registry`) that is not in `reachable` — the dependency
/// closure actually reachable from the workflow's own output. Such a node
/// was authored (its `Port` value exists, and may even have been bound to a
/// local variable and read) but never threaded, directly or indirectly,
/// into what the builder returned, so it would silently never run. Reported
/// once per orphaned node, not deduplicated by address, so two
/// independently-orphaned occurrences of the same step name are both named.
fn orphan_errors_for(
  scope: ScopeToken(e, u),
  output: Port(a, e, u),
  reachable: Dict(Int, Node(e, u)),
) -> List(DefinitionError) {
  case scope.id == output.scope.id {
    // A foreign-scoped output already reports `ForeignPort`; this scope's
    // own registry cannot be meaningfully diffed against a foreign node
    // table, so orphan detection is skipped rather than double-reporting.
    False -> []
    True ->
      cell.all_registered(scope.registry)
      |> list.filter_map(fn(a_node) {
        case dict.has_key(reachable, a_node.id) {
          True -> Error(Nil)
          False -> Ok(OrphanStep(address_from_node(a_node.address)))
        }
      })
  }
}

/// The workflow's own input port, keyed by `root_input_id` in the same
/// per-run `Store` every other node's value lives in. This is the *only*
/// port with no corresponding `Node` in the graph (nothing schedules or
/// admits it — its value is seeded directly by `for_run`, before any node
/// is admitted), so it uses `store.get`/`store.put` on that reserved id
/// exactly like an ordinary node's output would.
fn fresh_root_port(
  scope: ScopeToken(e, u),
  root_input_id: Int,
) -> Port(i, e, u) {
  Port(scope: scope, deps: set.new(), errors: [], fetch: fn() {
    fn(run_store) {
      let value = store.get(run_store, root_input_id)
      fn() { value }
    }
  })
}

/// Resolves builder-call-order occurrence ranks for every node produced by
/// one evaluation, ordered by ascending node id. Shared by `define`'s
/// static descriptors and `for_run`'s live graph, which both need the same
/// resolution so a define-time descriptor and its run-time address agree.
fn resolve_addresses(
  nodes_by_id: Dict(Int, Node(e, u)),
) -> #(List(Int), Dict(Int, StepAddress)) {
  let ordered_ids = dict.keys(nodes_by_id) |> list.sort(by: int.compare)
  let #(_counts, addresses_by_id) =
    list.fold(ordered_ids, #(dict.new(), dict.new()), fn(acc, id) {
      let #(counts, addresses) = acc
      let assert Ok(raw_node) = dict.get(nodes_by_id, id)
      let key = #(raw_node.address.scope, raw_node.address.name)
      let next_count = case dict.get(counts, key) {
        Ok(n) -> n + 1
        Error(_) -> 1
      }
      let resolved =
        StepAddress(
          scope: raw_node.address.scope,
          name: raw_node.address.name,
          occurrence: next_count,
        )
      #(
        dict.insert(counts, key, next_count),
        dict.insert(addresses, id, resolved),
      )
    })
  #(ordered_ids, addresses_by_id)
}

/// Builds every step's static descriptor from `resolved_nodes`'s own
/// output: a node table whose addresses are already resolved to their
/// correct builder-order occurrence rank, ordered by ascending node id
/// (`ordered_ids`). Takes both directly, rather than re-deriving them from
/// a `Port`, so this never re-runs `resolve_addresses` a second time over
/// the same table `define` already resolved once.
fn descriptors_for(
  ordered_ids: List(Int),
  nodes_by_id: Dict(Int, Node(e, u)),
) -> List(StepDescriptor) {
  list.map(ordered_ids, fn(id) {
    let assert Ok(raw_node) = dict.get(nodes_by_id, id)
    let depends_on =
      list.filter_map(raw_node.deps, fn(dep_id) {
        case dict.get(nodes_by_id, dep_id) {
          Ok(dep_node) -> Ok(address_from_node(dep_node.address))
          Error(Nil) -> Error(Nil)
        }
      })
    StepDescriptor(
      address: address_from_node(raw_node.address),
      depends_on: depends_on,
      undoable: raw_node.undoable,
      compensates: raw_node.compensates,
      max_attempts: raw_node.max_attempts,
      timeout: option.map(raw_node.timeout, duration.milliseconds),
    )
  })
}

/// A single node with its address resolved to the correct builder-order
/// occurrence rank, for the coordinator's node table.
fn resolved_nodes(
  nodes_by_id: Dict(Int, Node(e, u)),
) -> #(List(Int), Dict(Int, Node(e, u))) {
  let #(ordered_ids, addresses_by_id) = resolve_addresses(nodes_by_id)
  let with_resolved_addresses =
    dict.map_values(nodes_by_id, fn(id, raw_node) {
      let assert Ok(resolved_address) = dict.get(addresses_by_id, id)
      node.Node(..raw_node, address: address_to_node(resolved_address))
    })
  #(ordered_ids, with_resolved_addresses)
}

/// Prepares one fresh run of `workflow`: a brand-new `Store` seeded with
/// `input` at the workflow's reserved root-input id, alongside the
/// workflow's already-built node graph (shared, unchanged, across every
/// run — see `Workflow`'s doc comment) and a thunk that fetches the final
/// output once every node is done. Used only by `saga/execution`/
/// `saga/internal/coordinator`, which own process/run lifecycle; this
/// function performs no I/O and starts no process itself.
///
/// Every run gets its own initial `Store`, returned here so the coordinator
/// can pass it (and every later, updated version it produces via
/// `store.put` as nodes commit) to each node's `prepare_attempt`/
/// `prepare_crash_recovery` (see `saga/internal/node`) as it admits that
/// node — so concurrent or successive runs of the same `Workflow` never
/// observe each other's values despite replaying the identical node
/// closures.
///
/// The returned fetch-output thunk deliberately takes the `Store` as a
/// parameter, supplied by the caller at the moment every node is actually
/// done, rather than closing over the initial `run_store` above: `Store` is
/// immutable, so the coordinator's own, current, fully-committed store
/// (threaded through its `RunState` as each node's `commit` returns an
/// updated value) is a *different* value from the one seeded here, and is
/// the one that must be read from.
@internal
pub fn for_run(
  workflow: Workflow(i, o, e, u),
  input: i,
) -> #(
  Dict(Int, node.Node(e, u)),
  List(Int),
  Dict(Int, List(Int)),
  Store,
  fn(Store) -> ffi.RescueResult(o),
) {
  let run_store = store.new() |> store.put(workflow.root_input_id, input)
  // `workflow.fetch_output` (level 2 — see `Port`'s doc comment) must not be
  // called until every node is actually done: it is what performs the
  // dependency reads (`store.get`) for the workflow's final output, and
  // those values only exist once their producing nodes have committed. The
  // coordinator's own loop only invokes the thunk returned here once
  // `all_nodes_done`, passing its own current `Store` at that moment, so the
  // level-2 call is deferred to happen *inside* that thunk, alongside the
  // level-3 call and its `rescue` — never eagerly, here, at run setup,
  // before a single node has run.
  #(
    workflow.nodes,
    workflow.order,
    workflow.dependents,
    run_store,
    fn(current_store) {
      ffi.rescue(fn() {
        let produce_output = workflow.fetch_output(current_store)
        produce_output()
      })
    },
  )
}

/// The workflow's declared name.
pub fn name(workflow: Workflow(i, o, e, u)) -> String {
  workflow.name
}

/// Static, read-only descriptors for every step, in builder call order.
pub fn describe(workflow: Workflow(i, o, e, u)) -> List(StepDescriptor) {
  workflow.descriptors
}

/// Composes `workflow` into the graph being built for another workflow,
/// sharing the same run and journal (not an independent child). The embedded
/// steps are addressed under a nested scope named after `workflow`, so
/// repeated embeds and name collisions stay distinct in `describe`,
/// `address_to_string` and telemetry. An inner builder may use ports of the
/// outer workflow; a port from an unrelated definition is reported as
/// `ForeignPort`.
pub fn embed(
  input: Port(i, e, u),
  workflow: Workflow(i, o, e, u),
) -> Port(o, e, u) {
  let scoped_input =
    Port(
      ..input,
      scope: ScopeToken(
        id: input.scope.id,
        path: list.append(input.scope.path, [workflow.name]),
        registry: input.scope.registry,
      ),
    )
  let output = workflow.build(scoped_input)
  let foreign_errors = foreign_error_for(scoped_input.scope, output)
  // Restore the parent's own scope (path) on the output port, so a sibling
  // `perform`/`embed` chained after this one addresses its own steps at the
  // parent's path, not nested under this embed's.
  Port(
    ..output,
    scope: input.scope,
    errors: list.append(output.errors, foreign_errors),
  )
}

/// Adapts a whole workflow's error and undo-error types. The mapped
/// workflow reuses the original's built graph and wraps each step's results;
/// its builder never runs again for a standalone run, and runs once inside
/// another workflow's `define` when the mapped workflow is embedded.
pub fn map_errors(
  workflow: Workflow(i, o, e1, u1),
  error map_error: fn(e1) -> e2,
  undo_error map_undo_error: fn(u1) -> u2,
) -> Workflow(i, o, e2, u2) {
  let mapped_nodes =
    dict.map_values(workflow.nodes, fn(_id, a_node) {
      node.map_errors(a_node, map_error, map_undo_error)
    })
  let translating_build = fn(input: Port(i, e2, u2)) -> Port(o, e2, u2) {
    let shadow_registry = cell.new_registry()
    let shadow_input = retype_empty_port(input, shadow_registry)
    let shadow_output = workflow.build(shadow_input)
    // `workflow.build` is the same not-necessarily-pure builder `embed`'s
    // own doc comment warns about: it can return a `Port` stashed from a
    // different, unrelated `define`'s scope. Checked here, against the
    // *shadow* scope (the one `shadow_input` was actually built under),
    // before anything is registered into `input`'s own registry below —
    // exactly the same shape of check `embed` itself applies to its own
    // (unwrapped) builder call.
    let foreign_errors = foreign_error_for(shadow_input.scope, shadow_output)
    // Drains the shadow scope's own registry (holding every `Node(e1, u1)`
    // the original builder created, typed under its own original
    // vocabulary) and re-registers each one, translated, into `input`'s own
    // (e2, u2)-typed registry — an O(1) append per node, exactly like an
    // ordinary `perform` call's own registration, never a `dict.merge` of
    // two node maps. `input.scope.registry` is the *same* registry this
    // `embed`'s composing `define` will later read via `cell.all_registered`
    // to assemble its own node table and detect orphans, so a node
    // `workflow.build` creates but never threads into `shadow_output` is
    // still registered here — and still reported as an orphan there — as
    // it must be.
    cell.all_registered(shadow_registry)
    |> list.each(fn(a_node) {
      cell.register(
        input.scope.registry,
        node.map_errors(a_node, map_error, map_undo_error),
      )
    })
    cell.close(shadow_registry)
    Port(
      scope: input.scope,
      deps: shadow_output.deps,
      errors: list.append(shadow_output.errors, foreign_errors),
      fetch: shadow_output.fetch,
    )
  }
  Workflow(
    name: workflow.name,
    build: translating_build,
    root_input_id: workflow.root_input_id,
    nodes: mapped_nodes,
    order: workflow.order,
    // Unchanged by error-type mapping: `node.map_errors` only wraps a
    // node's closures, never its id or `deps`, so the dependency structure
    // `workflow.dependents` already describes is still exactly correct.
    dependents: workflow.dependents,
    fetch_output: workflow.fetch_output,
    descriptors: workflow.descriptors,
  )
}

/// Rebuilds a fresh port carrying the same scope id/path, dependencies,
/// errors, and fetch behaviour as `input`, but with a fresh `shadow_registry`
/// retyped for the original (e1, u1) vocabulary in place of `input`'s own
/// (e2, u2)-typed one. This is sound because a `Port`'s only field that
/// mentions `e`/`u` (besides `fetch`, untouched here) is its scope's
/// registry, and the shadow registry starts empty; every node the shadow
/// builder subsequently creates is registered into it under the correct
/// (e1, u1) type, then translated back through `node.map_errors` by
/// `translating_build` once the shadow build is done — never read under the
/// wrong type. Reusing `input.scope`'s `id` (not allocating a fresh one) is
/// what lets the original builder legitimately read a `Port` it captured
/// from its own outer scope: `foreign_error_for` only ever compares scope
/// *ids*, and this shadow evaluation must validate as the *same* scope the
/// composing `define` is building, not a foreign one. `deps` and `errors`
/// must be carried over unchanged (not reset): dropping `deps` would make
/// every step performed on this shadow input believe it has no dependencies
/// at all, admitting before its real upstream node finishes and deadlocking
/// on that node's still-unwritten cell; dropping `errors` would silently
/// discard definition errors collected before this `embed`/`map_errors`
/// boundary.
fn retype_empty_port(
  input: Port(i, e2, u2),
  shadow_registry: cell.Registry(Node(e1, u1)),
) -> Port(i, e1, u1) {
  Port(
    scope: ScopeToken(
      id: input.scope.id,
      path: input.scope.path,
      registry: shadow_registry,
    ),
    deps: input.deps,
    errors: input.errors,
    fetch: input.fetch,
  )
}

fn undo_option(undo: Undo(u)) -> Option(fn() -> Result(Nil, u)) {
  case undo {
    NoUndo -> None
    UndoWith(run) -> Some(run)
  }
}

/// Checks that every node can be restored and fingerprints its structural
/// dependencies, or lists every problem. Runtime node identifiers never
/// enter the fingerprint.
@internal
pub fn persistence_stamp(
  workflow: Workflow(i, o, e, u),
  version: String,
) -> Result(String, List(checkpoint.DefinitionProblem)) {
  let position = fn(id) {
    list.index_fold(workflow.order, -1, fn(found, candidate, index) {
      case candidate == id {
        True -> index
        False -> found
      }
    })
    |> int.to_string
  }
  let checked =
    list.map(workflow.order, fn(id) {
      let assert Ok(n) = dict.get(workflow.nodes, id)
      let address =
        checkpoint.Address(
          n.address.scope,
          n.address.name,
          n.address.occurrence,
        )
      case n.persistence {
        None -> Error([checkpoint.MissingRecoverable(address)])
        Some(p) -> {
          let problems =
            list.flatten([
              case p.step_version {
                "" -> [checkpoint.EmptyStepVersion(address)]
                _ -> []
              },
              case p.input_version {
                "" -> [
                  checkpoint.EmptyStepCodecVersion(checkpoint.StepInput(address)),
                ]
                _ -> []
              },
              case p.output_version {
                "" -> [
                  checkpoint.EmptyStepCodecVersion(checkpoint.StepOutput(
                    address,
                  )),
                ]
                _ -> []
              },
              case n.compensates && !p.recovery_undo_declared {
                True -> [checkpoint.MissingRestoreUndo(address)]
                False -> []
              },
            ])
          case problems {
            [_, ..] -> Error(problems)
            [] ->
              Ok(
                frame([
                  frame([
                    frame(n.address.scope),
                    n.address.name,
                    int.to_string(n.address.occurrence),
                  ]),
                  p.version,
                  case n.compensates {
                    True -> "compensates"
                    False -> "no-compensation"
                  },
                  int.to_string(n.max_attempts),
                  case n.timeout {
                    None -> "none"
                    Some(ms) -> int.to_string(ms)
                  },
                  case n.undoable {
                    True -> "undo"
                    False -> "no-undo"
                  },
                  frame(list.map(list.sort(n.deps, int.compare), position)),
                  frame(
                    list.map(n.conditions, fn(c) {
                      position(c.0)
                      <> case c.1 {
                        True -> "true"
                        False -> "false"
                      }
                    }),
                  ),
                ]),
              )
          }
        }
      }
    })
  let problems =
    list.flatten([
      case version {
        "" -> [checkpoint.EmptyWorkflowVersion]
        _ -> []
      },
      list.flat_map(checked, fn(node_check) {
        case node_check {
          Ok(_) -> []
          Error(problems) -> problems
        }
      }),
    ])
  case problems {
    [] -> Ok(frame([workflow.name, version, ..result.values(checked)]))
    _ -> Error(problems)
  }
}

fn frame(parts: List(String)) -> String {
  list.map(parts, fn(part) {
    int.to_string(string.byte_size(part)) <> ":" <> part
  })
  |> string.concat
}

fn has_undo(undo: Undo(u)) -> Bool {
  case undo {
    NoUndo -> False
    UndoWith(_) -> True
  }
}
