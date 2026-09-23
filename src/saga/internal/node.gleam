/// Erased per-node representation built while a `Workflow`'s builder is
/// evaluated for one run. A `Node(e, u)` hides the concrete input/output
/// types of one step behind closures bound at construction time, so the
/// coordinator's node table can be a homogeneous `Dict(Int, Node(e, u))`
/// without `Dynamic` or unsafe casts: the erased type parameters `e`/`u` are
/// still the caller's own workflow error and undo-error types.
import gleam/option.{type Option}
import saga/internal/ffi.{type CrashClass}

/// A step's recorded location: nested scope (from `embed`), a name, and the
/// 1-based occurrence rank among nodes sharing the same scope + name.
pub type StepAddress {
  StepAddress(scope: List(String), name: String, occurrence: Int)
}

pub type Attempt {
  Attempt(number: Int, remaining: Int)
}

pub type AttemptFailure(e) {
  Returned(error: e)
  Crashed(crash: Crash)
  TimedOut
}

pub type Crash {
  Crash(class: CrashClass, reason: String)
}

/// The erased form of `Recovery(o, e, u)`, produced by a node's recovery
/// decider closure. `EContinue` carries the same commit-then-undo shape as
/// `AttemptSucceeded`.
pub type ErasedRecovery(e, u) {
  ERetry
  ERetryAfter(milliseconds: Int)
  EContinue(commit: fn() -> Option(fn() -> Result(Nil, u)))
  EAbort(error: e)
  EAbortCleanup(error: e, cleanup_error: u)
  EHold(evidence: e)
}

/// The outcome of running one attempt body, computed inside a task process
/// and reported back to the coordinator.
///
/// On an application (`Returned`) failure, `recover` is already bound to
/// this exact failure and to the step's own (concrete-typed) decision
/// closure — it is not looked up separately by node id. This keeps
/// `saga.map_errors` sound: mapping a node only has to wrap the *outputs*
/// of `prepare_attempt` (the erased `e`/`u` values), never invert a
/// caller-supplied `e1 -> e2` map to reconstruct an original-typed failure
/// from a translated one. A crash or timeout is detected by the
/// coordinator itself (the task never returns), so it can never appear
/// here; those go through `Node.prepare_crash_recovery` instead, which
/// only ever receives an `AttemptFailure(e)` built from `Crashed`/`TimedOut`
/// — variants that carry no `e`-typed payload, so no inversion is needed
/// there either.
pub type AttemptResult(e, u) {
  AttemptSucceeded(commit: fn() -> Option(fn() -> Result(Nil, u)))
  AttemptFailed(
    failure: AttemptFailure(e),
    recover: Option(fn(Attempt) -> fn() -> ErasedRecovery(e, u)),
  )
}

/// Maps a node's erased error/undo-error vocabulary. Used to implement
/// `saga.map_errors` without unerasing the node's concrete step types:
/// every closure is wrapped to translate its result on the way out. This is
/// sound because `AttemptFailed.recover` is already bound to the original
/// failure (see `AttemptResult`); mapping only ever transforms outputs.
pub fn map_errors(
  a_node: Node(e1, u1),
  map_error: fn(e1) -> e2,
  map_undo_error: fn(u1) -> u2,
) -> Node(e2, u2) {
  Node(
    id: a_node.id,
    address: a_node.address,
    deps: a_node.deps,
    max_attempts: a_node.max_attempts,
    timeout: a_node.timeout,
    undoable: a_node.undoable,
    compensates: a_node.compensates,
    prepare_attempt: fn(attempt) {
      let body = a_node.prepare_attempt(attempt)
      fn() { map_attempt_result(body(), map_error, map_undo_error) }
    },
    prepare_crash_recovery: option.map(
      a_node.prepare_crash_recovery,
      fn(prepare) {
        fn(failure: AttemptFailure(e2), attempt: Attempt) {
          // `failure` only ever carries `Crashed`/`TimedOut` here (never
          // `Returned`, see `AttemptResult`'s doc comment), so re-tagging it
          // as `AttemptFailure(e1)` never touches an `e`-typed payload.
          let untagged = case failure {
            Crashed(crash) -> Crashed(crash)
            TimedOut -> TimedOut
            Returned(_) ->
              panic as "saga: Returned failure reached prepare_crash_recovery"
          }
          let body = prepare(untagged, attempt)
          fn() { map_erased_recovery(body(), map_error, map_undo_error) }
        }
      },
    ),
  )
}

fn map_undo_thunk(
  commit: fn() -> Option(fn() -> Result(Nil, u1)),
  map_undo_error: fn(u1) -> u2,
) -> fn() -> Option(fn() -> Result(Nil, u2)) {
  fn() {
    option.map(commit(), fn(undo_run) {
      fn() {
        case undo_run() {
          Ok(Nil) -> Ok(Nil)
          Error(error) -> Error(map_undo_error(error))
        }
      }
    })
  }
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

fn map_erased_recovery(
  recovery: ErasedRecovery(e1, u1),
  map_error: fn(e1) -> e2,
  map_undo_error: fn(u1) -> u2,
) -> ErasedRecovery(e2, u2) {
  case recovery {
    ERetry -> ERetry
    ERetryAfter(ms) -> ERetryAfter(ms)
    EContinue(commit) -> EContinue(map_undo_thunk(commit, map_undo_error))
    EAbort(error) -> EAbort(map_error(error))
    EAbortCleanup(error, cleanup_error) ->
      EAbortCleanup(map_error(error), map_undo_error(cleanup_error))
    EHold(evidence) -> EHold(map_error(evidence))
  }
}

fn map_attempt_result(
  result: AttemptResult(e1, u1),
  map_error: fn(e1) -> e2,
  map_undo_error: fn(u1) -> u2,
) -> AttemptResult(e2, u2) {
  case result {
    AttemptSucceeded(commit) ->
      AttemptSucceeded(map_undo_thunk(commit, map_undo_error))
    AttemptFailed(failure, recover) ->
      AttemptFailed(
        failure: map_attempt_failure(failure, map_error),
        recover: option.map(recover, fn(prepare) {
          fn(attempt) {
            let body = prepare(attempt)
            fn() { map_erased_recovery(body(), map_error, map_undo_error) }
          }
        }),
      )
  }
}

/// One node in the run's dependency graph. `prepare_attempt` is evaluated in
/// the coordinator (it only reads dependency cells) and returns a thunk
/// meant to run inside a task process. `prepare_crash_recovery` is the
/// coordinator's path to a recovery decision when the task itself never
/// returned a value (killed, timed out) — see the `AttemptResult` doc
/// comment for why this is a separate field from `AttemptFailed.recover`.
/// `undoable` and `compensates` record whether the authoring step
/// configured `undo`/`compensate`, independent of what a particular
/// attempt's closures happen to return at run time (kept for the static
/// `StepDescriptor`, which must not run any step to answer).
pub type Node(e, u) {
  Node(
    id: Int,
    address: StepAddress,
    deps: List(Int),
    max_attempts: Int,
    timeout: Option(Int),
    undoable: Bool,
    compensates: Bool,
    prepare_attempt: fn(Attempt) -> fn() -> AttemptResult(e, u),
    prepare_crash_recovery: Option(
      fn(AttemptFailure(e), Attempt) -> fn() -> ErasedRecovery(e, u),
    ),
  )
}
