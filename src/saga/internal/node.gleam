/// Erased per-node representation, built once when a `Workflow`'s builder is
/// evaluated at `define` time and replayed unchanged for every run of that
/// definition. A `Node(e, u)` hides the concrete input/output types of one
/// step behind closures bound at construction time, so the coordinator's
/// node table can be a homogeneous `Dict(Int, Node(e, u))` without `Dynamic`
/// or unsafe casts in *this* module: the erased type parameters `e`/`u` are
/// still the caller's own workflow error and undo-error types. Per-run
/// values themselves live in a `saga/internal/store.Store`, threaded
/// through `prepare_attempt`/`prepare_crash_recovery`/`commit` below — the
/// one unsafe coercion in the whole package lives in that module, never
/// here.
import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option}
import saga/internal/checkpoint
import saga/internal/ffi.{type CrashClass}
import saga/internal/store.{type Store}
import sinal/correlation.{type Correlation}

/// A step's recorded location: nested scope (from `embed`), a name, and the
/// 1-based occurrence rank among nodes sharing the same scope + name.
pub type StepAddress {
  StepAddress(scope: List(String), name: String, occurrence: Int)
}

/// One attempt as the coordinator dispatches it. `base` is the step's stable
/// key within its execution (`<bytes>:<execution>:<position>`), from which
/// `saga` derives every public `EffectKey` of the step.
pub type Attempt {
  Attempt(
    number: Int,
    remaining: Int,
    base: String,
    cancelled: Bool,
    persistent: Bool,
    saved_input: Option(String),
    admit: fn(String) -> Nil,
    correlation: Correlation,
  )
}

/// What a recovery resolver established about an interrupted action:
/// it completed with a value, failed with an error, provably never
/// happened, or may have happened.
pub type Evidence(o, e) {
  EvidenceCompleted(o)
  EvidenceFailed(e)
  EvidenceNotSent
  EvidenceMaybeSent
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
  EBlocked(checkpoint.Failure)
  ERetry
  ERetryAfter(milliseconds: Int)
  EContinue(commit: fn(Store) -> #(Store, Option(fn() -> Result(Nil, u))))
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
  AttemptBlocked(reason: checkpoint.Failure)
  AttemptAbsent
  AttemptSucceeded(
    commit: fn(Store) -> #(Store, Option(fn() -> Result(Nil, u))),
  )
  AttemptFailed(
    failure: AttemptFailure(e),
    recover: Option(fn(Attempt) -> fn() -> ErasedRecovery(e, u)),
    unknown: Bool,
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
    conditions: a_node.conditions,
    persistence: option.map(a_node.persistence, fn(p) {
      Persistence(
        version: p.version,
        step_version: p.step_version,
        input_version: p.input_version,
        output_version: p.output_version,
        recovery_undo_declared: p.recovery_undo_declared,
        freeze: p.freeze,
        thaw: fn(saved, run_store, key, correlation) {
          case p.thaw(saved, run_store, key, correlation) {
            Ok(commit) -> Ok(map_undo_thunk(commit, map_undo_error))
            Error(reason) -> Error(reason)
          }
        },
        resume_undo: fn(run_store, key, correlation) {
          let body = p.resume_undo(run_store, key, correlation)
          fn() {
            case body() {
              Ok(Ok(Nil)) -> Ok(Ok(Nil))
              Ok(Error(error)) -> Ok(Error(map_undo_error(error)))
              Error(reason) -> Error(reason)
            }
          }
        },
        resume_compensation: fn(attempt, run_store) {
          let body = p.resume_compensation(attempt, run_store)
          fn() { map_erased_recovery(body(), map_error, map_undo_error) }
        },
        resume: fn(attempt, run_store) {
          let body = p.resume(attempt, run_store)
          fn() { map_attempt_result(body(), map_error, map_undo_error) }
        },
      )
    }),
    max_attempts: a_node.max_attempts,
    timeout: a_node.timeout,
    undoable: a_node.undoable,
    compensates: a_node.compensates,
    rolls_back_unknown: a_node.rolls_back_unknown,
    prepare_attempt: fn(attempt, run_store) {
      let body = a_node.prepare_attempt(attempt, run_store)
      fn() { map_attempt_result(body(), map_error, map_undo_error) }
    },
    prepare_crash_recovery: option.map(
      a_node.prepare_crash_recovery,
      fn(prepare) {
        fn(failure: AttemptFailure(e2), attempt: Attempt, run_store: Store) {
          // `failure` only ever carries `Crashed`/`TimedOut` here (never
          // `Returned`, see `AttemptResult`'s doc comment), so re-tagging it
          // as `AttemptFailure(e1)` never touches an `e`-typed payload.
          let untagged = case failure {
            Crashed(crash) -> Crashed(crash)
            TimedOut -> TimedOut
            Returned(_) ->
              panic as "saga: Returned failure reached prepare_crash_recovery"
          }
          let body = prepare(untagged, attempt, run_store)
          fn() { map_erased_recovery(body(), map_error, map_undo_error) }
        }
      },
    ),
  )
}

fn map_undo_thunk(
  commit: fn(Store) -> #(Store, Option(fn() -> Result(Nil, u1))),
  map_undo_error: fn(u1) -> u2,
) -> fn(Store) -> #(Store, Option(fn() -> Result(Nil, u2))) {
  fn(run_store) {
    let #(next_store, undo_option) = commit(run_store)
    #(
      next_store,
      option.map(undo_option, fn(undo_run) {
        fn() {
          case undo_run() {
            Ok(Nil) -> Ok(Nil)
            Error(error) -> Error(map_undo_error(error))
          }
        }
      }),
    )
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
    EBlocked(reason) -> EBlocked(reason)
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
    AttemptBlocked(reason) -> AttemptBlocked(reason)
    AttemptAbsent -> AttemptAbsent
    AttemptSucceeded(commit) ->
      AttemptSucceeded(map_undo_thunk(commit, map_undo_error))
    AttemptFailed(failure, recover, unknown) ->
      AttemptFailed(
        failure: map_attempt_failure(failure, map_error),
        recover: option.map(recover, fn(prepare) {
          fn(attempt) {
            let body = prepare(attempt)
            fn() { map_erased_recovery(body(), map_error, map_undo_error) }
          }
        }),
        unknown: unknown,
      )
  }
}

/// One node in the run's dependency graph. `prepare_attempt` is evaluated in
/// the coordinator (it only reads dependency values from that run's
/// `saga/internal/store.Store`) and returns a thunk meant to run inside a
/// task process. `prepare_crash_recovery` is the
/// coordinator's path to a recovery decision when the task itself never
/// returned a value (killed, timed out) — see the `AttemptResult` doc
/// comment for why this is a separate field from `AttemptFailed.recover`.
/// `undoable` and `compensates` record whether the authoring step
/// configured `undo`/`compensate`, independent of what a particular
/// attempt's closures happen to return at run time (kept for the static
/// `StepDescriptor`, which must not run any step to answer).
/// `rolls_back_unknown` is the step's `saga.on_unknown(RollBack)`: whether
/// an error classified unknown, with no decision to settle it, rolls the
/// run back instead of ending it unresolved.
pub type Node(e, u) {
  Node(
    id: Int,
    address: StepAddress,
    deps: List(Int),
    conditions: List(#(Int, Bool)),
    persistence: Option(Persistence(e, u)),
    max_attempts: Int,
    timeout: Option(Int),
    undoable: Bool,
    compensates: Bool,
    rolls_back_unknown: Bool,
    prepare_attempt: fn(Attempt, Store) -> fn() -> AttemptResult(e, u),
    prepare_crash_recovery: Option(
      fn(AttemptFailure(e), Attempt, Store) -> fn() -> ErasedRecovery(e, u),
    ),
  )
}

/// The reverse-dependency index: for each node id, every other node that
/// directly depends on it. Depends only on each node's own fixed `deps`
/// list, which is itself fixed once a `Workflow`'s graph is built (at
/// `define` time) — so this is computed once, at `define`, and stored on
/// the `Workflow`, then reused unchanged across every run of that
/// definition, rather than recomputed per run. Prepends within each
/// dependent list and reverses once at the end, rather than
/// `list.append`ing one element at a time (which would be O(k) per
/// append, O(k^2) total for a node with k dependents), so building the
/// whole index is O(N) total, not O(N^2).
pub fn build_dependents(nodes: Dict(Int, Node(e, u))) -> Dict(Int, List(Int)) {
  let reversed =
    dict.fold(nodes, dict.new(), fn(acc, id, n) {
      list.fold(n.deps, acc, fn(acc2, dep_id) {
        dict.upsert(acc2, dep_id, fn(existing) {
          case existing {
            option.Some(ids) -> [id, ..ids]
            option.None -> [id]
          }
        })
      })
    })
  dict.map_values(reversed, fn(_dep_id, ids) { list.reverse(ids) })
}

/// Node-specific codecs remain bound to the node's concrete value types.
/// The `String` that `freeze`, `thaw` and `resume_undo` receive is the step's
/// stable key base (see `Attempt.base`); they also receive the run's
/// correlation, which the undo's `EffectKey` carries.
pub type Persistence(e, u) {
  Persistence(
    version: String,
    step_version: String,
    input_version: String,
    output_version: String,
    recovery_undo_declared: Bool,
    freeze: fn(Store, String, Correlation) ->
      Result(List(String), checkpoint.Failure),
    thaw: fn(List(String), Store, String, Correlation) ->
      Result(
        fn(Store) -> #(Store, Option(fn() -> Result(Nil, u))),
        checkpoint.Failure,
      ),
    resume_compensation: fn(Attempt, Store) -> fn() -> ErasedRecovery(e, u),
    resume_undo: fn(Store, String, Correlation) ->
      fn() -> Result(Result(Nil, u), checkpoint.Failure),
    resume: fn(Attempt, Store) -> fn() -> AttemptResult(e, u),
  )
}
