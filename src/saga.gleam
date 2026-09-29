/// Typed workflow authoring: steps, undo, compensation and recovery
/// vocabulary, typed ports, composition, and definition validation.
///
/// A `Workflow(input, output, error, undo_error)` is a pure description. Its
/// builder closure is evaluated exactly once, at `define` time (to validate
/// names, attempts, timeouts, and to compute the workflow's static node
/// graph) — never again by any run. Dependencies are expressed through
/// typed `Port` values rather than names, so wiring is checked by the
/// compiler: no step ever looks up a dependency by name in a shared map,
/// and no *authoring*-time value ever passes through `Dynamic` or an
/// unsafe cast. Per-run values do live in one central, run-scoped store
/// (`saga/internal/store`) keyed by node id — a deliberate trade for O(1)
/// reads instead of the O(N) per-node-mailbox reads an earlier design used
/// (see `bench/RESULTS.md` and the design-decisions note in README.md);
/// that store is the *only* place in this package with an unsafe coercion,
/// and it is sound by construction — see `saga/internal/store`'s doc
/// comment. `Workflow`'s own doc comment states precisely which two
/// functions are allowed to invoke a builder at all.
import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set.{type Set}
import gleam/string
import saga/codec.{type Codec}
import saga/internal/cell
import saga/internal/checkpoint
import saga/internal/ffi
import saga/internal/node.{
  type ErasedRecovery, type Node, AttemptSucceeded, Node,
}
import saga/internal/store.{type Store}
import saga/reconciliation

/// A step's recorded location: nested scope (from `embed`), a name, and the
/// 1-based occurrence rank among nodes sharing the same scope + name.
pub type StepAddress {
  StepAddress(scope: List(String), name: String, occurrence: Int)
}

/// One attempt of a step: its 1-based `number`, and how many further
/// attempts `remaining` allows (excluding this one).
pub type Attempt {
  Attempt(number: Int, remaining: Int)
}

/// Why one attempt did not succeed: the step's own `run` returned an
/// application error, the attempt task crashed, or (increment 2) it timed
/// out.
pub type AttemptFailure(e) {
  Returned(error: e)
  Crashed(crash: Crash)
  TimedOut
}

/// A reified native exception: which class was raised, and a formatted
/// reason. Produced only when a task crashes outside its own `Result`.
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

// ---------------------------------------------------------------------------
// Conversions to/from `saga/internal/node`'s own copies of this vocabulary.
//
// `node.gleam` cannot import these types from `saga.gleam` (that would
// create an import cycle: `saga` already imports `Node`/`ErasedRecovery`
// from `node`), so `node` keeps its own structurally identical definitions
// and every value crosses the boundary through these functions.
// ---------------------------------------------------------------------------

fn address_to_node(address: StepAddress) -> node.StepAddress {
  node.StepAddress(address.scope, address.name, address.occurrence)
}

/// Converts a `node`-owned address back to the public vocabulary. Exposed
/// for `saga/execution`, which must translate `saga/internal/coordinator`
/// outcomes (built in terms of `node`'s copies) into the public `Outcome`.
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

/// Converts a `node`-owned crash back to the public vocabulary. Exposed for
/// `saga/execution`, for the same reason as `address_from_node`.
@internal
pub fn crash_from_node(crash: node.Crash) -> Crash {
  Crash(crash_class_from_node(crash.class), crash.reason)
}

fn crash_to_node(crash: Crash) -> node.Crash {
  node.Crash(crash_class_to_node(crash.class), crash.reason)
}

/// Converts a `node`-owned failure back to the public vocabulary. Exposed
/// for `saga/execution`, for the same reason as `address_from_node`.
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
/// the budget allows); `Continue` accepts a replacement output with its own
/// undo; `Abort` fails the run and permits rollback of completed steps;
/// `AbortAfterCleanupFailure` additionally records a cleanup error that
/// happened while deciding; `Hold` leaves prior effects unresolved with no
/// rollback authority.
pub type Recovery(o, e, u) {
  Retry
  RetryAfter(milliseconds: Int)
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
pub type DefinitionError {
  EmptyWorkflowName
  EmptyStepName(scope: List(String))
  InvalidMaxAttempts(step: StepAddress, value: Int)
  InvalidTimeout(step: StepAddress, value: Int)
  ForeignPort(step: StepAddress)
  /// A node was created during the builder (via `perform`/`embed`) but its
  /// output port was never consumed by anything reaching the workflow's
  /// final output — the step would silently never run. Named so the
  /// message can point at exactly which step(s) are unreachable.
  OrphanStep(step: StepAddress)
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
    timeout: Option(Int),
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
/// `attempt`'s `Failed` case carries `recover_returned` already *bound* to
/// the concrete `e`-typed error it just produced and to the caller-supplied
/// `decide` — it is never re-exposed as a standalone function of an
/// abstract failure. This is what keeps `map_step_errors` sound: mapping a
/// step only ever has to translate the *outputs* `attempt` itself computed
/// (a fresh `e`/`u` value it produced, never one reconstructed from a
/// translated value), so `map_error: e1 -> e2` is only ever called forward,
/// never inverted. `decide_crash` is separate and covers the other two
/// `AttemptFailure` variants (`Crashed`/`TimedOut`), which is how the
/// coordinator asks for a recovery decision on a failure it detected
/// itself (the task never returned a value) without re-invoking — and
/// re-running the side effect of — `attempt`. Those variants carry no
/// `e`-typed payload, so `decide_crash` needs no such binding trick either.
pub opaque type Step(i, o, e, u) {
  Step(
    name: String,
    max_attempts: Int,
    timeout: Option(Int),
    undoable: Bool,
    compensates: Bool,
    attempt: fn(i, String) -> RunOutcome(o, u, e),
    persistence: Option(StepPersistence(i, o, e)),
    undo_for: fn(i, o, String) -> Undo(u),
    recovery_undo_declared: Bool,
    resolve_undo: fn(i, o, String) -> UndoStatus(u),
    resolve_compensation: fn(i, Attempt, String) -> CompensationStatus(o, e, u),
    decide_returned: Option(fn(i, e, Attempt, String) -> Recovery(o, e, u)),
    decide_crash: Option(
      fn(i, CrashOrTimeout, Attempt, String) -> Recovery(o, e, u),
    ),
  )
}

/// A recovery-triggering failure that never carries an `e`-typed payload:
/// exactly the two `AttemptFailure` variants the coordinator can detect on
/// its own, without the task ever returning a value.
pub type CrashOrTimeout {
  StepCrashed(crash: Crash)
  StepTimedOut
}

type RunOutcome(o, u, e) {
  Succeeded(output: o, undo: Undo(u))
  Failed(
    failure: AttemptFailure(e),
    recover_returned: Option(fn(Attempt, String) -> Recovery(o, e, u)),
  )
}

/// Creates a step from its name and its run function. With no further
/// modifiers, a failure is terminal after one attempt and nothing is undone
/// on rollback.
pub fn step(name: String, run: fn(i) -> Result(o, e)) -> Step(i, o, e, u) {
  effect(name, fn(input, _key) { run(input) })
}

/// An effect receives a stable per-attempt key when persistence is enabled.
pub fn effect(
  name: String,
  run: fn(i, String) -> Result(o, e),
) -> Step(i, o, e, u) {
  Step(
    name: name,
    max_attempts: 1,
    timeout: None,
    undoable: False,
    compensates: False,
    persistence: None,
    undo_for: fn(_, _, _) { NoUndo },
    recovery_undo_declared: False,
    resolve_undo: fn(_, _, _) { UndoUnknown },
    resolve_compensation: fn(_, _, _) { CompensationUnknown },
    decide_returned: None,
    attempt: fn(input, key) {
      case run(input, key) {
        Ok(output) -> Succeeded(output, NoUndo)
        Error(error) -> Failed(Returned(error), None)
      }
    },
    decide_crash: None,
  )
}

/// Attaches an undo action, run only if this step's attempt already
/// succeeded and the run later rolls back. Receives the same input and the
/// output that succeeded.
pub fn undo(
  step: Step(i, o, e, u),
  undo_fn: fn(i, o) -> Result(Nil, u),
) -> Step(i, o, e, u) {
  undo_effect(step, fn(input, output, _) { undo_fn(input, output) })
}

/// An undo action with the execution's stable undo key, used identically
/// during normal rollback and restoration.
pub fn undo_effect(
  step: Step(i, o, e, u),
  undo_fn: fn(i, o, String) -> Result(Nil, u),
) -> Step(i, o, e, u) {
  let attempt = step.attempt
  Step(
    ..step,
    undoable: True,
    undo_for: fn(i, o, key) { UndoWith(fn() { undo_fn(i, o, key) }) },
    attempt: fn(input, key) {
      case attempt(input, key) {
        Succeeded(output, NoUndo) ->
          Succeeded(
            output,
            UndoWith(fn() { undo_fn(input, output, undo_key(key)) }),
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
/// the outcome's `execution.unknown_effects`. In particular, `Abort(error)`
/// after a crash ends the run with `execution.StepFailed(step, error)`, the
/// same cause as an aborted typed error: choose an `error` that says the
/// attempt crashed if the caller must tell the two apart from the cause.
pub fn compensate(
  step: Step(i, o, e, u),
  max_attempts max_attempts: Int,
  with decide: fn(i, AttemptFailure(e), Attempt) -> Recovery(o, e, u),
) -> Step(i, o, e, u) {
  compensate_with_key(step, max_attempts, fn(input, failure, attempt, _) {
    decide(input, failure, attempt)
  })
}

/// A compensation callback receives a stable key for this step and attempt.
/// A restart consults `reconcile_compensation`; it never repeats this callback.
pub fn compensate_with_key(
  step: Step(i, o, e, u),
  max_attempts max_attempts: Int,
  with decide: fn(i, AttemptFailure(e), Attempt, String) -> Recovery(o, e, u),
) -> Step(i, o, e, u) {
  let attempt = step.attempt
  Step(
    ..step,
    max_attempts: max_attempts,
    compensates: True,
    decide_returned: Some(fn(input, error, attempt, key) {
      decide(input, Returned(error), attempt, key)
    }),
    attempt: fn(input, key) {
      case attempt(input, key) {
        Failed(failure, _) ->
          Failed(
            failure,
            Some(fn(attempt_no, key) { decide(input, failure, attempt_no, key) }),
          )
        unchanged -> unchanged
      }
    },
    decide_crash: Some(fn(input, crash_or_timeout, attempt_no, key) {
      let failure = case crash_or_timeout {
        StepCrashed(crash) -> Crashed(crash)
        StepTimedOut -> TimedOut
      }
      decide(input, failure, attempt_no, key)
    }),
  )
}

/// Bounds one attempt (and one compensation decision) to `milliseconds`.
pub fn timeout(step: Step(i, o, e, u), milliseconds: Int) -> Step(i, o, e, u) {
  Step(..step, timeout: Some(milliseconds))
}

/// Adapts a step's error and undo-error types into a unified workflow
/// vocabulary. See the `Step` doc comment for why this only ever
/// translates `attempt`/`decide_crash`'s *outputs*.
pub fn map_step_errors(
  step: Step(i, o, e1, u1),
  error map_error: fn(e1) -> e2,
  undo_error map_undo_error: fn(u1) -> u2,
) -> Step(i, o, e2, u2) {
  Step(
    name: step.name,
    max_attempts: step.max_attempts,
    timeout: step.timeout,
    undoable: step.undoable,
    compensates: step.compensates,
    persistence: None,
    recovery_undo_declared: step.recovery_undo_declared,
    resolve_undo: fn(i, o, key) {
      case step.resolve_undo(i, o, key) {
        UndoCompleted -> UndoCompleted
        UndoStillApplied -> UndoStillApplied
        UndoUnknown -> UndoUnknown
        UndoFailed(error) -> UndoFailed(map_undo_error(error))
      }
    },
    resolve_compensation: fn(input, attempt, key) {
      case step.resolve_compensation(input, attempt, key) {
        CompensationUnknown -> CompensationUnknown
        CompensationResolved(recovery) ->
          CompensationResolved(map_recovery(recovery, map_error, map_undo_error))
      }
    },
    decide_returned: None,
    undo_for: fn(i, o, key) {
      map_undo(step.undo_for(i, o, key), map_undo_error)
    },
    attempt: fn(input, key) {
      case step.attempt(input, key) {
        Succeeded(output, undo_choice) ->
          Succeeded(output, map_undo(undo_choice, map_undo_error))
        Failed(failure, recover_returned) ->
          Failed(
            failure: map_attempt_failure(failure, map_error),
            recover_returned: option.map(recover_returned, fn(recover_fn) {
              fn(attempt_no, key) {
                recover_fn(attempt_no, key)
                |> map_recovery(map_error, map_undo_error)
              }
            }),
          )
      }
    },
    decide_crash: option.map(step.decide_crash, fn(decide_fn) {
      fn(
        input: i,
        crash_or_timeout: CrashOrTimeout,
        attempt_no: Attempt,
        key: String,
      ) {
        decide_fn(input, crash_or_timeout, attempt_no, key)
        |> map_recovery(map_error, map_undo_error)
      }
    }),
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
    RetryAfter(ms) -> RetryAfter(ms)
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

/// A typed reference to a value produced somewhere in the workflow graph
/// being built: either the workflow's own input, or the output of a
/// `perform`/`map`/`both`/`all`/`embed`. Two consumers of the same `Port`
/// value depend on the same node, so that node executes once per run.
///
/// `fetch` has three levels, each meant to run in a different place:
///
///  1. The outer call happens while the graph is being built, exactly once
///     per `Workflow` (in `perform`, `both`, `all`, `map`, and `define`'s
///     final output read): purely structural, composing closures, never
///     touching a run's values.
///  2. The middle call happens in the coordinator, once per attempt
///     (`perform`'s `prepare_attempt`/`prepare_crash_recovery`) or once for
///     the final output (`for_run`'s `fetch_output`), and is given that
///     run's `Store`: it performs every underlying dependency read
///     (`store.get`, only ever called by the coordinator — the process that
///     owns this run's `Store`) and returns a *pure* thunk with the raw
///     dependency value(s) already captured.
///  3. The inner call happens wherever that pure thunk is actually run: for
///     `perform`, inside the spawned attempt/recovery task, under `rescue`.
///     This is the only level `map`'s `with` function is ever invoked from,
///     so a panicking or slow `map` becomes an ordinary attempt
///     crash/duration instead of reaching the coordinator.
///
/// Because the graph is now built once at `define` (see `Workflow`'s doc
/// comment) and every subsequent run of the same definition replays the
/// same node closures, `fetch`'s outer (level 1) call happens only once,
/// ever, per `Workflow` value -- not once per run as it did when the
/// builder was re-evaluated fresh for every run. What *is* per-run is the
/// `Store` threaded into level 2, which is why a value read can never leak
/// between two runs of the same definition despite sharing one build.
///
/// `deps` holds only this port's own *immediate* dependency ids (a single
/// id for a `perform` result, the union of two ports' immediate ids for
/// `both`/`map`'s pass-through) -- never the full transitive node graph.
/// The full node table is never carried by any `Port` at all: every node
/// `perform` creates is appended, once, to its scope's own `registry` (see
/// `ScopeToken`'s doc comment), and `define` assembles the final table by
/// walking `deps` transitively (via each node's own `deps` list) from the
/// workflow's output port, once, at the very end -- rather than every
/// `both`/`map` call merging an ever-larger node map copied from its
/// inputs. See `bench/RESULTS.md`'s "define time" section for the
/// superlinear cost this replaces.
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
        |> recoverable("choice-1", codec.bool(), codec.bool(), fn(value, _) {
          EffectCompleted(value)
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

  let to_erased_recovery = fn(recovery: Recovery(o, e, u), input: i) -> ErasedRecovery(
    e,
    u,
  ) {
    case recovery {
      Retry -> node.ERetry
      RetryAfter(ms) -> node.ERetryAfter(ms)
      Continue(output, undo_choice) ->
        node.EContinue(commit: fn(run_store) {
          #(
            run_store
              |> store.put(id, output)
              |> store.put_record(id, #(input, output, has_undo(undo_choice))),
            case undo_choice {
              NoUndo -> None
              UndoWith(run) -> Some(fn() { run() })
            },
          )
        })
      Abort(error) -> node.EAbort(error)
      AbortAfterCleanupFailure(error, cleanup_error) ->
        node.EAbortCleanup(error, cleanup_error)
      Hold(evidence) -> node.EHold(evidence)
    }
  }

  // `prepare_attempt`/`prepare_crash_recovery` are themselves called by the
  // coordinator, so this is where `capture_input` (the port's fetch,
  // level 2 — see `Port`'s doc comment) is invoked: every underlying
  // dependency read it performs (`store.get`, which only the coordinator —
  // the process that owns this run's `Store` — may call) happens here, in
  // the coordinator. Its result, `produce_value`, is the pure level-3
  // thunk: for a plain dependency it just returns the already-read value,
  // but for a `map`med port it also closes over the caller-supplied
  // (potentially panicking, potentially slow) transformation function.
  // `produce_value` is therefore only ever invoked from inside the spawned
  // task body below, under `rescue`, never here — so a panicking or slow
  // `map` becomes an ordinary attempt crash/duration instead of taking the
  // coordinator down or blocking it.
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
          use _ <- result.try(node_attempt.admit(encoded))
          Ok(value)
        }
        _, _ -> Ok(produce_value())
      }
      case prepared {
        Error(reason) -> node.AttemptBlocked(checkpoint.CodecFailure(reason))
        Ok(value) ->
          case step.attempt(value, node_attempt.key) {
            Succeeded(output, undo_choice) ->
              AttemptSucceeded(commit: fn(commit_store) {
                #(
                  commit_store
                    |> store.put(id, output)
                    |> store.put_record(id, #(
                      value,
                      output,
                      has_undo(undo_choice),
                    )),
                  case undo_choice {
                    NoUndo -> None
                    UndoWith(run) -> Some(fn() { run() })
                  },
                )
              })
            Failed(failure, recover_returned) ->
              node.AttemptFailed(
                failure: failure_to_node(failure),
                recover: option.map(recover_returned, fn(recover_fn) {
                  fn(node_attempt: node.Attempt) {
                    fn() {
                      to_erased_recovery(
                        recover_fn(
                          attempt_from_node(node_attempt),
                          compensation_key(node_attempt.key),
                        ),
                        value,
                      )
                    }
                  }
                }),
              )
          }
      }
    }
  }

  // The coordinator's path for a crash/timeout it observed itself (the task
  // never returned an `AttemptResult` at all, so `AttemptFailed.recover` was
  // never bound). This calls `decide_crash` directly with the input value
  // for this attempt — not a re-run of the step's effect, so nothing is
  // repeated. As with `prepare_attempt` above, `capture_input(run_store)`
  // (every underlying dependency read) runs here, in the coordinator, while
  // the resulting `produce_value` thunk is only invoked inside the returned
  // inner thunk (run inside the recovery task, under `rescue`), so a
  // panicking or slow upstream `map` cannot crash or block the coordinator.
  let prepare_crash_recovery =
    option.map(step.decide_crash, fn(decide_fn) {
      fn(
        node_failure: node.AttemptFailure(e),
        node_attempt: node.Attempt,
        run_store: Store,
      ) -> fn() -> ErasedRecovery(e, u) {
        let produce_value = capture_input(run_store)
        let crash_or_timeout = case node_failure {
          node.Crashed(crash) -> StepCrashed(crash_from_node(crash))
          node.TimedOut -> StepTimedOut
          node.Returned(_) ->
            panic as "saga: prepare_crash_recovery received a Returned failure"
        }
        let attempt = attempt_from_node(node_attempt)
        fn() {
          let input = case
            node_attempt.persistent,
            step.persistence,
            node_attempt.saved_input
          {
            True, Some(p), Some(encoded) -> codec.decode(p.input, encoded)
            True, _, None -> Error("compensation has no admitted input")
            _, _, _ -> Ok(produce_value())
          }
          case input {
            Error(reason) -> node.EBlocked(checkpoint.CodecFailure(reason))
            Ok(input) ->
              to_erased_recovery(
                decide_fn(
                  input,
                  crash_or_timeout,
                  attempt,
                  compensation_key(node_attempt.key),
                ),
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
          recovery_undo_declared: step.recovery_undo_declared,
          valid: !list.contains(
            [p.version, codec.version(p.input), codec.version(p.output)],
            "",
          ),
          freeze: fn(run_store, key) {
            use pair <- result.try(case store.get_record(run_store, id) {
              Ok(pair) -> Ok(pair)
              Error(Nil) -> Error("missing checkpoint value")
            })
            let #(input, output, undoable) = pair
            use _ <- result.try(
              case undoable && !has_undo(step.undo_for(input, output, key)) {
                True ->
                  Error(
                    "undo reconstruction contract returned NoUndo: "
                    <> step.name,
                  )
                False -> Ok(Nil)
              },
            )
            use input <- result.try(codec.encode(p.input, input))
            use output <- result.try(codec.encode(p.output, output))
            Ok([
              input,
              output,
              case undoable {
                True -> "undo"
                False -> "none"
              },
            ])
          },
          thaw: fn(saved, _run_store, key) {
            case saved {
              [input, output, undo_kind] -> {
                use input <- result.try(codec.decode(p.input, input))
                use output <- result.try(codec.decode(p.output, output))
                use undo <- result.try(case undo_kind {
                  "none" -> Ok(None)
                  "undo" ->
                    case step.undo_for(input, output, key) {
                      NoUndo ->
                        Error("saved undo requires restore_undo: " <> step.name)
                      UndoWith(run) -> Ok(Some(run))
                    }
                  _ -> Error("invalid undo capability")
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
              _ -> Error("invalid saved step")
            }
          },
          resume_compensation: fn(attempt, _run_store) {
            fn() {
              let key = compensation_key(attempt.key)
              case attempt.saved_input {
                None ->
                  node.EBlocked(checkpoint.InvalidState(
                    "compensation has no admitted input",
                  ))
                Some(encoded) ->
                  case codec.decode(p.input, encoded) {
                    Error(reason) ->
                      node.EBlocked(checkpoint.CodecFailure(reason))
                    Ok(input) ->
                      case
                        step.resolve_compensation(
                          input,
                          attempt_from_node(attempt),
                          key,
                        )
                      {
                        CompensationUnknown ->
                          node.EBlocked(
                            checkpoint.Uncertain(reconciliation.Required(
                              step.name,
                              reconciliation.Compensation,
                              key,
                            )),
                          )
                        CompensationResolved(recovery) ->
                          to_erased_recovery(recovery, input)
                      }
                  }
              }
            }
          },
          resume_undo: fn(run_store, key) {
            fn() {
              use pair <- result.try(case store.get_record(run_store, id) {
                Ok(pair) -> Ok(pair)
                Error(Nil) ->
                  Error(checkpoint.InvalidState("missing undo values"))
              })
              let #(input, output, _) = pair
              case step.resolve_undo(input, output, key) {
                UndoCompleted -> Ok(Ok(Nil))
                UndoFailed(error) -> Ok(Error(error))
                UndoUnknown ->
                  Error(
                    checkpoint.Uncertain(reconciliation.Required(
                      step.name,
                      reconciliation.Undo,
                      key,
                    )),
                  )
                UndoStillApplied ->
                  case step.undo_for(input, output, key) {
                    NoUndo ->
                      Error(checkpoint.InvalidState("missing restored undo"))
                    UndoWith(run) -> Ok(run())
                  }
              }
            }
          },
          resume: fn(attempt, run_store) {
            let produce = capture_input(run_store)
            fn() {
              let input = case attempt.saved_input {
                None -> Ok(produce())
                Some(encoded) -> codec.decode(p.input, encoded)
              }
              case input {
                Error(reason) ->
                  node.AttemptBlocked(checkpoint.CodecFailure(reason))
                Ok(input) ->
                  case attempt.saved_input {
                    None ->
                      case attempt.cancelled {
                        True -> node.AttemptAbsent
                        False -> prepare_attempt(attempt, run_store)()
                      }
                    Some(_) ->
                      case p.resolve(input, attempt.key) {
                        EffectUnknown ->
                          node.AttemptBlocked(
                            checkpoint.Uncertain(reconciliation.Required(
                              step.name,
                              reconciliation.Activity,
                              attempt.key,
                            )),
                          )
                        EffectAbsent ->
                          case attempt.cancelled {
                            True -> node.AttemptAbsent
                            False -> prepare_attempt(attempt, run_store)()
                          }
                        EffectCompleted(output) -> {
                          let undo =
                            step.undo_for(input, output, undo_key(attempt.key))
                          AttemptSucceeded(fn(run_store) {
                            #(
                              run_store
                                |> store.put(id, output)
                                |> store.put_record(id, #(
                                  input,
                                  output,
                                  has_undo(undo),
                                )),
                              undo_option(undo),
                            )
                          })
                        }
                        EffectFailed(error) ->
                          case step.decide_returned, step.compensates {
                            None, True ->
                              node.AttemptBlocked(checkpoint.InvalidState(
                                "mapped recovery requires a decider after error mapping",
                              ))
                            decider, _ ->
                              node.AttemptFailed(
                                node.Returned(error),
                                option.map(decider, fn(decide) {
                                  fn(node_attempt) {
                                    fn() {
                                      to_erased_recovery(
                                        decide(
                                          input,
                                          error,
                                          attempt_from_node(node_attempt),
                                          compensation_key(node_attempt.key),
                                        ),
                                        input,
                                      )
                                    }
                                  }
                                }),
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
      timeout: step.timeout,
      undoable: step.undoable,
      compensates: step.compensates,
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
    Some(ms) if ms > 0 -> []
    Some(ms) -> [InvalidTimeout(step: address, value: ms)]
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

fn attempt_from_node(attempt: node.Attempt) -> Attempt {
  Attempt(attempt.number, attempt.remaining)
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
/// **A builder function is invoked in exactly two situations, each exactly
/// once, and never any other way:**
///
///  1. `define(name, build)` calls `build` exactly once, in the calling
///     process, before any run exists, to validate the workflow and to
///     compute this static, immutable graph (`nodes`/`order`/
///     `root_input_id`/`fetch_output`).
///  2. `embed(input, workflow)`, called from *inside* some other,
///     unrelated `define`'s own builder, calls `workflow`'s `build` field
///     exactly once — this is that *other* `define`'s one evaluation
///     validating and incorporating `workflow`'s subgraph at a fresh input
///     port it owns, not a second evaluation of `workflow` itself.
///
/// No other function ever calls a builder. In particular: no run
/// (`execution.run`/`start`, however many, however concurrent) calls
/// `build` — `for_run` replays the *already-built* `nodes`/`order`/
/// `fetch_output` against a fresh `saga/internal/store.Store` instead (see
/// that function's doc comment for why this is safe despite the graph
/// being shared). And `map_errors` never calls `build` either: it reuses
/// `workflow`'s own already-built graph, wrapping only the node closures
/// (`node.map_errors`) — its own `build` field exists solely so a *later*
/// `embed` of the mapped workflow has one to call, per case 2 above; it is
/// never invoked to compute the mapped workflow's own `nodes`/`order`/
/// `fetch_output`.
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

/// Builds and validates a named workflow. Validation runs the builder once,
/// in the calling process, before any runtime resource exists: it checks
/// step names, attempt budgets, timeouts, and that every port used belongs
/// to this evaluation. All errors are collected, not just the first. This
/// one evaluation also *is* the workflow's graph construction for running
/// it — no run ever evaluates `build` again (see `Workflow`'s doc comment),
/// so the former "must be pure and deterministic because a real run
/// re-evaluates it" requirement is gone: nothing about running this
/// `Workflow` depends on calling `build` a second time and getting the
/// same answer. (`build` is still called again — exactly once — *at
/// another workflow's own `define` evaluation* if this one is later
/// composed in with `embed`; that is that *other* `define`'s own graph
/// construction, not a run of this one. `map_errors` never calls `build`
/// at all — see its own doc comment.)
pub fn define(
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
      timeout: raw_node.timeout,
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

/// Sequentially composes `workflow` into the port graph being built for
/// another workflow, sharing the same run and journal (not an independent
/// child). `input`'s scope *id* flows through unchanged, so an inner
/// builder may legitimately capture outer ports (and the parent's own
/// `define` still validates against a single scope id) — but every node the
/// inner builder creates is addressed under a nested scope path, extended
/// with `workflow`'s name, so repeated `embed` calls of the same workflow
/// (or a name that collides with an outer step) do not collide in
/// `saga.describe`/`saga.address_to_string` or in Sinal step names. See the
/// `StepAddress` doc comment.
///
/// `workflow.build` is not guaranteed to be a pure function of the
/// `scoped_input` it is given here (see `Workflow`'s doc comment on when a
/// builder is invoked) — it could close over mutable state and return a
/// `Port` stashed from a *different*, unrelated `define`'s own evaluation.
/// Before restoring the parent's own scope below, `foreign_error_for` checks
/// `output`'s scope id against this call's own scope, exactly as `both`/
/// `all`/`perform` already do for a foreign port used directly: skipping
/// that check and unconditionally overwriting `output.scope` would silently
/// launder a foreign port's scope into looking like this `embed`'s own,
/// masking the mismatch from the composing `define`'s root-scope check —
/// the foreign port's *nodes* would still belong to the other definition's
/// graph, so a run would panic in `store.get` instead of `define` failing.
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

/// Adapts a whole workflow's error and undo-error types.
///
/// **Running the mapped workflow standalone** (`execution.run`/`start`, or
/// `describe`) never invokes `workflow`'s own builder again: `nodes`,
/// `order`, `root_input_id`, and `fetch_output` are reused directly from
/// `workflow`'s already-built, already-validated graph, with only the
/// node closures themselves translated in place (`node.map_errors`, which
/// wraps each node's `e1`/`u1`-typed *outputs* on the way out — see that
/// function's doc comment for why this never needs to re-run, or even
/// look at, the original builder). This is what fixes a real hazard a
/// second builder evaluation could hit: a builder that is not a pure
/// function of its input (closing over mutable state, or returning a
/// `Port` stashed from an earlier call) could, if re-run, produce a graph
/// shape `define` never validated — `describe` would then disagree with
/// what an actual run executes, and a stashed `Port` from a *different*
/// evaluation could reference a node id the mapped workflow's own graph
/// never populated, panicking the run instead of completing it.
///
/// **Composing the mapped workflow with `embed`** is different: `embed`
/// splices a workflow's port graph into some *other*, unrelated `define`
/// call's own one-time builder evaluation, at a fresh input port that
/// evaluation itself owns — there is no way to reuse a fixed graph for
/// that (the new embedding site's own dependency wiring did not exist
/// when `workflow`/`map_errors` first ran). So `build` here still wraps
/// `workflow.build` (the *original*, already-validated builder) with the
/// same node-translation, exactly as `embed` needs; this is not a second
/// evaluation of `map_errors`'s own graph, it is the *one* evaluation
/// `embed`'s own composing `define` performs, validated there like any
/// other node that `define` call introduces.
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

/// Status of an interrupted external attempt. Absence explicitly authorizes a
/// retry under the original key; unknown never authorizes another effect.
pub type EffectStatus(o, e) {
  EffectCompleted(o)
  EffectFailed(e)
  EffectAbsent
  EffectUnknown
}

type StepPersistence(i, o, e) {
  StepPersistence(
    version: String,
    input: Codec(i),
    output: Codec(o),
    resolve: fn(i, String) -> EffectStatus(o, e),
  )
}

/// Adds persistence capability to an ordinary step. Call after error mapping.
/// The definition version covers callback semantics, including pure maps.
pub fn recoverable(
  step: Step(i, o, e, u),
  version: String,
  input: Codec(i),
  output: Codec(o),
  resolve: fn(i, String) -> EffectStatus(o, e),
) -> Step(i, o, e, u) {
  Step(
    ..step,
    persistence: Some(StepPersistence(version, input, output, resolve)),
  )
}

/// Declares how to reconstruct undo from saved values and a stable key.
/// Required for every persistent compensating step, including an explicit
/// `NoUndo` declaration. The factory must be pure; checkpoints may call it to
/// validate the capability before saving a Continue result.
pub fn restore_undo(
  step: Step(i, o, e, u),
  restore: fn(i, o, String) -> Undo(u),
) -> Step(i, o, e, u) {
  Step(..step, undo_for: restore, recovery_undo_declared: True)
}

fn undo_option(undo: Undo(u)) -> Option(fn() -> Result(Nil, u)) {
  case undo {
    NoUndo -> None
    UndoWith(run) -> Some(run)
  }
}

pub type UndoStatus(u) {
  UndoCompleted
  UndoStillApplied
  UndoFailed(u)
  UndoUnknown
}

pub fn reconcile_undo(
  step: Step(i, o, e, u),
  resolve: fn(i, o, String) -> UndoStatus(u),
) -> Step(i, o, e, u) {
  Step(..step, resolve_undo: resolve)
}

/// Checks that every node can be restored and fingerprints its structural
/// dependencies. Runtime node identifiers never enter the fingerprint.
@internal
pub fn persistence_stamp(
  workflow: Workflow(i, o, e, u),
  version: String,
) -> Result(String, String) {
  use _ <- result.try(case version {
    "" -> Error("empty workflow version")
    _ -> Ok(Nil)
  })
  use nodes <- result.try(
    list.try_map(workflow.order, fn(id) {
      let assert Ok(n) = dict.get(workflow.nodes, id)
      use p <- result.try(case n.persistence {
        Some(p) -> Ok(p)
        None ->
          Error(
            "step requires persistence codecs: "
            <> address_to_string(address_from_node(n.address)),
          )
      })
      use _ <- result.try(case p.valid {
        True -> Ok(Nil)
        False -> Error("empty step or codec version")
      })
      use _ <- result.try(case n.compensates && !p.recovery_undo_declared {
        True ->
          Error(
            "persistent compensation requires restore_undo: "
            <> address_to_string(address_from_node(n.address)),
          )
        False -> Ok(Nil)
      })
      let position = fn(id) {
        list.index_fold(workflow.order, -1, fn(found, candidate, index) {
          case candidate == id {
            True -> index
            False -> found
          }
        })
        |> int.to_string
      }
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
    }),
  )
  Ok(frame([workflow.name, version, ..nodes]))
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

fn undo_key(attempt_key: String) -> String {
  attempt_key
  |> string.split(":")
  |> list.reverse
  |> list.drop(2)
  |> list.reverse
  |> string.join(":")
  |> string.append(":undo")
}

/// Evidence for an interrupted compensation decision. Unknown keeps the run
/// suspended. A resolved decision is subject to the original attempt budget
/// and the execution's cancellation state.
pub type CompensationStatus(o, e, u) {
  CompensationResolved(Recovery(o, e, u))
  CompensationUnknown
}

/// Reads external evidence using the original input, attempt, and stable key.
/// The resolver must be safe to call repeatedly. Configuration order relative
/// to `recoverable` does not matter.
pub fn reconcile_compensation(
  step: Step(i, o, e, u),
  resolve: fn(i, Attempt, String) -> CompensationStatus(o, e, u),
) -> Step(i, o, e, u) {
  Step(..step, resolve_compensation: resolve)
}

fn compensation_key(attempt_key: String) -> String {
  let parts = string.split(attempt_key, ":") |> list.reverse
  let number = list.first(parts) |> result.unwrap("1")
  parts
  |> list.drop(2)
  |> list.reverse
  |> string.join(":")
  |> string.append(":compensation:" <> number)
}
