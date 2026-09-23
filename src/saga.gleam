/// Typed workflow authoring: steps, undo, compensation and recovery
/// vocabulary, typed ports, composition, and definition validation.
///
/// A `Workflow(input, output, error, undo_error)` is a pure description. Its
/// builder closure is evaluated once at `define` time (to validate names,
/// attempts, timeouts, and to compute static descriptors) and again, fresh,
/// at the start of every run (see `saga/execution`). Dependencies are
/// expressed through typed `Port` values rather than names, so wiring is
/// checked by the compiler and the scheduler never touches a central
/// heterogeneous value map.
import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/set.{type Set}
import gleam/string
import saga/internal/cell
import saga/internal/ffi
import saga/internal/node.{
  type ErasedRecovery, type Node, AttemptSucceeded, Node,
}

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
    attempt: fn(i) -> RunOutcome(o, u, e),
    decide_crash: Option(fn(i, CrashOrTimeout, Attempt) -> Recovery(o, e, u)),
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
    recover_returned: Option(fn(Attempt) -> Recovery(o, e, u)),
  )
}

/// Creates a step from its name and its run function. With no further
/// modifiers, a failure is terminal after one attempt and nothing is undone
/// on rollback.
pub fn step(name: String, run: fn(i) -> Result(o, e)) -> Step(i, o, e, u) {
  Step(
    name: name,
    max_attempts: 1,
    timeout: None,
    undoable: False,
    compensates: False,
    attempt: fn(input) {
      case run(input) {
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
  let attempt = step.attempt
  Step(..step, undoable: True, attempt: fn(input) {
    case attempt(input) {
      Succeeded(output, NoUndo) ->
        Succeeded(output, UndoWith(fn() { undo_fn(input, output) }))
      unchanged -> unchanged
    }
  })
}

/// Attaches an explicit recovery decision for a *failing* attempt (an
/// application error, a crash, or a timeout), with a total attempt budget.
/// With no `compensate`, a failure aborts after exactly one attempt.
pub fn compensate(
  step: Step(i, o, e, u),
  max_attempts max_attempts: Int,
  with decide: fn(i, AttemptFailure(e), Attempt) -> Recovery(o, e, u),
) -> Step(i, o, e, u) {
  let attempt = step.attempt
  Step(
    ..step,
    max_attempts: max_attempts,
    compensates: True,
    attempt: fn(input) {
      case attempt(input) {
        Failed(failure, _) ->
          Failed(
            failure,
            Some(fn(attempt_no) { decide(input, failure, attempt_no) }),
          )
        unchanged -> unchanged
      }
    },
    decide_crash: Some(fn(input, crash_or_timeout, attempt_no) {
      let failure = case crash_or_timeout {
        StepCrashed(crash) -> Crashed(crash)
        StepTimedOut -> TimedOut
      }
      decide(input, failure, attempt_no)
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
    attempt: fn(input) {
      case step.attempt(input) {
        Succeeded(output, undo_choice) ->
          Succeeded(output, map_undo(undo_choice, map_undo_error))
        Failed(failure, recover_returned) ->
          Failed(
            failure: map_attempt_failure(failure, map_error),
            recover_returned: option.map(recover_returned, fn(recover_fn) {
              fn(attempt_no) {
                recover_fn(attempt_no)
                |> map_recovery(map_error, map_undo_error)
              }
            }),
          )
      }
    },
    decide_crash: option.map(step.decide_crash, fn(decide_fn) {
      fn(input: i, crash_or_timeout: CrashOrTimeout, attempt_no: Attempt) {
        decide_fn(input, crash_or_timeout, attempt_no)
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
/// passes the parent scope through unchanged, so an inner builder may
/// legitimately capture outer ports.
type ScopeToken {
  ScopeToken(id: Int, path: List(String))
}

fn root_scope() -> ScopeToken {
  ScopeToken(id: ffi.unique_integer(), path: [])
}

// ---------------------------------------------------------------------------
// Port
// ---------------------------------------------------------------------------

/// A typed reference to a value produced somewhere in the workflow graph
/// being built: either the workflow's own input, or the output of a
/// `perform`/`map`/`both`/`all`/`embed`. Two consumers of the same `Port`
/// value depend on the same node, so that node executes once per run.
pub opaque type Port(a, e, u) {
  Port(
    scope: ScopeToken,
    nodes: Dict(Int, Node(e, u)),
    deps: Set(Int),
    errors: List(DefinitionError),
    fetch: fn() -> fn() -> a,
  )
}

fn merge_ports(
  scope: ScopeToken,
  first: Port(a, e, u),
  second: Port(b, e, u),
  fetch: fn() -> fn() -> c,
) -> Port(c, e, u) {
  let foreign_errors =
    list.append(
      foreign_error_for(scope, first),
      foreign_error_for(scope, second),
    )
  Port(
    scope: scope,
    nodes: dict.merge(first.nodes, second.nodes),
    deps: set.union(first.deps, second.deps),
    errors: list.flatten([first.errors, second.errors, foreign_errors]),
    fetch: fetch,
  )
}

fn foreign_error_for(
  scope: ScopeToken,
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
/// it re-runs in every task that consumes the resulting port.
pub fn map(port: Port(a, e, u), with: fn(a) -> b) -> Port(b, e, u) {
  Port(..port, fetch: fn() {
    let read_a = port.fetch()
    fn() { with(read_a()) }
  })
}

/// Combines two ports into one pair, without adding a scheduled node. Both
/// original ports still execute according to their own dependencies.
pub fn both(
  first: Port(a, e, u),
  second: Port(b, e, u),
) -> Port(#(a, b), e, u) {
  merge_ports(first.scope, first, second, fn() {
    let read_a = first.fetch()
    let read_b = second.fetch()
    fn() { #(read_a(), read_b()) }
  })
}

/// Combines a list of ports into one port producing their values, in list
/// order.
pub fn all(ports: List(Port(a, e, u))) -> Port(List(a), e, u) {
  case ports {
    [] -> panic as "saga.all requires at least one port"
    [first, ..rest] ->
      list.fold(rest, map(first, fn(a) { [a] }), fn(acc, port) {
        both(acc, port) |> map(fn(pair) { list.append(pair.0, [pair.1]) })
      })
  }
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
  let read_input = input.fetch()

  let output_cell = cell.new()

  let to_erased_recovery = fn(recovery: Recovery(o, e, u)) -> ErasedRecovery(
    e,
    u,
  ) {
    case recovery {
      Retry -> node.ERetry
      RetryAfter(ms) -> node.ERetryAfter(ms)
      Continue(output, undo_choice) ->
        node.EContinue(commit: fn() {
          cell.write(output_cell, output)
          case undo_choice {
            NoUndo -> None
            UndoWith(run) -> Some(fn() { run() })
          }
        })
      Abort(error) -> node.EAbort(error)
      AbortAfterCleanupFailure(error, cleanup_error) ->
        node.EAbortCleanup(error, cleanup_error)
      Hold(evidence) -> node.EHold(evidence)
    }
  }

  // `read_input` is a cell read (a selective receive), which only the
  // owning process — the coordinator — may perform. `prepare_attempt` and
  // `prepare_crash_recovery` are themselves called by the coordinator, so
  // the read happens here, before the returned thunk is handed to a
  // spawned task; the thunk itself only runs the pure/effectful step body,
  // never touches a cell.
  let prepare_attempt = fn(_node_attempt: node.Attempt) -> fn() ->
    node.AttemptResult(e, u) {
    let value = read_input()
    fn() {
      case step.attempt(value) {
        Succeeded(output, undo_choice) ->
          AttemptSucceeded(commit: fn() {
            cell.write(output_cell, output)
            case undo_choice {
              NoUndo -> None
              UndoWith(run) -> Some(fn() { run() })
            }
          })
        Failed(failure, recover_returned) ->
          node.AttemptFailed(
            failure: failure_to_node(failure),
            recover: option.map(recover_returned, fn(recover_fn) {
              fn(node_attempt: node.Attempt) {
                fn() {
                  to_erased_recovery(
                    recover_fn(attempt_from_node(node_attempt)),
                  )
                }
              }
            }),
          )
      }
    }
  }

  // The coordinator's path for a crash/timeout it observed itself (the task
  // never returned an `AttemptResult` at all, so `AttemptFailed.recover` was
  // never bound). This calls `decide_crash` directly with the input read for
  // this attempt — `read_input` is a cell read, not a re-run of the step's
  // effect, so nothing is repeated.
  let prepare_crash_recovery =
    option.map(step.decide_crash, fn(decide_fn) {
      fn(node_failure: node.AttemptFailure(e), node_attempt: node.Attempt) -> fn() ->
        ErasedRecovery(e, u) {
        let value = read_input()
        let crash_or_timeout = case node_failure {
          node.Crashed(crash) -> StepCrashed(crash_from_node(crash))
          node.TimedOut -> StepTimedOut
          node.Returned(_) ->
            panic as "saga: prepare_crash_recovery received a Returned failure"
        }
        let attempt = attempt_from_node(node_attempt)
        fn() { to_erased_recovery(decide_fn(value, crash_or_timeout, attempt)) }
      }
    })

  let this_node =
    Node(
      id: id,
      address: address_to_node(address),
      deps: deps,
      max_attempts: step.max_attempts,
      timeout: step.timeout,
      undoable: step.undoable,
      compensates: step.compensates,
      prepare_attempt: prepare_attempt,
      prepare_crash_recovery: prepare_crash_recovery,
    )

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
    nodes: dict.insert(input.nodes, id, this_node),
    deps: set.from_list([id]),
    errors: list.flatten([
      input.errors,
      name_error,
      attempts_error,
      timeout_error,
    ]),
    fetch: fn() { fn() { cell.read(output_cell) } },
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
/// and undo-error types. `define` evaluates `build` once to validate it;
/// `saga/execution` evaluates it again, fresh, at the start of every run.
pub opaque type Workflow(i, o, e, u) {
  Workflow(
    name: String,
    build: fn(Port(i, e, u)) -> Port(o, e, u),
    descriptors: List(StepDescriptor),
  )
}

/// Builds and validates a named workflow. Validation runs the builder once,
/// in the calling process, before any runtime resource exists: it checks
/// step names, attempt budgets, timeouts, and that every port used belongs
/// to this evaluation. All errors are collected, not just the first.
pub fn define(
  name: String,
  build: fn(Port(i, e, u)) -> Port(o, e, u),
) -> Result(Workflow(i, o, e, u), List(DefinitionError)) {
  let scope = root_scope()
  let root_input = fresh_root_port(scope)
  let output = build(root_input)

  let name_errors = case name {
    "" -> [EmptyWorkflowName]
    _ -> []
  }
  let root_scope_errors = foreign_error_for(scope, output)
  let all_errors = list.flatten([name_errors, output.errors, root_scope_errors])

  case all_errors {
    [] ->
      Ok(Workflow(
        name: name,
        build: build,
        descriptors: descriptors_for(output),
      ))
    _ -> Error(all_errors)
  }
}

fn fresh_root_port(scope: ScopeToken) -> Port(i, e, u) {
  let input_cell = cell.new()
  Port(
    scope: scope,
    nodes: dict.new(),
    deps: set.new(),
    errors: [],
    fetch: fn() { fn() { cell.read(input_cell) } },
  )
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

fn descriptors_for(output: Port(a, e, u)) -> List(StepDescriptor) {
  let #(ordered_ids, addresses_by_id) = resolve_addresses(output.nodes)
  list.map(ordered_ids, fn(id) {
    let assert Ok(raw_node) = dict.get(output.nodes, id)
    let assert Ok(resolved_address) = dict.get(addresses_by_id, id)
    let depends_on =
      list.filter_map(raw_node.deps, fn(dep_id) {
        dict.get(addresses_by_id, dep_id)
      })
    StepDescriptor(
      address: resolved_address,
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

/// Evaluates `workflow`'s builder fresh, for one run, and returns the
/// resolved node graph in builder-call order plus a thunk that fetches the
/// final output (must be called only after every node is done). Used only
/// by `saga/execution`, which owns process/run lifecycle; this function
/// performs no I/O and starts no process itself.
///
/// Returns `Error(Nil)` if the freshly evaluated graph's descriptor shape
/// (addresses and dependencies, in node-id order) differs from the
/// define-time shape — a nondeterministic builder, which `saga/execution`
/// reports as `DefinitionChanged` before admitting any work.
@internal
pub fn for_run(
  workflow: Workflow(i, o, e, u),
  input: i,
) -> Result(
  #(Dict(Int, node.Node(e, u)), List(Int), fn() -> ffi.RescueResult(o)),
  Nil,
) {
  let scope = root_scope()
  let input_cell = cell.new()
  cell.write(input_cell, input)
  let root_input =
    Port(
      scope: scope,
      nodes: dict.new(),
      deps: set.new(),
      errors: [],
      fetch: fn() { fn() { cell.read(input_cell) } },
    )
  let output = workflow.build(root_input)
  let #(ordered_ids, nodes) = resolved_nodes(output.nodes)
  let live_descriptors = descriptors_for(output)
  case live_descriptors == workflow.descriptors {
    False -> Error(Nil)
    True -> {
      let fetch = output.fetch()
      Ok(#(nodes, ordered_ids, fn() { ffi.rescue(fetch) }))
    }
  }
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
/// child). The parent's scope flows through unchanged via `input`, so an
/// inner builder may legitimately capture outer ports.
pub fn embed(
  input: Port(i, e, u),
  workflow: Workflow(i, o, e, u),
) -> Port(o, e, u) {
  workflow.build(input)
}

/// Adapts a whole workflow's error and undo-error types.
pub fn map_errors(
  workflow: Workflow(i, o, e1, u1),
  error map_error: fn(e1) -> e2,
  undo_error map_undo_error: fn(u1) -> u2,
) -> Workflow(i, o, e2, u2) {
  Workflow(
    name: workflow.name,
    build: fn(input: Port(i, e2, u2)) -> Port(o, e2, u2) {
      let shadow_input = retype_empty_port(input)
      let shadow_output = workflow.build(shadow_input)
      let mapped_nodes =
        dict.map_values(shadow_output.nodes, fn(_id, a_node) {
          node.map_errors(a_node, map_error, map_undo_error)
        })
      Port(
        scope: shadow_output.scope,
        nodes: dict.merge(input.nodes, mapped_nodes),
        deps: shadow_output.deps,
        errors: shadow_output.errors,
        fetch: shadow_output.fetch,
      )
    },
    descriptors: workflow.descriptors,
  )
}

/// Rebuilds a fresh port carrying the same scope and fetch behaviour as
/// `input` but with an empty node dictionary retyped for the original
/// (e1, u1) vocabulary. This is sound because a `Port`'s only field that
/// mentions `e`/`u` is its node dictionary, and it starts empty here; every
/// node the shadow builder subsequently creates is later converted back
/// through `node.map_errors`, never read under the wrong type.
fn retype_empty_port(input: Port(i, e2, u2)) -> Port(i, e1, u1) {
  Port(
    scope: input.scope,
    nodes: dict.new(),
    deps: set.new(),
    errors: [],
    fetch: input.fetch,
  )
}
