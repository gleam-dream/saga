/// The per-run coordinator process: scheduler, journal, settlement,
/// deadlines, timeouts, and cancellation.
///
/// One coordinator is spawned per run. `saga.for_run` hands it the
/// workflow's already-built node graph (built once, at `define` — see that
/// module's doc comments) plus a fresh, run-scoped `Store` seeded with this
/// run's input; the coordinator never evaluates the workflow's builder
/// itself. It then runs a bounded admission loop over the node graph until
/// every node is done or the run has a terminal cause.
///
/// A terminal trigger — a step's own terminal failure, a run deadline, or a
/// cancellation request (explicit, or the owner's exit) — moves the run
/// into `Settling`: admission stops, waiting nodes are marked `Skipped`,
/// and already in-flight attempts/compensations are given up to
/// `settle_timeout` to finish on their own. Whichever remain when the
/// settle timer fires are killed and reported `interrupted` — their effect
/// is unknown and is never journaled or undone. Once settling drains, the
/// run either becomes `Unresolved` (a `Hold` occurred) or walks the journal
/// in reverse completion order, undoing one entry at a time, bounded by
/// `cleanup_timeout` per entry, retaining every undo failure.
///
/// `saga/observation` events are emitted after each corresponding state
/// transition, from this process; a raising Sinal handler cannot change the
/// run's outcome (`sinal.emit`'s own dispatch isolates handler failure).
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Monitor, type Pid, type Subject, type Timer}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set.{type Set}
import gleam/string
import saga/internal/checkpoint
import saga/internal/ffi
import saga/internal/min_heap.{type MinHeap}
import saga/internal/node.{
  type Attempt, type AttemptFailure, type AttemptResult, type ErasedRecovery,
  type Node, type StepAddress, Attempt, AttemptFailed, AttemptSucceeded, Crashed,
  EAbort, EAbortCleanup, EContinue, EHold, ERetry, ERetryAfter, Returned,
  TimedOut,
}
import saga/internal/store.{type Store}
import saga/observation
import sinal

/// One journal entry: a completed node's address and its undo thunk, if it
/// has one. The journal's head is the most recently completed node, so
/// walking it front-to-back is reverse completion order.
type JournalEntry(u) {
  JournalEntry(
    node_id: Int,
    address: StepAddress,
    undo: Option(fn() -> Result(Nil, u)),
  )
}

pub type UndoFailure(u) {
  UndoFailed(step: StepAddress, error: u)
  UndoCrashed(step: StepAddress, crash: node.Crash)
  UndoTimedOut(step: StepAddress)
}

pub type CompensationFailure(u) {
  CleanupFailed(step: StepAddress, error: u)
  CompensationCrashed(step: StepAddress, crash: node.Crash)
  CompensationTimedOut(step: StepAddress)
}

pub type Cause(e) {
  StepFailed(step: StepAddress, error: e)
  StepCrashed(step: StepAddress, crash: node.Crash)
  StepTimedOut(step: StepAddress)
  RetryLimitReached(step: StepAddress, last: AttemptFailure(e))
  /// A `Retry`/`RetryAfter` decision was refused not because its attempt
  /// budget was exhausted, but because settling had already begun for a
  /// different, unrelated trigger by the time it was decided (§3.3): a
  /// fresh attempt would race the settle window. Kept distinct from
  /// `RetryLimitReached` so a caller inspecting a sibling failure's cause
  /// can tell "this step genuinely ran out of attempts" from "this step
  /// could have retried, but the run was already stopping for another
  /// reason".
  RetrySuperseded(step: StepAddress, last: AttemptFailure(e))
  OutputCrashed(crash: node.Crash)
  DeadlineExceeded
}

pub type CancelReason {
  CancelRequested
  OwnerExited
}

pub type Settlement(e, u) {
  Settlement(
    undone: List(StepAddress),
    undo_failures: List(UndoFailure(u)),
    not_undoable: List(StepAddress),
    held: List(StepAddress),
    interrupted: List(StepAddress),
    compensation_failures: List(CompensationFailure(u)),
    sibling_failures: List(Cause(e)),
    unknown_effects: List(UnknownEffect),
  )
}

/// One action of the run that ended without a result, so its effect is
/// unknown: it may or may not have happened, and it was never journaled or
/// undone. Every such action is recorded when it ends, whatever is decided
/// afterwards.
pub type UnknownEffect {
  UnknownEffect(step: StepAddress, action: Action, ending: UnknownEnding)
}

/// Which of a step's actions ended: its attempt with this number, the
/// compensation decision about the attempt with this number, or its undo.
pub type Action {
  StepAttempt(attempt: Int)
  StepCompensation(attempt: Int)
  StepUndo
}

/// How an action ended without a result: it raised or its process exited,
/// it was killed at its time bound (the step's timeout, or
/// `cleanup_timeout`), or it was killed when the settle window closed.
pub type UnknownEnding {
  ActionCrashed(crash: node.Crash)
  ActionTimedOut
  ActionInterrupted
}

fn empty_settlement() -> Settlement(e, u) {
  Settlement(
    undone: [],
    undo_failures: [],
    not_undoable: [],
    held: [],
    interrupted: [],
    compensation_failures: [],
    sibling_failures: [],
    unknown_effects: [],
  )
}

/// `CompletedWithUnknownEffects` is `Completed`'s counterpart for a run
/// that reached its output although a step attempt crashed or was killed by
/// its own `timeout` and its recovery decider then chose
/// `Retry`/`RetryAfter`/`Continue`. That attempt's effect is still unknown
/// and was never journaled or undone — only the *replacement* attempt (the
/// retry, or `Continue`'s supplied output) is known. `unknown_effects` is
/// never empty on this variant — an empty list is always plain `Completed`
/// instead. Every other outcome kind carries the same record in
/// `Settlement.unknown_effects`.
pub type Outcome(o, e, u) {
  Completed(output: o)
  CompletedWithUnknownEffects(output: o, unknown_effects: List(UnknownEffect))
  Failed(cause: Cause(e), settlement: Settlement(e, u))
  Cancelled(reason: CancelReason, settlement: Settlement(e, u))
  Unresolved(step: StepAddress, evidence: e, settlement: Settlement(e, u))
}

pub type StepState {
  Waiting
  Attempting(attempt: Int)
  Compensating(attempt: Int)
  RetryScheduled(next_attempt: Int)
  Succeeded
  FailedStep
  Interrupted
  Undoing
  Undone
  UndoFailedStep
  Skipped
}

pub type StepProgress {
  StepProgress(address: StepAddress, state: StepState)
}

pub type Phase {
  Running
  Settling
  RollingBack
}

pub type Progress {
  Progress(run_id: Int, phase: Phase, steps: List(StepProgress))
}

/// Messages the control subject accepts. Owned by the coordinator process;
/// `saga/execution` only ever talks to it through these.
pub type Control(o, e, u) {
  InputPrepared(node_id: Int, seq: Int, input: String, reply: Subject(Nil))
  AttemptDone(node_id: Int, seq: Int, result: AttemptResult(e, u))
  RecoveryDone(node_id: Int, seq: Int, recovery: ErasedRecovery(e, u))
  TaskCrashed(node_id: Int, seq: Int, crash: node.Crash)
  RetryFire(node_id: Int, seq: Int)
  UndoDone(node_id: Int, seq: Int, outcome: UndoOutcome(u))
  ProgressRequest(reply: Subject(Progress))
  CancelRequest
  DeadlineFired(seq: Int)
  StepTimeoutFired(node_id: Int, seq: Int)
  CleanupTimeoutFired(node_id: Int, seq: Int)
  SettleFired(seq: Int)
  OwnerDown(reason: process.ExitReason)
  /// A linked task exited without sending a result — killed externally, or
  /// crashed outside `ffi.rescue` (should not happen, since every task body
  /// is wrapped in `rescue`, but a trapped exit is the only way to observe
  /// an externally-killed task at all). The coordinator traps exits so this
  /// arrives as a message instead of taking the coordinator down with it.
  TaskExited(pid: Pid, reason: process.ExitReason)
}

pub type UndoOutcome(u) {
  UndoOk
  UndoErr(error: u)
  UndoCrash(crash: node.Crash)
  UndoBlocked(reason: checkpoint.Failure)
}

/// State for one node during the run. `NodeAttempting`/`NodeCompensating`/
/// `NodeUndoing` carry an optional timer for whichever bound currently
/// applies (a step's own `timeout`, or the run's `cleanup_timeout`), so it
/// can be cancelled the moment the task finishes on its own.
type NodeRunState {
  NodeWaiting(remaining_deps: Int)
  /// `started_at` is `ffi.monotonic_time()` when this attempt/compensation
  /// began, so `emit_step_stopped`/`emit_compensation_stopped` can report
  /// its real elapsed duration instead of a hard-coded `0`.
  NodeAttempting(
    seq: Int,
    attempt: Int,
    pid: Pid,
    timer: Option(Timer),
    started_at: Int,
  )
  NodeCompensating(
    seq: Int,
    attempt: Int,
    pid: Pid,
    timer: Option(Timer),
    started_at: Int,
  )
  NodeRetryScheduled(seq: Int, attempt: Int, due_at: Int)
  /// A scheduled retry's backoff has fired (or an immediate `Retry` was
  /// decided) and the node is ready to attempt again, but is waiting for
  /// `admit` to grant it a concurrency slot — exactly like a fresh node
  /// whose dependencies just became ready, so a retry can never itself
  /// exceed `max_concurrency`.
  NodeReadyForRetry(attempt: Int)
  NodeDone
  NodeFailedTerminal
  NodeSkipped
  NodeUndoing(seq: Int, pid: Pid, timer: Option(Timer), started_at: Int)
  NodeUndone
  NodeUndoFailedTerminal
  /// A task was killed (settle deadline, or an unrelated fatal condition)
  /// while attempting or compensating: its effect is unknown and is never
  /// journaled or undone.
  NodeInterrupted
}

type RunState(o, e, u) {
  RunState(
    persistence: Option(Session(e, u)),
    dispatch: List(Subject(Nil)),
    pending_starts: List(Int),
    blocked: Option(checkpoint.Failure),
    resume_nodes: Set(Int),
    inputs: Dict(Int, String),
    resume_undos: Set(Int),
    deadline_at: Option(Int),
    workflow_name: String,
    run_id: Int,
    control: Subject(Control(o, e, u)),
    nodes: Dict(Int, Node(e, u)),
    order: List(Int),
    positions: Dict(Int, Int),
    total_nodes: Int,
    dependents: Dict(Int, List(Int)),
    state: Dict(Int, NodeRunState),
    // Node ids currently eligible for admission (dependency count reached
    // zero, or a retry became ready) but not yet started, ordered by node
    // id — i.e. by builder-call order, matching what a full scan of `order`
    // would have yielded. Pushed to in `mark_ready`, popped in `admit`, so
    // an admission decision is O(log total_nodes) instead of an O(N) scan
    // over `order` on every call. See `saga/internal/min_heap`'s doc
    // comment for why a full scan's *worst-case* total cost across a whole
    // run (O(N) per admission decision, O(N) decisions) is what this
    // replaces, not merely a constant-factor speedup.
    ready: MinHeap,
    // Nodes that have permanently left the running/waiting lifecycle
    // (`NodeDone`), maintained incrementally so `all_nodes_done` (checked
    // on every message processed) is an O(1) comparison against
    // `total_nodes` instead of an O(N) scan over `state.state`'s values.
    done_count: Int,
    // Whether some node is currently `NodeUndoing`. Undo is strictly
    // sequential (`undo_next` never starts a second undo while one is
    // active — see `has_active_undo`'s call site), so this is a plain
    // `Bool`, not a count; maintained incrementally so `rollback_complete`
    // and `undo_next` (both checked on every message processed, or once
    // per journal entry during rollback) are O(1) instead of an O(N) scan
    // over `state.state`'s values.
    undoing: Bool,
    // This run's own value store (`saga/internal/store`): every completed
    // node's output, keyed by node id, and nothing else — no other run,
    // concurrent or otherwise, of the same `Workflow` ever touches this
    // value. See `saga.for_run`'s doc comment for why the shared, built-once
    // node graph does not compromise run isolation.
    store: Store,
    // The failure that triggered the currently in-flight (or most recently
    // finished) recovery decision for a node, kept so `RetryLimitReached`
    // can report which failure exhausted the budget.
    last_failure: Dict(Int, AttemptFailure(e)),
    running: Int,
    max_concurrency: Int,
    deadline: Option(Int),
    // The per-attempt timeout applied when a step declares none of its own
    // (`n.timeout` is `None`) — see `start_attempt`. `None` here means no
    // default is enforced (the explicit opt-out); it is never itself
    // overridden by a step's own `timeout`, only the other way around.
    step_timeout: Option(Int),
    settle_timeout: Int,
    cleanup_timeout: Int,
    deadline_timer: Option(Timer),
    owner_monitor: Monitor,
    journal: List(JournalEntry(u)),
    phase: RunPhase(e, u),
    start_time: Int,
    // Every action that ended with an unknown effect (see `UnknownEffect`),
    // newest first, recorded the moment it ends — independent of
    // `phase`/`Settlement`, since the run may still be `PhaseRunning` (a
    // Retry/Continue decision can let it proceed to a normal output) when
    // this needs to be remembered. See `Outcome`'s doc comment.
    unknown_effects: List(UnknownEffect),
  )
}

type RunPhase(e, u) {
  PhaseRunning
  PhaseSettling(
    trigger: Trigger(e),
    settlement: Settlement(e, u),
    settle_timer: Option(Timer),
    settle_seq: Int,
  )
  PhaseRollingBack(trigger: Trigger(e), settlement: Settlement(e, u))
}

pub type Trigger(e) {
  TriggerFailure(cause: Cause(e))
  TriggerUnresolved(step: StepAddress, evidence: e)
  TriggerCancel(reason: CancelReason)
}

/// Spawns a coordinator for one run and returns once it is alive, with its
/// pid and the run's id. `build_graph` calls `saga.for_run` for this one
/// run: the workflow's already-built node graph (built once, at `define`)
/// plus a fresh run-scoped `Store` and the output-fetch thunk. `owner` is
/// monitored: its exit is treated as a cancellation with `OwnerExited`.
/// `deliver` is called once, from the coordinator process, with the run's
/// terminal outcome, just before the coordinator exits. It is the only path
/// by which an outcome leaves this process, and it runs whether or not
/// `owner` is still alive.
pub fn start(
  workflow_name workflow_name: String,
  owner owner: Pid,
  max_concurrency max_concurrency: Int,
  deadline deadline: Option(Int),
  step_timeout step_timeout: Option(Int),
  settle_timeout settle_timeout: Int,
  cleanup_timeout cleanup_timeout: Int,
  build_graph build_graph: fn() ->
    #(
      Dict(Int, Node(e, u)),
      List(Int),
      Dict(Int, List(Int)),
      Store,
      fn(Store) -> ffi.RescueResult(o),
    ),
  deliver deliver: fn(Outcome(o, e, u)) -> Nil,
  control_subject_out control_subject_out: Subject(Subject(Control(o, e, u))),
) -> #(Pid, Int) {
  let run_id = ffi.unique_integer()
  let ready = process.new_subject()
  let pid =
    process.spawn_unlinked(fn() {
      run(
        workflow_name,
        owner,
        run_id,
        max_concurrency,
        deadline,
        step_timeout,
        settle_timeout,
        cleanup_timeout,
        build_graph,
        deliver,
        control_subject_out,
        ready,
        None,
      )
    })
  // This wait is a startup smoke-check, not the caller's actual failure
  // path: whether or not `ready` arrives in time, `saga/execution.start`
  // separately waits on `control_subject_out` (with its own bounded
  // timeout) before handing back a usable `Execution`, and reports
  // `ExecutionLost` if the coordinator never gets that far. A timeout here
  // is therefore not silently swallowed overall — it is handled one level
  // up, where a `Result` can actually be returned (`#(Pid, Int)` here has
  // no room for one).
  let _ = process.receive(ready, 5000)
  #(pid, run_id)
}

fn run(
  workflow_name: String,
  owner: Pid,
  run_id: Int,
  max_concurrency: Int,
  deadline: Option(Int),
  step_timeout: Option(Int),
  settle_timeout: Int,
  cleanup_timeout: Int,
  build_graph: fn() ->
    #(
      Dict(Int, Node(e, u)),
      List(Int),
      Dict(Int, List(Int)),
      Store,
      fn(Store) -> ffi.RescueResult(o),
    ),
  deliver: fn(Outcome(o, e, u)) -> Nil,
  control_subject_out: Subject(Subject(Control(o, e, u))),
  ready: Subject(Nil),
  persistence: Option(Session(e, u)),
) -> Nil {
  process.trap_exits(True)
  let control = process.new_subject()
  let owner_monitor = process.monitor(owner)
  process.send(control_subject_out, control)
  process.send(ready, Nil)
  let #(nodes, order, dependents, run_store, fetch_output) = build_graph()
  let node_state =
    dict.map_values(nodes, fn(_id, n) { NodeWaiting(list.length(n.deps)) })
  // Every node with no dependencies is ready from the start. Pushed in
  // ascending node-id order (`order` is already sorted ascending — see
  // `saga.gleam`'s `resolve_addresses`), matching what a full scan of
  // `order` would have yielded, so the heap-based `admit` below picks
  // exactly the same first candidate a linear scan always did.
  let initial_ready =
    list.fold(order, min_heap.new(), fn(heap, id) {
      case dict.get(node_state, id) {
        Ok(NodeWaiting(0)) -> min_heap.insert(heap, id)
        _ -> heap
      }
    })
  let deadline_timer = case deadline {
    None -> None
    Some(ms) -> {
      let seq = ffi.unique_integer()
      Some(process.send_after(control, ms, DeadlineFired(seq)))
    }
  }
  let start_time = ffi.monotonic_time()
  emit_run_started(workflow_name, run_id)
  let initial =
    RunState(
      persistence: persistence,
      dispatch: [],
      pending_starts: [],
      blocked: None,
      resume_nodes: set.new(),
      inputs: dict.new(),
      resume_undos: set.new(),
      deadline_at: option.map(deadline, fn(ms) { ffi.system_time() + ms }),
      workflow_name: workflow_name,
      run_id: run_id,
      control: control,
      nodes: nodes,
      order: order,
      positions: list.index_map(order, fn(id, index) { #(id, index) })
        |> dict.from_list,
      total_nodes: dict.size(nodes),
      dependents: dependents,
      state: node_state,
      ready: initial_ready,
      done_count: 0,
      undoing: False,
      store: run_store,
      last_failure: dict.new(),
      running: 0,
      max_concurrency: max_concurrency,
      deadline: deadline,
      step_timeout: step_timeout,
      settle_timeout: settle_timeout,
      cleanup_timeout: cleanup_timeout,
      deadline_timer: deadline_timer,
      owner_monitor: owner_monitor,
      journal: [],
      phase: PhaseRunning,
      start_time: start_time,
      unknown_effects: [],
    )
  let restored = case persistence {
    None -> Ok(initial)
    Some(session) ->
      case session.snapshot {
        None -> Ok(initial)
        Some(saved) -> restore(initial, saved)
      }
  }
  case restored {
    Error(reason) ->
      case persistence {
        Some(session) -> session.failed(reason)
        None -> Nil
      }
    Ok(initial) -> {
      let initial = case persistence {
        Some(Session(cancelled: True, ..)) -> handle_cancel_request(initial)
        _ -> initial
      }
      loop(admit(initial), fetch_output, deliver)
    }
  }
}

fn loop(
  state: RunState(o, e, u),
  fetch_output: fn(Store) -> ffi.RescueResult(o),
  deliver: fn(Outcome(o, e, u)) -> Nil,
) -> Nil {
  case checkpoint(state) {
    Error(reason) -> {
      stop_workers(state)
      cancel_deadline_timer(state)
      case state.persistence {
        Some(session) -> session.failed(reason)
        None -> Nil
      }
    }
    Ok(Nil) -> {
      let state = start_saved_timers(state)
      list.each(state.dispatch, fn(permit) { process.send(permit, Nil) })
      loop_committed(
        RunState(..state, dispatch: [], pending_starts: []),
        fetch_output,
        deliver,
      )
    }
  }
}

fn loop_committed(
  state: RunState(o, e, u),
  fetch_output: fn(Store) -> ffi.RescueResult(o),
  deliver: fn(Outcome(o, e, u)) -> Nil,
) -> Nil {
  case run_finished(state) {
    Some(outcome) -> {
      cancel_deadline_timer(state)
      emit_run_stopped(state, outcome)
      deliver(outcome)
      Nil
    }
    None ->
      case all_nodes_done(state) {
        True ->
          case fetch_output(state.store) {
            ffi.Rescued(output) -> {
              cancel_deadline_timer(state)
              // `unknown_effects` is accumulated newest-first; reverse so
              // callers see them in the order they occurred.
              let unknown_effects = list.reverse(state.unknown_effects)
              let outcome = case unknown_effects {
                [] -> Completed(output)
                _ -> CompletedWithUnknownEffects(output, unknown_effects)
              }
              emit_run_stopped(state, outcome)
              deliver(outcome)
            }
            ffi.Raised(class, reason) ->
              begin_settling(
                state,
                TriggerFailure(OutputCrashed(node.Crash(class, reason))),
              )
              |> loop(fetch_output, deliver)
          }
        False -> {
          let selector =
            process.new_selector()
            |> process.select(state.control)
            |> process.select_trapped_exits(fn(exit_message) {
              TaskExited(exit_message.pid, exit_message.reason)
            })
            |> process.select_specific_monitor(state.owner_monitor, fn(down) {
              case down {
                process.ProcessDown(_, _, reason) -> OwnerDown(reason)
                process.PortDown(_, _, reason) -> OwnerDown(reason)
              }
            })
          let message = process.selector_receive_forever(selector)
          let next = handle_control(state, message)
          loop(next, fetch_output, deliver)
        }
      }
  }
}

fn cancel_deadline_timer(state: RunState(o, e, u)) -> Nil {
  case state.deadline_timer {
    None -> Nil
    Some(timer) -> {
      let _ = process.cancel_timer(timer)
      Nil
    }
  }
}

fn run_finished(state: RunState(o, e, u)) -> Option(Outcome(o, e, u)) {
  case state.phase {
    PhaseRunning -> None
    PhaseSettling(..) -> None
    PhaseRollingBack(trigger, settlement) ->
      case rollback_complete(state) {
        False -> None
        True -> {
          // Every action that ended with an unknown effect during this run
          // (regardless of when it happened, or what its recovery decision
          // was) becomes `unknown_effects` here. Every step-timeout kill
          // among them is also folded into `interrupted`, which reported
          // them before `unknown_effects` existed. See `Outcome`'s doc
          // comment for the parallel case where the run *did* complete.
          //
          // `undone`/`undo_failures`/`not_undoable`/`held` were all built
          // newest-first (prepended, not appended — see
          // `record_undone`/`record_undo_failure`/`record_not_undoable`/
          // `record_held`'s own doc comment) precisely so accumulating them
          // across up to N journal entries during rollback stays O(N) total
          // instead of O(N^2); this is the one place they are reversed back
          // into their documented (completion/processing) order, since this
          // `Settlement` is now final and externally observed.
          let unknown_effects = list.reverse(state.unknown_effects)
          let timed_out_attempts =
            list.filter_map(unknown_effects, fn(effect) {
              case effect {
                UnknownEffect(step, StepAttempt(_), ActionTimedOut) -> Ok(step)
                _ -> Error(Nil)
              }
            })
          let settlement =
            Settlement(
              ..settlement,
              undone: list.reverse(settlement.undone),
              undo_failures: list.reverse(settlement.undo_failures),
              not_undoable: list.reverse(settlement.not_undoable),
              held: list.reverse(settlement.held),
              interrupted: list.append(
                timed_out_attempts,
                settlement.interrupted,
              ),
              unknown_effects: unknown_effects,
            )
          Some(case trigger {
            TriggerFailure(cause) -> Failed(cause, settlement)
            TriggerUnresolved(step, evidence) ->
              Unresolved(step, evidence, settlement)
            TriggerCancel(reason) -> Cancelled(reason, settlement)
          })
        }
      }
  }
}

/// O(1): `state.running` already excludes `NodeUndoing` (only attempts and
/// compensations increment it — see its own doc comment), and `state.
/// undoing` is the only other lifecycle state `rollback_complete` must rule
/// out (undo is strictly sequential, so a `Bool` suffices — see `RunState`'s
/// doc comment). Equivalent to the previous O(N) scan over every node's
/// current state, checked here on every message processed.
fn rollback_complete(state: RunState(o, e, u)) -> Bool {
  state.journal == [] && state.running == 0 && !state.undoing
}

/// O(1): `state.done_count` is incremented exactly once per node, in
/// `commit_success` (the only place a node ever becomes `NodeDone`), so
/// comparing it against `state.total_nodes` is equivalent to the previous
/// O(N) scan over every node's current state — checked here on every
/// message processed while `PhaseRunning`.
fn all_nodes_done(state: RunState(o, e, u)) -> Bool {
  case state.phase {
    PhaseSettling(..) | PhaseRollingBack(..) -> False
    PhaseRunning -> state.done_count == state.total_nodes
  }
}

// ---------------------------------------------------------------------------
// Admission
// ---------------------------------------------------------------------------

/// Admits ready nodes up to `max_concurrency`, one at a time, recursing
/// until either the bound is hit or nothing is left ready. Pops the
/// smallest node id off `state.ready` (a min-heap — see `RunState`'s doc
/// comment) rather than scanning `state.order` for every decision: since
/// `mark_ready`/`retry_now`/`handle_retry_fire` push a node the *moment* it
/// becomes eligible (dependency count reaches zero, or a retry's backoff
/// fires), the heap's minimum is always the same node a full ascending scan
/// of `order` would have found first. A node ready to retry is exactly as
/// eligible as a node whose dependencies just became ready — neither may
/// start ahead of `max_concurrency`, and this is the one place that bound
/// is enforced, so a fired retry is granted a slot here rather than at
/// `RetryFire`/decision time.
fn admit(state: RunState(o, e, u)) -> RunState(o, e, u) {
  case state.phase {
    PhaseSettling(..) | PhaseRollingBack(..) -> state
    PhaseRunning ->
      case state.running >= state.max_concurrency {
        True -> state
        False ->
          case min_heap.extract_min(state.ready) {
            Error(Nil) -> state
            Ok(#(next, remaining_ready)) -> {
              let state = RunState(..state, ready: remaining_ready)
              let attempt_number = case dict.get(state.state, next) {
                Ok(NodeReadyForRetry(attempt)) -> attempt
                _ -> 1
              }
              let assert Ok(n) = dict.get(state.nodes, next)
              let enabled =
                list.all(n.conditions, fn(condition) {
                  let selected: Bool = store.get(state.store, condition.0)
                  selected == condition.1
                })
              case enabled {
                True -> admit(start_attempt(state, next, attempt_number))
                False -> {
                  let state =
                    RunState(
                      ..state,
                      state: dict.insert(state.state, next, NodeSkipped),
                      done_count: state.done_count + 1,
                    )
                  let dependents =
                    dict.get(state.dependents, next) |> option_unwrap_list
                  admit(decrement_dependents(state, dependents))
                }
              }
            }
          }
      }
  }
}

/// Pushes `node_id` onto the ready heap. Called exactly at the moment a
/// node becomes eligible for admission: its last outstanding dependency
/// commits (`decrement_dependents`), an immediate `Retry` is decided
/// (`retry_now`), or a `RetryAfter` backoff timer fires
/// (`handle_retry_fire`). Never called twice for the same "becoming ready"
/// event — each of those call sites transitions the node's own
/// `NodeRunState` in the same step, so a node cannot be pushed while
/// already pending in the heap.
fn mark_ready(state: RunState(o, e, u), node_id: Int) -> RunState(o, e, u) {
  RunState(..state, ready: min_heap.insert(state.ready, node_id))
}

fn start_attempt(
  state: RunState(o, e, u),
  node_id: Int,
  attempt_number: Int,
) -> RunState(o, e, u) {
  let assert Ok(n) = dict.get(state.nodes, node_id)
  let seq = ffi.unique_integer()
  let attempt = attempt_context(state, node_id, attempt_number, n.max_attempts)
  let attempt =
    node.Attempt(..attempt, admit: fn(input) {
      let reply = process.new_subject()
      process.send(state.control, InputPrepared(node_id, seq, input, reply))
      process.receive_forever(reply)
      Ok(Nil)
    })
  let body = case set.contains(state.resume_nodes, node_id), n.persistence {
    True, Some(p) -> p.resume(attempt, state.store)
    _, _ -> n.prepare_attempt(attempt, state.store)
  }
  let control = state.control
  let #(pid, permits) =
    spawn_task(state, fn() {
      case ffi.rescue(body) {
        ffi.Rescued(result) ->
          process.send(control, AttemptDone(node_id, seq, result))
        ffi.Raised(class, reason) ->
          process.send(
            control,
            TaskCrashed(node_id, seq, node.Crash(class, reason)),
          )
      }
    })
  emit_step_started(state, n.address, attempt_number)
  // A step's own `timeout` (`n.timeout`) always wins when set; only a step
  // that declared none at all falls back to the run's `step_timeout`
  // default. `option.or` picks the first `Some`, so this is exactly that
  // override direction — never the reverse.
  let timer = case
    option.is_some(state.persistence),
    option.or(n.timeout, state.step_timeout)
  {
    True, _ -> None
    False, None -> None
    False, Some(ms) ->
      Some(process.send_after(control, ms, StepTimeoutFired(node_id, seq)))
  }
  RunState(
    ..state,
    dispatch: list.append(permits, state.dispatch),
    pending_starts: case permits {
      [] -> state.pending_starts
      _ -> [node_id, ..state.pending_starts]
    },
    resume_nodes: set.delete(state.resume_nodes, node_id),
    inputs: case set.contains(state.resume_nodes, node_id) {
      True -> state.inputs
      False -> dict.delete(state.inputs, node_id)
    },
    running: state.running + 1,
    state: dict.insert(
      state.state,
      node_id,
      NodeAttempting(seq, attempt_number, pid, timer, ffi.monotonic_time()),
    ),
  )
}

// ---------------------------------------------------------------------------
// Control message handling
// ---------------------------------------------------------------------------

fn handle_control(
  state: RunState(o, e, u),
  message: Control(o, e, u),
) -> RunState(o, e, u) {
  case message {
    InputPrepared(id, seq, input, reply) ->
      case current_attempt_seq(state, id) == Some(seq) {
        True ->
          RunState(
            ..state,
            inputs: dict.insert(state.inputs, id, input),
            dispatch: [reply, ..state.dispatch],
          )
        False -> state
      }
    AttemptDone(node_id, seq, result) ->
      handle_attempt_done(state, node_id, seq, result)
    RecoveryDone(node_id, seq, recovery) ->
      handle_recovery_done(state, node_id, seq, recovery)
    TaskCrashed(node_id, seq, crash) ->
      handle_task_crashed(state, node_id, seq, crash)
    RetryFire(node_id, seq) -> handle_retry_fire(state, node_id, seq)
    UndoDone(node_id, seq, outcome) ->
      handle_undo_done(state, node_id, seq, outcome)
    ProgressRequest(reply) -> {
      process.send(reply, build_progress(state))
      state
    }
    CancelRequest -> handle_cancel_request(state)
    DeadlineFired(_seq) -> handle_deadline_fired(state)
    StepTimeoutFired(node_id, seq) ->
      handle_step_timeout_fired(state, node_id, seq)
    CleanupTimeoutFired(node_id, seq) ->
      case dict.get(state.state, node_id) {
        Ok(NodeCompensating(..)) ->
          handle_cleanup_timeout_fired(state, node_id, seq)
        Ok(NodeUndoing(..)) -> handle_undo_timeout(state, node_id, seq)
        _ -> state
      }
    SettleFired(seq) -> handle_settle_fired(state, seq)
    OwnerDown(_reason) -> begin_settling(state, TriggerCancel(OwnerExited))
    TaskExited(pid, reason) -> handle_task_exited(state, pid, reason)
  }
}

fn handle_cancel_request(state: RunState(o, e, u)) -> RunState(o, e, u) {
  begin_settling(state, TriggerCancel(CancelRequested))
}

fn handle_deadline_fired(state: RunState(o, e, u)) -> RunState(o, e, u) {
  begin_settling(state, TriggerFailure(DeadlineExceeded))
}

/// Finds whichever node currently has `pid` attempting or compensating and
/// routes it through the same crash path a normal task-body exception
/// would take, via a fresh `TaskCrashed`. A task that already reported its
/// result before dying (an ordinary `Normal` exit after `send`) finds no
/// matching node here — its node was already transitioned by
/// `handle_attempt_done`/`handle_recovery_done`, so this is a no-op. A task
/// killed by the settle sweep is already `NodeInterrupted` by the time its
/// exit signal arrives, so it also finds no matching node here.
fn handle_task_exited(
  state: RunState(o, e, u),
  pid: Pid,
  reason: process.ExitReason,
) -> RunState(o, e, u) {
  case reason {
    process.Normal -> state
    _ ->
      case find_node_for_pid(state, pid) {
        None -> state
        Some(#(node_id, seq)) ->
          handle_task_crashed(
            state,
            node_id,
            seq,
            node.Crash(ffi.ExitClass, exit_reason_to_string(reason)),
          )
      }
  }
}

fn find_node_for_pid(
  state: RunState(o, e, u),
  pid: Pid,
) -> Option(#(Int, Int)) {
  dict.to_list(state.state)
  |> list.find_map(fn(entry) {
    let #(node_id, node_state) = entry
    case node_state {
      NodeAttempting(seq, _attempt, task_pid, _timer, _started_at)
        if task_pid == pid
      -> Ok(#(node_id, seq))
      NodeCompensating(seq, _attempt, task_pid, _timer, _started_at)
        if task_pid == pid
      -> Ok(#(node_id, seq))
      _ -> Error(Nil)
    }
  })
  |> option.from_result
}

fn exit_reason_to_string(reason: process.ExitReason) -> String {
  case reason {
    process.Normal -> "normal"
    process.Killed -> "killed"
    process.Abnormal(reason) -> "abnormal: " <> string.inspect(reason)
  }
}

fn current_attempt_seq(state: RunState(o, e, u), node_id: Int) -> Option(Int) {
  case dict.get(state.state, node_id) {
    Ok(NodeAttempting(seq, ..)) -> Some(seq)
    Ok(NodeCompensating(seq, ..)) -> Some(seq)
    _ -> None
  }
}

fn cancel_node_timer(state: RunState(o, e, u), node_id: Int) -> Nil {
  case dict.get(state.state, node_id) {
    Ok(NodeAttempting(_, _, _, Some(timer), _))
    | Ok(NodeCompensating(_, _, _, Some(timer), _))
    | Ok(NodeUndoing(_, _, Some(timer), _)) -> {
      let _ = process.cancel_timer(timer)
      Nil
    }
    _ -> Nil
  }
}

fn handle_attempt_done(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
  result: AttemptResult(e, u),
) -> RunState(o, e, u) {
  case current_attempt_seq(state, node_id) == Some(seq) {
    False -> state
    True -> {
      cancel_node_timer(state, node_id)
      case result {
        node.AttemptBlocked(reason) -> RunState(..state, blocked: Some(reason))
        node.AttemptAbsent ->
          maybe_finish_settling(
            RunState(
              ..state,
              running: state.running - 1,
              state: dict.insert(state.state, node_id, NodeSkipped),
            ),
          )
        AttemptSucceeded(commit) ->
          commit_success(state, node_id, commit, observation.AttemptSucceeded)
        AttemptFailed(failure, recover) ->
          case recover {
            None ->
              fail_terminal(
                state,
                node_id,
                failure,
                attempt_result_kind(failure),
              )
            Some(prepare_recovery) ->
              start_recovery_from_returned(
                state,
                node_id,
                failure,
                prepare_recovery,
              )
          }
      }
    }
  }
}

fn attempt_result_kind(failure: AttemptFailure(e)) -> observation.AttemptKind {
  case failure {
    Returned(_) -> observation.AttemptFailed
    Crashed(_) -> observation.AttemptCrashed
    TimedOut -> observation.AttemptTimedOut
  }
}

fn commit_success(
  state: RunState(o, e, u),
  node_id: Int,
  commit: fn(Store) -> #(Store, Option(fn() -> Result(Nil, u))),
  step_kind: observation.AttemptKind,
) -> RunState(o, e, u) {
  let assert Ok(n) = dict.get(state.nodes, node_id)
  let attempt_number = attempt_number_for(state, node_id)
  let duration = duration_since_started(state, node_id)
  let #(next_store, undo) = commit(state.store)
  emit_step_stopped(state, n.address, attempt_number, step_kind, duration)
  let entry = JournalEntry(node_id: node_id, address: n.address, undo: undo)
  let dependent_ids = dict.get(state.dependents, node_id) |> option_unwrap_list
  let state =
    RunState(
      ..state,
      running: state.running - 1,
      journal: [entry, ..state.journal],
      state: dict.insert(state.state, node_id, NodeDone),
      store: next_store,
      done_count: state.done_count + 1,
    )
  let state = decrement_dependents(state, dependent_ids)
  // While running, a success may unblock new admissions. While settling or
  // rolling back (a sibling finishing after the primary trigger), this
  // success still has to be undone like any other journal entry, but
  // `admit` itself is a no-op there — `undo_next` is what actually drains
  // the journal, and it must be nudged here or this entry (and any undo
  // work following it) would never start once rollback begins. During
  // settling itself nothing drains the journal yet (rollback has not
  // started), so a plain no-op is correct there.
  case state.phase {
    PhaseRunning -> admit(state)
    PhaseSettling(..) -> maybe_finish_settling(state)
    PhaseRollingBack(..) -> undo_next(state)
  }
}

fn attempt_number_for(state: RunState(o, e, u), node_id: Int) -> Int {
  case dict.get(state.state, node_id) {
    Ok(NodeAttempting(_, attempt, ..)) -> attempt
    Ok(NodeCompensating(_, attempt, ..)) -> attempt
    _ -> 0
  }
}

/// The elapsed time since `node_id`'s current attempt, compensation, or
/// undo began, for real `step_stopped`/`compensation_stopped`/
/// `undo_stopped` durations instead of a hard-coded `0`. `0` here (rather
/// than a lookup failure) only if `node_id` is already out of its
/// attempting/compensating/undoing state by the time this is read, which
/// no caller does — every call site reads this before transitioning the
/// node's state.
fn duration_since_started(state: RunState(o, e, u), node_id: Int) -> Int {
  let started_at = case dict.get(state.state, node_id) {
    Ok(NodeAttempting(_, _, _, _, started_at)) -> started_at
    Ok(NodeCompensating(_, _, _, _, started_at)) -> started_at
    Ok(NodeUndoing(_, _, _, started_at)) -> started_at
    _ -> ffi.monotonic_time()
  }
  ffi.monotonic_time() - started_at
}

fn option_unwrap_list(result: Result(List(a), Nil)) -> List(a) {
  case result {
    Ok(values) -> values
    Error(_) -> []
  }
}

fn decrement_dependents(
  state: RunState(o, e, u),
  dependent_ids: List(Int),
) -> RunState(o, e, u) {
  list.fold(dependent_ids, state, fn(acc, id) {
    case dict.get(acc.state, id) {
      Ok(NodeWaiting(remaining)) -> {
        let next_remaining = remaining - 1
        let acc =
          RunState(
            ..acc,
            state: dict.insert(acc.state, id, NodeWaiting(next_remaining)),
          )
        case next_remaining {
          0 -> mark_ready(acc, id)
          _ -> acc
        }
      }
      _ -> acc
    }
  })
}

fn start_recovery_from_returned(
  state: RunState(o, e, u),
  node_id: Int,
  failure: AttemptFailure(e),
  prepare_recovery: fn(Attempt) -> fn() -> ErasedRecovery(e, u),
) -> RunState(o, e, u) {
  let assert Ok(NodeAttempting(_seq, attempt_number, _pid, _timer, _started_at)) =
    dict.get(state.state, node_id)
  let assert Ok(n) = dict.get(state.nodes, node_id)
  let attempt = attempt_context(state, node_id, attempt_number, n.max_attempts)
  spawn_recovery(
    state,
    node_id,
    attempt_number,
    failure,
    prepare_recovery(attempt),
  )
}

fn spawn_recovery(
  state: RunState(o, e, u),
  node_id: Int,
  attempt_number: Int,
  failure: AttemptFailure(e),
  body: fn() -> ErasedRecovery(e, u),
) -> RunState(o, e, u) {
  let state =
    RunState(
      ..state,
      last_failure: dict.insert(state.last_failure, node_id, failure),
    )
  let seq = ffi.unique_integer()
  let control = state.control
  let #(pid, permits) =
    spawn_task(state, fn() {
      case ffi.rescue(body) {
        ffi.Rescued(recovery) ->
          process.send(control, RecoveryDone(node_id, seq, recovery))
        ffi.Raised(class, reason) ->
          process.send(
            control,
            TaskCrashed(node_id, seq, node.Crash(class, reason)),
          )
      }
    })
  let timer = case state.persistence {
    Some(_) -> None
    None ->
      Some(process.send_after(
        control,
        state.cleanup_timeout,
        CleanupTimeoutFired(node_id, seq),
      ))
  }
  RunState(
    ..state,
    dispatch: list.append(permits, state.dispatch),
    pending_starts: case permits {
      [] -> state.pending_starts
      _ -> [node_id, ..state.pending_starts]
    },
    state: dict.insert(
      state.state,
      node_id,
      NodeCompensating(seq, attempt_number, pid, timer, ffi.monotonic_time()),
    ),
  )
}

fn handle_recovery_done(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
  recovery: ErasedRecovery(e, u),
) -> RunState(o, e, u) {
  case current_attempt_seq(state, node_id) == Some(seq) {
    False -> state
    True -> {
      cancel_node_timer(state, node_id)
      let assert Ok(NodeCompensating(
        _seq,
        attempt_number,
        _pid,
        _timer,
        _started_at,
      )) = dict.get(state.state, node_id)
      let assert Ok(n) = dict.get(state.nodes, node_id)
      let duration = duration_since_started(state, node_id)
      case recovery {
        node.EBlocked(_) -> Nil
        _ ->
          emit_compensation_stopped(
            state,
            n.address,
            attempt_number,
            decision_kind(recovery),
            duration,
          )
      }
      let settling_or_rolling_back = case state.phase {
        PhaseRunning -> False
        PhaseSettling(..) | PhaseRollingBack(..) -> True
      }
      case recovery, settling_or_rolling_back {
        node.EBlocked(reason), _ -> RunState(..state, blocked: Some(reason))
        // Retry/RetryAfter decisions are not honored once settling has
        // begun (§3.3): the run is already stopping admission, so a fresh
        // attempt would race the settle window. The exhausted-vs-not
        // distinction stops mattering once settling is underway; the node
        // is simply not retried, and this fact is recorded as a sibling
        // failure using whatever the last observed failure was.
        ERetry, True | ERetryAfter(_), True ->
          fail_retry_superseded_as_sibling(state, node_id)
        ERetry, False ->
          case attempt_number < n.max_attempts {
            True -> {
              let state = RunState(..state, running: state.running - 1)
              retry_now(state, node_id, attempt_number + 1)
            }
            False -> fail_terminal_retry_limit(state, node_id)
          }
        ERetryAfter(ms), False ->
          case attempt_number < n.max_attempts {
            True -> {
              let state = RunState(..state, running: state.running - 1)
              retry_after(state, node_id, attempt_number + 1, ms)
            }
            False -> fail_terminal_retry_limit(state, node_id)
          }
        EContinue(commit), _ ->
          commit_success(state, node_id, commit, observation.AttemptSucceeded)
        EAbort(error), _ ->
          fail_terminal(
            state,
            node_id,
            node.Returned(error),
            observation.AttemptFailed,
          )
        EAbortCleanup(error, cleanup_error), _ -> {
          let cleanup_failure =
            CleanupFailed(node_address(state, node_id), cleanup_error)
          state
          |> fail_terminal(
            node_id,
            node.Returned(error),
            observation.AttemptFailed,
          )
          |> record_compensation_failure(cleanup_failure)
        }
        EHold(evidence), _ ->
          begin_unresolved(
            RunState(..state, running: state.running - 1),
            node_id,
            evidence,
          )
      }
    }
  }
}

fn decision_kind(recovery: ErasedRecovery(e, u)) -> observation.DecisionKind {
  case recovery {
    ERetry | ERetryAfter(_) -> observation.DecisionRetry
    EContinue(_) -> observation.DecisionContinue
    EAbort(_) | EAbortCleanup(_, _) -> observation.DecisionAbort
    EHold(_) | node.EBlocked(_) -> observation.DecisionHold
  }
}

/// An immediate `Retry` decision: the node is marked ready and handed to
/// `admit`, which starts it only if a concurrency slot is free — never
/// unconditionally, so an immediate retry can no more exceed
/// `max_concurrency` than a fresh node's first attempt can.
fn retry_now(
  state: RunState(o, e, u),
  node_id: Int,
  next_attempt: Int,
) -> RunState(o, e, u) {
  RunState(
    ..state,
    state: dict.insert(state.state, node_id, NodeReadyForRetry(next_attempt)),
  )
  |> mark_ready(node_id)
  |> admit
}

fn retry_after(
  state: RunState(o, e, u),
  node_id: Int,
  next_attempt: Int,
  ms: Int,
) -> RunState(o, e, u) {
  let delay = case ms < 0 {
    True -> 0
    False -> ms
  }
  let seq = ffi.unique_integer()
  let control = state.control
  process.send_after(control, delay, RetryFire(node_id, seq))
  let state =
    RunState(
      ..state,
      state: dict.insert(
        state.state,
        node_id,
        NodeRetryScheduled(seq, next_attempt, ffi.system_time() + delay),
      ),
    )
  // The attempt that just failed already freed its `running` slot (the
  // caller decremented it before calling `retry_after`); this node itself
  // won't reclaim that slot until its backoff fires, so nudge `admit` now
  // in case a *different* ready node can use the freed slot in the
  // meantime, rather than leaving it idle until `RetryFire`.
  admit(state)
}

/// The backoff timer for a `RetryAfter` decision fired: the node becomes
/// ready, and — exactly like `retry_now` — is hedged through `admit` rather
/// than started unconditionally, so a fired retry can never itself exceed
/// `max_concurrency`; it simply joins the ready queue and waits its turn
/// for a free slot like any other ready node.
fn handle_retry_fire(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
) -> RunState(o, e, u) {
  case dict.get(state.state, node_id) {
    Ok(NodeRetryScheduled(current_seq, next_attempt, _)) if current_seq == seq ->
      RunState(
        ..state,
        state: dict.insert(
          state.state,
          node_id,
          NodeReadyForRetry(next_attempt),
        ),
      )
      |> mark_ready(node_id)
      |> admit
    _ -> state
  }
}

fn handle_task_crashed(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
  crash: node.Crash,
) -> RunState(o, e, u) {
  case current_attempt_seq(state, node_id) == Some(seq) {
    False -> state
    True ->
      case dict.get(state.state, node_id) {
        Ok(NodeAttempting(_seq, attempt_number, ..)) -> {
          let assert Ok(n) = dict.get(state.nodes, node_id)
          let state =
            record_unknown_effect(
              state,
              node_id,
              StepAttempt(attempt_number),
              ActionCrashed(crash),
            )
          case n.prepare_crash_recovery {
            None ->
              fail_terminal(
                state,
                node_id,
                Crashed(crash),
                observation.AttemptCrashed,
              )
            Some(prepare) ->
              start_crash_recovery(state, node_id, prepare, Crashed(crash))
          }
        }
        Ok(NodeCompensating(_seq, attempt_number, ..)) -> {
          let compensation_failure =
            CompensationCrashed(node_address(state, node_id), crash)
          state
          |> record_unknown_effect(
            node_id,
            StepCompensation(attempt_number),
            ActionCrashed(crash),
          )
          |> fail_terminal(node_id, Crashed(crash), observation.AttemptCrashed)
          |> record_compensation_failure(compensation_failure)
        }
        _ -> state
      }
  }
}

fn start_crash_recovery(
  state: RunState(o, e, u),
  node_id: Int,
  prepare: fn(AttemptFailure(e), Attempt, Store) -> fn() -> ErasedRecovery(e, u),
  failure: AttemptFailure(e),
) -> RunState(o, e, u) {
  let assert Ok(NodeAttempting(_seq, attempt_number, _pid, _timer, _started_at)) =
    dict.get(state.state, node_id)
  let assert Ok(n) = dict.get(state.nodes, node_id)
  let attempt = attempt_context(state, node_id, attempt_number, n.max_attempts)
  spawn_recovery(
    state,
    node_id,
    attempt_number,
    failure,
    prepare(failure, attempt, state.store),
  )
}

// ---------------------------------------------------------------------------
// Step and cleanup timeouts
// ---------------------------------------------------------------------------

/// A step's own `timeout` fired while it was attempting. The task is killed
/// (its effect is unknown — never journaled); if the step has a compensate
/// decider it is asked to decide on a `TimedOut` failure, otherwise the
/// failure is terminal. This never touches `settlement.interrupted` — that
/// is reserved for tasks still running when the *settle* window closes, not
/// for a step's own configured timeout — but the killed attempt is always
/// recorded as an unknown effect regardless of what the recovery decider
/// (if any) subsequently decides: even a `Retry` or `Continue` that lets
/// the run proceed leaves this one attempt's effect permanently unknown,
/// and that fact must survive to the final `Outcome` (see `Outcome`'s doc
/// comment; a terminal outcome also folds it into `settlement.interrupted`,
/// in `run_finished`).
fn handle_step_timeout_fired(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
) -> RunState(o, e, u) {
  case current_attempt_seq(state, node_id) == Some(seq) {
    False -> state
    True ->
      case dict.get(state.state, node_id) {
        Ok(NodeAttempting(_seq, attempt_number, pid, _timer, started_at)) -> {
          process.kill(pid)
          let assert Ok(n) = dict.get(state.nodes, node_id)
          let state =
            RunState(
              ..state,
              state: dict.insert(
                state.state,
                node_id,
                // Bump to a fresh (unreachable) seq so a stray late message
                // from the killed task cannot be mistaken for this attempt,
                // but keep the *original* `started_at` — this killed
                // attempt's own `step_stopped` (emitted either by
                // `fail_terminal` just below, when there is no decider, or
                // right here otherwise) must report its real elapsed time,
                // not the ~0 duration a freshly-taken timestamp would give.
                NodeAttempting(
                  ffi.unique_integer(),
                  attempt_number,
                  pid,
                  None,
                  started_at,
                ),
              ),
            )
            |> record_unknown_effect(
              node_id,
              StepAttempt(attempt_number),
              ActionTimedOut,
            )
          case n.prepare_crash_recovery {
            None ->
              // No decider: `fail_terminal` below is this attempt's own
              // terminal outcome, and it emits `step_stopped` using this
              // same attempt's real duration (its `started_at` was just
              // preserved above) — a single, correct event, so nothing
              // extra is needed here.
              fail_terminal(
                state,
                node_id,
                TimedOut,
                observation.AttemptTimedOut,
              )
            Some(prepare) -> {
              // A decider exists: the decision's own resolution
              // (`fail_terminal`/`commit_success`, from
              // `handle_recovery_done`) will emit a *different*
              // `step_stopped` later, using the decider task's own
              // (much shorter) duration and its own outcome kind — never
              // `AttemptTimedOut`. Without emitting one here, the killed
              // attempt's own timeout would never be reported at all, so
              // it is emitted now, using its real elapsed duration, before
              // `start_crash_recovery` moves the node into
              // `NodeCompensating` with its own fresh `started_at`.
              let duration = ffi.monotonic_time() - started_at
              emit_step_stopped(
                state,
                n.address,
                attempt_number,
                observation.AttemptTimedOut,
                duration,
              )
              start_crash_recovery(state, node_id, prepare, TimedOut)
            }
          }
        }
        _ -> state
      }
  }
}

/// The run's `cleanup_timeout` fired for an in-flight compensation decision.
/// The decider task is killed and this is treated exactly like a
/// compensation crash: terminal, with `CompensationTimedOut` recorded.
fn handle_cleanup_timeout_fired(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
) -> RunState(o, e, u) {
  case current_attempt_seq(state, node_id) == Some(seq) {
    False -> state
    True ->
      case dict.get(state.state, node_id) {
        Ok(NodeCompensating(_seq, attempt_number, pid, _timer, _started_at)) -> {
          process.kill(pid)
          let compensation_failure =
            CompensationTimedOut(node_address(state, node_id))
          state
          |> record_unknown_effect(
            node_id,
            StepCompensation(attempt_number),
            ActionTimedOut,
          )
          |> fail_terminal(node_id, TimedOut, observation.AttemptTimedOut)
          |> record_compensation_failure(compensation_failure)
        }
        _ -> state
      }
  }
}

fn node_address(state: RunState(o, e, u), node_id: Int) -> StepAddress {
  let assert Ok(n) = dict.get(state.nodes, node_id)
  n.address
}

/// Records that `node_id`'s `action` ended with an unknown effect. Called
/// at the moment the action ends, before any decision about it, so the
/// record survives whatever the run does next.
fn record_unknown_effect(
  state: RunState(o, e, u),
  node_id: Int,
  action: Action,
  ending: UnknownEnding,
) -> RunState(o, e, u) {
  let effect = UnknownEffect(node_address(state, node_id), action, ending)
  RunState(..state, unknown_effects: [effect, ..state.unknown_effects])
}

fn record_compensation_failure(
  state: RunState(o, e, u),
  failure: CompensationFailure(u),
) -> RunState(o, e, u) {
  update_settlement(state, fn(settlement) {
    Settlement(
      ..settlement,
      compensation_failures: list.append(settlement.compensation_failures, [
        failure,
      ]),
    )
  })
}

/// Applies `f` to the in-flight settlement, whichever phase currently holds
/// one (`Settling` or `RollingBack`). A no-op while `PhaseRunning`, which
/// cannot happen for any caller here (every caller runs only after a
/// terminal transition), but is kept total rather than partial.
fn update_settlement(
  state: RunState(o, e, u),
  f: fn(Settlement(e, u)) -> Settlement(e, u),
) -> RunState(o, e, u) {
  case state.phase {
    PhaseRunning -> state
    PhaseSettling(trigger, settlement, timer, seq) ->
      RunState(
        ..state,
        phase: PhaseSettling(trigger, f(settlement), timer, seq),
      )
    PhaseRollingBack(trigger, settlement) ->
      RunState(..state, phase: PhaseRollingBack(trigger, f(settlement)))
  }
}

/// Marks `node_id` terminally failed. If this is the first terminal
/// trigger, it starts settling with this cause as the run's primary
/// trigger; if settling (or rollback) is already underway, this node's
/// cause is recorded as a sibling failure instead (§3.2: "the first
/// terminal failure sets the primary cause; later failures go to
/// sibling_failures"). Always decrements `running` by one for the task that
/// just finished (an attempt or a compensation decision); callers must not
/// also decrement it.
fn fail_terminal(
  state: RunState(o, e, u),
  node_id: Int,
  failure: AttemptFailure(e),
  step_kind: observation.AttemptKind,
) -> RunState(o, e, u) {
  let address = node_address(state, node_id)
  let attempt_number = attempt_number_for(state, node_id)
  let duration = duration_since_started(state, node_id)
  emit_step_stopped(state, address, attempt_number, step_kind, duration)
  let cause = case failure {
    Returned(error) -> StepFailed(address, error)
    Crashed(crash) -> StepCrashed(address, crash)
    TimedOut -> StepTimedOut(address)
  }
  let state =
    RunState(
      ..state,
      running: state.running - 1,
      state: dict.insert(state.state, node_id, NodeFailedTerminal),
    )
  case is_terminal(state.phase) {
    True ->
      update_settlement(state, fn(s) {
        Settlement(
          ..s,
          sibling_failures: list.append(s.sibling_failures, [
            cause,
          ]),
        )
      })
      |> maybe_finish_settling
    False -> begin_settling(state, TriggerFailure(cause))
  }
}

/// A `Retry`/`RetryAfter` decision arrived after settling had already begun
/// for a different trigger: the node cannot be retried (it would race the
/// settle window), so its last observed failure is recorded as a sibling
/// failure — as `RetrySuperseded`, since its attempt budget was never
/// necessarily exhausted; that is a separate condition from
/// `RetryLimitReached`.
fn fail_retry_superseded_as_sibling(
  state: RunState(o, e, u),
  node_id: Int,
) -> RunState(o, e, u) {
  let address = node_address(state, node_id)
  let assert Ok(last) = dict.get(state.last_failure, node_id)
  let cause = RetrySuperseded(address, last)
  let state =
    RunState(
      ..state,
      running: state.running - 1,
      state: dict.insert(state.state, node_id, NodeFailedTerminal),
    )
  update_settlement(state, fn(s) {
    Settlement(..s, sibling_failures: list.append(s.sibling_failures, [cause]))
  })
  |> maybe_finish_settling
}

fn is_terminal(phase: RunPhase(e, u)) -> Bool {
  case phase {
    PhaseRunning -> False
    PhaseSettling(..) | PhaseRollingBack(..) -> True
  }
}

fn fail_terminal_retry_limit(
  state: RunState(o, e, u),
  node_id: Int,
) -> RunState(o, e, u) {
  let address = node_address(state, node_id)
  let assert Ok(last) = dict.get(state.last_failure, node_id)
  let cause = RetryLimitReached(address, last)
  let state =
    RunState(
      ..state,
      running: state.running - 1,
      state: dict.insert(state.state, node_id, NodeFailedTerminal),
    )
  begin_settling(state, TriggerFailure(cause))
}

/// A `Hold` decision grants no rollback authority for its own step. If it
/// is the run's first terminal trigger the whole run ends `Unresolved` and
/// nothing is ever undone (§3.3). If settling (or rollback) is already
/// underway for a different trigger, this step's completed effect is still
/// recorded `held` (never undone), without changing the run's outcome kind
/// — a documented extension of "later failures go to sibling_failures" to
/// the `Hold` decision, which has no `Cause` representation of its own.
fn begin_unresolved(
  state: RunState(o, e, u),
  node_id: Int,
  evidence: e,
) -> RunState(o, e, u) {
  let address = node_address(state, node_id)
  let state =
    RunState(
      ..state,
      state: dict.insert(state.state, node_id, NodeFailedTerminal),
    )
  case is_terminal(state.phase) {
    True ->
      update_settlement(state, fn(s) {
        Settlement(..s, held: [address, ..s.held])
      })
      |> maybe_finish_settling
    False -> begin_settling(state, TriggerUnresolved(address, evidence))
  }
}

/// Enters `Settling` on the first terminal trigger (failure, deadline, or
/// cancellation): stops admission, marks every waiting or backoff-scheduled
/// node `Skipped` (cancelling its backoff timer), and starts the
/// `settle_timeout` window for whatever is still attempting or
/// compensating. A no-op once settling or rollback has already begun —
/// `cancel` is idempotent, and a second failure becomes a sibling failure
/// via `fail_terminal`'s own check instead of re-entering here.
fn begin_settling(
  state: RunState(o, e, u),
  trigger: Trigger(e),
) -> RunState(o, e, u) {
  case state.phase {
    PhaseSettling(..) | PhaseRollingBack(..) -> state
    PhaseRunning -> {
      let waiting_ids =
        list.filter(state.order, fn(id) {
          case dict.get(state.state, id) {
            Ok(NodeWaiting(_)) -> True
            Ok(NodeRetryScheduled(..)) -> True
            Ok(NodeReadyForRetry(..)) -> True
            _ -> False
          }
        })
      let state =
        list.fold(waiting_ids, state, fn(acc, id) {
          RunState(..acc, state: dict.insert(acc.state, id, NodeSkipped))
        })
      let seq = ffi.unique_integer()
      let settle_timer = case has_in_flight_task(state) {
        False -> None
        True ->
          Some(process.send_after(
            state.control,
            state.settle_timeout,
            SettleFired(seq),
          ))
      }
      let state =
        RunState(
          ..state,
          phase: PhaseSettling(trigger, empty_settlement(), settle_timer, seq),
        )
      maybe_finish_settling(state)
    }
  }
}

fn has_in_flight_task(state: RunState(o, e, u)) -> Bool {
  list.any(dict.values(state.state), fn(s) {
    case s {
      NodeAttempting(..) | NodeCompensating(..) -> True
      _ -> False
    }
  })
}

/// Once nothing is attempting or compensating any more (either everything
/// finished on its own within the settle window, or the settle timer swept
/// the rest), moves on to `Unresolved` (a `Hold` was the trigger) or starts
/// rolling back the journal.
fn maybe_finish_settling(state: RunState(o, e, u)) -> RunState(o, e, u) {
  case state.phase {
    PhaseRunning | PhaseRollingBack(..) -> state
    PhaseSettling(trigger, settlement, timer, _seq) ->
      case has_in_flight_task(state) {
        True -> state
        False -> {
          case timer {
            None -> Nil
            Some(t) -> {
              let _ = process.cancel_timer(t)
              Nil
            }
          }
          let state =
            RunState(..state, phase: PhaseRollingBack(trigger, settlement))
          case trigger {
            TriggerUnresolved(..) -> hold_journal(state)
            TriggerFailure(..) | TriggerCancel(..) -> undo_next(state)
          }
        }
      }
  }
}

/// The settle window closed with tasks still in flight: kill each of them
/// and mark its node `Interrupted` — its effect is unknown, and it is never
/// journaled or undone (§3.4, §8.8: never claim a kill reversed anything).
fn handle_settle_fired(
  state: RunState(o, e, u),
  seq: Int,
) -> RunState(o, e, u) {
  case state.phase {
    PhaseSettling(_trigger, _settlement, _timer, current_seq)
      if current_seq == seq
    -> {
      let in_flight =
        dict.to_list(state.state)
        |> list.filter_map(fn(entry) {
          let #(node_id, node_state) = entry
          case node_state {
            NodeAttempting(_, attempt, pid, _, _) ->
              Ok(#(node_id, pid, StepAttempt(attempt)))
            NodeCompensating(_, attempt, pid, _, _) ->
              Ok(#(node_id, pid, StepCompensation(attempt)))
            _ -> Error(Nil)
          }
        })
      let state =
        list.fold(in_flight, state, fn(acc, entry) {
          let #(node_id, pid, action) = entry
          process.kill(pid)
          let acc =
            record_unknown_effect(acc, node_id, action, ActionInterrupted)
          let address = node_address(acc, node_id)
          let attempt_number = attempt_number_for(acc, node_id)
          let duration = duration_since_started(acc, node_id)
          emit_step_stopped(
            acc,
            address,
            attempt_number,
            observation.AttemptInterrupted,
            duration,
          )
          let acc =
            RunState(
              ..acc,
              running: acc.running - 1,
              state: dict.insert(acc.state, node_id, NodeInterrupted),
            )
          update_settlement(acc, fn(s) {
            Settlement(..s, interrupted: list.append(s.interrupted, [address]))
          })
        })
      maybe_finish_settling(state)
    }
    _ -> state
  }
}

/// `Hold` grants no rollback authority: every journal entry (whether or
/// not it has an undo action) is reported in `held` and nothing runs. This
/// is the `Unresolved` outcome's settlement, never `undo_next`'s.
fn hold_journal(state: RunState(o, e, u)) -> RunState(o, e, u) {
  case state.journal {
    [] -> state
    [JournalEntry(_node_id, address, _undo), ..rest] -> {
      // The node's own state stays `NodeDone` (it completed, and a hold
      // never touches it), unlike `undo_next` which transitions each
      // entry as it processes it.
      let state = RunState(..state, journal: rest)
      record_held(state, address) |> hold_journal
    }
  }
}

fn record_held(
  state: RunState(o, e, u),
  address: StepAddress,
) -> RunState(o, e, u) {
  update_settlement(state, fn(s) { Settlement(..s, held: [address, ..s.held]) })
}

/// `state.undoing` (an O(1) flag, not a scan — see `RunState`'s doc
/// comment) is the guard for "one undo at a time"; it is set `True` exactly
/// where a node enters `NodeUndoing` below, and cleared by
/// `handle_undo_done`/`handle_undo_timeout`, the only two places a node
/// ever leaves it.
fn undo_next(state: RunState(o, e, u)) -> RunState(o, e, u) {
  case state.undoing {
    True -> state
    False ->
      case state.journal {
        [] -> state
        [JournalEntry(node_id, address, None), ..rest] -> {
          let state =
            RunState(
              ..state,
              journal: rest,
              state: dict.insert(state.state, node_id, NodeUndone),
            )
          record_not_undoable(state, address) |> undo_next
        }
        [JournalEntry(node_id, _address, Some(undo_fn)), ..rest] -> {
          let control = state.control
          let seq = ffi.unique_integer()
          let assert Ok(n) = dict.get(state.nodes, node_id)
          let undo_body = case
            set.contains(state.resume_undos, node_id),
            n.persistence
          {
            True, Some(p) ->
              p.resume_undo(state.store, action_key(state, node_id, "undo"))
            _, _ -> fn() { Ok(undo_fn()) }
          }
          let #(pid, permits) =
            spawn_task(state, fn() {
              case ffi.rescue(undo_body) {
                ffi.Rescued(Ok(Ok(Nil))) ->
                  process.send(control, UndoDone(node_id, seq, UndoOk))
                ffi.Rescued(Ok(Error(error))) ->
                  process.send(control, UndoDone(node_id, seq, UndoErr(error)))
                ffi.Rescued(Error(reason)) ->
                  process.send(
                    control,
                    UndoDone(node_id, seq, UndoBlocked(reason)),
                  )
                ffi.Raised(class, reason) ->
                  process.send(
                    control,
                    UndoDone(node_id, seq, UndoCrash(node.Crash(class, reason))),
                  )
              }
            })
          let timer = case state.persistence {
            Some(_) -> None
            None ->
              Some(process.send_after(
                control,
                state.cleanup_timeout,
                CleanupTimeoutFired(node_id, seq),
              ))
          }
          RunState(
            ..state,
            journal: rest,
            dispatch: list.append(permits, state.dispatch),
            pending_starts: case permits {
              [] -> state.pending_starts
              _ -> [node_id, ..state.pending_starts]
            },
            resume_undos: set.delete(state.resume_undos, node_id),
            state: dict.insert(
              state.state,
              node_id,
              NodeUndoing(seq, pid, timer, ffi.monotonic_time()),
            ),
            undoing: True,
          )
        }
      }
  }
}

fn handle_undo_done(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
  outcome: UndoOutcome(u),
) -> RunState(o, e, u) {
  case outcome {
    UndoBlocked(reason) -> RunState(..state, blocked: Some(reason))
    _ -> handle_known_undo(state, node_id, seq, outcome)
  }
}

fn handle_known_undo(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
  outcome: UndoOutcome(u),
) -> RunState(o, e, u) {
  case dict.get(state.state, node_id) {
    Ok(NodeUndoing(current_seq, _pid, timer, _started_at))
      if current_seq == seq
    -> {
      case timer {
        None -> Nil
        Some(t) -> {
          let _ = process.cancel_timer(t)
          Nil
        }
      }
      let address = node_address(state, node_id)
      let attempt_number = 1
      let duration = duration_since_started(state, node_id)
      let new_state = case outcome {
        UndoOk -> NodeUndone
        UndoErr(_) -> NodeUndoFailedTerminal
        UndoCrash(_) | UndoBlocked(_) -> NodeUndoFailedTerminal
      }
      emit_undo_stopped(state, address, undo_outcome_kind(outcome), duration)
      let state =
        RunState(
          ..state,
          state: dict.insert(state.state, node_id, new_state),
          undoing: False,
        )
      let _ = attempt_number
      let state = case outcome {
        UndoBlocked(reason) -> RunState(..state, blocked: Some(reason))
        UndoOk -> record_undone(state, address)
        UndoErr(error) -> record_undo_failure(state, UndoFailed(address, error))
        UndoCrash(crash) ->
          state
          |> record_unknown_effect(node_id, StepUndo, ActionCrashed(crash))
          |> record_undo_failure(UndoCrashed(address, crash))
      }
      undo_next(state)
    }
    _ -> state
  }
}

fn undo_outcome_kind(outcome: UndoOutcome(u)) -> observation.UndoKind {
  case outcome {
    UndoBlocked(_) -> observation.UndoCrashedKind
    UndoOk -> observation.UndoUndone
    UndoErr(_) -> observation.UndoFailedKind
    UndoCrash(_) -> observation.UndoCrashedKind
  }
}

/// The undo action's `cleanup_timeout` fired: kill the task, record
/// `UndoTimedOut`, and continue to the next journal entry — undo is never
/// retried (§3.3 and increment-1 precedent for undo failures in general).
fn handle_undo_timeout(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
) -> RunState(o, e, u) {
  case dict.get(state.state, node_id) {
    Ok(NodeUndoing(current_seq, pid, _timer, _started_at))
      if current_seq == seq
    -> {
      process.kill(pid)
      let address = node_address(state, node_id)
      let duration = duration_since_started(state, node_id)
      emit_undo_stopped(state, address, observation.UndoTimedOutKind, duration)
      let state =
        RunState(
          ..state,
          state: dict.insert(state.state, node_id, NodeUndoFailedTerminal),
          undoing: False,
        )
      let state =
        state
        |> record_unknown_effect(node_id, StepUndo, ActionTimedOut)
        |> record_undo_failure(UndoTimedOut(address))
      undo_next(state)
    }
    _ -> state
  }
}

// `record_undone`/`record_undo_failure`/`record_not_undoable`/`record_held`
// (below) all prepend rather than `list.append` (which is O(k) per call,
// O(k^2) total across k calls accumulating into the same list — and
// rollback can call these once per journal entry, i.e. up to N times).
// Each field is therefore built newest-first internally and reversed
// exactly once, at `run_finished`'s single point where a `Settlement`
// becomes the final, externally observed value — nowhere else reads these
// fields' order before then (`build_progress` never reads `Settlement` at
// all, and the one other read, `emit_run_stopped`'s `list.length`, is
// order-independent).

fn record_undone(
  state: RunState(o, e, u),
  address: StepAddress,
) -> RunState(o, e, u) {
  update_settlement(state, fn(s) {
    Settlement(..s, undone: [address, ..s.undone])
  })
}

fn record_undo_failure(
  state: RunState(o, e, u),
  failure: UndoFailure(u),
) -> RunState(o, e, u) {
  update_settlement(state, fn(s) {
    Settlement(..s, undo_failures: [failure, ..s.undo_failures])
  })
}

fn record_not_undoable(
  state: RunState(o, e, u),
  address: StepAddress,
) -> RunState(o, e, u) {
  update_settlement(state, fn(s) {
    Settlement(..s, not_undoable: [address, ..s.not_undoable])
  })
}

fn build_progress(state: RunState(o, e, u)) -> Progress {
  let phase = case state.phase {
    PhaseRunning -> Running
    PhaseSettling(..) -> Settling
    PhaseRollingBack(..) -> RollingBack
  }
  let steps =
    list.map(state.order, fn(id) {
      let assert Ok(n) = dict.get(state.nodes, id)
      let node_state = case dict.get(state.state, id) {
        Ok(NodeWaiting(_)) -> Waiting
        Ok(NodeAttempting(_, attempt, _, _, _)) -> Attempting(attempt)
        Ok(NodeCompensating(_, attempt, _, _, _)) -> Compensating(attempt)
        Ok(NodeRetryScheduled(_, next_attempt, _)) ->
          RetryScheduled(next_attempt)
        Ok(NodeReadyForRetry(next_attempt)) -> RetryScheduled(next_attempt)
        Ok(NodeDone) -> Succeeded
        Ok(NodeFailedTerminal) -> FailedStep
        Ok(NodeInterrupted) -> Interrupted
        Ok(NodeSkipped) -> Skipped
        Ok(NodeUndoing(..)) -> Undoing
        Ok(NodeUndone) -> Undone
        Ok(NodeUndoFailedTerminal) -> UndoFailedStep
        Error(_) -> Waiting
      }
      StepProgress(address: n.address, state: node_state)
    })
  Progress(run_id: state.run_id, phase: phase, steps: steps)
}

// ---------------------------------------------------------------------------
// Observations (saga/observation, via Sinal). Emission always happens after
// the corresponding state transition above: observations never control the
// run.
// ---------------------------------------------------------------------------

fn address_to_string(address: StepAddress) -> String {
  let path = case address.scope {
    [] -> address.name
    scope -> string.join(list.append(scope, [address.name]), "/")
  }
  case address.occurrence > 1 {
    True -> path <> "#" <> int.to_string(address.occurrence)
    False -> path
  }
}

fn emit_run_started(workflow_name: String, run_id: Int) -> Nil {
  let event = observation.run_started()
  sinal.emit(
    event,
    observation.RunStartMeasurements(system_time: ffi.system_time()),
    observation.RunMetadata(workflow: workflow_name, run: run_id),
  )
}

fn emit_run_stopped(
  state: RunState(o, e, u),
  outcome: Outcome(o, e, u),
) -> Nil {
  let duration = ffi.monotonic_time() - state.start_time
  let #(undone, undo_failures, interrupted) = case outcome {
    Completed(_output) -> #(0, 0, 0)
    CompletedWithUnknownEffects(_output, unknown_effects) -> #(
      0,
      0,
      list.length(unknown_effects),
    )
    Failed(_, settlement)
    | Cancelled(_, settlement)
    | Unresolved(_, _, settlement) -> #(
      list.length(settlement.undone),
      list.length(settlement.undo_failures),
      list.length(settlement.interrupted),
    )
  }
  let kind = case outcome {
    Completed(_) -> observation.OutcomeCompleted
    CompletedWithUnknownEffects(_, _) -> observation.OutcomeCompleted
    Failed(_, _) -> observation.OutcomeFailed
    Cancelled(_, _) -> observation.OutcomeCancelled
    Unresolved(_, _, _) -> observation.OutcomeUnresolved
  }
  let event = observation.run_stopped()
  sinal.emit(
    event,
    observation.RunStopMeasurements(
      duration: duration,
      undone: undone,
      undo_failures: undo_failures,
      interrupted: interrupted,
    ),
    observation.RunStopMetadata(
      workflow: state.workflow_name,
      run: state.run_id,
      outcome: kind,
    ),
  )
}

fn emit_step_started(
  state: RunState(o, e, u),
  address: StepAddress,
  attempt: Int,
) -> Nil {
  let event = observation.step_started()
  sinal.emit(
    event,
    observation.StepStartMeasurements(system_time: ffi.system_time()),
    observation.StepMetadata(
      workflow: state.workflow_name,
      run: state.run_id,
      step: address_to_string(address),
      attempt: attempt,
    ),
  )
}

fn emit_step_stopped(
  state: RunState(o, e, u),
  address: StepAddress,
  attempt: Int,
  result: observation.AttemptKind,
  duration: Int,
) -> Nil {
  let event = observation.step_stopped()
  sinal.emit(
    event,
    observation.StepStopMeasurements(duration: duration),
    observation.StepStopMetadata(
      workflow: state.workflow_name,
      run: state.run_id,
      step: address_to_string(address),
      attempt: attempt,
      result: result,
    ),
  )
}

fn emit_compensation_stopped(
  state: RunState(o, e, u),
  address: StepAddress,
  attempt: Int,
  decision: observation.DecisionKind,
  duration: Int,
) -> Nil {
  let event = observation.compensation_stopped()
  sinal.emit(
    event,
    observation.StepStopMeasurements(duration: duration),
    observation.CompensationMetadata(
      workflow: state.workflow_name,
      run: state.run_id,
      step: address_to_string(address),
      attempt: attempt,
      decision: decision,
    ),
  )
}

fn emit_undo_stopped(
  state: RunState(o, e, u),
  address: StepAddress,
  result: observation.UndoKind,
  duration: Int,
) -> Nil {
  let event = observation.undo_stopped()
  sinal.emit(
    event,
    observation.StepStopMeasurements(duration: duration),
    observation.UndoMetadata(
      workflow: state.workflow_name,
      run: state.run_id,
      step: address_to_string(address),
      result: result,
    ),
  )
}

/// Runtime-independent execution progress. Node positions refer to the checked
/// definition's construction order, never to VM-generated node identifiers.
pub type Snapshot(e, u) {
  Snapshot(
    nodes: List(SavedNode),
    journal: List(Int),
    phase: SavedPhase(e, u),
    last_failure: List(#(Int, AttemptFailure(e))),
    unknown_effects: List(UnknownEffect),
    deadline_at: Option(Int),
  )
}

pub type SavedNode {
  SavedNode(progress: StepState, values: List(String), retry_at: Int)
}

pub type SavedPhase(e, u) {
  SavedRunning
  SavedSettling(trigger: Trigger(e), settlement: Settlement(e, u))
  SavedRollback(trigger: Trigger(e), settlement: Settlement(e, u))
}

pub type Session(e, u) {
  Session(
    execution_id: String,
    snapshot: Option(Snapshot(e, u)),
    cancelled: Bool,
    save: fn(Snapshot(e, u)) -> Result(Nil, checkpoint.Failure),
    failed: fn(checkpoint.Failure) -> Nil,
  )
}

/// Used by the optional persistent runner. The same admission and lifecycle
/// functions serve local execution; ownership and storage belong to `session`.
pub fn execute_saved(
  workflow_name: String,
  max_concurrency: Int,
  deadline: Option(Int),
  step_timeout: Option(Int),
  settle_timeout: Int,
  cleanup_timeout: Int,
  build_graph: fn() ->
    #(
      Dict(Int, Node(e, u)),
      List(Int),
      Dict(Int, List(Int)),
      Store,
      fn(Store) -> ffi.RescueResult(o),
    ),
  session: Session(e, u),
  deliver: fn(Outcome(o, e, u)) -> Nil,
) -> Nil {
  let control_out = process.new_subject()
  let ready = process.new_subject()
  run(
    workflow_name,
    process.self(),
    ffi.unique_integer(),
    max_concurrency,
    deadline,
    step_timeout,
    settle_timeout,
    cleanup_timeout,
    build_graph,
    deliver,
    control_out,
    ready,
    Some(session),
  )
  let _ = process.receive(control_out, 0)
  let _ = process.receive(ready, 0)
  Nil
}

fn action_key(state: RunState(o, e, u), id: Int, action: String) -> String {
  let execution = case state.persistence {
    Some(session) -> session.execution_id
    None -> int.to_string(state.run_id)
  }
  let position = dict.get(state.positions, id) |> result.unwrap(-1)
  int.to_string(string.byte_size(execution))
  <> ":"
  <> execution
  <> ":"
  <> int.to_string(position)
  <> ":"
  <> action
}

fn checkpoint(state: RunState(o, e, u)) -> Result(Nil, checkpoint.Failure) {
  case state.blocked, state.persistence {
    Some(reason), _ -> Error(reason)
    None, None -> Ok(Nil)
    None, Some(session) -> {
      use snapshot <- result.try(
        freeze(state) |> result.map_error(checkpoint.CodecFailure),
      )
      session.save(snapshot)
    }
  }
}

fn freeze(state: RunState(o, e, u)) -> Result(Snapshot(e, u), String) {
  let progress = build_progress(state)
  use nodes <- result.try(
    list.try_map(list.zip(state.order, progress.steps), fn(pair) {
      let #(id, progress) = pair
      let assert Ok(n) = dict.get(state.nodes, id)
      let values = case progress.state {
        Succeeded | Undoing | Undone | UndoFailedStep ->
          case n.persistence {
            Some(p) -> p.freeze(state.store, action_key(state, id, "undo"))
            None -> Error("step has no persistence capability")
          }
        Attempting(_) | Compensating(_) ->
          Ok(case dict.get(state.inputs, id) {
            Ok(input) -> [input]
            Error(_) -> []
          })
        _ -> Ok([])
      }
      use values <- result.try(values)
      let retry_at = case dict.get(state.state, id) {
        Ok(NodeRetryScheduled(_, _, due)) -> due
        _ -> 0
      }
      Ok(SavedNode(progress.state, values, retry_at))
    }),
  )
  let phase = case state.phase {
    PhaseRunning -> SavedRunning
    PhaseSettling(trigger, settlement, _, _) ->
      SavedSettling(trigger, settlement)
    PhaseRollingBack(trigger, settlement) -> SavedRollback(trigger, settlement)
  }
  Ok(Snapshot(
    nodes,
    list.map(state.journal, fn(entry) {
      dict.get(state.positions, entry.node_id) |> result.unwrap(-1)
    }),
    phase,
    dict.to_list(state.last_failure)
      |> list.map(fn(pair) {
        #(dict.get(state.positions, pair.0) |> result.unwrap(-1), pair.1)
      }),
    state.unknown_effects,
    state.deadline_at,
  ))
}

fn stop_workers(state: RunState(o, e, u)) -> Nil {
  dict.values(state.state)
  |> list.each(fn(node_state) {
    case node_state {
      NodeAttempting(_, _, pid, _, _)
      | NodeCompensating(_, _, pid, _, _)
      | NodeUndoing(_, pid, _, _) -> process.kill(pid)
      _ -> Nil
    }
  })
}

fn restore(
  state: RunState(o, e, u),
  saved: Snapshot(e, u),
) -> Result(RunState(o, e, u), checkpoint.Failure) {
  use _ <- result.try(
    case list.length(saved.nodes) == list.length(state.order) {
      True -> Ok(Nil)
      False -> Error(checkpoint.InvalidState("checkpoint graph size mismatch"))
    },
  )
  use _ <- result.try(
    case
      list.count(saved.nodes, fn(n) {
        case n.progress {
          Attempting(_) | Compensating(_) -> True
          _ -> False
        }
      })
      > state.max_concurrency
    {
      True ->
        Error(checkpoint.InvalidState(
          "concurrency limit below saved in-flight count",
        ))
      False -> Ok(Nil)
    },
  )
  let pairs = list.zip(state.order, saved.nodes)
  use restored <- result.try(
    list.try_fold(pairs, #(state.store, dict.new()), fn(acc, pair) {
      let #(id, saved_node) = pair
      case saved_node.values, saved_node.progress {
        [], _ | _, Attempting(_) | _, Compensating(_) -> Ok(acc)
        values, _ -> {
          let assert Ok(n) = dict.get(state.nodes, id)
          use p <- result.try(case n.persistence {
            Some(p) -> Ok(p)
            None ->
              Error(checkpoint.InvalidState("missing persistence capability"))
          })
          use commit <- result.try(
            p.thaw(values, acc.0, action_key(state, id, "undo"))
            |> result.map_error(checkpoint.CodecFailure),
          )
          let #(store, undo) = commit(acc.0)
          Ok(#(store, dict.insert(acc.1, id, undo)))
        }
      }
    }),
  )
  let #(run_store, undos) = restored
  let statuses =
    dict.from_list(
      list.map(pairs, fn(pair) {
        let #(id, n) = pair
        let status = case n.progress {
          Waiting -> NodeWaiting(0)
          Attempting(attempt) | RetryScheduled(attempt) ->
            NodeReadyForRetry(attempt)
          Compensating(_) -> NodeInterrupted
          Succeeded -> NodeDone
          FailedStep -> NodeFailedTerminal
          Interrupted -> NodeInterrupted
          Undoing -> NodeDone
          Undone -> NodeUndone
          UndoFailedStep -> NodeUndoFailedTerminal
          Skipped -> NodeSkipped
        }
        #(id, status)
      }),
    )
  let statuses =
    dict.map_values(statuses, fn(id, status) {
      case status {
        NodeWaiting(_) -> {
          let assert Ok(n) = dict.get(state.nodes, id)
          let remaining =
            list.count(n.deps, fn(dep) {
              case dict.get(statuses, dep) {
                Ok(NodeDone) | Ok(NodeSkipped) -> False
                _ -> True
              }
            })
          NodeWaiting(remaining)
        }
        _ -> status
      }
    })
  use journal <- result.try(
    list.try_map(saved.journal, fn(index) {
      use id <- result.try(case list.first(list.drop(state.order, index)) {
        Ok(id) -> Ok(id)
        Error(_) -> Error(checkpoint.InvalidState("invalid journal position"))
      })
      let assert Ok(n) = dict.get(state.nodes, id)
      use undo <- result.try(case dict.get(undos, id) {
        Ok(undo) -> Ok(undo)
        Error(_) ->
          Error(checkpoint.InvalidState("journal has no saved output"))
      })
      Ok(JournalEntry(id, n.address, undo))
    }),
  )
  let active_undo =
    list.filter_map(pairs, fn(pair) {
      case pair.1.progress {
        Undoing -> {
          let assert Ok(n) = dict.get(state.nodes, pair.0)
          let assert Ok(undo) = dict.get(undos, pair.0)
          Ok(JournalEntry(pair.0, n.address, undo))
        }
        _ -> Error(Nil)
      }
    })
  let phase = case saved.phase {
    SavedRunning -> PhaseRunning
    SavedSettling(trigger, settlement) -> {
      let seq = ffi.unique_integer()
      PhaseSettling(
        trigger,
        settlement,
        Some(process.send_after(
          state.control,
          state.settle_timeout,
          SettleFired(seq),
        )),
        seq,
      )
    }
    SavedRollback(trigger, settlement) -> PhaseRollingBack(trigger, settlement)
  }
  use failures <- result.try(
    list.try_map(saved.last_failure, fn(pair) {
      case list.first(list.drop(state.order, pair.0)) {
        Ok(id) -> Ok(#(id, pair.1))
        Error(_) -> Error(checkpoint.InvalidState("invalid failure position"))
      }
    }),
  )
  use _ <- result.try(
    list.try_each(pairs, fn(pair) {
      case pair.1.progress, dict.get(dict.from_list(failures), pair.0) {
        Compensating(_), Error(_) ->
          Error(checkpoint.InvalidState("compensation has no saved failure"))
        _, _ -> Ok(Nil)
      }
    }),
  )
  cancel_deadline_timer(state)
  let deadline_timer =
    option.map(saved.deadline_at, fn(due) {
      process.send_after(
        state.control,
        int.max(0, due - ffi.system_time()),
        DeadlineFired(ffi.unique_integer()),
      )
    })
  let restored =
    RunState(
      ..state,
      inputs: list.filter_map(pairs, fn(pair) {
          case pair.1.progress, pair.1.values {
            Attempting(_), [input] | Compensating(_), [input] ->
              Ok(#(pair.0, input))
            _, _ -> Error(Nil)
          }
        })
        |> dict.from_list,
      store: run_store,
      state: statuses,
      journal: list.append(active_undo, journal),
      phase: phase,
      last_failure: dict.from_list(failures),
      unknown_effects: saved.unknown_effects,
      deadline_at: saved.deadline_at,
      deadline_timer: deadline_timer,
      ready: min_heap.new(),
      done_count: list.count(dict.values(statuses), fn(s) {
        case s {
          NodeDone | NodeSkipped -> True
          _ -> False
        }
      }),
      resume_nodes: set.from_list(
        list.filter_map(pairs, fn(pair) {
          case pair.1.progress {
            Attempting(_) -> Ok(pair.0)
            _ -> Error(Nil)
          }
        }),
      ),
      resume_undos: set.from_list(
        list.map(active_undo, fn(entry) { entry.node_id }),
      ),
    )
  let restored =
    list.fold(pairs, restored, fn(acc, pair) {
      let #(id, saved_node) = pair
      case saved_node.progress, acc.phase {
        Compensating(attempt), PhaseRunning
        | Compensating(attempt), PhaseSettling(..)
        -> {
          let assert Ok(n) = dict.get(acc.nodes, id)
          let assert Some(p) = n.persistence
          let assert Ok(failure) = dict.get(acc.last_failure, id)
          let context = attempt_context(acc, id, attempt, n.max_attempts)
          spawn_recovery(
            RunState(..acc, running: acc.running + 1),
            id,
            attempt,
            failure,
            p.resume_compensation(context, acc.store),
          )
        }
        Attempting(attempt), PhaseSettling(..)
        | Attempting(attempt), PhaseRunning
        -> start_attempt(acc, id, attempt)
        RetryScheduled(attempt), PhaseRunning if saved_node.retry_at > 0 -> {
          let seq = ffi.unique_integer()
          process.send_after(
            acc.control,
            int.max(0, saved_node.retry_at - ffi.system_time()),
            RetryFire(id, seq),
          )
          RunState(
            ..acc,
            state: dict.insert(
              acc.state,
              id,
              NodeRetryScheduled(seq, attempt, saved_node.retry_at),
            ),
          )
        }
        _, PhaseRunning ->
          case dict.get(acc.state, id) {
            Ok(NodeWaiting(0)) | Ok(NodeReadyForRetry(_)) -> mark_ready(acc, id)
            _ -> acc
          }
        _, _ -> acc
      }
    })
  Ok(case restored.phase {
    PhaseRunning -> restored
    PhaseSettling(..) -> maybe_finish_settling(restored)
    PhaseRollingBack(..) -> undo_next(restored)
  })
}

fn spawn_waiting(body: fn() -> Nil) -> #(Pid, Subject(Nil)) {
  let ready = process.new_subject()
  let pid =
    process.spawn(fn() {
      let permit = process.new_subject()
      process.send(ready, permit)
      process.receive_forever(permit)
      body()
    })
  #(pid, process.receive_forever(ready))
}

fn spawn_task(
  state: RunState(o, e, u),
  body: fn() -> Nil,
) -> #(Pid, List(Subject(Nil))) {
  case state.persistence {
    None -> #(process.spawn(body), [])
    Some(_) -> {
      let #(pid, permit) = spawn_waiting(body)
      #(pid, [permit])
    }
  }
}

fn attempt_context(
  state: RunState(o, e, u),
  id: Int,
  number: Int,
  maximum: Int,
) -> Attempt {
  Attempt(
    number,
    maximum - number,
    action_key(state, id, "attempt:" <> int.to_string(number)),
    case state.persistence {
      Some(Session(cancelled: True, ..)) -> True
      _ -> False
    },
    option.is_some(state.persistence),
    dict.get(state.inputs, id) |> option.from_result,
    fn(_) { Ok(Nil) },
  )
}

/// Effect timeouts begin after the admission checkpoint, when work is released.
fn start_saved_timers(state: RunState(o, e, u)) -> RunState(o, e, u) {
  list.fold(state.pending_starts, state, fn(acc, id) {
    let updated = case dict.get(acc.state, id) {
      Ok(NodeAttempting(seq, attempt, pid, _, _)) -> {
        let assert Ok(n) = dict.get(acc.nodes, id)
        let timer =
          option.map(option.or(n.timeout, acc.step_timeout), fn(ms) {
            process.send_after(acc.control, ms, StepTimeoutFired(id, seq))
          })
        Some(NodeAttempting(seq, attempt, pid, timer, ffi.monotonic_time()))
      }
      Ok(NodeCompensating(seq, attempt, pid, _, _)) ->
        Some(NodeCompensating(
          seq,
          attempt,
          pid,
          Some(process.send_after(
            acc.control,
            acc.cleanup_timeout,
            CleanupTimeoutFired(id, seq),
          )),
          ffi.monotonic_time(),
        ))
      Ok(NodeUndoing(seq, pid, _, _)) ->
        Some(NodeUndoing(
          seq,
          pid,
          Some(process.send_after(
            acc.control,
            acc.cleanup_timeout,
            CleanupTimeoutFired(id, seq),
          )),
          ffi.monotonic_time(),
        ))
      _ -> None
    }
    case updated {
      Some(updated) ->
        RunState(..acc, state: dict.insert(acc.state, id, updated))
      None -> acc
    }
  })
}
