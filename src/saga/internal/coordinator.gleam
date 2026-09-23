/// The per-run coordinator process: scheduler, journal, settlement,
/// deadlines, timeouts, and cancellation.
///
/// One coordinator is spawned per run. It evaluates the workflow's builder
/// fresh (see `saga.gleam`'s per-run evaluation model), checks it against
/// the define-time descriptor shape, then runs a bounded admission loop
/// over the node graph until every node is done or the run has a terminal
/// cause.
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
/// run's outcome (`sinal.emit`'s own dispatch isolates handler failure, and
/// this module ignores `emit`'s `Result`).
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Monitor, type Pid, type Subject, type Timer}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
import saga/internal/ffi
import saga/internal/node.{
  type Attempt, type AttemptFailure, type AttemptResult, type ErasedRecovery,
  type Node, type StepAddress, Attempt, AttemptFailed, AttemptSucceeded, Crashed,
  EAbort, EAbortCleanup, EContinue, EHold, ERetry, ERetryAfter, Returned,
  TimedOut,
}
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
  OutputCrashed(crash: node.Crash)
  DeadlineExceeded
  DefinitionChanged
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
  )
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
  )
}

pub type Outcome(o, e, u) {
  Completed(output: o)
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
  Finishing
}

pub type Progress {
  Progress(run_id: Int, phase: Phase, steps: List(StepProgress))
}

/// Messages the control subject accepts. Owned by the coordinator process;
/// `saga/execution` only ever talks to it through these.
pub type Control(o, e, u) {
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
}

/// State for one node during the run. `NodeAttempting`/`NodeCompensating`/
/// `NodeUndoing` carry an optional timer for whichever bound currently
/// applies (a step's own `timeout`, or the run's `cleanup_timeout`), so it
/// can be cancelled the moment the task finishes on its own.
type NodeRunState {
  NodeWaiting(remaining_deps: Int)
  NodeAttempting(seq: Int, attempt: Int, pid: Pid, timer: Option(Timer))
  NodeCompensating(seq: Int, attempt: Int, pid: Pid, timer: Option(Timer))
  NodeRetryScheduled(seq: Int, attempt: Int)
  NodeDone
  NodeFailedTerminal
  NodeSkipped
  NodeUndoing(seq: Int, pid: Pid, timer: Option(Timer))
  NodeUndone
  NodeUndoFailedTerminal
  /// A task was killed (settle deadline, or an unrelated fatal condition)
  /// while attempting or compensating: its effect is unknown and is never
  /// journaled or undone.
  NodeInterrupted
}

type RunState(o, e, u) {
  RunState(
    workflow_name: String,
    run_id: Int,
    control: Subject(Control(o, e, u)),
    nodes: Dict(Int, Node(e, u)),
    order: List(Int),
    dependents: Dict(Int, List(Int)),
    state: Dict(Int, NodeRunState),
    // The failure that triggered the currently in-flight (or most recently
    // finished) recovery decision for a node, kept so `RetryLimitReached`
    // can report which failure exhausted the budget.
    last_failure: Dict(Int, AttemptFailure(e)),
    running: Int,
    max_concurrency: Int,
    deadline: Option(Int),
    settle_timeout: Int,
    cleanup_timeout: Int,
    deadline_timer: Option(Timer),
    owner_monitor: Monitor,
    journal: List(JournalEntry(u)),
    phase: RunPhase(e, u),
    start_time: Int,
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

type Trigger(e) {
  TriggerFailure(cause: Cause(e))
  TriggerUnresolved(step: StepAddress, evidence: e)
  TriggerCancel(reason: CancelReason)
}

/// Spawns a coordinator for one run and returns once it is alive, with its
/// pid, the subject the outcome will be sent to, and the run's id.
/// `build_graph` evaluates the workflow's builder fresh, inside the
/// coordinator, and must return `Error(Nil)` if the resulting graph doesn't
/// match the define-time shape (nondeterministic builder). `owner` is
/// monitored: its exit is treated as a cancellation with `OwnerExited`.
pub fn start(
  workflow_name workflow_name: String,
  owner owner: Pid,
  max_concurrency max_concurrency: Int,
  deadline deadline: Option(Int),
  settle_timeout settle_timeout: Int,
  cleanup_timeout cleanup_timeout: Int,
  build_graph build_graph: fn() ->
    Result(
      #(Dict(Int, Node(e, u)), List(Int), fn() -> ffi.RescueResult(o)),
      Nil,
    ),
  result_subject result_subject: Subject(Outcome(o, e, u)),
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
        settle_timeout,
        cleanup_timeout,
        build_graph,
        result_subject,
        control_subject_out,
        ready,
      )
    })
  let _ = process.receive(ready, 5000)
  #(pid, run_id)
}

fn run(
  workflow_name: String,
  owner: Pid,
  run_id: Int,
  max_concurrency: Int,
  deadline: Option(Int),
  settle_timeout: Int,
  cleanup_timeout: Int,
  build_graph: fn() ->
    Result(
      #(Dict(Int, Node(e, u)), List(Int), fn() -> ffi.RescueResult(o)),
      Nil,
    ),
  result_subject: Subject(Outcome(o, e, u)),
  control_subject_out: Subject(Subject(Control(o, e, u))),
  ready: Subject(Nil),
) -> Nil {
  process.trap_exits(True)
  let control = process.new_subject()
  let owner_monitor = process.monitor(owner)
  process.send(control_subject_out, control)
  process.send(ready, Nil)
  case build_graph() {
    Error(Nil) -> {
      process.send(
        result_subject,
        Failed(DefinitionChanged, empty_settlement()),
      )
      Nil
    }
    Ok(#(nodes, order, fetch_output)) -> {
      let dependents = build_dependents(nodes)
      let node_state =
        dict.map_values(nodes, fn(_id, n) { NodeWaiting(list.length(n.deps)) })
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
          workflow_name: workflow_name,
          run_id: run_id,
          control: control,
          nodes: nodes,
          order: order,
          dependents: dependents,
          state: node_state,
          last_failure: dict.new(),
          running: 0,
          max_concurrency: max_concurrency,
          deadline: deadline,
          settle_timeout: settle_timeout,
          cleanup_timeout: cleanup_timeout,
          deadline_timer: deadline_timer,
          owner_monitor: owner_monitor,
          journal: [],
          phase: PhaseRunning,
          start_time: start_time,
        )
      let admitted = admit(initial)
      loop(admitted, fetch_output, result_subject)
    }
  }
}

fn build_dependents(nodes: Dict(Int, Node(e, u))) -> Dict(Int, List(Int)) {
  dict.fold(nodes, dict.new(), fn(acc, id, n) {
    list.fold(n.deps, acc, fn(acc2, dep_id) {
      dict.upsert(acc2, dep_id, fn(existing) {
        case existing {
          Some(ids) -> list.append(ids, [id])
          None -> [id]
        }
      })
    })
  })
}

fn loop(
  state: RunState(o, e, u),
  fetch_output: fn() -> ffi.RescueResult(o),
  result_subject: Subject(Outcome(o, e, u)),
) -> Nil {
  case run_finished(state) {
    Some(outcome) -> {
      cancel_deadline_timer(state)
      emit_run_stopped(state, outcome)
      process.send(result_subject, outcome)
      Nil
    }
    None ->
      case all_nodes_done(state) {
        True ->
          case fetch_output() {
            ffi.Rescued(output) -> {
              cancel_deadline_timer(state)
              emit_run_stopped(state, Completed(output))
              process.send(result_subject, Completed(output))
            }
            ffi.Raised(class, reason) ->
              begin_settling(
                state,
                TriggerFailure(OutputCrashed(node.Crash(class, reason))),
              )
              |> loop(fetch_output, result_subject)
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
          loop(next, fetch_output, result_subject)
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
        True ->
          Some(case trigger {
            TriggerFailure(cause) -> Failed(cause, settlement)
            TriggerUnresolved(step, evidence) ->
              Unresolved(step, evidence, settlement)
            TriggerCancel(reason) -> Cancelled(reason, settlement)
          })
      }
  }
}

fn rollback_complete(state: RunState(o, e, u)) -> Bool {
  state.journal == []
  && state.running == 0
  && list.all(dict.values(state.state), fn(s) {
    case s {
      NodeAttempting(..) | NodeCompensating(..) | NodeUndoing(..) -> False
      _ -> True
    }
  })
}

fn all_nodes_done(state: RunState(o, e, u)) -> Bool {
  case state.phase {
    PhaseSettling(..) | PhaseRollingBack(..) -> False
    PhaseRunning ->
      list.all(dict.values(state.state), fn(s) {
        case s {
          NodeDone -> True
          _ -> False
        }
      })
  }
}

// ---------------------------------------------------------------------------
// Admission
// ---------------------------------------------------------------------------

fn admit(state: RunState(o, e, u)) -> RunState(o, e, u) {
  case state.phase {
    PhaseSettling(..) | PhaseRollingBack(..) -> state
    PhaseRunning ->
      case state.running >= state.max_concurrency {
        True -> state
        False -> {
          let ready_ids =
            list.filter(state.order, fn(id) {
              case dict.get(state.state, id) {
                Ok(NodeWaiting(0)) -> True
                _ -> False
              }
            })
          case ready_ids {
            [] -> state
            [next, ..] -> admit(start_attempt(state, next, 1))
          }
        }
      }
  }
}

fn start_attempt(
  state: RunState(o, e, u),
  node_id: Int,
  attempt_number: Int,
) -> RunState(o, e, u) {
  let assert Ok(n) = dict.get(state.nodes, node_id)
  let seq = ffi.unique_integer()
  let attempt =
    Attempt(number: attempt_number, remaining: n.max_attempts - attempt_number)
  let body = n.prepare_attempt(attempt)
  let control = state.control
  let pid =
    process.spawn(fn() {
      case ffi.rescue(body) {
        ffi.Rescued(result) ->
          process.send(control, AttemptDone(node_id, seq, result))
        ffi.Raised(_class, reason) ->
          process.send(
            control,
            TaskCrashed(node_id, seq, node.Crash(ffi.ErrorClass, reason)),
          )
      }
    })
  emit_step_started(state, n.address, attempt_number)
  let timer = case n.timeout {
    None -> None
    Some(ms) ->
      Some(process.send_after(control, ms, StepTimeoutFired(node_id, seq)))
  }
  RunState(
    ..state,
    running: state.running + 1,
    state: dict.insert(
      state.state,
      node_id,
      NodeAttempting(seq, attempt_number, pid, timer),
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
      NodeAttempting(seq, _attempt, task_pid, _timer) if task_pid == pid ->
        Ok(#(node_id, seq))
      NodeCompensating(seq, _attempt, task_pid, _timer) if task_pid == pid ->
        Ok(#(node_id, seq))
      _ -> Error(Nil)
    }
  })
  |> option.from_result
}

fn exit_reason_to_string(reason: process.ExitReason) -> String {
  case reason {
    process.Normal -> "normal"
    process.Killed -> "killed"
    process.Abnormal(_) -> "abnormal"
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
    Ok(NodeAttempting(_, _, _, Some(timer)))
    | Ok(NodeCompensating(_, _, _, Some(timer)))
    | Ok(NodeUndoing(_, _, Some(timer))) -> {
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
  commit: fn() -> Option(fn() -> Result(Nil, u)),
  step_kind: observation.AttemptKind,
) -> RunState(o, e, u) {
  let assert Ok(n) = dict.get(state.nodes, node_id)
  let attempt_number = attempt_number_for(state, node_id)
  let undo = commit()
  emit_step_stopped(state, n.address, attempt_number, step_kind)
  let entry = JournalEntry(node_id: node_id, address: n.address, undo: undo)
  let dependent_ids = dict.get(state.dependents, node_id) |> option_unwrap_list
  let state =
    RunState(
      ..state,
      running: state.running - 1,
      journal: [entry, ..state.journal],
      state: dict.insert(state.state, node_id, NodeDone),
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
      Ok(NodeWaiting(remaining)) ->
        RunState(
          ..acc,
          state: dict.insert(acc.state, id, NodeWaiting(remaining - 1)),
        )
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
  let assert Ok(NodeAttempting(_seq, attempt_number, _pid, _timer)) =
    dict.get(state.state, node_id)
  let assert Ok(n) = dict.get(state.nodes, node_id)
  let attempt =
    Attempt(number: attempt_number, remaining: n.max_attempts - attempt_number)
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
  let pid =
    process.spawn(fn() {
      case ffi.rescue(body) {
        ffi.Rescued(recovery) ->
          process.send(control, RecoveryDone(node_id, seq, recovery))
        ffi.Raised(_class, reason) ->
          process.send(
            control,
            TaskCrashed(node_id, seq, node.Crash(ffi.ErrorClass, reason)),
          )
      }
    })
  let timer =
    Some(process.send_after(
      control,
      state.cleanup_timeout,
      CleanupTimeoutFired(node_id, seq),
    ))
  RunState(
    ..state,
    state: dict.insert(
      state.state,
      node_id,
      NodeCompensating(seq, attempt_number, pid, timer),
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
      let assert Ok(NodeCompensating(_seq, attempt_number, _pid, _timer)) =
        dict.get(state.state, node_id)
      let assert Ok(n) = dict.get(state.nodes, node_id)
      emit_compensation_stopped(
        state,
        n.address,
        attempt_number,
        decision_kind(recovery),
      )
      let settling_or_rolling_back = case state.phase {
        PhaseRunning -> False
        PhaseSettling(..) | PhaseRollingBack(..) -> True
      }
      case recovery, settling_or_rolling_back {
        // Retry/RetryAfter decisions are not honored once settling has
        // begun (§3.3): the run is already stopping admission, so a fresh
        // attempt would race the settle window. The exhausted-vs-not
        // distinction stops mattering once settling is underway; the node
        // is simply not retried, and this fact is recorded as a sibling
        // failure using whatever the last observed failure was.
        ERetry, True | ERetryAfter(_), True ->
          fail_terminal_as_sibling(state, node_id)
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
    EHold(_) -> observation.DecisionHold
  }
}

fn retry_now(
  state: RunState(o, e, u),
  node_id: Int,
  next_attempt: Int,
) -> RunState(o, e, u) {
  start_attempt(state, node_id, next_attempt) |> admit
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
  RunState(
    ..state,
    state: dict.insert(
      state.state,
      node_id,
      NodeRetryScheduled(seq, next_attempt),
    ),
  )
}

fn handle_retry_fire(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
) -> RunState(o, e, u) {
  case dict.get(state.state, node_id) {
    Ok(NodeRetryScheduled(current_seq, next_attempt)) if current_seq == seq ->
      start_attempt(state, node_id, next_attempt) |> admit
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
        Ok(NodeAttempting(..)) -> {
          let assert Ok(n) = dict.get(state.nodes, node_id)
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
        Ok(NodeCompensating(..)) -> {
          let compensation_failure =
            CompensationCrashed(node_address(state, node_id), crash)
          state
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
  prepare: fn(AttemptFailure(e), Attempt) -> fn() -> ErasedRecovery(e, u),
  failure: AttemptFailure(e),
) -> RunState(o, e, u) {
  let assert Ok(NodeAttempting(_seq, attempt_number, _pid, _timer)) =
    dict.get(state.state, node_id)
  let assert Ok(n) = dict.get(state.nodes, node_id)
  let attempt =
    Attempt(number: attempt_number, remaining: n.max_attempts - attempt_number)
  spawn_recovery(
    state,
    node_id,
    attempt_number,
    failure,
    prepare(failure, attempt),
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
/// for a step's own configured timeout.
fn handle_step_timeout_fired(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
) -> RunState(o, e, u) {
  case current_attempt_seq(state, node_id) == Some(seq) {
    False -> state
    True ->
      case dict.get(state.state, node_id) {
        Ok(NodeAttempting(_seq, attempt_number, pid, _timer)) -> {
          process.kill(pid)
          let assert Ok(n) = dict.get(state.nodes, node_id)
          let state =
            RunState(
              ..state,
              state: dict.insert(
                state.state,
                node_id,
                // Bump to a fresh (unreachable) seq so a stray late message
                // from the killed task cannot be mistaken for this attempt.
                NodeAttempting(ffi.unique_integer(), attempt_number, pid, None),
              ),
            )
          case n.prepare_crash_recovery {
            None ->
              fail_terminal(
                state,
                node_id,
                TimedOut,
                observation.AttemptTimedOut,
              )
            Some(prepare) ->
              start_crash_recovery(state, node_id, prepare, TimedOut)
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
        Ok(NodeCompensating(_seq, _attempt, pid, _timer)) -> {
          process.kill(pid)
          let compensation_failure =
            CompensationTimedOut(node_address(state, node_id))
          state
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
  emit_step_stopped(state, address, attempt_number, step_kind)
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

fn fail_terminal_as_sibling(
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
        Settlement(..s, held: list.append(s.held, [address]))
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
            NodeAttempting(_, _, pid, _) -> Ok(#(node_id, pid))
            NodeCompensating(_, _, pid, _) -> Ok(#(node_id, pid))
            _ -> Error(Nil)
          }
        })
      let state =
        list.fold(in_flight, state, fn(acc, entry) {
          let #(node_id, pid) = entry
          process.kill(pid)
          let address = node_address(acc, node_id)
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
  update_settlement(state, fn(s) {
    Settlement(..s, held: list.append(s.held, [address]))
  })
}

fn undo_next(state: RunState(o, e, u)) -> RunState(o, e, u) {
  case has_active_undo(state) {
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
          let pid =
            process.spawn(fn() {
              case ffi.rescue(undo_fn) {
                ffi.Rescued(Ok(Nil)) ->
                  process.send(control, UndoDone(node_id, seq, UndoOk))
                ffi.Rescued(Error(error)) ->
                  process.send(control, UndoDone(node_id, seq, UndoErr(error)))
                ffi.Raised(_class, reason) ->
                  process.send(
                    control,
                    UndoDone(
                      node_id,
                      seq,
                      UndoCrash(node.Crash(ffi.ErrorClass, reason)),
                    ),
                  )
              }
            })
          let timer =
            Some(process.send_after(
              control,
              state.cleanup_timeout,
              CleanupTimeoutFired(node_id, seq),
            ))
          RunState(
            ..state,
            journal: rest,
            state: dict.insert(
              state.state,
              node_id,
              NodeUndoing(seq, pid, timer),
            ),
          )
        }
      }
  }
}

fn has_active_undo(state: RunState(o, e, u)) -> Bool {
  list.any(dict.values(state.state), fn(s) {
    case s {
      NodeUndoing(..) -> True
      _ -> False
    }
  })
}

fn handle_undo_done(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
  outcome: UndoOutcome(u),
) -> RunState(o, e, u) {
  case dict.get(state.state, node_id) {
    Ok(NodeUndoing(current_seq, _pid, timer)) if current_seq == seq -> {
      case timer {
        None -> Nil
        Some(t) -> {
          let _ = process.cancel_timer(t)
          Nil
        }
      }
      let address = node_address(state, node_id)
      let attempt_number = 1
      let new_state = case outcome {
        UndoOk -> NodeUndone
        UndoErr(_) -> NodeUndoFailedTerminal
        UndoCrash(_) -> NodeUndoFailedTerminal
      }
      emit_undo_stopped(state, address, undo_outcome_kind(outcome))
      let state =
        RunState(..state, state: dict.insert(state.state, node_id, new_state))
      let _ = attempt_number
      let state = case outcome {
        UndoOk -> record_undone(state, address)
        UndoErr(error) -> record_undo_failure(state, UndoFailed(address, error))
        UndoCrash(crash) ->
          record_undo_failure(state, UndoCrashed(address, crash))
      }
      undo_next(state)
    }
    _ -> state
  }
}

fn undo_outcome_kind(outcome: UndoOutcome(u)) -> observation.UndoKind {
  case outcome {
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
    Ok(NodeUndoing(current_seq, pid, _timer)) if current_seq == seq -> {
      process.kill(pid)
      let address = node_address(state, node_id)
      emit_undo_stopped(state, address, observation.UndoTimedOutKind)
      let state =
        RunState(
          ..state,
          state: dict.insert(state.state, node_id, NodeUndoFailedTerminal),
        )
      let state = record_undo_failure(state, UndoTimedOut(address))
      undo_next(state)
    }
    _ -> state
  }
}

fn record_undone(
  state: RunState(o, e, u),
  address: StepAddress,
) -> RunState(o, e, u) {
  update_settlement(state, fn(s) {
    Settlement(..s, undone: list.append(s.undone, [address]))
  })
}

fn record_undo_failure(
  state: RunState(o, e, u),
  failure: UndoFailure(u),
) -> RunState(o, e, u) {
  update_settlement(state, fn(s) {
    Settlement(..s, undo_failures: list.append(s.undo_failures, [failure]))
  })
}

fn record_not_undoable(
  state: RunState(o, e, u),
  address: StepAddress,
) -> RunState(o, e, u) {
  update_settlement(state, fn(s) {
    Settlement(..s, not_undoable: list.append(s.not_undoable, [address]))
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
        Ok(NodeAttempting(_, attempt, _, _)) -> Attempting(attempt)
        Ok(NodeCompensating(_, attempt, _, _)) -> Compensating(attempt)
        Ok(NodeRetryScheduled(_, next_attempt)) -> RetryScheduled(next_attempt)
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
// the corresponding state transition above and its `Result` is discarded:
// observations never control the run.
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
  let _ =
    sinal.emit(
      event,
      observation.RunStartMeasurements(system_time: ffi.system_time()),
      observation.RunMetadata(workflow: workflow_name, run: run_id),
    )
  Nil
}

fn emit_run_stopped(
  state: RunState(o, e, u),
  outcome: Outcome(o, e, u),
) -> Nil {
  let duration = ffi.monotonic_time() - state.start_time
  let #(undone, undo_failures, interrupted) = case outcome {
    Completed(_) -> #(0, 0, 0)
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
    Failed(_, _) -> observation.OutcomeFailed
    Cancelled(_, _) -> observation.OutcomeCancelled
    Unresolved(_, _, _) -> observation.OutcomeUnresolved
  }
  let event = observation.run_stopped()
  let _ =
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
  Nil
}

fn emit_step_started(
  state: RunState(o, e, u),
  address: StepAddress,
  attempt: Int,
) -> Nil {
  let event = observation.step_started()
  let _ =
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
  Nil
}

fn emit_step_stopped(
  state: RunState(o, e, u),
  address: StepAddress,
  attempt: Int,
  result: observation.AttemptKind,
) -> Nil {
  let event = observation.step_stopped()
  let _ =
    sinal.emit(
      event,
      observation.StepStopMeasurements(duration: 0),
      observation.StepStopMetadata(
        workflow: state.workflow_name,
        run: state.run_id,
        step: address_to_string(address),
        attempt: attempt,
        result: result,
      ),
    )
  Nil
}

fn emit_compensation_stopped(
  state: RunState(o, e, u),
  address: StepAddress,
  attempt: Int,
  decision: observation.DecisionKind,
) -> Nil {
  let event = observation.compensation_stopped()
  let _ =
    sinal.emit(
      event,
      observation.StepStopMeasurements(duration: 0),
      observation.CompensationMetadata(
        workflow: state.workflow_name,
        run: state.run_id,
        step: address_to_string(address),
        attempt: attempt,
        decision: decision,
      ),
    )
  Nil
}

fn emit_undo_stopped(
  state: RunState(o, e, u),
  address: StepAddress,
  result: observation.UndoKind,
) -> Nil {
  let event = observation.undo_stopped()
  let _ =
    sinal.emit(
      event,
      observation.StepStopMeasurements(duration: 0),
      observation.UndoMetadata(
        workflow: state.workflow_name,
        run: state.run_id,
        step: address_to_string(address),
        result: result,
      ),
    )
  Nil
}
