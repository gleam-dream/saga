/// The per-run coordinator process: scheduler, journal, and settlement.
///
/// One coordinator is spawned per run. It evaluates the workflow's builder
/// fresh (see `saga.gleam`'s per-run evaluation model), checks it against
/// the define-time descriptor shape, then runs a bounded admission loop
/// over the node graph until every node is done or the run has a terminal
/// cause. On any terminal failure it walks the journal in reverse
/// completion order, undoing one entry at a time, and retains every undo
/// failure.
///
/// Increment 1 scope: bounded concurrency, retry/compensation, undo, and
/// independent runs. Deadlines, step timeouts, cancellation settlement of
/// active siblings, and Sinal observations are increment 2 — `Config`'s
/// `deadline`/`settle_timeout`/`cleanup_timeout` fields exist for API
/// stability but are not yet enforced by this coordinator.
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import saga/internal/ffi
import saga/internal/node.{
  type Attempt, type AttemptFailure, type AttemptResult, type ErasedRecovery,
  type Node, type StepAddress, Attempt, AttemptFailed, AttemptSucceeded, Crashed,
  EAbort, EAbortCleanup, EContinue, EHold, ERetry, ERetryAfter, Returned,
  TimedOut,
}

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
}

pub type CompensationFailure(u) {
  CleanupFailed(step: StepAddress, error: u)
  CompensationCrashed(step: StepAddress, crash: node.Crash)
}

pub type Cause(e) {
  StepFailed(step: StepAddress, error: e)
  StepCrashed(step: StepAddress, crash: node.Crash)
  StepTimedOut(step: StepAddress)
  RetryLimitReached(step: StepAddress, last: AttemptFailure(e))
  OutputCrashed(crash: node.Crash)
  DefinitionChanged
}

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

fn empty_settlement() -> Settlement(e, u) {
  Settlement(
    undone: [],
    undo_failures: [],
    not_undoable: [],
    held: [],
    compensation_failures: [],
    sibling_failures: [],
  )
}

pub type Outcome(o, e, u) {
  Completed(output: o)
  Failed(cause: Cause(e), settlement: Settlement(e, u))
  Unresolved(step: StepAddress, evidence: e, settlement: Settlement(e, u))
}

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

pub type Phase {
  Running
  RollingBack
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

/// State for one node during the run.
type NodeRunState {
  NodeWaiting(remaining_deps: Int)
  NodeAttempting(seq: Int, attempt: Int, pid: Pid)
  NodeCompensating(seq: Int, attempt: Int, pid: Pid)
  NodeRetryScheduled(seq: Int, attempt: Int)
  NodeDone
  NodeFailedTerminal
  NodeSkipped
  NodeUndoing(seq: Int, pid: Pid)
  NodeUndone
  NodeUndoFailedTerminal
}

type RunState(o, e, u) {
  RunState(
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
    journal: List(JournalEntry(u)),
    phase: RunPhase(e, u),
  )
}

type RunPhase(e, u) {
  PhaseRunning
  PhaseRollingBack(primary: PrimaryOutcome(e), settlement: Settlement(e, u))
}

type PrimaryOutcome(e) {
  PrimaryFailure(cause: Cause(e))
  PrimaryUnresolved(step: StepAddress, evidence: e)
}

/// Spawns a coordinator for one run and returns once it is alive, with its
/// pid, the subject the outcome will be sent to, and the run's id.
/// `build_graph` evaluates the workflow's builder fresh, inside the
/// coordinator, and must return `Error(Nil)` if the resulting graph doesn't
/// match the define-time shape (nondeterministic builder).
pub fn start(
  max_concurrency max_concurrency: Int,
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
        run_id,
        max_concurrency,
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
  run_id: Int,
  max_concurrency: Int,
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
      let initial =
        RunState(
          run_id: run_id,
          control: control,
          nodes: nodes,
          order: order,
          dependents: dependents,
          state: node_state,
          last_failure: dict.new(),
          running: 0,
          max_concurrency: max_concurrency,
          journal: [],
          phase: PhaseRunning,
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
    Some(outcome) -> process.send(result_subject, outcome)
    None ->
      case all_nodes_done(state) {
        True ->
          case fetch_output() {
            ffi.Rescued(output) ->
              process.send(result_subject, Completed(output))
            ffi.Raised(class, reason) ->
              begin_rollback(
                state,
                PrimaryFailure(OutputCrashed(node.Crash(class, reason))),
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
          let message = process.selector_receive_forever(selector)
          let next = handle_control(state, message)
          loop(next, fetch_output, result_subject)
        }
      }
  }
}

fn run_finished(state: RunState(o, e, u)) -> Option(Outcome(o, e, u)) {
  case state.phase {
    PhaseRunning -> None
    PhaseRollingBack(primary, settlement) ->
      case rollback_complete(state) {
        False -> None
        True ->
          Some(case primary {
            PrimaryFailure(cause) -> Failed(cause, settlement)
            PrimaryUnresolved(step, evidence) ->
              Unresolved(step, evidence, settlement)
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
    PhaseRollingBack(..) -> False
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
    PhaseRollingBack(..) -> state
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
  RunState(
    ..state,
    running: state.running + 1,
    state: dict.insert(
      state.state,
      node_id,
      NodeAttempting(seq, attempt_number, pid),
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
    CancelRequest -> state
    TaskExited(pid, reason) -> handle_task_exited(state, pid, reason)
  }
}

/// Finds whichever node currently has `pid` attempting or compensating and
/// routes it through the same crash path a normal task-body exception
/// would take, via a fresh `TaskCrashed`. A task that already reported its
/// result before dying (an ordinary `Normal` exit after `send`) finds no
/// matching node here — its node was already transitioned by
/// `handle_attempt_done`/`handle_recovery_done`, so this is a no-op.
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
      NodeAttempting(seq, _attempt, task_pid) if task_pid == pid ->
        Ok(#(node_id, seq))
      NodeCompensating(seq, _attempt, task_pid) if task_pid == pid ->
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

fn handle_attempt_done(
  state: RunState(o, e, u),
  node_id: Int,
  seq: Int,
  result: AttemptResult(e, u),
) -> RunState(o, e, u) {
  case current_attempt_seq(state, node_id) == Some(seq) {
    False -> state
    True ->
      case result {
        AttemptSucceeded(commit) -> commit_success(state, node_id, commit)
        AttemptFailed(failure, recover) ->
          case recover {
            None -> fail_terminal(state, node_id, failure)
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

fn commit_success(
  state: RunState(o, e, u),
  node_id: Int,
  commit: fn() -> Option(fn() -> Result(Nil, u)),
) -> RunState(o, e, u) {
  let assert Ok(n) = dict.get(state.nodes, node_id)
  let undo = commit()
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
  // While running, a success may unblock new admissions. While rolling
  // back (a sibling finishing after the primary failure), this success
  // still has to be undone like any other journal entry, but `admit`
  // itself is a no-op during rollback — `undo_next` is what actually
  // drains the journal, and it must be nudged here or this entry (and any
  // undo work following it) would never start.
  case state.phase {
    PhaseRunning -> admit(state)
    PhaseRollingBack(..) -> undo_next(state)
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
  let assert Ok(NodeAttempting(_seq, attempt_number, _pid)) =
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
  RunState(
    ..state,
    state: dict.insert(
      state.state,
      node_id,
      NodeCompensating(seq, attempt_number, pid),
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
      let assert Ok(NodeCompensating(_seq, attempt_number, _pid)) =
        dict.get(state.state, node_id)
      let assert Ok(n) = dict.get(state.nodes, node_id)
      case recovery {
        ERetry ->
          case attempt_number < n.max_attempts {
            True -> {
              let state = RunState(..state, running: state.running - 1)
              retry_now(state, node_id, attempt_number + 1)
            }
            False -> fail_terminal_retry_limit(state, node_id)
          }
        ERetryAfter(ms) ->
          case attempt_number < n.max_attempts {
            True -> {
              let state = RunState(..state, running: state.running - 1)
              retry_after(state, node_id, attempt_number + 1, ms)
            }
            False -> fail_terminal_retry_limit(state, node_id)
          }
        EContinue(commit) -> commit_success(state, node_id, commit)
        EAbort(error) -> fail_terminal(state, node_id, node.Returned(error))
        EAbortCleanup(error, cleanup_error) -> {
          let cleanup_failure =
            CleanupFailed(node_address(state, node_id), cleanup_error)
          state
          |> fail_terminal(node_id, node.Returned(error))
          |> record_compensation_failure(cleanup_failure)
        }
        EHold(evidence) ->
          begin_unresolved(
            RunState(..state, running: state.running - 1),
            node_id,
            evidence,
          )
      }
    }
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
            None -> fail_terminal(state, node_id, Crashed(crash))
            Some(prepare) ->
              start_crash_recovery(state, node_id, prepare, Crashed(crash))
          }
        }
        Ok(NodeCompensating(..)) -> {
          let compensation_failure =
            CompensationCrashed(node_address(state, node_id), crash)
          state
          |> fail_terminal(node_id, Crashed(crash))
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
  let assert Ok(NodeAttempting(_seq, attempt_number, _pid)) =
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

fn node_address(state: RunState(o, e, u), node_id: Int) -> StepAddress {
  let assert Ok(n) = dict.get(state.nodes, node_id)
  n.address
}

fn record_compensation_failure(
  state: RunState(o, e, u),
  failure: CompensationFailure(u),
) -> RunState(o, e, u) {
  case state.phase {
    // Callers always transition to PhaseRollingBack (via fail_terminal)
    // before recording, so this is unreachable in practice; kept total.
    PhaseRunning -> state
    PhaseRollingBack(primary, settlement) ->
      RunState(
        ..state,
        phase: PhaseRollingBack(
          primary,
          Settlement(
            ..settlement,
            compensation_failures: list.append(
              settlement.compensation_failures,
              [failure],
            ),
          ),
        ),
      )
  }
}

/// Marks `node_id` terminally failed and starts rollback. Always decrements
/// `running` by one for the task that just finished (whichever kind of
/// task it was — an attempt or a compensation decision); callers must not
/// also decrement it.
fn fail_terminal(
  state: RunState(o, e, u),
  node_id: Int,
  failure: AttemptFailure(e),
) -> RunState(o, e, u) {
  let address = node_address(state, node_id)
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
  begin_rollback(state, PrimaryFailure(cause))
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
  begin_rollback(state, PrimaryFailure(cause))
}

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
  begin_rollback(state, PrimaryUnresolved(address, evidence))
}

fn begin_rollback(
  state: RunState(o, e, u),
  primary: PrimaryOutcome(e),
) -> RunState(o, e, u) {
  case state.phase {
    PhaseRollingBack(..) -> state
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
      let state =
        RunState(..state, phase: PhaseRollingBack(primary, empty_settlement()))
      case primary {
        PrimaryUnresolved(..) -> hold_journal(state)
        PrimaryFailure(..) -> undo_next(state)
      }
    }
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
  case state.phase {
    PhaseRunning -> state
    PhaseRollingBack(primary, settlement) ->
      RunState(
        ..state,
        phase: PhaseRollingBack(
          primary,
          Settlement(
            ..settlement,
            held: list.append(settlement.held, [
              address,
            ]),
          ),
        ),
      )
  }
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
          RunState(
            ..state,
            journal: rest,
            state: dict.insert(state.state, node_id, NodeUndoing(seq, pid)),
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
    Ok(NodeUndoing(current_seq, _pid)) if current_seq == seq -> {
      let address = node_address(state, node_id)
      let new_state = case outcome {
        UndoOk -> NodeUndone
        UndoErr(_) -> NodeUndoFailedTerminal
        UndoCrash(_) -> NodeUndoFailedTerminal
      }
      let state =
        RunState(..state, state: dict.insert(state.state, node_id, new_state))
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

fn record_undone(
  state: RunState(o, e, u),
  address: StepAddress,
) -> RunState(o, e, u) {
  case state.phase {
    PhaseRunning -> state
    PhaseRollingBack(primary, settlement) ->
      RunState(
        ..state,
        phase: PhaseRollingBack(
          primary,
          Settlement(
            ..settlement,
            undone: list.append(settlement.undone, [
              address,
            ]),
          ),
        ),
      )
  }
}

fn record_undo_failure(
  state: RunState(o, e, u),
  failure: UndoFailure(u),
) -> RunState(o, e, u) {
  case state.phase {
    PhaseRunning -> state
    PhaseRollingBack(primary, settlement) ->
      RunState(
        ..state,
        phase: PhaseRollingBack(
          primary,
          Settlement(
            ..settlement,
            undo_failures: list.append(settlement.undo_failures, [failure]),
          ),
        ),
      )
  }
}

fn record_not_undoable(
  state: RunState(o, e, u),
  address: StepAddress,
) -> RunState(o, e, u) {
  case state.phase {
    PhaseRunning -> state
    PhaseRollingBack(primary, settlement) ->
      RunState(
        ..state,
        phase: PhaseRollingBack(
          primary,
          Settlement(
            ..settlement,
            not_undoable: list.append(settlement.not_undoable, [address]),
          ),
        ),
      )
  }
}

fn build_progress(state: RunState(o, e, u)) -> Progress {
  let phase = case state.phase {
    PhaseRunning -> Running
    PhaseRollingBack(..) -> RollingBack
  }
  let steps =
    list.map(state.order, fn(id) {
      let assert Ok(n) = dict.get(state.nodes, id)
      let node_state = case dict.get(state.state, id) {
        Ok(NodeWaiting(_)) -> Waiting
        Ok(NodeAttempting(_, attempt, _)) -> Attempting(attempt)
        Ok(NodeCompensating(_, attempt, _)) -> Compensating(attempt)
        Ok(NodeRetryScheduled(_, next_attempt)) -> RetryScheduled(next_attempt)
        Ok(NodeDone) -> Succeeded
        Ok(NodeFailedTerminal) -> FailedStep
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
