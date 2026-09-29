/// Run lifecycle for a `saga.Workflow`: config and its validation, starting
/// a run, waiting for or inspecting its outcome, and cancellation.
///
/// **Who learns the outcome.** `run` returns it; `start` delivers it to the
/// starting process, which alone may `await` it; `start_reporting` delivers
/// it as one message to a caller-supplied `Subject`, which a surviving
/// process can hold even after the starting process exits, or which the
/// starting process can add to its own `Selector`. In every case the
/// starting process owns the run: its exit cancels the run, which still
/// settles and rolls back before ending.
///
/// `run`/`start`/`await`/`cancel`/`progress` are implemented with bounded
/// concurrency, retry/compensation, reverse-order undo, a run deadline,
/// per-step timeouts, and cancellation settlement of active siblings.
///
/// **Resource bounds.** Worst-case run time is bounded by
/// `deadline + settle_timeout + (undone entries + compensations) *
/// cleanup_timeout`: once a run stops admitting new work (on a step
/// failure, the deadline, or a cancellation), in-flight attempts and
/// compensations are given up to `settle_timeout` to finish on their own
/// before being killed, and each compensation decision or undo action
/// individually is bounded by `cleanup_timeout`.
///
/// **Cancellation never reverses an unknown effect.** A step whose attempt
/// or compensation is killed — by its own `timeout`, or by the settle
/// window closing — is reported `interrupted`: its effect is unknown and is
/// never journaled or undone. Only steps that are known to have completed
/// are undone.
///
/// **Known results and unknown effects.** Every action a run performs — a
/// step attempt, a compensation decision, an undo — ends either with a
/// known result (it returned `Ok` or a typed error) or with an unknown
/// effect (it crashed or its process exited, it was killed at its time
/// bound, or it was killed when the settle window closed).
/// `unknown_effects(outcome)` names every action of the second kind, for
/// every outcome kind; it is `[]` exactly when every action returned.
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
import saga.{type Workflow}
import saga/internal/coordinator
import saga/internal/ffi

/// Bounds and pacing for one run.
pub type Config {
  Config(
    max_concurrency: Int,
    deadline: Option(Int),
    step_timeout: Option(Int),
    settle_timeout: Int,
    cleanup_timeout: Int,
  )
}

/// Sensible defaults: one attempt/compensation task per scheduler, no run
/// deadline, a 60 second default per-attempt `step_timeout`, a 5 second
/// settle window, and a 5 second cleanup bound.
///
/// **Why the run `deadline` stays `None` while `step_timeout` does not.**
/// `step_timeout` alone already bounds every individual attempt, and
/// `saga.compensate`'s `max_attempts` already bounds how many attempts (plus
/// backoff waits) a step can accumulate — together those two already give
/// every step a finite worst-case duration without a run-wide deadline
/// forcing one. A `deadline` is a different, coarser knob (a ceiling on the
/// *whole run*, cutting across still-healthy steps too) that only some
/// callers need; unlike a hung step, that is not a hazard the library can
/// safely default on behalf of every caller, so it stays an opt-in via
/// `deadline: Some(_)`.
pub fn config() -> Config {
  Config(
    max_concurrency: schedulers_online(),
    deadline: None,
    step_timeout: Some(60_000),
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
  StepTimeoutNotPositive(value: Int)
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
      case config.step_timeout {
        None -> []
        Some(ms) if ms > 0 -> []
        Some(ms) -> [StepTimeoutNotPositive(ms)]
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
/// execution failure (`StepCrashed`/`StepTimedOut`), retry exhaustion, and
/// an output-transform crash as distinct variants, so no report can
/// conflate them.
///
/// A cause names the *last* failure of a step, not every attempt before
/// it. Whether any attempt, compensation decision or undo left an effect of
/// unknown status is reported by `unknown_effects`, never by the cause's
/// variant.
pub type Cause(e) {
  /// The step's attempt returned `error` and the step has no `compensate`
  /// decider, or its decider chose `Abort(error)` or
  /// `AbortAfterCleanupFailure(error, _)`.
  ///
  /// **A `StepFailed` may follow a crash.** A decider is asked about a
  /// crashed or timed-out attempt too (`saga.Crashed`/`saga.TimedOut`), and
  /// an `Abort` it returns is reported as `StepFailed` with the decider's
  /// error, exactly like an aborted typed error. The crashed or timed-out
  /// attempt is then named in `Settlement.unknown_effects`: a decider
  /// author who aborts after a crash should choose an error that says so,
  /// and a consumer should consult `unknown_effects` rather than infer a
  /// known result from `StepFailed`.
  StepFailed(step: StepAddress, error: e)
  StepCrashed(step: StepAddress, crash: Crash)
  StepTimedOut(step: StepAddress)
  RetryLimitReached(step: StepAddress, last: saga.AttemptFailure(e))
  /// A `Retry`/`RetryAfter` decision was refused because settling had
  /// already begun for a different, unrelated trigger, not because this
  /// step's own attempt budget was exhausted — distinct from
  /// `RetryLimitReached` for that reason.
  RetrySuperseded(step: StepAddress, last: saga.AttemptFailure(e))
  OutputCrashed(crash: Crash)
  DeadlineExceeded
}

/// One completed step's undo did not succeed, during rollback.
pub type UndoFailure(u) {
  UndoFailed(step: StepAddress, error: u)
  UndoCrashed(step: StepAddress, crash: Crash)
  UndoTimedOut(step: StepAddress)
}

/// A compensation decision itself failed to produce a clean outcome.
pub type CompensationFailure(u) {
  CleanupFailed(step: StepAddress, error: u)
  CompensationCrashed(step: StepAddress, crash: Crash)
  CompensationTimedOut(step: StepAddress)
}

/// Why a run was cancelled: an explicit `cancel` call, or the owning
/// process exiting.
pub type CancelReason {
  CancelRequested
  OwnerExited
}

/// What happened to every step once a run stopped admitting new work:
/// which were undone (in reverse completion order), which undo actions
/// failed (all retained, not just the first), which had no undo configured,
/// which were held by an unresolved `Hold`, which attempts or compensations
/// were killed by their step's own `timeout` or in flight when the settle
/// window closed (`interrupted`), and which sibling failures settled after
/// the primary cause.
///
/// `unknown_effects` is the complete record: every action of the whole run
/// — attempt, compensation decision or undo, whether before or after the
/// run stopped admitting work — that ended with an unknown effect, in the
/// order they ended. It includes each `interrupted` entry, a crashed
/// attempt whatever its decider then chose, a crashed or timed-out
/// compensation, and a crashed or timed-out undo. It is `[]` exactly when
/// every action of the run returned a result.
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

/// One action of a run that ended without a result, so its effect is
/// unknown: it may or may not have happened, and saga never journaled or
/// undid it. Recorded when the action ends, whatever is decided afterwards:
/// a crashed attempt retried to success, continued, aborted or held is
/// still named.
pub type UnknownEffect {
  UnknownEffect(step: StepAddress, action: Action, ending: UnknownEnding)
}

/// Which of a step's actions ended with an unknown effect.
pub type Action {
  /// The step's attempt with this number (the first is `1`).
  StepAttempt(attempt: Int)
  /// The `saga.compensate` decider, deciding about the attempt with this
  /// number.
  StepCompensation(attempt: Int)
  /// The step's `saga.undo`, during rollback.
  StepUndo
}

/// How an action ended without a result.
pub type UnknownEnding {
  /// It raised, or its process exited (for example, killed from outside).
  ActionCrashed(crash: Crash)
  /// It was killed at its time bound: the step's `timeout` (or
  /// `Config.step_timeout`) for an attempt, `Config.cleanup_timeout` for a
  /// compensation decision or an undo.
  ActionTimedOut
  /// It was still running when the settle window closed, and was killed.
  ActionInterrupted
}

/// A run's terminal result: success, a failure with its settlement, a
/// cancellation, or an unresolved `Hold` that left completed effects
/// untouched. `Cancelled`'s settlement follows the same rules as `Failed`'s:
/// completed steps are undone, and interrupted or not-undoable effects are
/// listed rather than claimed reversed.
///
/// `CompletedWithUnknownEffects` is `Completed`'s counterpart for a run
/// that reached its output although a step attempt crashed (or its process
/// exited) or was killed at its timeout, and the step's recovery decider
/// then chose `Retry`/`RetryAfter`/`Continue`. That attempt's own effect is
/// still unknown and was never journaled or undone — only the
/// *replacement* attempt (the retry, or `Continue`'s supplied output) is
/// known. Kept as its own variant (never an always-present field on
/// `Completed`) so a `case` matching only `Completed` cannot silently drop
/// the uncertainty. `unknown_effects` is never empty on this variant, and a
/// plain `Completed` means every action of the run returned a result.
pub type Outcome(o, e, u) {
  Completed(output: o)
  CompletedWithUnknownEffects(output: o, unknown_effects: List(UnknownEffect))
  Failed(cause: Cause(e), settlement: Settlement(e, u))
  Cancelled(reason: CancelReason, settlement: Settlement(e, u))
  Unresolved(step: StepAddress, evidence: e, settlement: Settlement(e, u))
}

/// The run's current admission phase: accepting new work, letting active
/// siblings settle after a terminal trigger, or undoing completed steps.
/// The coordinator reports its outcome and exits in the same step that
/// finishes rollback (or reaches `Unresolved`/`Completed`), so there is no
/// observable window for `progress` to report a "finishing" phase.
pub type Phase {
  Running
  Settling
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
  Interrupted
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
///
/// `AlreadyAwaited` is reported for the ordinary case (a previous `await`
/// on this `Execution` already consumed its outcome) and also for a second
/// `await` after a previous one already reported `Lost` — both leave
/// nothing further to observe, since the coordinator is gone either way and
/// no fresh `Down` will ever arrive again for a monitor that already fired.
/// A caller that needs the crash detail should keep the first `await`'s own
/// `Lost` result instead of relying on a second call to repeat it.
///
/// **Timing.** If `await` is issued immediately after a previous `await`
/// already succeeded (returned `Ok`), it may still report
/// `Error(AwaitTimedOut)` rather than `Error(AlreadyAwaited)`: succeeding
/// only means the coordinator has sent its outcome, not that the
/// coordinator process has actually exited yet, and `AlreadyAwaited` is
/// only guaranteed once it has. A caller relying on `AlreadyAwaited` to
/// detect a repeated `await` should either retry past a timeout or give a
/// long enough one, rather than treating a single short-timeout call right
/// after success as conclusive.
pub type AwaitError {
  AwaitTimedOut
  /// The calling process does not receive this run's outcome: it is not the
  /// process that called `start`, or the run was started with
  /// `start_reporting` and delivers its outcome to a report subject.
  NotOwner
  AlreadyAwaited
  Lost(crash: Crash)
}

/// A started run. Only the process that called `start` may `await` it; a
/// run started with `start_reporting` delivers its outcome to its report
/// subject instead, and cannot be awaited at all. Either way, `cancel`,
/// `progress`, `pid` and `run_id` work from any process.
///
/// Deliberately stateless on the owner's side: no process-dictionary flag
/// or other bookkeeping outside this immutable value survives between
/// `await` calls, so starting and awaiting any number of `Execution`s never
/// grows the owner's process dictionary. `ToOwner.monitor` is the
/// *original* monitor set up once, in `start` — an ordinary value held on
/// `Execution`, not owner-process state, so keeping it costs nothing once
/// the `Execution` itself is dropped. It is what makes a coordinator that
/// dies abnormally *before ever being awaited* reliably reported as `Lost`:
/// a monitor set up while the coordinator was still alive is the only way
/// to see its *real* exit reason, since a monitor set up afterwards (once
/// the coordinator is already dead) always reports the synthetic reason
/// `noproc` instead. See `await`'s doc comment for how a second `await`
/// (after `monitor` has already fired and been torn down) is still
/// detected — without extra owner-side state — using a second, freshly
/// created monitor for that call only.
///
/// **Lifetime.** Every `await` that actually consumes a signal (an outcome,
/// or a `Down`) demonitors/drains as it returns, so nothing is left in the
/// owner's mailbox *once that call happens* — but a call that never
/// happens cannot clean up after itself. An `Execution` you stop awaiting
/// (an `await` that timed out, followed by `cancel`, with no `await`
/// afterwards) can still leave a monitor `Down` or an unclaimed outcome
/// message sitting in the owner's mailbox once the run eventually settles.
/// Always `await` again after `cancel` (even with a short timeout) so the
/// run's terminal outcome is consumed before the `Execution` is dropped.
/// A `start_reporting` run sets up no monitor in the starting process and
/// sends it nothing, so it leaves nothing behind there.
pub opaque type Execution(o, e, u) {
  Execution(
    pid: Pid,
    run_id: Int,
    control: Subject(coordinator.Control(o, e, u)),
    delivery: Delivery(o, e, u),
  )
}

/// Where a run's outcome goes.
type Delivery(o, e, u) {
  /// `start`: to `result`, owned by `owner`, which alone may `await` it.
  ToOwner(
    owner: Pid,
    monitor: process.Monitor,
    result: Subject(coordinator.Outcome(o, e, u)),
  )
  /// `start_reporting`: to the caller's report subject; not awaitable.
  ToReport
}

/// Runs `workflow` with `input` to completion, validating `config` first.
/// This is the ordinary path: it blocks until the run finishes.
pub fn run(
  workflow: Workflow(i, o, e, u),
  input: i,
  config: Config,
) -> Result(Outcome(o, e, u), RunError) {
  case start(workflow, input, config) {
    Error(error) -> Error(error)
    Ok(execution) ->
      case await_forever(execution) {
        Ok(outcome) -> Ok(outcome)
        Error(Lost(crash)) -> Error(ExecutionLost(crash))
        // `await_forever` cannot produce `AwaitTimedOut` (no timeout was
        // given) or `NotOwner` (the same process that started also awaits).
        // `AlreadyAwaited` should likewise be unreachable here: this is the
        // only `await` ever raced against `original_monitor`, and that
        // monitor has been armed since `start` — so if the coordinator has
        // already died by the time this call's fresh monitor is created,
        // the original's real `Down` was necessarily enqueued first (mailbox
        // order is FIFO) and `await_signal`'s zero-timeout check on
        // `FreshDown` finds it, reporting `Lost` rather than
        // `AlreadyAwaited`. Mapped to `ExecutionLost` with a synthetic crash
        // instead of a panic, on the same "report defensively rather than
        // prove unreachable" principle as `await_signal`'s own `OriginalDown`
        // branch — a future refactor that reintroduces the gap fails a run
        // instead of crashing the caller's process.
        Error(_other) ->
          Error(
            ExecutionLost(saga.Crash(
              saga.ExitClass,
              "saga: await reported AlreadyAwaited on a run's first and only await",
            )),
          )
      }
  }
}

/// Starts a run without blocking, returning an `Execution` handle. Only the
/// calling process may `await` it. Returns `Error(InvalidConfig(_))` if
/// `config` fails `validate`, or `Error(ExecutionLost(_))` if the
/// coordinator process fails to complete its startup handshake within 5
/// seconds (it should not normally take anywhere near that long; this
/// guards against a wedged or unschedulable coordinator rather than
/// reporting it as a caller configuration error).
pub fn start(
  workflow: Workflow(i, o, e, u),
  input: i,
  config: Config,
) -> Result(Execution(o, e, u), RunError) {
  let owner = process.self()
  let result = process.new_subject()
  use #(pid, run_id, control) <- result_try(
    launch(workflow, input, config, process.send(result, _)),
  )
  Ok(Execution(
    pid: pid,
    run_id: run_id,
    control: control,
    delivery: ToOwner(owner: owner, monitor: process.monitor(pid), result:),
  ))
}

/// Starts a run like `start`, but delivers its outcome to `report` as one
/// message instead of to the calling process. `report` may belong to any
/// process — the caller itself, to receive the outcome in its own
/// `Selector` next to its other messages, or another process, which then
/// learns the outcome even if the caller is gone — or be a
/// `process.named_subject`, resolved when the outcome is sent.
///
/// **Ownership.** The calling process is still the run's owner: if it
/// exits before the run ends, the run is cancelled with `OwnerExited`,
/// settles and rolls back exactly as for `cancel`, and the resulting
/// `Cancelled(OwnerExited, settlement)` is still delivered to `report`.
/// The settlement names every step undone, every undo or compensation that
/// failed or timed out, and every step interrupted with an unknown effect.
///
/// **Delivery.** `report` receives at most one message per run, sent by the
/// run's coordinator when the run ends, never before. It receives exactly
/// one unless the coordinator itself is killed (then nothing is sent) or,
/// for a named subject, no process holds the name when the outcome is sent
/// (then the outcome is dropped and the coordinator still exits normally).
/// To detect a lost run, the receiver monitors `pid(execution)`: the
/// coordinator's `Down` always arrives after its outcome, so a `Down` with
/// no outcome before it means the run was lost. A monitor set up after the
/// coordinator exited still fires (with reason `noproc`) and still arrives
/// after the outcome, if one was sent.
///
/// `await` on the returned `Execution` reports `Error(NotOwner)`, from any
/// process; `cancel`, `progress`, `pid` and `run_id` work as for `start`.
/// The starting process gets no monitor and no message from the run.
pub fn start_reporting(
  workflow: Workflow(i, o, e, u),
  input: i,
  config: Config,
  to report: Subject(Outcome(o, e, u)),
) -> Result(Execution(o, e, u), RunError) {
  use #(pid, run_id, control) <- result_try(
    launch(workflow, input, config, fn(outcome) {
      // A named subject with no process behind it raises; the outcome is
      // then dropped rather than crashing the coordinator as it exits.
      case
        ffi.rescue(fn() { process.send(report, from_coordinator(outcome)) })
      {
        ffi.Rescued(Nil) | ffi.Raised(..) -> Nil
      }
    }),
  )
  Ok(Execution(pid: pid, run_id: run_id, control: control, delivery: ToReport))
}

/// Validates `config` and spawns the coordinator, owned by the calling
/// process, delivering its outcome through `deliver`.
fn launch(
  workflow: Workflow(i, o, e, u),
  input: i,
  config: Config,
  deliver: fn(coordinator.Outcome(o, e, u)) -> Nil,
) -> Result(#(Pid, Int, Subject(coordinator.Control(o, e, u))), RunError) {
  use validated <- result_try(case validate(config) {
    Ok(validated) -> Ok(validated)
    Error(errors) -> Error(InvalidConfig(errors))
  })
  let control_subject_out = process.new_subject()
  let #(pid, run_id) =
    coordinator.start(
      workflow_name: saga.name(workflow),
      owner: process.self(),
      max_concurrency: validated.max_concurrency,
      deadline: validated.deadline,
      step_timeout: validated.step_timeout,
      settle_timeout: validated.settle_timeout,
      cleanup_timeout: validated.cleanup_timeout,
      build_graph: fn() { saga.for_run(workflow, input) },
      deliver: deliver,
      control_subject_out: control_subject_out,
    )
  case process.receive(control_subject_out, 5000) {
    Error(_) ->
      Error(
        ExecutionLost(saga.Crash(
          saga.ExitClass,
          "coordinator did not complete its startup handshake within 5000ms",
        )),
      )
    Ok(control) -> Ok(#(pid, run_id, control))
  }
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

type AwaitSignal(o, e, u) {
  GotOutcome(coordinator.Outcome(o, e, u))
  OriginalDown(process.ExitReason)
  FreshDown(process.ExitReason)
}

/// Builds the selector for one `await`/`await_forever` call: it races
/// `result` against *two* monitors on the coordinator —
/// `original_monitor` (the original one, set up once in `start`, still
/// armed only until it has fired and been torn down once) and a brand new
/// one created here, just for this call. Returns the fresh monitor
/// alongside the selector so the caller can demonitor it afterwards;
/// `original_monitor` is demonitored by `await_signal` instead, since
/// whether *it* still needs tearing down depends on which signal actually
/// matched.
///
/// Racing both, rather than only the original, is what makes a *second*
/// `await` (after the first already consumed the outcome, or already
/// observed the original's `Down`) resolve promptly instead of idling out
/// the full timeout: `original_monitor` cannot fire again once spent, but
/// Erlang delivers a brand new monitor's `Down` immediately — with the
/// synthetic reason `noproc` — when the monitored process is already dead,
/// which `await_signal` recognizes as "nothing further to await". See
/// `Execution`'s doc comment for why the *original* monitor still has to
/// exist at all (a monitor created after the fact can never report a real
/// exit reason, only `noproc`).
///
/// Erlang mailboxes are FIFO, and a coordinator's real `Down` for
/// `original_monitor` (fired while that monitor was live) is always
/// enqueued before the fresh monitor's synthetic post-mortem `Down`
/// (created afterwards) can be, so racing both together can never lose the
/// original's real exit reason to the fresh monitor's `noproc`.
fn await_selector(
  pid: Pid,
  original_monitor: process.Monitor,
  result: Subject(coordinator.Outcome(o, e, u)),
) -> #(process.Monitor, process.Selector(AwaitSignal(o, e, u))) {
  let fresh_monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_map(result, GotOutcome)
    |> process.select_specific_monitor(original_monitor, fn(down) {
      OriginalDown(down_reason(down))
    })
    |> process.select_specific_monitor(fresh_monitor, fn(down) {
      FreshDown(down_reason(down))
    })
  #(fresh_monitor, selector)
}

fn down_reason(down: process.Down) -> process.ExitReason {
  case down {
    process.ProcessDown(_, _, reason) -> reason
    process.PortDown(_, _, reason) -> reason
  }
}

/// `fresh_monitor` (created fresh for this one call, see `await_selector`)
/// is always demonitored here, with `[flush]`, regardless of which branch
/// matched — it has either already fired (its `Down`, if any, must not sit
/// in the owner's mailbox forever) or never will (a `GotOutcome` or
/// `OriginalDown` win instead, or this call is timing out) and either way
/// must be torn down before the next `await`. `original_monitor` (the
/// original) is demonitored (with `[flush]`) on every branch: `GotOutcome`
/// and `OriginalDown` consume it directly, and `FreshDown` either finds and
/// consumes its `Down` too (see below) or tears down a monitor that can now
/// never fire (the coordinator is confirmed dead either way), so nothing is
/// ever left armed after this call returns.
fn await_signal(
  original_monitor: process.Monitor,
  fresh_monitor: process.Monitor,
  signal: AwaitSignal(o, e, u),
) -> Result(Outcome(o, e, u), AwaitError) {
  process.demonitor_process(fresh_monitor)
  case signal {
    GotOutcome(outcome) -> {
      process.demonitor_process(original_monitor)
      Ok(from_coordinator(outcome))
    }
    // A first-hand `Down` from the *original* monitor: it was still armed
    // when the coordinator exited, so this is a genuine, fresh report of
    // that exit, never seen by any earlier `await` (the original monitor
    // only ever fires once). A live `Normal` here would mean the
    // coordinator exited without ever sending an outcome, which its own
    // contract rules out; kept as `Lost` rather than panicking, since it is
    // cheaper to report defensively than to prove unreachable.
    OriginalDown(reason) -> {
      process.demonitor_process(original_monitor)
      Error(Lost(saga.Crash(saga.ExitClass, exit_reason_to_string(reason))))
    }
    // The *fresh* monitor fired instead, with the synthetic reason `noproc`
    // — the coordinator was already dead by the time this call's fresh
    // monitor was set up. That leaves two possibilities that a selector
    // race between two simultaneously-eligible monitors cannot be trusted
    // to tell apart by delivery order alone:
    //
    //   1. The coordinator died *before this `await`* (an earlier `await`
    //      already consumed its outcome, or already observed and reported
    //      the original's `Down`, or no `await` ever raced it at all). The
    //      original monitor's `Down` is not sitting in the mailbox — either
    //      already drained by that earlier call, or (the "coordinator
    //      killed, never awaited" case) not yet delivered, but with no
    //      further relevance to this one.
    //   2. The run was lost *during this very `await`* — the coordinator
    //      died only just now, and both the original and fresh monitors'
    //      `Down` messages are in flight together. The original's real
    //      exit reason is then genuinely available and must win over the
    //      fresh monitor's uninformative `noproc`, exactly as `await`'s own
    //      first race already prefers `OriginalDown` when it arrives first.
    //
    // A zero-timeout selective receive on the original monitor alone
    // resolves this without polling or a process-dictionary flag: if its
    // `Down` is already in the mailbox, case 2 applies and its real reason
    // is reported as `Lost`; otherwise case 1 applies. Either way
    // `original_monitor` is demonitored with `[flush]` afterwards, so it
    // never lingers into a later `await`.
    FreshDown(_reason) -> {
      let original_selector =
        process.new_selector()
        |> process.select_specific_monitor(original_monitor, fn(down) {
          down_reason(down)
        })
      let outcome = case process.selector_receive(original_selector, 0) {
        Ok(reason) ->
          Error(Lost(saga.Crash(saga.ExitClass, exit_reason_to_string(reason))))
        Error(_) -> Error(AlreadyAwaited)
      }
      process.demonitor_process(original_monitor)
      outcome
    }
  }
}

fn exit_reason_to_string(reason: process.ExitReason) -> String {
  case reason {
    process.Normal -> "normal"
    process.Killed -> "killed"
    process.Abnormal(reason) -> "abnormal: " <> string.inspect(reason)
  }
}

/// Waits up to `milliseconds` for the run's outcome. Returns
/// `Error(AwaitTimedOut)` on timeout — the run continues, and `await` may
/// be called again. Returns `Error(AlreadyAwaited)` if a previous `await`
/// on this same `Execution` already consumed the outcome, *or* if a
/// previous `await` already reported `Lost` for it (see `AwaitError`'s doc
/// comment). Returns `Error(Lost(crash))` if the coordinator was killed
/// externally before it could report an outcome, whether that happened
/// before this call started or during it (`execution.pid` lets
/// applications monitor it themselves independently, for comparison).
///
/// **Timing.** An `await` issued immediately after a previous `await`
/// already returned `Ok` may still return `Error(AwaitTimedOut)` rather
/// than `Error(AlreadyAwaited)`: a successful outcome only means the
/// coordinator has sent it, not that the coordinator process has exited
/// yet. `AlreadyAwaited` is only guaranteed once the coordinator has
/// actually exited — see `AwaitError`'s doc comment.
///
/// Stateless on the owner's side: no process-dictionary flag or other
/// owner-process bookkeeping is used to detect a repeated `await` — see
/// `Execution`/`await_selector`'s doc comments for how a fresh, per-call
/// monitor takes its place instead, so starting and awaiting any number of
/// runs never grows the owner's process dictionary.
pub fn await(
  execution: Execution(o, e, u),
  timeout milliseconds: Int,
) -> Result(Outcome(o, e, u), AwaitError) {
  use #(monitor, result) <- awaited_by_self(execution)
  let #(fresh_monitor, selector) =
    await_selector(execution.pid, monitor, result)
  case process.selector_receive(selector, milliseconds) {
    Ok(signal) -> await_signal(monitor, fresh_monitor, signal)
    Error(_) -> {
      process.demonitor_process(fresh_monitor)
      Error(AwaitTimedOut)
    }
  }
}

fn await_forever(
  execution: Execution(o, e, u),
) -> Result(Outcome(o, e, u), AwaitError) {
  use #(monitor, result) <- awaited_by_self(execution)
  let #(fresh_monitor, selector) =
    await_selector(execution.pid, monitor, result)
  await_signal(
    monitor,
    fresh_monitor,
    process.selector_receive_forever(selector),
  )
}

/// Runs `then` with the run's original monitor and result subject when the
/// calling process is the one `start` delivers the outcome to; otherwise
/// `NotOwner` (another process, or a `start_reporting` run).
fn awaited_by_self(
  execution: Execution(o, e, u),
  then: fn(#(process.Monitor, Subject(coordinator.Outcome(o, e, u)))) ->
    Result(Outcome(o, e, u), AwaitError),
) -> Result(Outcome(o, e, u), AwaitError) {
  let self = process.self()
  case execution.delivery {
    ToOwner(owner:, monitor:, result:) if owner == self ->
      then(#(monitor, result))
    ToOwner(..) | ToReport -> Error(NotOwner)
  }
}

/// Requests cancellation. Returns immediately; the request is idempotent
/// and a no-op once settling or later has already begun. Cancellation stops
/// admission, lets active siblings settle within `settle_timeout`, then
/// kills whatever remains (reported `interrupted`) and rolls back known
/// completed effects. It never reverses an interrupted or not-undoable
/// effect.
///
/// The settle window is the run's `Config.settle_timeout`, fixed at start;
/// a cancellation cannot shorten it, because the owner's exit cancels a
/// run with no call to carry a value. It is an upper bound, not a delay:
/// settling ends as soon as nothing is in flight. `settle_timeout: 0`
/// kills in-flight work at once, reporting it `interrupted`; a longer
/// window lets it finish, so that it is known and undone.
pub fn cancel(execution: Execution(o, e, u)) -> Nil {
  process.send(execution.control, coordinator.CancelRequest)
}

type ProgressSignal {
  GotProgress(coordinator.Progress)
  ProgressCoordinatorDown
}

/// Synchronously inspects the run's current phase and per-step states.
/// Once the run has already ended, the coordinator process is gone and
/// `control` is a dead subject: sending a request to it is a silent no-op,
/// so without a monitor this would always time out rather than report
/// `ExecutionEnded` promptly. A short-lived monitor (armed only for this
/// one call, always demonitored with `[flush]` before returning) races the
/// reply against the coordinator's exit instead.
pub fn progress(
  execution: Execution(o, e, u),
  timeout milliseconds: Int,
) -> Result(Progress, ProgressError) {
  let reply = process.new_subject()
  let monitor = process.monitor(execution.pid)
  process.send(execution.control, coordinator.ProgressRequest(reply))
  let selector =
    process.new_selector()
    |> process.select_map(reply, GotProgress)
    |> process.select_specific_monitor(monitor, fn(_down) {
      ProgressCoordinatorDown
    })
  let outcome = case process.selector_receive(selector, milliseconds) {
    Ok(GotProgress(progress)) -> Ok(to_public_progress(progress))
    Ok(ProgressCoordinatorDown) -> Error(ExecutionEnded)
    Error(_) -> Error(ProgressTimedOut)
  }
  process.demonitor_process(monitor)
  outcome
}

/// Every action of the run that ended with an unknown effect, in the order
/// they ended: `[]` for `Completed`, the variant's own list for
/// `CompletedWithUnknownEffects`, and `settlement.unknown_effects` for
/// `Failed`, `Cancelled` and `Unresolved`. `[]` means every step attempt,
/// compensation decision and undo that ran returned a result, so no effect
/// of the run is of unknown status; effects a result left in place (an
/// undo that returned an error, a step with no undo, a held step) are
/// reported by the settlement, not here.
pub fn unknown_effects(outcome: Outcome(o, e, u)) -> List(UnknownEffect) {
  case outcome {
    Completed(_) -> []
    CompletedWithUnknownEffects(_, unknown_effects) -> unknown_effects
    Failed(_, settlement)
    | Cancelled(_, settlement)
    | Unresolved(_, _, settlement) -> settlement.unknown_effects
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

@internal
pub fn from_coordinator(
  outcome: coordinator.Outcome(o, e, u),
) -> Outcome(o, e, u) {
  case outcome {
    coordinator.Completed(output) -> Completed(output)
    coordinator.CompletedWithUnknownEffects(output, unknown_effects) ->
      CompletedWithUnknownEffects(
        output,
        list.map(unknown_effects, to_public_unknown_effect),
      )
    coordinator.Failed(cause, settlement) ->
      Failed(to_public_cause(cause), to_public_settlement(settlement))
    coordinator.Unresolved(step, evidence, settlement) ->
      Unresolved(
        saga.address_from_node(step),
        evidence,
        to_public_settlement(settlement),
      )
    coordinator.Cancelled(reason, settlement) ->
      Cancelled(
        to_public_cancel_reason(reason),
        to_public_settlement(settlement),
      )
  }
}

fn to_public_cancel_reason(reason: coordinator.CancelReason) -> CancelReason {
  case reason {
    coordinator.CancelRequested -> CancelRequested
    coordinator.OwnerExited -> OwnerExited
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
    coordinator.RetrySuperseded(step, last) ->
      RetrySuperseded(
        saga.address_from_node(step),
        saga.failure_from_node(last),
      )
    coordinator.OutputCrashed(crash) ->
      OutputCrashed(saga.crash_from_node(crash))
    coordinator.DeadlineExceeded -> DeadlineExceeded
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
    interrupted: list.map(settlement.interrupted, saga.address_from_node),
    compensation_failures: list.map(
      settlement.compensation_failures,
      to_public_compensation_failure,
    ),
    sibling_failures: list.map(settlement.sibling_failures, to_public_cause),
    unknown_effects: list.map(
      settlement.unknown_effects,
      to_public_unknown_effect,
    ),
  )
}

fn to_public_unknown_effect(
  effect: coordinator.UnknownEffect,
) -> UnknownEffect {
  UnknownEffect(
    step: saga.address_from_node(effect.step),
    action: case effect.action {
      coordinator.StepAttempt(attempt) -> StepAttempt(attempt)
      coordinator.StepCompensation(attempt) -> StepCompensation(attempt)
      coordinator.StepUndo -> StepUndo
    },
    ending: case effect.ending {
      coordinator.ActionCrashed(crash) ->
        ActionCrashed(saga.crash_from_node(crash))
      coordinator.ActionTimedOut -> ActionTimedOut
      coordinator.ActionInterrupted -> ActionInterrupted
    },
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
    coordinator.UndoTimedOut(step) -> UndoTimedOut(saga.address_from_node(step))
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
    coordinator.CompensationTimedOut(step) ->
      CompensationTimedOut(saga.address_from_node(step))
  }
}

fn to_public_progress(progress: coordinator.Progress) -> Progress {
  Progress(
    run_id: progress.run_id,
    phase: case progress.phase {
      coordinator.Running -> Running
      coordinator.Settling -> Settling
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
    coordinator.Interrupted -> Interrupted
    coordinator.Undoing -> Undoing
    coordinator.Undone -> Undone
    coordinator.UndoFailedStep -> UndoFailedStep
    coordinator.Skipped -> Skipped
  }
}
