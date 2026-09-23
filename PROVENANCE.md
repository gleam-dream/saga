# Provenance: Saga tests adapted from Reactor 1.0.6

Saga's design uses [`reactor`](https://github.com/ash-project/reactor)
`v1.0.6` (commit `e4ddc9e438f08562e9fb31b08255a86f4bade8ef`) as its main
behavioral oracle for scheduling and rollback (see
[`oversight/saga-design.md`](../oversight/saga-design.md)). This document
records which Saga tests were adapted from which upstream Reactor tests,
what behavior each one proves, and every place Saga was _deliberately_
built to differ. Reactor is MIT-licensed; see
[`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md) for the full license
text, which this adaptation requires.

Classifications:

- **faithfully adapted** — same scenario shape and same expected outcome
  as upstream, translated to Saga's API.
- **differential comparison** — verified directly against a live Reactor
  1.0.6 run in `oracle/reactor/`, not just read from the upstream source;
  see `scripts/oracle.sh`.
- **inspired** — the upstream test motivated the scenario, but Saga's
  outcome, mechanism, or scope intentionally differs (documented per row).

## Adapted upstream tests

| #   | Upstream revision | Source test                                                                                          | Behavior                                                                                    | Local equivalent                                                                                                                                                                   | Classification                                    | Deliberate differences                                                                                            |
| --- | ----------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| R1  | v1.0.6 / e4ddc9e  | `test/reactor/executor_test.exs:13-48` "it executes the steps"                                       | Dependent sequential steps run in dependency order                                          | `test/scheduling_test.gleam:sequential_dependency_order_test`; also `test/oracle_test.gleam:oracle_d1_sequential_dependency_test` (D1)                                             | faithfully adapted + differential comparison (D1) | Typed ports instead of `result(:name)`; D1 diffs against a live Reactor run                                       |
| R2  | v1.0.6 / e4ddc9e  | `test/reactor/executor_test.exs:50-102` "the steps execute in separate pids"                         | Independent steps run in separate processes                                                 | `test/scheduling_test.gleam:independent_steps_run_in_distinct_processes_test`                                                                                                      | faithfully adapted                                | none                                                                                                              |
| R3  | v1.0.6 / e4ddc9e  | `test/reactor/executor_test.exs:353-411` "successful steps can be undone"                            | A failure undoes all prior undoable steps                                                   | `test/rollback_test.gleam:failure_undoes_completed_steps_test`; also `test/oracle_test.gleam:oracle_d2_undo_order_and_failures_test` (D2)                                          | faithfully adapted + differential comparison (D2) | Undo order reversed: see D2 below                                                                                 |
| R4  | v1.0.6 / e4ddc9e  | `test/reactor/executor/async_test.exs:82-99`                                                         | Non-undoable completions are not undone                                                     | `test/rollback_test.gleam:not_undoable_steps_are_reported_test`                                                                                                                    | faithfully adapted                                | Reported explicitly in `settlement.not_undoable`                                                                  |
| R5  | v1.0.6 / e4ddc9e  | `test/reactor/executor/async_test.exs:166-205`                                                       | A failure while sibling steps are still running                                             | `test/scheduling_test.gleam:failure_with_active_siblings_settles_test`; also `test/oracle_test.gleam:oracle_d3_failure_with_active_sibling_settles_test` (D3)                      | differential comparison (D3)                      | Saga settles siblings instead of orphaning them: see D3 below                                                     |
| R6  | v1.0.6 / e4ddc9e  | `test/reactor/executor/async_test.exs:206-227`                                                       | A crashed step task becomes an error, not a silent hang                                     | `test/config_test.gleam:killed_step_task_is_crash_test`                                                                                                                            | faithfully adapted                                | Surfaces as `StepCrashed(Crash(ExitClass, ...))`                                                                  |
| R7  | v1.0.6 / e4ddc9e  | `test/reactor/executor/async_test.exs:228-262`, `test/reactor/executor/sync_test.exs:44-89`          | Retry budget and exhaustion triggers rollback                                               | `test/retry_test.gleam:retry_until_success_test`, `test/retry_test.gleam:retry_limit_triggers_rollback_test`; also `test/oracle_test.gleam:oracle_d4_retry_then_success_test` (D4) | faithfully adapted + differential comparison (D4) | Retry only through the explicit `compensate` decision; Saga's `max_attempts = Reactor's max_retries + 1`          |
| R8  | v1.0.6 / e4ddc9e  | `test/reactor/executor/compensation_test.exs:46-81` "increments current_try"                         | Compensate `:retry` starts a new attempt with an incremented try count                      | `test/retry_test.gleam:compensation_receives_attempt_numbers_test`                                                                                                                 | faithfully adapted                                | `Attempt.number` is 1-based (Reactor's `current_try` is 0-based)                                                  |
| R9  | v1.0.6 / e4ddc9e  | `test/reactor/executor/step_runner_test.exs:170-250`                                                 | Failed-step compensation decisions (no compensation, raise, continue, retry, retry+backoff) | `test/retry_test.gleam:compensation_decisions_test` (one case per upstream case)                                                                                                   | faithfully adapted                                | `Continue` carries its own `Undo`; backoff is `RetryAfter(ms)`                                                    |
| R10 | v1.0.6 / e4ddc9e  | `test/reactor/executor/step_runner_test.exs:252-307`                                                 | Undo receives the step's original args and output; bounded undo retry                       | `test/rollback_test.gleam:undo_receives_input_and_output_test`                                                                                                                     | args faithfully adapted; retry inspired           | No undo retry at all (§3.3 of the design); a failing undo is retained once, never retried                         |
| R11 | v1.0.6 / e4ddc9e  | `test/reactor/executor_test.exs:604-647` "when the timeout is elapsed, it halts the reactor"         | A run-level timeout stops the run                                                           | `test/lifecycle_test.gleam:deadline_interrupts_run_test`                                                                                                                           | inspired                                          | Saga fails with `DeadlineExceeded` and rolls back; Reactor halts for a later resume (Saga has no resume)          |
| R12 | v1.0.6 / e4ddc9e  | `test/reactor/executor_test.exs:910-928`                                                             | A deadline firing while a step is waiting on a retry backoff                                | `test/lifecycle_test.gleam:deadline_during_backoff_test`                                                                                                                           | faithfully adapted (outcome differs as in R11)    | as R11                                                                                                            |
| R13 | v1.0.6 / e4ddc9e  | `test/reactor/executor_test.exs:810-837,980-1012`                                                    | A step-provided backoff delays the next retry                                               | `test/retry_test.gleam:compensation_decisions_test` (the `RetryAfter` case)                                                                                                        | inspired                                          | No elapsed-time assertions; a relative delay via `send_after`-style scheduling                                    |
| R14 | v1.0.6 / e4ddc9e  | `test/reactor/executor/concurrency_tracker_test.exs:34-80`, `test/reactor/executor_test.exs:649-688` | `max_concurrency` bounds the number of steps running at once                                | `test/scheduling_test.gleam:max_concurrency_bounds_running_attempts_test`; also `test/oracle_test.gleam:oracle_d6_max_concurrency_bound_test` (D6)                                 | inspired + differential comparison (D6)           | Per-run bound only; shared/global concurrency pools are deferred (see `CAPABILITIES.md`)                          |
| R15 | v1.0.6 / e4ddc9e  | `test/reactor/planner_test.exs:72-104`                                                               | Graph validity: cyclic graphs and unknown-step references are rejected                      | compiler negatives under `examples/order_consumer/fixtures/negative/` (owned by the increment-3 consumer work; see that increment's own PROVENANCE note)                           | inspired                                          | Rejected at Gleam compile time, not at runtime; a cycle cannot even be constructed in Saga's builder              |
| R16 | v1.0.6 / e4ddc9e  | `test/reactor/step_test.exs:11-37` "can?/2"                                                          | Capability introspection: whether a step can undo or compensate                             | `test/authoring_test.gleam:describe_reports_capabilities_test`                                                                                                                     | inspired                                          | Exposed as `StepDescriptor.undoable`/`StepDescriptor.compensates` on a static descriptor, not a runtime predicate |
| R17 | v1.0.6 / e4ddc9e  | `test/reactor/middleware/telemetry_test.exs`                                                         | Lifecycle events emitted during a run                                                       | `test/observation_test.gleam:observation_events_test`                                                                                                                              | inspired                                          | Saga-owned Sinal event names and metadata fields, not Reactor's telemetry event names                             |

Deferred and excluded upstream behaviors (halt/resume, abandoned steps,
`undo/2` of a whole successful reactor, dynamic steps, the DSL transformer
suite, and the Mermaid exporter) are recorded in
[`CAPABILITIES.md`](CAPABILITIES.md), not adapted here.

## Differential oracle scenarios (`oracle/reactor/`)

Each scenario below runs against a real, hex-pinned `reactor == 1.0.6` in
`oracle/reactor/` and against Saga in `test/oracle_test.gleam`, compared by
`scripts/oracle.sh`. `oracle/reactor/expected/d*.txt` is the literal,
captured output of the Reactor side — evidence, not a guess — captured by
running each `oracle/reactor/scenarios/d*.exs` script directly.

| D   | Scenario                                                  | Reactor result (`expected/d*.txt`)                                                                                                    | Saga assertion (`test/oracle_test.gleam`)                                                                                        | Verdict                                                  |
| --- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| D1  | Sequential dependency chain a→b→c                         | Runs in order `[run: a, run: b, run: c]`                                                                                              | Identical order                                                                                                                  | match                                                    |
| D2  | e1→e2→e3→e4(fail), e2 and e3's undo fail                  | Undoes forward: `undo: e1, e2, e3`; retains 3 error classes (`UndoStepError`, `UndoStepError`, `RunStepError`)                        | Undoes in reverse: `undo: e3, e2, e1`; retains 2 `UndoFailed` entries in `settlement.undo_failures`, `e1` in `settlement.undone` | **deliberate difference** (undo order)                   |
| D3  | `slow` (300ms), `fast_fail` (fails fast), `quick`, joined | Returns `{:error, _}` as soon as `fast_fail`/`quick` settle; `slow` finishes _after_ the return and is never undone (orphaned effect) | Settles `slow` before returning: it finishes within `settle_timeout` and is undone (verified via `settlement.undone`)            | **deliberate difference** (sibling settlement)           |
| D4  | Fails twice then succeeds, `max_retries: 2`               | 3 total run attempts, `{:ok, :done}`                                                                                                  | `max_attempts: 3`, 3 attempts recorded, `Completed`                                                                              | match (naming differs: `max_attempts = max_retries + 1`) |
| D5  | Compensate returns `{:continue, :replacement}`            | `{:ok, :replacement}`                                                                                                                 | `Continue("replacement", NoUndo)` → `Completed("replacement")`                                                                   | match                                                    |
| D6  | 10 independent steps, `max_concurrency: 3`                | Peak concurrency `3`                                                                                                                  | Peak concurrency `3`                                                                                                             | match                                                    |
| D7  | Diamond: one producer feeds two consumers that join       | Producer runs once                                                                                                                    | Producer runs once (native Saga scenario; no isolated upstream test — Reactor's memoization is implicit in its DAG)              | match                                                    |

Run the oracle with:

```sh
nix develop .#oracle --command scripts/oracle.sh
```

or, if the `oversight` dev shell (which also carries Elixir) is already
active:

```sh
nix develop /code/gleam-dream/oversight -c scripts/oracle.sh
```

The script fails loudly if a live Reactor 1.0.6 run ever stops matching
`expected/d*.txt` — that would mean the recorded oracle evidence is stale,
and every deliberate-difference claim above needs re-checking before it is
trusted again.
