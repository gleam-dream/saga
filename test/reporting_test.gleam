import gleam/erlang/process
import gleam/option.{None, Some}
import gleam/string
import gleam/time/duration
import gleeunit/should
import saga
import saga/execution
import saga/outcome
import saga/reporting

fn workflow() -> saga.Workflow(Int, Int, String, String) {
  saga.define("echo", fn(input) {
    saga.perform(input, saga.step("identity", fn(value) { Ok(value) }))
  })
}

pub fn invalid_reporting_budget_starts_no_work_test() {
  let invoked = process.new_subject()
  let flow =
    saga.define("probe", fn(input) {
      saga.perform(
        input,
        saga.step("probe", fn(value) {
          process.send(invoked, Nil)
          Ok(value)
        }),
      )
    })
  let assert Error(failure) =
    reporting.run_owned(
      flow,
      42,
      execution.config(),
      fn(_) { Nil },
      duration.milliseconds(0),
    )
  reporting.error_kind(failure) |> should.equal(reporting.InvalidRollback)
  reporting.effect_status(failure) |> should.equal(reporting.NotStarted)
  reporting.invalid_rollback_within(failure)
  |> should.equal(Some(duration.milliseconds(0)))
  reporting.run_error(failure) |> should.equal(None)
  reporting.exit_reason(failure) |> should.equal(None)
  process.receive(invoked, 0) |> should.equal(Error(Nil))
}

pub fn a_normal_task_exit_does_not_report_a_second_outcome_test() {
  let results = process.new_subject()
  let stopped = process.new_subject()
  let owner =
    process.spawn_unlinked(fn() {
      process.send(
        results,
        reporting.run_owned(
          workflow(),
          42,
          execution.config(),
          fn(_) { process.send(stopped, Nil) },
          duration.seconds(1),
        ),
      )
    })
  let monitor = process.monitor(owner)
  process.receive(results, 1000)
  |> should.equal(Ok(Ok(execution.Completed(42))))
  let assert Ok(Nil) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
    |> process.selector_receive(1000)
  process.receive(stopped, 100) |> should.equal(Error(Nil))
}

pub fn a_stopped_task_reports_its_outcome_and_bounds_a_hung_notification_test() {
  let results = process.new_subject()
  let notified = process.new_subject()
  let owner =
    process.spawn_unlinked(fn() {
      let result =
        reporting.run_owned(
          workflow(),
          42,
          execution.config(),
          fn(result) {
            process.send(notified, #(result, process.self()))
            process.receive_forever(process.new_subject())
          },
          duration.milliseconds(100),
        )
      process.send(results, result)
      process.receive_forever(process.new_subject())
    })
  process.receive(results, 1000)
  |> should.equal(Ok(Ok(execution.Completed(42))))
  process.kill(owner)
  let assert Ok(#(Ok(execution.Completed(42)), worker)) =
    process.receive(notified, 1000)
  let monitor = process.monitor(worker)
  let assert Ok(Nil) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
    |> process.selector_receive(1000)
}

pub fn invalid_execution_config_preserves_typed_cause_and_starts_no_work_test() {
  let config = execution.config() |> execution.with_max_concurrency(0)
  let assert Error(error) = owned(workflow(), 42, config)
  reporting.error_kind(error) |> should.equal(reporting.ExecutionAdmission)
  reporting.effect_status(error) |> should.equal(reporting.NotStarted)
  reporting.run_error(error)
  |> should.equal(
    Some(
      execution.InvalidConfig([
        execution.MaxConcurrencyNotPositive(0),
      ]),
    ),
  )
  reporting.exit_reason(error) |> should.equal(None)
}

type BusinessError {
  Declined(Int)
}

type UndoError {
  Refused(String)
}

pub fn a_failed_workflow_returns_its_typed_execution_report_test() {
  let first =
    saga.step("reserve", fn(value) { Ok(value) })
    |> saga.undo(fn(_) { Error(Refused("PRIVATE-UNDO")) })
  let next = saga.step("pay", fn(_) { Error(Declined(42)) })
  let flow =
    saga.define("order", fn(input) {
      input |> saga.perform(first) |> saga.perform(next)
    })
  let assert Ok(report) = owned(flow, 100, execution.config())
  let assert execution.Failed(
    execution.StepFailed(step, Declined(42)),
    settlement,
  ) = report
  step.name |> should.equal("pay")
  let assert [execution.UndoFailed(undo_step, Refused("PRIVATE-UNDO"))] =
    settlement.undo_failures
  undo_step.name |> should.equal("reserve")
  outcome.summary(report) |> string.contains("PRIVATE-UNDO") |> should.be_false
}

@external(erlang, "reporting_probe", "parent")
fn parent() -> process.Pid

@external(erlang, "reporting_probe", "monitors")
fn monitors(pid: process.Pid) -> List(process.Pid)

fn blocked(coordinator) {
  saga.define("blocked", fn(input) {
    saga.perform(
      input,
      saga.step("wait", fn(_) {
        process.send(coordinator, parent())
        process.receive_forever(process.new_subject())
      }),
    )
  })
}

pub fn a_lost_coordinator_is_an_unknown_reporting_error_test() {
  let coordinator = process.new_subject()
  let result = process.new_subject()
  let flow = blocked(coordinator)
  process.spawn_unlinked(fn() {
    process.send(
      result,
      reporting.run_owned(
        flow,
        42,
        execution.config(),
        fn(_) { Nil },
        duration.seconds(1),
      ),
    )
  })
  let assert Ok(pid) = process.receive(coordinator, 1000)
  process.kill(pid)
  let assert Ok(Error(error)) = process.receive(result, 1000)
  reporting.error_kind(error) |> should.equal(reporting.CoordinatorLost)
  reporting.effect_status(error) |> should.equal(reporting.Unknown)
  let assert Some(_) = reporting.exit_reason(error)
  reporting.run_error(error) |> should.equal(None)
}

pub fn a_lost_receiver_retains_its_exit_reason_and_reports_unknown_effects_test() {
  let coordinator = process.new_subject()
  let result = process.new_subject()
  let flow = blocked(coordinator)
  let owner =
    process.spawn_unlinked(fn() {
      process.send(
        result,
        reporting.run_owned(
          flow,
          42,
          execution.config()
            |> execution.with_settle_timeout(duration.milliseconds(0)),
          fn(_) { Nil },
          duration.seconds(1),
        ),
      )
    })
  let assert Ok(coordinator) = process.receive(coordinator, 1000)
  let assert [receiver] = monitors(owner)
  process.send_abnormal_exit(receiver, "PRIVATE-RECEIVER-REASON")
  let assert Ok(Error(error)) = process.receive(result, 1000)
  reporting.error_kind(error) |> should.equal(reporting.ReceiverLost)
  reporting.effect_status(error) |> should.equal(reporting.Unknown)
  let assert Some(process.Abnormal(_)) = reporting.exit_reason(error)
  reporting.describe_error(error)
  |> string.contains("PRIVATE-RECEIVER-REASON")
  |> should.be_false
  let monitor = process.monitor(coordinator)
  process.new_selector()
  |> process.select_specific_monitor(monitor, fn(_) { Nil })
  |> process.selector_receive(1000)
  |> should.equal(Ok(Nil))
}

pub fn owner_death_returns_the_full_rollback_report_to_the_callback_test() {
  let running = process.new_subject()
  let undone = process.new_subject()
  let notified = process.new_subject()
  let first =
    saga.step("reserve", fn(value) { Ok(value) })
    |> saga.undo(fn(_) {
      process.send(undone, Nil)
      Ok(Nil)
    })
  let next =
    saga.step("blocked", fn(_) {
      process.send(running, Nil)
      process.receive_forever(process.new_subject())
    })
  let flow =
    saga.define("order", fn(input) {
      input |> saga.perform(first) |> saga.perform(next)
    })
  let owner =
    process.spawn_unlinked(fn() {
      reporting.run_owned(
        flow,
        42,
        execution.config()
          |> execution.with_settle_timeout(duration.milliseconds(0)),
        fn(report) { process.send(notified, report) },
        duration.seconds(1),
      )
    })
  process.receive(running, 1000) |> should.equal(Ok(Nil))
  process.kill(owner)
  let assert Ok(Ok(report)) = process.receive(notified, 2000)
  let assert execution.Cancelled(execution.OwnerExited, settlement) = report
  process.receive(undone, 0) |> should.equal(Ok(Nil))
  let assert [step] = settlement.undone
  step.name |> should.equal("reserve")
  let assert [effect] = settlement.unknown_effects
  effect.step.name |> should.equal("blocked")
  effect.ending |> should.equal(execution.ActionInterrupted)
}

fn owned(flow, input, config) {
  let result = process.new_subject()
  process.spawn_unlinked(fn() {
    process.send(
      result,
      reporting.run_owned(
        flow,
        input,
        config,
        fn(_) { Nil },
        duration.seconds(1),
      ),
    )
  })
  let assert Ok(report) = process.receive(result, 2000)
  report
}

pub fn stopped_owner_callback_preserves_the_obtained_business_failure_report_test() {
  let returned = process.new_subject()
  let stopped = process.new_subject()
  let flow =
    saga.define("failure", fn(input) {
      saga.perform(input, saga.step("decline", fn(_) { Error(Declined(42)) }))
    })
  let owner =
    process.spawn_unlinked(fn() {
      let report =
        reporting.run_owned(
          flow,
          Nil,
          execution.config(),
          fn(report) { process.send(stopped, report) },
          duration.seconds(1),
        )
      process.send(returned, report)
      process.receive_forever(process.new_subject())
    })
  let assert Ok(Ok(report)) = process.receive(returned, 1000)
  let assert execution.Failed(execution.StepFailed(_, Declined(42)), _) = report
  process.kill(owner)
  process.receive(stopped, 1000) |> should.equal(Ok(Ok(report)))
}

pub fn rejected_config_removes_the_receiver_monitor_and_preserves_other_mail_test() {
  let before = monitors(process.self())
  let unrelated = process.new_subject()
  process.send(unrelated, "keep")
  let assert Error(_) =
    reporting.run_owned(
      workflow(),
      42,
      execution.config() |> execution.with_max_concurrency(0),
      fn(_) { Nil },
      duration.seconds(1),
    )
  monitors(process.self()) |> should.equal(before)
  process.receive(unrelated, 0) |> should.equal(Ok("keep"))
  process.new_selector()
  |> process.select_monitors(fn(_) { Nil })
  |> process.selector_receive(0)
  |> should.equal(Error(Nil))
}
