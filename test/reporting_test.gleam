import gleam/erlang/process
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
      fn(_) { "failed" },
      fn(_, _) { Nil },
      duration.milliseconds(0),
    )
  outcome.failure_kind(failure) |> should.equal(outcome.Compensated)
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
          fn(error) { error },
          fn(_, _) { process.send(stopped, Nil) },
          duration.seconds(1),
        ),
      )
    })
  let monitor = process.monitor(owner)
  process.receive(results, 1000) |> should.equal(Ok(Ok(42)))
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
          fn(error) { error },
          fn(result, summary) {
            process.send(notified, #(result, summary, process.self()))
            process.receive_forever(process.new_subject())
          },
          duration.milliseconds(100),
        )
      process.send(results, result)
      process.receive_forever(process.new_subject())
    })
  process.receive(results, 1000) |> should.equal(Ok(Ok(42)))
  process.kill(owner)
  let assert Ok(#(Ok(42), "Saga reported completed", worker)) =
    process.receive(notified, 1000)
  let monitor = process.monitor(worker)
  let assert Ok(Nil) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
    |> process.selector_receive(1000)
}
