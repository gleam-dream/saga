import gleam/erlang/process
import gleam/option.{Some}
import gleam/time/duration
import gleeunit/should
import order_consumer/domain
import order_consumer/workflows
import saga/execution
import saga/outcome
import saga/reporting

fn checkout(declined, undo_failures) {
  workflows.checkout_workflow(
    orders: [domain.Order("order-42", "Ada", 4200, ["widget"])],
    unavailable_items: [],
    declined_orders: declined,
    undo_fails_for: undo_failures,
  )
}

// Reporting owns a short-lived invocation; application values remain native.
fn run(workflow, config) {
  let result = process.new_subject()
  process.spawn_unlinked(fn() {
    process.send(
      result,
      reporting.run_owned(
        workflow,
        "order-42",
        config,
        fn(_) { Nil },
        duration.seconds(1),
      ),
    )
  })
  let assert Ok(report) = process.receive(result, 2000)
  report
}

pub fn owned_checkout_returns_the_native_output_test() {
  let assert Ok(execution.Completed(checkout)) =
    run(
      checkout([], []),
      execution.config()
        |> execution.with_deadline(execution.After(duration.seconds(1))),
    )
  checkout.payment.amount_cents |> should.equal(4200)
  checkout.inventory.items |> should.equal(["widget"])
}

pub fn obtained_failure_report_preserves_business_and_undo_errors_test() {
  let assert Ok(report) =
    run(checkout(["order-42"], ["order-42"]), execution.config())
  let assert execution.Failed(
    execution.StepFailed(_, domain.PaymentDeclined("order-42", _)),
    settlement,
  ) = report
  let assert [
    execution.UndoFailed(step, domain.ReleaseInventoryFailed("order-42", _)),
  ] = settlement.undo_failures
  step.name |> should.equal("reserve_inventory")
  outcome.kind(report) |> should.equal(outcome.Unresolved)
}

pub fn no_report_has_typed_operational_cause_and_effect_status_test() {
  let assert Error(error) =
    run(
      checkout([], []),
      execution.config() |> execution.with_max_concurrency(0),
    )
  reporting.error_kind(error) |> should.equal(reporting.ExecutionAdmission)
  reporting.effect_status(error) |> should.equal(reporting.NotStarted)
  reporting.run_error(error)
  |> should.equal(
    Some(execution.InvalidConfig([execution.MaxConcurrencyNotPositive(0)])),
  )
}
