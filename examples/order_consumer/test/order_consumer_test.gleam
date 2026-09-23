import gleam/list
import gleam/option.{None, Some}
import gleeunit
import gleeunit/should
import order_consumer/domain.{
  type Order, InventoryUnavailable, Order, PaymentDeclined,
}
import order_consumer/workflows
import saga
import saga/execution
import support/cleanup
import support/gate

pub fn main() -> Nil {
  gleeunit.main()
}

fn sample_orders() -> List(Order) {
  [
    Order("ord-1", "ada", 4200, ["widget", "gadget"]),
    Order("ord-2", "bo", 1500, ["widget"]),
  ]
}

// ---------------------------------------------------------------------------
// (a) shared data + parallel independent work
// ---------------------------------------------------------------------------

pub fn shared_order_and_parallel_steps_succeed_test() {
  let assert Ok(workflow) =
    workflows.checkout_workflow(
      orders: sample_orders(),
      unavailable_items: [],
      declined_orders: [],
      undo_fails_for: [],
    )

  let assert Ok(execution.Completed(checkout)) =
    execution.run(workflow, "ord-1", execution.config())

  checkout.inventory.order_id |> should.equal("ord-1")
  checkout.payment.order_id |> should.equal("ord-1")
  checkout.payment.amount_cents |> should.equal(4200)
}

pub fn load_order_runs_once_and_is_shared_test() {
  // Both `reserve_inventory` and `authorize_payment` depend on the same
  // `load_order` port; saga.describe must show one `load_order` node with
  // two dependents, not two.
  let assert Ok(workflow) =
    workflows.checkout_workflow(
      orders: sample_orders(),
      unavailable_items: [],
      declined_orders: [],
      undo_fails_for: [],
    )

  let descriptors = saga.describe(workflow)
  list.length(descriptors) |> should.equal(3)
  let load_order_count =
    list.length(
      list.filter(descriptors, fn(d) { d.address.name == "load_order" }),
    )
  load_order_count |> should.equal(1)
}

pub fn unknown_order_fails_at_load_order_test() {
  let assert Ok(workflow) =
    workflows.checkout_workflow(
      orders: sample_orders(),
      unavailable_items: [],
      declined_orders: [],
      undo_fails_for: [],
    )

  let assert Ok(execution.Failed(cause, _settlement)) =
    execution.run(workflow, "missing", execution.config())

  case cause {
    execution.StepFailed(step, domain.OrderNotFound("missing")) ->
      step.name |> should.equal("load_order")
    _ -> panic as "expected StepFailed(load_order, OrderNotFound)"
  }
}

// ---------------------------------------------------------------------------
// (b) failure -> compensation, with a retained undo failure
// ---------------------------------------------------------------------------

pub fn payment_failure_rolls_back_inventory_test() {
  let assert Ok(workflow) =
    workflows.checkout_workflow(
      orders: sample_orders(),
      unavailable_items: [],
      declined_orders: ["ord-2"],
      undo_fails_for: [],
    )

  let assert Ok(execution.Failed(cause, settlement)) =
    execution.run(workflow, "ord-2", execution.config())

  case cause {
    execution.StepFailed(step, PaymentDeclined("ord-2", _reason)) ->
      step.name |> should.equal("authorize_payment")
    _ -> panic as "expected StepFailed(authorize_payment, PaymentDeclined)"
  }

  settlement.undone
  |> list.map(fn(address) { address.name })
  |> should.equal(["reserve_inventory"])
  settlement.undo_failures |> should.equal([])
}

pub fn inventory_unavailable_fails_before_payment_runs_test() {
  let assert Ok(workflow) =
    workflows.checkout_workflow(
      orders: sample_orders(),
      unavailable_items: ["gadget"],
      declined_orders: [],
      undo_fails_for: [],
    )

  let assert Ok(execution.Failed(cause, _settlement)) =
    execution.run(workflow, "ord-1", execution.config())

  case cause {
    execution.StepFailed(step, InventoryUnavailable("ord-1", missing)) -> {
      step.name |> should.equal("reserve_inventory")
      missing |> should.equal(["gadget"])
    }
    _ -> panic as "expected StepFailed(reserve_inventory, InventoryUnavailable)"
  }
}

pub fn undo_failure_is_preserved_in_settlement_test() {
  // Inventory reservation completes and is undoable; payment then fails.
  // The inventory release action is configured to itself fail, so the
  // failure must be retained in `settlement.undo_failures` rather than
  // silently dropped or aborting the rollback of other steps.
  let assert Ok(workflow) =
    workflows.checkout_workflow(
      orders: sample_orders(),
      unavailable_items: [],
      declined_orders: ["ord-2"],
      undo_fails_for: ["ord-2"],
    )

  let assert Ok(execution.Failed(_cause, settlement)) =
    execution.run(workflow, "ord-2", execution.config())

  settlement.undone |> should.equal([])
  list.length(settlement.undo_failures) |> should.equal(1)
  case settlement.undo_failures {
    [execution.UndoFailed(step, domain.ReleaseInventoryFailed("ord-2", _))] ->
      step.name |> should.equal("reserve_inventory")
    _ ->
      panic as "expected one retained UndoFailed(reserve_inventory, ReleaseInventoryFailed)"
  }
}

// ---------------------------------------------------------------------------
// (c) advanced config + cancellation via start/cancel/await
// ---------------------------------------------------------------------------

pub fn advanced_config_runs_with_bounded_concurrency_and_deadline_test() {
  let assert Ok(workflow) =
    workflows.checkout_workflow(
      orders: sample_orders(),
      unavailable_items: [],
      declined_orders: [],
      undo_fails_for: [],
    )

  let config = workflows.bounded_config(Some(2000))
  let assert Ok(execution.Completed(checkout)) =
    execution.run(workflow, "ord-1", config)
  checkout.inventory.order_id |> should.equal("ord-1")
}

pub fn cancel_while_step_in_flight_rolls_back_and_reports_cancelled_test() {
  let release_gate = gate.new_gate()

  let assert Ok(workflow) =
    workflows.blocking_workflow(fn() { gate.enter(release_gate) })

  let config = workflows.bounded_config(None)
  let assert Ok(execution) = execution.start(workflow, "ord-1", config)

  cleanup.with_execution(execution, fn() {
    // The step has started (announced itself), so `progress` should show
    // it `Attempting` before cancellation.
    let assert Ok(_task_pid) = gate.wait_entered(release_gate, 2000)
    let assert Ok(progress) = execution.progress(execution, 1000)
    progress.phase |> should.equal(execution.Running)

    execution.cancel(execution)

    let assert Ok(execution.Cancelled(reason, settlement)) =
      execution.await(execution, 2000)
    reason |> should.equal(execution.CancelRequested)
    // The blocked step was killed by the settle window, not undone: its
    // effect is unknown, so it must show up as interrupted, never undone.
    settlement.interrupted
    |> list.map(fn(address) { address.name })
    |> should.equal(["await_release"])
    settlement.undone |> should.equal([])
  })
}

pub fn cancel_is_idempotent_test() {
  let release_gate = gate.new_gate()

  let assert Ok(workflow) =
    workflows.blocking_workflow(fn() { gate.enter(release_gate) })

  let assert Ok(execution) =
    execution.start(workflow, "ord-1", workflows.bounded_config(None))

  cleanup.with_execution(execution, fn() {
    let assert Ok(_task_pid) = gate.wait_entered(release_gate, 2000)
    execution.cancel(execution)
    // A second cancel on an already-settling run must be a harmless no-op.
    execution.cancel(execution)

    let assert Ok(execution.Cancelled(_reason, _settlement)) =
      execution.await(execution, 2000)
    Nil
  })
}

// ---------------------------------------------------------------------------
// (d) caller-owned error types via map_errors
// ---------------------------------------------------------------------------

pub fn map_errors_reports_application_owned_error_type_test() {
  let assert Ok(workflow) =
    workflows.reported_checkout_workflow(
      orders: sample_orders(),
      unavailable_items: [],
      declined_orders: ["ord-2"],
      undo_fails_for: [],
    )

  let assert Ok(execution.Failed(cause, _settlement)) =
    execution.run(workflow, "ord-2", execution.config())

  case cause {
    execution.StepFailed(_step, workflows.ReportedError(reason)) ->
      reason |> should.equal("payment declined for ord-2: issuer declined")
    _ -> panic as "expected StepFailed with a ReportedError"
  }
}
