/// Runs the order-checkout scenarios and prints a readable trace of each
/// one's outcome. This is the same behavior `gleam test` verifies with
/// assertions; `main` exists so a human (or CI) can see saga actually work
/// end to end through only its public API, outside a test framework.
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import order_consumer/domain.{type Order, Order}
import order_consumer/workflows
import saga
import saga/execution
import saga/observation
import sinal

pub fn main() -> Nil {
  io.println("== saga order_consumer: external acceptance scenarios ==\n")
  scenario_a_shared_data_and_parallel_work()
  scenario_b_failure_and_compensation()
  scenario_c_advanced_config_and_cancellation()
  scenario_d_caller_owned_error_types()
  io.println("\n== all scenarios completed ==")
}

fn sample_orders() -> List(Order) {
  [
    Order("ord-1", "ada", 4200, ["widget", "gadget"]),
    Order("ord-2", "bo", 1500, ["widget"]),
  ]
}

fn scenario_a_shared_data_and_parallel_work() -> Nil {
  io.println("--- (a) shared data + parallel independent work ---")
  let assert Ok(workflow) =
    workflows.checkout_workflow(
      orders: sample_orders(),
      unavailable_items: [],
      declined_orders: [],
      undo_fails_for: [],
    )

  case execution.run(workflow, "ord-1", execution.config()) {
    Ok(execution.Completed(checkout)) ->
      io.println(
        "completed: reserved "
        <> int.to_string(list.length(checkout.inventory.items))
        <> " item(s), authorized "
        <> int.to_string(checkout.payment.amount_cents)
        <> " cents (auth "
        <> checkout.payment.auth_code
        <> ")",
      )
    _other -> io.println("unexpected outcome")
  }
  io.println("")
}

fn scenario_b_failure_and_compensation() -> Nil {
  io.println(
    "--- (b) failure triggers compensation with a retained undo failure ---",
  )
  let assert Ok(workflow) =
    workflows.checkout_workflow(
      orders: sample_orders(),
      unavailable_items: [],
      declined_orders: ["ord-2"],
      undo_fails_for: ["ord-2"],
    )

  case execution.run(workflow, "ord-2", execution.config()) {
    Ok(execution.Failed(cause, settlement)) -> {
      io.println("failed as expected: " <> describe_cause(cause))
      io.println(
        "settlement.undone: "
        <> int.to_string(list.length(settlement.undone))
        <> ", settlement.undo_failures: "
        <> int.to_string(list.length(settlement.undo_failures))
        <> " (retained, not silently dropped)",
      )
    }
    _other -> io.println("unexpected outcome")
  }
  io.println("")
}

fn scenario_c_advanced_config_and_cancellation() -> Nil {
  io.println("--- (c) advanced config, an observer, and cancellation ---")
  let observed_stops =
    sinal.subscriptions([
      sinal.subscription(observation.run_stopped(), fn(_measurements, metadata) {
        io.println(
          "  [observed] run "
          <> int.to_string(metadata.run)
          <> " stopped: "
          <> string_of_outcome_kind(metadata.outcome),
        )
      }),
    ])

  let assert Ok(_completion) =
    sinal.with_subscriptions(observed_stops, fn() {
      let assert Ok(workflow) =
        workflows.blocking_workflow(fn() {
          // A `Subject` may only be received on by the process that
          // created it, and this closure runs inside a fresh task process
          // per attempt, so it creates its own short-lived subject rather
          // than receiving on one made by `main`. Nothing ever sends to
          // it: the step blocks until `cancel` kills its task.
          let never_released = process.new_subject()
          let assert Error(Nil) = process.receive(never_released, 5000)
          Nil
        })

      let config = workflows.bounded_config(Some(2000))
      let assert Ok(execution) = execution.start(workflow, "ord-1", config)

      case execution.progress(execution, 500) {
        Ok(progress) ->
          io.println(
            "progress before cancel: phase=" <> string_of_phase(progress.phase),
          )
        Error(_) -> io.println("progress: timed out")
      }

      execution.cancel(execution)
      case execution.await(execution, 2000) {
        Ok(execution.Cancelled(reason, settlement)) ->
          io.println(
            "cancelled as expected: reason="
            <> string_of_cancel_reason(reason)
            <> ", interrupted="
            <> int.to_string(list.length(settlement.interrupted))
            <> ", undone="
            <> int.to_string(list.length(settlement.undone)),
          )
        _other -> io.println("unexpected outcome")
      }
    })
  io.println("")
}

fn scenario_d_caller_owned_error_types() -> Nil {
  io.println("--- (d) caller-owned error types via map_errors ---")
  let assert Ok(workflow) =
    workflows.reported_checkout_workflow(
      orders: sample_orders(),
      unavailable_items: [],
      declined_orders: ["ord-2"],
      undo_fails_for: [],
    )

  case execution.run(workflow, "ord-2", execution.config()) {
    Ok(execution.Failed(
      execution.StepFailed(_step, workflows.ReportedError(reason)),
      _settlement,
    )) -> io.println("failed with an application-owned error type: " <> reason)
    _other -> io.println("unexpected outcome")
  }
  io.println("")
}

// ---------------------------------------------------------------------------
// Rendering helpers (pure formatting for the trace; not assertions)
// ---------------------------------------------------------------------------

fn describe_cause(cause: execution.Cause(domain.CheckoutError)) -> String {
  case cause {
    execution.StepFailed(step, error) ->
      saga.address_to_string(step)
      <> " failed: "
      <> describe_checkout_error(error)
    execution.StepCrashed(step, _crash) ->
      saga.address_to_string(step) <> " crashed"
    execution.StepTimedOut(step) -> saga.address_to_string(step) <> " timed out"
    execution.RetryLimitReached(step, _last) ->
      saga.address_to_string(step) <> " exhausted its retry budget"
    execution.RetrySuperseded(step, _last) ->
      saga.address_to_string(step)
      <> " could not retry: the run was already settling"
    execution.OutputCrashed(_crash) -> "an output transform crashed"
    execution.DeadlineExceeded -> "the run's deadline was exceeded"
    execution.DefinitionChanged -> "the workflow builder was nondeterministic"
  }
}

fn describe_checkout_error(error: domain.CheckoutError) -> String {
  case error {
    domain.OrderNotFound(order_id) -> "order not found: " <> order_id
    domain.InventoryUnavailable(order_id, _missing) ->
      "inventory unavailable for " <> order_id
    domain.PaymentDeclined(order_id, reason) ->
      "payment declined for " <> order_id <> ": " <> reason
    domain.ShippingUnavailable(order_id) ->
      "shipping unavailable for " <> order_id
  }
}

fn string_of_phase(phase: execution.Phase) -> String {
  case phase {
    execution.Running -> "running"
    execution.Settling -> "settling"
    execution.RollingBack -> "rolling_back"
  }
}

fn string_of_cancel_reason(reason: execution.CancelReason) -> String {
  case reason {
    execution.CancelRequested -> "cancel_requested"
    execution.OwnerExited -> "owner_exited"
  }
}

fn string_of_outcome_kind(kind: observation.OutcomeKind) -> String {
  case kind {
    observation.OutcomeCompleted -> "completed"
    observation.OutcomeFailed -> "failed"
    observation.OutcomeCancelled -> "cancelled"
    observation.OutcomeUnresolved -> "unresolved"
  }
}
