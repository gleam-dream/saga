/// Order-checkout workflows built entirely from saga's public API
/// (`import saga`, `import saga/execution`) and this application's own
/// records and errors (`order_consumer/domain`). No `saga/internal/*`
/// import is legal from here — that is what makes this package an external
/// acceptance test of saga's public facade, not a white-box test.
import gleam/option.{type Option, None, Some}
import order_consumer/domain.{
  type CheckoutError, type InventoryHold, type Order, type PaymentAuthorization,
  type UndoError, InventoryHold, InventoryUnavailable, PaymentAuthorization,
  PaymentDeclined, RefundFailed, ReleaseInventoryFailed,
}
import saga.{type Port, type Step, type Workflow}
import saga/execution.{type Config}

// ---------------------------------------------------------------------------
// Shared steps
// ---------------------------------------------------------------------------

/// Looks up an order by id in the in-memory catalog `orders`. Returns
/// `OrderNotFound` for an unknown id.
fn load_order_step(
  orders: List(Order),
) -> Step(String, Order, CheckoutError, u) {
  saga.step("load_order", fn(order_id: String) {
    case list_find(orders, fn(order) { order.id == order_id }) {
      Ok(order) -> Ok(order)
      Error(Nil) -> Error(domain.OrderNotFound(order_id))
    }
  })
}

fn list_find(items: List(a), matches: fn(a) -> Bool) -> Result(a, Nil) {
  case items {
    [] -> Error(Nil)
    [first, ..rest] ->
      case matches(first) {
        True -> Ok(first)
        False -> list_find(rest, matches)
      }
  }
}

fn list_filter(items: List(a), keep: fn(a) -> Bool) -> List(a) {
  case items {
    [] -> []
    [first, ..rest] ->
      case keep(first) {
        True -> [first, ..list_filter(rest, keep)]
        False -> list_filter(rest, keep)
      }
  }
}

fn list_contains(items: List(a), value: a) -> Bool {
  case items {
    [] -> False
    [first, ..rest] -> first == value || list_contains(rest, value)
  }
}

/// Reserves every item on the order. Undoable: a completed reservation is
/// released on rollback. `undo_fails_for` names order ids whose release
/// action itself fails, so a caller can exercise a retained undo failure.
fn reserve_inventory_step(
  unavailable: List(String),
  undo_fails_for: List(String),
) -> Step(Order, InventoryHold, CheckoutError, UndoError) {
  saga.step("reserve_inventory", fn(order: Order) {
    let missing =
      list_filter(order.items, fn(item) { list_contains(unavailable, item) })
    case missing {
      [] -> Ok(InventoryHold(order.id, order.items))
      _ -> Error(InventoryUnavailable(order.id, missing))
    }
  })
  |> saga.undo(fn(undo) {
    let saga.UndoRequest(output: hold, ..) = undo

    case list_contains(undo_fails_for, hold.order_id) {
      True -> Error(ReleaseInventoryFailed(hold.order_id, "warehouse offline"))
      False -> Ok(Nil)
    }
  })
}

/// Authorizes payment for the order's total. Undoable: a completed
/// authorization is refunded on rollback.
fn authorize_payment_step(
  declined: List(String),
) -> Step(Order, PaymentAuthorization, CheckoutError, UndoError) {
  saga.step("authorize_payment", fn(order: Order) {
    case list_contains(declined, order.id) {
      True -> Error(PaymentDeclined(order.id, "issuer declined"))
      False ->
        Ok(PaymentAuthorization(
          order_id: order.id,
          amount_cents: order.total_cents,
          auth_code: "auth-" <> order.id,
        ))
    }
  })
  |> saga.undo(fn(_undo) { Ok(Nil) })
}

// ---------------------------------------------------------------------------
// Scenario (a): shared data + parallel independent work
//
// `load_order` runs once and its output `Port` is shared by two independent
// consumers (`reserve_inventory` and `authorize_payment`), which run
// concurrently because neither depends on the other's output.
// ---------------------------------------------------------------------------

/// The checkout workflow's final result: the reservation and the payment
/// authorization for one order.
pub type Checkout {
  Checkout(inventory: InventoryHold, payment: PaymentAuthorization)
}

/// Builds the checkout workflow. `orders` is the fake catalog `load_order`
/// searches, `unavailable_items` makes `reserve_inventory` fail for any
/// order that includes one of them, and `declined_orders` makes
/// `authorize_payment` fail for those order ids. `undo_fails_for` makes a
/// completed reservation's release action itself fail on rollback, for the
/// compensation-with-retained-undo-failure scenario. One builder covers
/// success, an inventory failure, and a payment failure.
pub fn checkout_workflow(
  orders orders: List(Order),
  unavailable_items unavailable_items: List(String),
  declined_orders declined_orders: List(String),
  undo_fails_for undo_fails_for: List(String),
) -> Workflow(String, Checkout, CheckoutError, UndoError) {
  saga.define("checkout", fn(input: Port(String, CheckoutError, UndoError)) {
    let order = input |> saga.perform(load_order_step(orders))
    let inventory =
      order
      |> saga.perform(reserve_inventory_step(unavailable_items, undo_fails_for))
    let payment = order |> saga.perform(authorize_payment_step(declined_orders))
    saga.both(inventory, payment)
    |> saga.map(fn(pair) { Checkout(pair.0, pair.1) })
  })
}

// ---------------------------------------------------------------------------
// Scenario (c): advanced config + cancellation
//
// A single blocking step that only returns once `release` is called (or the
// run is cancelled/times out), so a caller can start the run, observe
// `Attempting` via `progress`, then either release it or `cancel` it.
// ---------------------------------------------------------------------------

/// Builds a one-step workflow whose step blocks on `wait_for_release` until
/// it is told to proceed. Used to demonstrate `start`/`progress`/`cancel`/
/// `await` against a step that is actually in flight when control is
/// exercised.
pub fn blocking_workflow(
  wait_for_release: fn() -> Nil,
) -> Workflow(String, String, CheckoutError, UndoError) {
  saga.define(
    "blocking_checkout",
    fn(input: Port(String, CheckoutError, UndoError)) {
      input
      |> saga.perform(
        saga.step("await_release", fn(order_id: String) {
          wait_for_release()
          Ok(order_id)
        }),
      )
    },
  )
}

/// A config with a tight concurrency limit and an optional deadline, for the
/// advanced-configuration scenario. Demonstrates composing over the public
/// opaque `Config` with the `execution.with_*` setters.
pub fn bounded_config(deadline_ms: Option(Int)) -> Config {
  let config =
    execution.config()
    |> execution.with_max_concurrency(1)
    |> execution.with_settle_timeout(200)
    |> execution.with_cleanup_timeout(200)
  case deadline_ms {
    Some(ms) -> execution.with_deadline(config, ms)
    None -> config
  }
}

// ---------------------------------------------------------------------------
// Scenario (d): caller-owned error types via map_step_errors / map_errors
// ---------------------------------------------------------------------------

/// The application's own, narrower error vocabulary for the parts of
/// checkout it chooses to expose to a wrapping workflow: everything from
/// `CheckoutError` collapses to a `String` reason. This shows a consumer
/// picking its own reporting type instead of being handed saga's or the
/// original step's error type.
pub type ReportedError {
  ReportedError(reason: String)
}

pub type ReportedUndoError {
  ReportedUndoError(reason: String)
}

fn describe_checkout_error(error: CheckoutError) -> String {
  case error {
    domain.OrderNotFound(order_id) -> "order not found: " <> order_id
    InventoryUnavailable(order_id, _missing) ->
      "inventory unavailable for " <> order_id
    PaymentDeclined(order_id, reason) ->
      "payment declined for " <> order_id <> ": " <> reason
    domain.ShippingUnavailable(order_id) ->
      "shipping unavailable for " <> order_id
  }
}

fn describe_undo_error(error: UndoError) -> String {
  case error {
    ReleaseInventoryFailed(order_id, reason) ->
      "release failed for " <> order_id <> ": " <> reason
    RefundFailed(order_id, reason) ->
      "refund failed for " <> order_id <> ": " <> reason
  }
}

/// Wraps `checkout_workflow`'s errors into `ReportedError`/`ReportedUndoError`
/// with `saga/execution.map_errors`, purely through public imports.
pub fn reported_checkout_workflow(
  orders orders: List(Order),
  unavailable_items unavailable_items: List(String),
  declined_orders declined_orders: List(String),
  undo_fails_for undo_fails_for: List(String),
) -> Workflow(String, Checkout, ReportedError, ReportedUndoError) {
  checkout_workflow(
    orders:,
    unavailable_items:,
    declined_orders:,
    undo_fails_for:,
  )
  |> saga.map_errors(
    error: fn(e) { ReportedError(describe_checkout_error(e)) },
    undo_error: fn(u) { ReportedUndoError(describe_undo_error(u)) },
  )
}
