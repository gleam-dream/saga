/// Application-owned records and errors for the order-checkout example.
/// Nothing here imports saga: this module is what an application brings to
/// the workflow, not part of saga's own vocabulary.
pub type Order {
  Order(id: String, customer: String, total_cents: Int, items: List(String))
}

pub type InventoryHold {
  InventoryHold(order_id: String, items: List(String))
}

pub type PaymentAuthorization {
  PaymentAuthorization(order_id: String, amount_cents: Int, auth_code: String)
}

pub type Shipment {
  Shipment(order_id: String, tracking_code: String)
}

/// Everything that can go wrong in the checkout workflow, from the
/// application's own point of view. Saga never sees this type directly: each
/// step returns `Result(_, CheckoutError)` and saga only threads it through.
pub type CheckoutError {
  OrderNotFound(order_id: String)
  InventoryUnavailable(order_id: String, missing: List(String))
  PaymentDeclined(order_id: String, reason: String)
  ShippingUnavailable(order_id: String)
}

/// Everything that can go wrong while undoing a step's effect.
pub type UndoError {
  ReleaseInventoryFailed(order_id: String, reason: String)
  RefundFailed(order_id: String, reason: String)
}
