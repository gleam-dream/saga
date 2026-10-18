//// An effect whose outcome is unknown: the run holds earlier work for
//// reconciliation by default, and `on_unknown(RollBack)` opts into releasing it.

import order_consumer/usage/checkout.{
  type CheckoutError, MaybeCharged, describe_checkout_error, release, reserve,
}
import saga
import saga/execution

fn maybe_charged(
  _reservation: String,
  _key: saga.EffectKey,
) -> Result(String, CheckoutError) {
  Error(MaybeCharged)
}

pub fn run() -> Nil {
  let reserve =
    saga.step("reserve_inventory", reserve)
    |> saga.undo(fn(undo) { release(undo.output) })
  let charge =
    saga.effect("charge_payment", maybe_charged)
    |> saga.unknown_when(fn(error) { error == MaybeCharged })
  let checkout = fn(charge) {
    saga.define("checkout", fn(order) {
      order |> saga.perform(reserve) |> saga.perform(charge)
    })
  }
  let reserved = saga.StepAddress([], "reserve_inventory", 1)
  // By default the run holds the reservation for reconciliation.
  let assert Ok(execution.Unresolved(_, MaybeCharged, settlement)) =
    execution.run(checkout(charge), "o-4", execution.config())
  let assert [held] = settlement.held
  let assert True = held == reserved
  // `on_unknown(RollBack)` opts into releasing it.
  let assert Ok(execution.Failed(cause, settlement)) =
    execution.run(
      checkout(charge |> saga.on_unknown(saga.RollBack)),
      "o-4",
      execution.config(),
    )
  let assert [undone] = settlement.undone
  let assert True = undone == reserved
  let assert "step charge_payment returned an error: the charge may have been taken" =
    execution.describe_cause(cause, error: describe_checkout_error)
  Nil
}
