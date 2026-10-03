//// The README's examples, compiled and run with fake services, so the
//// documented common path cannot drift from the API.

import gleam/dynamic/decode
import gleam/json
import gleam/time/duration
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage/memory
import saga/telemetry
import sinal
import sinal/correlation

pub type CheckoutError {
  OutOfStock
  Declined
  MaybeCharged
}

fn reserve(order: String) -> Result(String, CheckoutError) {
  Ok("hold-" <> order)
}

fn release(_hold: String) -> Result(Nil, CheckoutError) {
  Ok(Nil)
}

fn charge(
  reservation: String,
  idempotency_key key: String,
) -> Result(String, CheckoutError) {
  case reservation {
    "hold-declined" -> Error(Declined)
    _ -> Ok("receipt-" <> key)
  }
}

pub fn checkout() {
  let reserve =
    saga.step("reserve_inventory", reserve)
    |> saga.undo(fn(undo) { release(undo.output) })
  let charge =
    saga.effect("charge_payment", fn(reservation, key) {
      // `key.idempotency` is the same for every attempt of this step.
      charge(reservation, idempotency_key: key.idempotency)
    })
    |> saga.unknown_when(fn(error) { error == MaybeCharged })
    |> saga.compensate(max_attempts: 3, with: fn(failed) {
      case failed.failure {
        saga.Returned(MaybeCharged) ->
          saga.RetryAfter(duration.milliseconds(500))
        saga.Returned(error) -> saga.Abort(error)
        saga.Crashed(_) | saga.TimedOut -> saga.Hold(MaybeCharged)
      }
    })
  saga.define("checkout", fn(order) {
    order |> saga.perform(reserve) |> saga.perform(charge)
  })
}

pub fn run_checkout(workflow, order) {
  case execution.run(workflow, order, execution.config()) {
    Ok(execution.Completed(receipt)) -> Ok(receipt)
    Ok(outcome) -> Error(execution.unknown_effects(outcome))
    Error(_run_error) -> Error([])
  }
}

pub fn readme_common_path_test() {
  let workflow = checkout()
  let assert Ok("receipt-" <> _) = run_checkout(workflow, "o-1")
  let assert Error([]) = run_checkout(workflow, "declined")
}

pub fn readme_configuration_test() {
  let workflow = checkout()
  let config =
    execution.config()
    |> execution.with_max_concurrency(4)
    |> execution.with_deadline(execution.After(duration.seconds(30)))
    |> execution.with_correlation(correlation.unique())
  let assert Ok(exec) = execution.start(workflow, "o-2", config)
  let _ = execution.progress(exec, timeout: duration.seconds(1))
  let assert Ok(execution.Completed(_)) =
    execution.await(exec, timeout: duration.seconds(10))
}

type Lookup {
  Charged(String)
  NoCharge
}

fn lookup_charge(_order: String, _key: String) -> Result(Lookup, Nil) {
  Ok(NoCharge)
}

fn lookup_refund(_undo: saga.UndoRequest(String, String)) {
  durable.Completed(Nil)
}

fn refund(_receipt: String, _key: String) -> Result(Nil, String) {
  Ok(Nil)
}

pub fn readme_durable_run_test() {
  let order =
    codec.json("order-1", fn(id) { Ok(json.string(id)) }, decode.string)
  let text = codec.text()
  let charge =
    saga.effect("charge", fn(order, key: saga.EffectKey) {
      Ok("receipt-" <> order <> "-" <> key.idempotency)
    })
    |> saga.undo(fn(undo) { refund(undo.output, undo.key.idempotency) })
    |> durable.recoverable(
      version: "1",
      input: order,
      output: text,
      resolve: fn(order, key) {
        case lookup_charge(order, key.idempotency) {
          Ok(Charged(receipt)) -> durable.Completed(receipt)
          Ok(NoCharge) -> durable.NotSent
          Error(_) -> durable.MaybeSent
        }
      },
    )
    |> durable.resolve_undo(lookup_refund)
  let workflow = saga.define("checkout", saga.perform(_, charge))
  let persistence =
    durable.new(
      workflow,
      input: order,
      output: text,
      error: text,
      undo_error: text,
    )
  let assert Ok(store) = memory.start()
  let assert Ok(run) =
    durable.start_or_reconnect(
      persistence,
      memory.storage(store),
      id: "checkout:o-1",
      input: "o-1",
    )
  let handled = case durable.drive(run, timeout: duration.seconds(30)) {
    Ok(execution.Completed(_)) -> "completed"
    Ok(_) -> "other outcome"
    Error(error) ->
      case durable.error_kind(error) {
        durable.Busy | durable.Transient -> "retry later"
        durable.NeedsReconciliation -> "alert"
        durable.Incompatible | durable.Defect -> durable.describe_error(error)
      }
  }
  let assert "completed" = handled
  memory.stop(store)
}

pub fn readme_telemetry_test() {
  let workflow = checkout()
  let attachment =
    sinal.observe(telemetry.run_stopped(), fn(_measurements, metadata) {
      let _ = #(metadata.correlation, metadata.execution, metadata.outcome)
      Nil
    })
  let assert Ok(_) = run_checkout(workflow, "o-3")
  let _ = sinal.detach(attachment)
  Nil
}

fn describe_checkout_error(error: CheckoutError) -> String {
  case error {
    OutOfStock -> "out of stock"
    Declined -> "declined"
    MaybeCharged -> "the charge may have been taken"
  }
}

pub fn readme_unknown_effect_test() {
  let reserve =
    saga.step("reserve_inventory", reserve)
    |> saga.undo(fn(undo) { release(undo.output) })
  let charge =
    saga.effect("charge_payment", fn(_reservation, _key) { Error(MaybeCharged) })
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
}
