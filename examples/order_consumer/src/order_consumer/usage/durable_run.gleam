//// A durable run: saved progress outlives the driver. Attach codecs and an
//// attempt resolver to each step, start or reconnect under a stable id, drive,
//// and classify any error with `durable.error_kind`.

import gleam/dynamic/decode
import gleam/json
import gleam/time/duration
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage/memory

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

/// Build the persistence for a one-step payment: the effect, its undo, and a
/// resolver that asks the downstream system what happened to an interrupted
/// attempt (`Completed`, `NotSent`, or `MaybeSent`).
pub fn persistence() {
  let order =
    codec.json("order-1", fn(id) { Ok(json.string(id)) }, decode.string)
  let text = codec.text()
  let charge =
    saga.effect("charge", fn(order, key: saga.EffectKey) {
      Ok("receipt-" <> order <> "-" <> saga.idempotency_key(key))
    })
    |> saga.undo(fn(undo) {
      refund(undo.output, saga.idempotency_key(undo.key))
    })
    |> durable.recoverable(
      version: "1",
      input: order,
      output: text,
      resolve: fn(order, key) {
        case lookup_charge(order, saga.idempotency_key(key)) {
          Ok(Charged(receipt)) -> durable.Completed(receipt)
          Ok(NoCharge) -> durable.NotSent
          Error(_) -> durable.MaybeSent
        }
      },
    )
    |> durable.resolve_undo(lookup_refund)
  let workflow = saga.define("checkout", saga.perform(_, charge))
  durable.new(
    workflow,
    input: order,
    output: text,
    error: text,
    undo_error: text,
  )
}

/// What an application does with a `durable.Error`, by its closed kind.
pub fn action_for(error: durable.Error) -> String {
  case durable.error_kind(error) {
    durable.Busy | durable.Transient -> "retry later"
    durable.NeedsReconciliation -> "alert"
    durable.Incompatible | durable.Defect -> durable.describe_error(error)
  }
}

pub fn run() -> Nil {
  let assert Ok(store) = memory.start()
  let assert Ok(run) =
    durable.start_or_reconnect(
      persistence(),
      memory.storage(store),
      id: "checkout:o-1",
      input: "o-1",
    )
  let handled = case durable.drive(run, timeout: duration.seconds(30)) {
    Ok(execution.Completed(_)) -> "completed"
    Ok(_) -> "other outcome"
    Error(error) -> action_for(error)
  }
  let assert "completed" = handled
  memory.stop(store)
  Nil
}
