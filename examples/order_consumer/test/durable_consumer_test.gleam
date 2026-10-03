//// The durable path, from outside saga: caller-owned types saved with JSON
//// codecs, a payment that may have been sent, the `Run` handle, bounded
//// `drive`, error classification and the adapter conformance suite.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/option.{None, Some}
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage
import saga/storage/conformance
import saga/storage/memory

pub type Charge {
  Charge(order: String, amount: Int)
}

pub type Receipt {
  Receipt(id: String, idempotency: String)
}

pub type PayError {
  Declined
  MaybeCharged
}

fn charge_codec() -> codec.Codec(Charge) {
  codec.json(
    "charge-1",
    fn(charge: Charge) {
      Ok(
        json.object([
          #("order", json.string(charge.order)),
          #("amount", json.int(charge.amount)),
        ]),
      )
    },
    {
      use order <- decode.field("order", decode.string)
      use amount <- decode.field("amount", decode.int)
      decode.success(Charge(order:, amount:))
    },
  )
}

fn receipt_codec() -> codec.Codec(Receipt) {
  codec.json(
    "receipt-1",
    fn(receipt: Receipt) {
      Ok(
        json.object([
          #("id", json.string(receipt.id)),
          #("idempotency", json.string(receipt.idempotency)),
        ]),
      )
    },
    {
      use id <- decode.field("id", decode.string)
      use idempotency <- decode.field("idempotency", decode.string)
      decode.success(Receipt(id:, idempotency:))
    },
  )
}

fn error_codec() -> codec.Codec(PayError) {
  codec.json(
    "pay-error-1",
    fn(error) {
      Ok(
        json.string(case error {
          Declined -> "declined"
          MaybeCharged -> "maybe_charged"
        }),
      )
    },
    decode.string
      |> decode.then(fn(tag) {
        case tag {
          "declined" -> decode.success(Declined)
          "maybe_charged" -> decode.success(MaybeCharged)
          _ -> decode.failure(Declined, "PayError")
        }
      }),
  )
}

/// A payment provider fake: the first call times out after charging, every
/// later call with the same idempotency key returns the original receipt.
fn payment_workflow(
  calls: process.Subject(String),
) -> saga.Workflow(Charge, Receipt, PayError, Nil) {
  let pay =
    saga.effect("pay", fn(charge: Charge, key: saga.EffectKey) {
      process.send(calls, key.idempotency)
      case key.attempt {
        1 -> Error(MaybeCharged)
        _ -> Ok(Receipt("rcpt-" <> charge.order, key.idempotency))
      }
    })
    |> saga.unknown_when(fn(error) { error == MaybeCharged })
    |> saga.compensate(max_attempts: 2, with: fn(failed) {
      case failed.failure {
        saga.Returned(MaybeCharged) -> saga.Retry
        saga.Returned(error) -> saga.Abort(error)
        saga.Crashed(_) | saga.TimedOut -> saga.Hold(MaybeCharged)
      }
    })
    |> durable.restore_undo(fn(_undo) { saga.NoUndo })
    |> durable.resolve_compensation(fn(_charge, _key) { None })
    |> durable.recoverable(
      version: "1",
      input: charge_codec(),
      output: receipt_codec(),
      resolve: fn(_charge, _key) { durable.MaybeSent },
    )
  let assert Ok(workflow) = saga.define("payment", saga.perform(_, pay))
  workflow
}

pub fn durable_payment_with_caller_types_test() {
  let calls = process.new_subject()
  let workflow = payment_workflow(calls)
  let assert Ok(persistence) =
    durable.new(
      workflow,
      version: "1",
      input: charge_codec(),
      output: receipt_codec(),
      error: error_codec(),
      undo_error: codec.json(
        "nil-1",
        fn(_) { Ok(json.null()) },
        decode.success(Nil),
      ),
    )
  let assert Ok(store) = memory.start()
  let backend = memory.storage(store)
  let assert Ok(run) =
    durable.start_or_reconnect(
      persistence,
      backend,
      id: "payment:o-1",
      input: Charge("o-1", 4200),
    )
  let assert Ok(outcome) = durable.drive(run, timeout: 10_000)
  // The "maybe charged" first attempt is reported, and the retry reused the
  // provider idempotency key.
  let assert execution.CompletedWithUnknownEffects(receipt, [unknown]) = outcome
  unknown.ending |> should_be(execution.ActionReturnedUnknown)
  let assert Ok(first) = process.receive(calls, 1000)
  let assert Ok(second) = process.receive(calls, 1000)
  first |> should_be(second)
  receipt.idempotency |> should_be(first)
  let assert Ok(durable.Finished(_)) = durable.read(run)
  memory.stop(store)
}

pub fn durable_errors_are_classified_test() {
  let calls = process.new_subject()
  let assert Ok(store) = memory.start()
  let text = codec.text()
  let assert Error(durable.NotPersistable(problems)) =
    durable.new(
      payment_workflow(calls),
      version: "1",
      input: charge_codec(),
      output: receipt_codec(),
      error: error_codec(),
      undo_error: codec.new("", fn(_) { Ok("") }, fn(_) { Ok(Nil) }),
    )
  problems |> should_be([durable.EmptyCodecVersion(durable.RunUndoError)])
  let assert Ok(echo_workflow) =
    saga.define("echo", fn(input) {
      saga.perform(
        input,
        saga.step("echo", fn(value: String) { Ok(value) })
          |> durable.recoverable(
            version: "1",
            input: text,
            output: text,
            resolve: fn(_, _) { durable.MaybeSent },
          ),
      )
    })
  let assert Ok(persistence) =
    durable.new(
      echo_workflow,
      version: "1",
      input: text,
      output: text,
      error: text,
      undo_error: text,
    )
  let assert Ok(run) =
    durable.start_or_reconnect(
      persistence,
      memory.storage(store),
      id: "echo",
      input: "a",
    )
  let assert Error(error) =
    durable.start_or_reconnect(
      persistence,
      memory.storage(store),
      id: "echo",
      input: "b",
    )
  durable.error_kind(error) |> should_be(durable.Incompatible)
  let assert Error(durable.InvalidTimeout(0)) = durable.drive(run, timeout: 0)
  let assert Ok(execution.Completed("a")) = durable.drive(run, timeout: 5000)
  memory.stop(store)
}

pub fn public_durable_choice_test() {
  let text = codec.text()
  let assert Ok(workflow) =
    saga.define("consumer", fn(input) {
      saga.choose(
        input,
        "route",
        saga.map(input, fn(_) { False }),
        fn(port) {
          saga.perform(
            port,
            saga.step("left", fn(_) { Error("unchosen branch ran") })
              |> durable.recoverable(
                version: "1",
                input: text,
                output: text,
                resolve: fn(_, _) { durable.MaybeSent },
              ),
          )
        },
        fn(port) {
          saga.perform(
            port,
            saga.step("right", fn(value) { Ok(value <> "-right") })
              |> durable.recoverable(
                version: "1",
                input: text,
                output: text,
                resolve: fn(_, _) { durable.MaybeSent },
              ),
          )
        },
      )
    })
  let assert Ok(execution.Completed("input-right")) =
    execution.run(workflow, "input", execution.config())
  let assert Ok(persistence) =
    durable.new(
      workflow,
      version: "1",
      input: text,
      output: text,
      error: text,
      undo_error: text,
    )
  let assert Ok(store) = memory.start()
  let assert Ok(run) =
    durable.start_or_reconnect(
      persistence,
      memory.storage(store),
      id: "consumer-ref",
      input: "input",
    )
  let assert Ok(execution.Completed("input-right")) =
    durable.drive(run, timeout: 5000)
  let assert Ok(durable.Finished(execution.Completed("input-right"))) =
    durable.read(run)
  memory.stop(store)
}

pub fn public_adapter_contract_test() {
  let assert Ok(Nil) =
    conformance.run(
      fn() {
        let assert Ok(store) = memory.start()
        Ok(
          conformance.fixture(memory.storage(store), cleanup: fn() {
            memory.stop(store)
          }),
        )
      },
      timeout: 5000,
      owner_loss_within: 200,
    )
}

/// A supervised store is found by name.
pub fn supervised_memory_store_test() {
  let name = process.new_name("consumer_store")
  let child = memory.supervised(name)
  let _ = child
  let store = memory.named(name)
  // Not started: operations report the store as unavailable.
  let assert Error(storage.Unavailable(_)) =
    durable.unfinished(memory.storage(store), limit: 1)
    |> unwrap_storage
}

fn unwrap_storage(
  result: Result(a, durable.Error),
) -> Result(a, storage.Error) {
  case result {
    Ok(value) -> Ok(value)
    Error(durable.StorageFailure(error)) -> Error(error)
    Error(_) -> Error(storage.Corrupt)
  }
}

pub fn public_compensation_configuration_test() {
  let text = codec.text()
  let step =
    saga.step("recovered", fn(_) { Error("declined") })
    |> saga.compensate(max_attempts: 1, with: fn(failed) {
      saga.Continue(failed.input, saga.NoUndo)
    })
    |> durable.resolve_compensation(fn(input, _) {
      Some(saga.Continue(input, saga.NoUndo))
    })
    |> durable.restore_undo(fn(_undo) { saga.NoUndo })
    |> durable.recoverable(
      version: "1",
      input: text,
      output: text,
      resolve: fn(_, _) { durable.MaybeSent },
    )
  let assert Ok(workflow) =
    saga.define("compensation", fn(input) { saga.perform(input, step) })
  let assert Ok(persistence) =
    durable.new(
      workflow,
      version: "1",
      input: text,
      output: text,
      error: text,
      undo_error: text,
    )
  let assert Ok(store) = memory.start()
  let assert Ok(run) =
    durable.start_or_reconnect(
      persistence,
      memory.storage(store),
      id: "compensation",
      input: "x",
    )
  let assert Ok(execution.Completed("x")) = durable.drive(run, timeout: 5000)
  memory.stop(store)
}

fn should_be(actual: a, expected: a) -> Nil {
  case actual == expected {
    True -> Nil
    False -> panic as "values differ"
  }
}
