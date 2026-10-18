//// Recover a committed local effect. The application owns the posting record:
//// when a runner stops after the posting committed but before Saga saved the
//// result, the resolver reads the application's record and answers
//// `Completed`, so the posting is not repeated. A missing checkpoint is never
//// evidence that the posting rolled back.
////
//// The posting record here is a process-independent in-memory table standing
//// in for an application transaction. The real PostgreSQL version of this
//// sequence is exercised by the Oversight financial-recovery consumer
//// (https://github.com/gleam-dream/oversight/tree/master/apps/financial_recovery).

import gleam/erlang/process
import gleam/int
import gleam/time/duration
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage/memory

@external(erlang, "persistent_term", "put")
fn table_put(key: String, value: a) -> Nil

@external(erlang, "persistent_term", "get")
fn table_get(key: String, default: a) -> a

fn posting_key(operation: String) -> String {
  "order_consumer_posting:" <> operation
}

fn attempts_key(operation: String) -> String {
  "order_consumer_posting_attempts:" <> operation
}

/// The application's posting record: the operation id is the business identity.
fn recorded_posting(operation: String) -> Result(String, Nil) {
  table_get(posting_key(operation), Error(Nil))
}

fn attempts(operation: String) -> Int {
  table_get(attempts_key(operation), 0)
}

/// The posting commits in the application's own transaction, then the runner
/// is slow to report: a stand-in for a runner lost after the commit.
fn post(operation: String, key: saga.EffectKey) -> Result(String, String) {
  let receipt = "posted-" <> operation
  table_put(attempts_key(operation), attempts(operation) + 1)
  table_put(posting_key(operation), Ok(receipt))
  let _ = key
  process.sleep(10_000)
  Ok(receipt)
}

fn persistence() {
  let text = codec.text()
  let posting =
    saga.effect("post", post)
    |> durable.recoverable(
      version: "1",
      input: text,
      output: text,
      resolve: fn(operation, _key) {
        // Completed only when the application's record establishes the
        // result; an absent record is NotSent only with proof of absence.
        case recorded_posting(operation) {
          Ok(receipt) -> durable.Completed(receipt)
          Error(Nil) -> durable.NotSent
        }
      },
    )
  durable.new(
    saga.define("posting", saga.perform(_, posting)),
    input: text,
    output: text,
    error: text,
    undo_error: text,
  )
}

pub fn run() -> Nil {
  let operation = "op-" <> int.to_string(int.random(1_000_000_000))
  let assert Ok(store) = memory.start()
  let storage = memory.storage(store)
  let id = "posting:" <> operation

  let assert Ok(run) =
    durable.start_or_reconnect(persistence(), storage, id: id, input: operation)
  // The first drive stops while the effect is in flight.
  let assert Error(durable.DriveTimedOut) =
    durable.drive(run, timeout: duration.milliseconds(300))
  let assert 1 = attempts(operation)

  // Reconnect with a compatible definition. The resolver finds the committed
  // posting and restores its result; the posting callback is not repeated.
  let assert Ok(run) = durable.reconnect(persistence(), storage, id: id)
  let assert Ok(execution.Completed("posted-" <> _)) =
    durable.drive(run, timeout: duration.seconds(30))
  let assert 1 = attempts(operation)
  memory.stop(store)
  Nil
}
