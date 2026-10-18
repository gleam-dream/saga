//// Step correlation: a step reads its run's correlation from the `EffectKey`
//// it receives and passes it to its own client.

import gleam/erlang/process
import gleam/string
import gleam/time/duration
import order_consumer/usage/checkout.{
  type CheckoutError, describe_checkout_error,
}
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage/memory
import sinal/correlation.{type Correlation}

/// A client that tags its calls with a correlation, like an HTTP client view.
type Client {
  Client(correlation: Correlation, calls: process.Subject(String))
}

fn with_correlation(client: Client, correlation: Correlation) -> Client {
  Client(..client, correlation: correlation)
}

fn call_refund(client: Client, key: String) -> Result(String, CheckoutError) {
  let tag = correlation.to_string(client.correlation)
  process.send(client.calls, tag <> " " <> key)
  Ok("refund-" <> key)
}

/// Every run has a correlation: a local run without one gets a fresh
/// correlation, a durable run `from_key(id)`.
pub fn run() -> Nil {
  let calls = process.new_subject()
  let client = Client(correlation.from_key("unset"), calls)
  let step =
    saga.effect("refund", fn(order: String, key) {
      let client = with_correlation(client, saga.correlation_of(key))
      call_refund(client, saga.idempotency_key(key) <> ":" <> order)
    })
    |> durable.recoverable(
      version: "1",
      input: codec.text(),
      output: codec.text(),
      resolve: fn(_, _) { durable.MaybeSent },
    )
  let workflow = saga.define("refund", saga.perform(_, step))

  // A local run uses the correlation of its configuration.
  let config =
    execution.config()
    |> execution.with_correlation(correlation.from_key("o-9"))
  let assert Ok(execution.Completed(_)) = execution.run(workflow, "o-9", config)
  let assert Ok("o-9 " <> _) = process.receive(calls, 1000)

  // A local run without one gets a fresh correlation (32 hex characters).
  let assert Ok(execution.Completed(_)) =
    execution.run(workflow, "o-9", execution.config())
  let assert Ok(unset) = process.receive(calls, 1000)
  let assert [tag, _] = string.split(unset, " ")
  let assert 32 = string.length(tag)

  // A durable run without one carries `correlation.from_key(id)`.
  let text = codec.text()
  let persistence =
    durable.new(
      workflow
        |> saga.map_errors(describe_checkout_error, describe_checkout_error),
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
      id: "refund:o-9",
      input: "o-9",
    )
  let assert Ok(execution.Completed(_)) =
    durable.drive(run, timeout: duration.seconds(30))
  let assert Ok("refund:o-9 " <> _) = process.receive(calls, 1000)
  memory.stop(store)
  Nil
}
