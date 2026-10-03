//// One JSON Blueprint codec serves a saga checkpoint codec through
//// `codec.json` and `result.map_error`.

import gleam/result
import json/blueprint/codec as blueprint
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage/memory

pub type Order {
  Order(id: String, quantity: Int)
}

fn order_blueprint() -> blueprint.Codec(Order) {
  use id <- blueprint.field("id", blueprint.string(), get: fn(o: Order) { o.id })
  use quantity <- blueprint.field(
    "quantity",
    blueprint.int(),
    get: fn(o: Order) { o.quantity },
  )
  blueprint.success(Order(id:, quantity:))
}

fn order_codec() -> codec.Codec(Order) {
  codec.json(
    "order-1",
    fn(order) {
      blueprint.to_json(order_blueprint(), order)
      |> result.map_error(blueprint.describe_encode_error)
    },
    blueprint.decoder(order_blueprint()),
  )
}

pub fn blueprint_codec_saves_a_durable_input_test() {
  let text = codec.text()
  let assert Ok(workflow) =
    saga.define("blueprint", fn(input) {
      saga.perform(
        input,
        saga.step("ship", fn(order: Order) { Ok(order) })
          |> durable.recoverable(
            version: "1",
            input: order_codec(),
            output: order_codec(),
            resolve: fn(_, _) { durable.MaybeSent },
          ),
      )
    })
  let assert Ok(persistence) =
    durable.new(
      workflow,
      version: "1",
      input: order_codec(),
      output: order_codec(),
      error: text,
      undo_error: text,
    )
  let assert Ok(store) = memory.start()
  let assert Ok(run) =
    durable.start_or_reconnect(
      persistence,
      memory.storage(store),
      id: "blueprint",
      input: Order("o-1", 2),
    )
  let assert Ok(execution.Completed(Order("o-1", 2))) =
    durable.drive(run, timeout: 5000)
  memory.stop(store)
}
