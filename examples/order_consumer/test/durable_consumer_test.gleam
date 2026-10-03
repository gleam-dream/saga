import gleam/option.{Some}
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage/conformance
import saga/storage/memory

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
    durable.prepare(workflow, "1", text, text, text, text)
  let memory = memory.new()
  let storage = memory.storage(memory)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "consumer-ref", persistence, "input")
  let assert Ok(execution.Completed("input-right")) =
    durable.drive(storage, reference, persistence, execution.config())
  let assert Ok(durable.Finished(execution.Completed("input-right"))) =
    durable.read(storage, reference, persistence)
  memory.close(memory)
}

pub fn public_adapter_contract_test() {
  let assert Ok(Nil) =
    conformance.run(
      fn() {
        let backend = memory.new()
        Ok(
          conformance.Fixture(memory.storage(backend), fn() {
            memory.close(backend)
          }),
        )
      },
      5000,
    )
}

pub fn public_compensation_configuration_test() {
  let text = codec.text()
  let step =
    saga.step("recovered", fn(_) { Error("declined") })
    |> saga.compensate(max_attempts: 1, with: fn(failed) {
      let saga.FailedAttempt(input: input, ..) = failed

      saga.Continue(input, saga.NoUndo)
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
    durable.prepare(workflow, "1", text, text, text, text)
  let memory = memory.new()
  let storage = memory.storage(memory)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "compensation", persistence, "x")
  let assert Ok(execution.Completed("x")) =
    durable.drive(storage, reference, persistence, execution.config())
  memory.close(memory)
}
