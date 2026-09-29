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
              |> saga.recoverable("1", text, text, fn(_, _) {
                saga.EffectUnknown
              }),
          )
        },
        fn(port) {
          saga.perform(
            port,
            saga.step("right", fn(value) { Ok(value <> "-right") })
              |> saga.recoverable("1", text, text, fn(_, _) {
                saga.EffectUnknown
              }),
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
    |> saga.compensate_with_key(1, fn(input, _, _, _) {
      saga.Continue(input, saga.NoUndo)
    })
    |> saga.reconcile_compensation(fn(input, _, _) {
      saga.CompensationResolved(saga.Continue(input, saga.NoUndo))
    })
    |> saga.restore_undo(fn(_, _, _) { saga.NoUndo })
    |> saga.recoverable("1", text, text, fn(_, _) { saga.EffectUnknown })
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
