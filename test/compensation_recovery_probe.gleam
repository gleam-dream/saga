/// Fresh-VM compensation recovery, invoked by check_durable_restart.sh.
import gleam/erlang/process
import gleam/io
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage/file

@external(erlang, "recovery_probe_ffi", "mode")
fn mode() -> String

@external(erlang, "recovery_probe_ffi", "path")
fn path() -> String

@external(erlang, "recovery_probe_ffi", "write_ledger")
fn write_ledger(path: String, key: String) -> Nil

@external(erlang, "recovery_probe_ffi", "ledger_has")
fn ledger_has(path: String, key: String) -> Bool

pub fn main() -> Nil {
  let path = path()
  let first_vm = mode() == "prepare"
  let text = codec.text()
  let branch = fn(name) {
    saga.step(name, fn(_) { Error("declined") })
    |> saga.compensate_with_key(2, fn(_, _, _, key) {
      case first_vm {
        True -> {
          write_ledger(path <> "." <> name, key)
          process.sleep(60_000)
          saga.Abort("VM should have stopped")
        }
        False -> saga.Abort("compensation repeated")
      }
    })
    |> saga.reconcile_compensation(fn(input, attempt, key) {
      let assert 1 = attempt.number
      let assert 1 = attempt.remaining
      case ledger_has(path <> "." <> name, key) {
        True ->
          saga.CompensationResolved(saga.Continue(input <> name, saga.NoUndo))
        False -> saga.CompensationUnknown
      }
    })
    |> saga.restore_undo(fn(_, _, _) { saga.NoUndo })
    |> saga.recoverable("1", text, text, fn(_, _) { saga.EffectUnknown })
  }
  let assert Ok(workflow) =
    saga.define("vm-compensation", fn(input) {
      saga.both(
        saga.perform(input, branch("a")),
        saga.perform(input, branch("b")),
      )
      |> saga.map(fn(pair) { pair.0 <> pair.1 })
    })
  let assert Ok(persistence) =
    durable.prepare(workflow, "1", text, text, text, text)
  let storage = file.open(path)
  let assert Ok(reference) =
    durable.start_or_reconnect(
      storage,
      "vm-compensation-ref",
      persistence,
      "order",
    )
  let result =
    durable.drive(
      storage,
      reference,
      persistence,
      execution.Config(..execution.config(), max_concurrency: 2),
    )
  case first_vm {
    True -> Nil
    False -> {
      let assert Ok(execution.Completed("orderaorderb")) = result
      let assert Ok(durable.Finished(execution.Completed("orderaorderb"))) =
        durable.read(storage, reference, persistence)
      io.println("RECOVERED")
    }
  }
}
