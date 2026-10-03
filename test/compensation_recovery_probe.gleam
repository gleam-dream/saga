/// Fresh-VM compensation recovery, invoked by check_durable_restart.sh.
import gleam/erlang/process
import gleam/io
import gleam/option.{None, Some}
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
    |> saga.compensate(max_attempts: 2, with: fn(failed) {
      let key = failed.key.attempt_key

      case first_vm {
        True -> {
          write_ledger(path <> "/" <> name, key)
          process.sleep(60_000)
          saga.Abort("VM should have stopped")
        }
        False -> saga.Abort("compensation repeated")
      }
    })
    |> durable.resolve_compensation(fn(input, key) {
      let assert 1 = key.attempt
      case ledger_has(path <> "/" <> name, key.attempt_key) {
        True -> Some(saga.Continue(input <> name, saga.NoUndo))
        False -> None
      }
    })
    |> durable.restore_undo(fn(_undo) { saga.NoUndo })
    |> durable.recoverable(
      version: "1",
      input: text,
      output: text,
      resolve: fn(_, _) { durable.MaybeSent },
    )
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
    durable.new(
      workflow,
      version: "1",
      input: text,
      output: text,
      error: text,
      undo_error: text,
    )
  let storage = file.open(path)
  let assert Ok(run) =
    durable.start_or_reconnect(
      persistence
        |> durable.with_config(
          execution.config() |> execution.with_max_concurrency(2),
        ),
      storage,
      id: "vm-compensation-ref",
      input: "order",
    )
  let result = durable.drive(run, timeout: 60_000)
  case first_vm {
    True -> Nil
    False -> {
      let assert Ok(execution.Completed("orderaorderb")) = result
      let assert Ok(durable.Finished(execution.Completed("orderaorderb"))) =
        durable.read(run)
      io.println("RECOVERED")
    }
  }
}
