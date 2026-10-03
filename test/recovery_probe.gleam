/// Invoked by check_durable_restart.sh in two separate Erlang VMs.
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

fn definition(
  path: String,
  first_vm: Bool,
) -> durable.Persistence(String, String, String, String) {
  let text = codec.text()
  let branch = fn(name) {
    saga.effect(name, fn(input, key) {
      case first_vm {
        True -> {
          write_ledger(path <> "." <> name, key.attempt_key)
          process.sleep(60_000)
          Ok(input <> name)
        }
        False -> Error("effect ran twice")
      }
    })
    |> durable.recoverable(
      version: "1",
      input: text,
      output: text,
      resolve: fn(input, key) {
        case ledger_has(path <> "." <> name, key.attempt_key) {
          True -> durable.Completed(input <> name)
          False -> durable.MaybeSent
        }
      },
    )
  }
  let assert Ok(workflow) =
    saga.define("vm-recovery", fn(input) {
      let shared =
        saga.perform(
          input,
          saga.step("shared", fn(input) {
            case first_vm {
              True -> Ok(input <> "!")
              False -> Error("saved step reran")
            }
          })
            |> durable.recoverable(
              version: "1",
              input: text,
              output: text,
              resolve: fn(_, _) { durable.MaybeSent },
            ),
        )
      saga.both(
        saga.perform(shared, branch("a")),
        saga.perform(shared, branch("b")),
      )
      |> saga.map(fn(pair) { pair.0 <> pair.1 })
    })
  let assert Ok(persistence) =
    durable.prepare(workflow, "1", text, text, text, text)
  persistence
}

pub fn main() -> Nil {
  let path = path()
  let first_vm = mode() == "prepare"
  let persistence = definition(path, first_vm)
  let storage = file.open(path)
  let assert Ok(reference) =
    durable.start_or_reconnect(storage, "vm-ref", persistence, "order")
  let result =
    durable.drive(
      storage,
      reference,
      persistence,
      execution.config() |> execution.with_max_concurrency(2),
    )
  case first_vm {
    True -> Nil
    False -> {
      let assert Ok(execution.Completed("order!aorder!b")) = result
      let assert Ok(durable.Finished(execution.Completed("order!aorder!b"))) =
        durable.read(storage, reference, persistence)
      io.println("RECOVERED")
    }
  }
}
