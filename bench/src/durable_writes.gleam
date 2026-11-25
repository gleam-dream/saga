/// Storage cost of one durable run against step count: how many checkpoint
/// commits a chain of N steps makes, how many bytes they carry in total, and
/// how long the run takes on the in-memory adapter. Each step saves a 1 KB
/// input and a 1 KB output; the "encodes" column counts calls of the step
/// output encoder. Run with `gleam run -m durable_writes`.
///
/// Commit counts are exact except for timer messages that lose a race; times
/// are observational.
import bench_ffi
import counter
import gleam/bit_array
import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/string
import gleam/time/duration
import saga
import saga/codec
import saga/durable
import saga/execution
import saga/storage
import saga/storage/memory

const sizes = [10, 50, 100, 200]

const runs = 3

pub fn main() {
  io.println(
    "| steps | run | commits | bytes committed | encodes | time (ms) |",
  )
  io.println("| ---: | ---: | ---: | ---: | ---: | ---: |")
  list.each(sizes, fn(steps) {
    list.each(numbers(runs, []), fn(run) {
      let #(commits, bytes, encodes, microseconds) = measure(steps, run)
      io.println(
        "| "
        <> string.join(
          list.map(
            [steps, run, commits, bytes, encodes, microseconds / 1000],
            int.to_string,
          ),
          " | ",
        )
        <> " |",
      )
    })
  })
}

fn numbers(up_to: Int, acc: List(Int)) -> List(Int) {
  case up_to {
    0 -> acc
    _ -> numbers(up_to - 1, [up_to, ..acc])
  }
}

fn measure(steps: Int, run: Int) -> #(Int, Int, Int, Int) {
  let encodes = counter.new()
  let counted =
    codec.new(
      "payload-1",
      fn(value: String) {
        counter.increment(encodes)
        Ok(value)
      },
      Ok,
    )
  let payload = string.repeat("x", 1024)
  let step = fn(number: Int) {
    saga.step("step-" <> int.to_string(number), fn(_input: String) {
      Ok(payload)
    })
    |> durable.recoverable(
      version: "1",
      input: codec.text(),
      output: counted,
      resolve: fn(_, _) { durable.MaybeSent },
    )
  }
  let workflow: saga.Workflow(String, String, String, String) =
    saga.define("chain", fn(input) {
      list.fold(numbers(steps, []), input, fn(port, number) {
        saga.perform(port, step(number))
      })
    })
  let text = codec.text()
  let persistence =
    durable.new(
      workflow,
      input: text,
      output: text,
      error: text,
      undo_error: text,
    )
  let assert Ok(store) = memory.start()
  let backend = memory.storage(store)
  let sizes = process.new_subject()
  let counting =
    storage.new(
      create: fn(id, records) { storage.do_create(backend, id, records) },
      load: fn(id) { storage.do_load(backend, id) },
      claim: fn(id) { storage.do_claim(backend, id) },
      commit: fn(owner, change) {
        process.send(
          sizes,
          dict.fold(change.records, 0, fn(total, _, bytes) {
            total + bit_array.byte_size(bytes)
          }),
        )
        storage.do_commit(backend, owner, change)
      },
      release: fn(owner) { storage.do_release(backend, owner) },
      cancel: fn(id) { storage.do_cancel(backend, id) },
      list: fn(listing) { storage.do_list(backend, listing) },
    )
  let id = int.to_string(steps) <> "-" <> int.to_string(run)
  let assert Ok(started) =
    durable.start_or_reconnect(persistence, counting, id: id, input: payload)
  let before = bench_ffi.monotonic_time()
  let assert Ok(execution.Completed(_)) =
    durable.drive(started, timeout: duration.seconds(120))
  let elapsed = bench_ffi.monotonic_time() - before
  memory.stop(store)
  let #(commits, bytes) = drain(sizes, 0, 0)
  #(commits, bytes, counter.read(encodes), elapsed)
}

fn drain(from: process.Subject(Int), commits: Int, bytes: Int) -> #(Int, Int) {
  case process.receive(from, 0) {
    Ok(size) -> drain(from, commits + 1, bytes + size)
    Error(Nil) -> #(commits, bytes)
  }
}
