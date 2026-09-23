import gleeunit/should
import saga
import saga/execution
import support/probe

pub type DemoError {
  Boom
}

pub type DemoUndoError {
  UndoBoom
}

pub fn sequential_dependency_order_test() {
  let assert Ok(workflow) =
    saga.define("seq", fn(input) {
      let a = input |> saga.perform(saga.step("a", fn(x: Int) { Ok(x + 1) }))
      let b = a |> saga.perform(saga.step("b", fn(x: Int) { Ok(x * 2) }))
      b |> saga.perform(saga.step("c", fn(x: Int) { Ok(x - 1) }))
    })

  let assert Ok(execution.Completed(output)) =
    execution.run(workflow, 1, execution.config())
  // (1 + 1) * 2 - 1 = 3
  output |> should.equal(3)
}

pub fn shared_dependency_executes_once_test() {
  let counter = probe.new_counter()
  let assert Ok(workflow) =
    saga.define("diamond", fn(input) {
      let order =
        input
        |> saga.perform(
          saga.step("order", fn(x: Int) {
            probe.counter_enter(counter)
            Ok(x + 1)
          }),
        )
      let fraud =
        order |> saga.perform(saga.step("fraud", fn(x: Int) { Ok(x) }))
      let inventory =
        order |> saga.perform(saga.step("inventory", fn(x: Int) { Ok(x) }))
      saga.both(fraud, inventory) |> saga.map(fn(pair) { #(pair.0, pair.1) })
    })

  let assert Ok(execution.Completed(#(a, b))) =
    execution.run(workflow, 0, execution.config())
  a |> should.equal(1)
  b |> should.equal(1)
  probe.total_entries(counter) |> should.equal(1)
}

pub fn independent_steps_run_in_distinct_processes_test() {
  let assert Ok(workflow) =
    saga.define("parallel", fn(input) {
      let a =
        input |> saga.perform(saga.step("a", fn(_x: Int) { Ok(erlang_self()) }))
      let b =
        input |> saga.perform(saga.step("b", fn(_x: Int) { Ok(erlang_self()) }))
      saga.both(a, b)
    })

  let assert Ok(execution.Completed(#(pid_a, pid_b))) =
    execution.run(workflow, 0, execution.config())
  { pid_a == pid_b } |> should.be_false
}

@external(erlang, "erlang", "self")
fn erlang_self() -> a

pub fn max_concurrency_bounds_running_attempts_test() {
  let gate = probe.new_gate()
  let counter = probe.new_counter()

  let make_step = fn(name: String) {
    saga.step(name, fn(_x: Int) {
      probe.counter_enter(counter)
      probe.enter(gate)
      probe.counter_leave(counter)
      Ok(Nil)
    })
  }

  let assert Ok(workflow) =
    saga.define("fanout", fn(input) {
      let steps =
        list_range(1, 10)
        |> list_map(fn(i) { make_step("step" <> int_to_string(i)) })
      let ports = list_map(steps, fn(s) { input |> saga.perform(s) })
      saga.all(ports)
    })

  let config = execution.Config(..execution.config(), max_concurrency: 3)

  probe.with_run(workflow, 0, config, fn(exec) {
    // Wait until exactly 3 are attempting (bounded by max_concurrency).
    probe.await_high_water(counter, 3, 2000)
    probe.high_water(counter) |> should.equal(3)

    // Release one; exactly one more should be admitted, keeping the total
    // in-flight count at 3.
    probe.open(gate)
    probe.await_total_entries(counter, 4, 2000)
    probe.high_water(counter) |> should.equal(3)

    // Release the rest.
    let _ =
      list_range(1, 9)
      |> list_each(fn(_i) { probe.open(gate) })

    let assert Ok(execution.Completed(_)) = execution.await(exec, 2000)
    Nil
  })
}

fn list_range(from: Int, to: Int) -> List(Int) {
  case from > to {
    True -> []
    False -> [from, ..list_range(from + 1, to)]
  }
}

fn list_map(items: List(a), f: fn(a) -> b) -> List(b) {
  case items {
    [] -> []
    [first, ..rest] -> [f(first), ..list_map(rest, f)]
  }
}

fn list_each(items: List(a), f: fn(a) -> Nil) -> Nil {
  case items {
    [] -> Nil
    [first, ..rest] -> {
      f(first)
      list_each(rest, f)
    }
  }
}

fn int_to_string(value: Int) -> String {
  erlang_integer_to_binary(value)
}

@external(erlang, "erlang", "integer_to_binary")
fn erlang_integer_to_binary(value: Int) -> String
