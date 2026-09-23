import gleam/list
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
      let assert [first, ..rest] = ports
      saga.all(first, rest)
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

/// A `RetryAfter` backoff firing must re-enter the ready queue and go
/// through the same concurrency gate as any other admission — it must
/// never push the number of concurrently-attempting steps above
/// `max_concurrency`. Ported from the independent review's REPRO3.
///
/// `a` (compensated) and `d` (gated) are admitted first (`max_concurrency:
/// 2`); `b`/`c` (sharing another gate) stay queued. `a` fails immediately
/// and schedules a 300ms `RetryAfter`; `d` is released quickly afterwards,
/// freeing a slot that admission fills with *both* `b` and `c` (only one
/// other attempt — `a`'s in-flight recovery decision — was occupying a
/// slot at that moment). By the time the 300ms backoff fires, `b` and `c`
/// are already both concurrently blocked, holding the run's entire
/// `max_concurrency: 2` budget: if the fired retry bypassed admission, the
/// high-water mark would reach 3.
pub fn retry_after_backoff_honors_max_concurrency_test() {
  let dgate = probe.new_gate()
  let gate = probe.new_gate()
  let counter = probe.new_counter()
  let gated = fn(name: String, g: probe.Gate) {
    saga.step(name, fn(x: Int) -> Result(Int, Nil) {
      probe.counter_enter(counter)
      probe.enter(g)
      probe.counter_leave(counter)
      Ok(x)
    })
  }
  let assert Ok(wf) =
    saga.define("rc", fn(input) {
      let a =
        input
        |> saga.perform(
          saga.step("a", fn(_x: Int) -> Result(Int, Nil) {
            probe.counter_enter(counter)
            probe.counter_leave(counter)
            Error(Nil)
          })
          |> saga.compensate(max_attempts: 2, with: fn(_i, _f, attempt) {
            case attempt.number {
              1 -> saga.RetryAfter(300)
              _ -> saga.Continue(0, saga.NoUndo)
            }
          }),
        )
      let d = input |> saga.perform(gated("d", dgate))
      let b = input |> saga.perform(gated("b", gate))
      let c = input |> saga.perform(gated("c", gate))
      saga.all(a, [d, b, c])
    })
  let cfg = execution.Config(..execution.config(), max_concurrency: 2)

  probe.with_run(wf, 1, cfg, fn(exec) {
    let assert Ok(_pid) = probe.wait_entered(dgate, 2000)
    // Wait until `a`'s compensation decider is actually running (not just
    // its attempt, which already failed synchronously) before releasing
    // `d`: that is what confirms admission has settled to exactly `d`
    // holding the one occupied slot, with the decider's own task occupying
    // the other, rather than sleeping a fixed guess at how long that
    // takes.
    let assert Ok(_progress) =
      probe.wait_until_progress(exec, 2000, fn(p) {
        list.any(p.steps, fn(sp) {
          sp.address.name == "a" && is_compensating(sp.state)
        })
      })
    probe.open(dgate)
    // Wait until `b` and `c` have both been admitted and are concurrently
    // blocked in `gate` (the state the 300ms `RetryAfter` backoff must not
    // be able to exceed): `b`+`c` entering brings the counter's total
    // entries to 4 (`a`, `d`, `b`, `c`), and `a`'s own decision settles
    // into `RetryScheduled(2)` once its backoff timer is armed. Waiting on
    // both replaces sleeping past a fixed guess at the backoff delay.
    probe.await_total_entries(counter, 4, 2000)
    let assert Ok(_progress) =
      probe.wait_until_progress(exec, 2000, fn(p) {
        list.any(p.steps, fn(sp) {
          sp.address.name == "a" && sp.state == execution.RetryScheduled(2)
        })
      })
    probe.high_water(counter) |> should.equal(2)

    probe.open(gate)
    probe.open(gate)
    let assert Ok(execution.Completed(_)) = execution.await(exec, 3000)
    probe.high_water(counter) |> should.equal(2)
    Nil
  })
}

fn is_compensating(state: execution.StepState) -> Bool {
  case state {
    execution.Compensating(_) -> True
    _ -> False
  }
}
