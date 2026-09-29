import gleam/erlang/process
import gleeunit/should
import saga
import saga/execution

pub fn only_selected_branch_runs_test() {
  let effects = process.new_subject()
  let assert Ok(workflow) =
    saga.define("choice", fn(input) {
      saga.choose(
        input,
        "route",
        saga.map(input, fn(x) { x > 0 }),
        fn(port) {
          saga.perform(
            port,
            saga.step("positive", fn(x) {
              process.send(effects, "positive")
              Ok(x + 1)
            }),
          )
        },
        fn(port) {
          saga.perform(
            port,
            saga.step("negative", fn(x) {
              process.send(effects, "negative")
              Ok(x - 1)
            }),
          )
        },
      )
    })
  execution.run(workflow, 1, execution.config())
  |> should.equal(Ok(execution.Completed(2)))
  process.receive(effects, 100) |> should.equal(Ok("positive"))
  process.receive(effects, 0) |> should.equal(Error(Nil))
  execution.run(workflow, -1, execution.config())
  |> should.equal(Ok(execution.Completed(-2)))
  process.receive(effects, 100) |> should.equal(Ok("negative"))
  process.receive(effects, 0) |> should.equal(Error(Nil))
}

pub fn nested_unchosen_branch_never_reads_missing_values_test() {
  let assert Ok(workflow) =
    saga.define("nested", fn(input) {
      saga.choose(
        input,
        "outer",
        saga.map(input, fn(_) { False }),
        fn(port) {
          saga.choose(
            port,
            "inner",
            saga.map(port, fn(_) { True }),
            fn(port) {
              saga.perform(
                port,
                saga.step("bad-a", fn(_) { Error("unchosen") }),
              )
            },
            fn(port) {
              saga.perform(
                port,
                saga.step("bad-b", fn(_) { Error("unchosen") }),
              )
            },
          )
        },
        fn(port) { saga.map(port, fn(value) { value <> "-chosen" }) },
      )
    })
  execution.run(workflow, "x", execution.config())
  |> should.equal(Ok(execution.Completed("x-chosen")))
}
