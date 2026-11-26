//// A run whose startup handshake fails is reported to its starter as lost.
//// These tests pin that such a run performs no step and leaves no process
//// behind: the starter is the only process that could have observed it.

import gleam/erlang/process.{type Subject}
import gleam/option.{None}
import gleeunit/should
import saga
import saga/internal/coordinator
import sinal/correlation

fn settings() -> coordinator.Settings {
  coordinator.Settings(
    max_concurrency: 1,
    deadline: None,
    step_timeout: None,
    settle_timeout: 1000,
    cleanup_timeout: 1000,
    max_retry_delay: 1000,
    correlation: correlation.unique(),
    execution: None,
  )
}

/// Starts runs with a handshake wait of zero until one start is refused,
/// which happens whenever the new coordinator has not yet answered. Returns
/// how many starts were accepted first.
fn accepted_before_refusal(
  workflow: saga.Workflow(Int, Int, String, String),
  accepted: Int,
) -> Int {
  let assert True = accepted < 1000
  case
    coordinator.start(
      workflow_name: saga.name(workflow),
      owner: process.self(),
      settings: settings(),
      build_graph: fn() { saga.for_run(workflow, 1) },
      deliver: fn(_outcome) { Nil },
      handshake_timeout: 0,
    )
  {
    Error(Nil) -> accepted
    Ok(_) -> accepted_before_refusal(workflow, accepted + 1)
  }
}

fn count(from: Subject(Nil), seen: Int) -> Int {
  case process.receive(from, 200) {
    Ok(Nil) -> count(from, seen + 1)
    Error(Nil) -> seen
  }
}

pub fn refused_start_runs_no_step_test() {
  let performed = process.new_subject()
  let workflow =
    saga.define("wf", fn(input) {
      input
      |> saga.perform(
        saga.step("effect", fn(value: Int) {
          process.send(performed, Nil)
          Ok(value)
        }),
      )
    })
  let accepted = accepted_before_refusal(workflow, 0)
  // Each accepted run performs the step once; the refused one must not.
  count(performed, 0) |> should.equal(accepted)
}
