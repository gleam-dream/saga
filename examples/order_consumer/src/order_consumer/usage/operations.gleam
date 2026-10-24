//// Operating durable executions: keep the id stable, reconnect to the saved
//// run, read it without a live runner, discover unfinished executions, and
//// expect `InputMismatch` for a reused id with another input.

import gleam/time/duration
import order_consumer/usage/durable_run
import saga/durable
import saga/execution
import saga/storage/memory

/// The id is the identity: reconnecting under it finds the saved run, and a
/// finished run returns its saved outcome without running the effect again.
pub fn run_stable_id() -> Nil {
  let assert Ok(store) = memory.start()
  let storage = memory.storage(store)
  let persistence = durable_run.persistence()
  let id = "checkout:o-7"

  let assert Ok(first) =
    durable.start_or_reconnect(persistence, storage, id: id, input: "o-7")
  let assert Ok(durable.Pending) = durable.read(first)
  let assert Ok(execution.Completed(receipt)) =
    durable.drive(first, timeout: duration.seconds(30))

  // Reconnect under the same id: the saved outcome is the completion
  // authority, readable without a live runner.
  let assert Ok(again) =
    durable.start_or_reconnect(persistence, storage, id: id, input: "o-7")
  let assert Ok(durable.Finished(execution.Completed(saved))) =
    durable.read(again)
  let assert True = saved == receipt
  let assert Ok(execution.Completed(replayed)) =
    durable.drive(again, timeout: duration.seconds(30))
  let assert True = replayed == receipt

  // The same id with another input is refused before anything runs.
  let assert Error(durable.InputMismatch) =
    durable.start_or_reconnect(persistence, storage, id: id, input: "o-8")
  memory.stop(store)
  Nil
}

/// Saga supplies listing and reconnect, not a sweeper: the application drives
/// each unfinished id under its own capacity and delivery policy.
pub fn run_discover_unfinished() -> Nil {
  let assert Ok(store) = memory.start()
  let storage = memory.storage(store)
  let persistence = durable_run.persistence()

  let assert Ok(_) =
    durable.start_or_reconnect(
      persistence,
      storage,
      id: "checkout:o-10",
      input: "o-10",
    )
  let assert Ok(["checkout:o-10"]) = durable.unfinished(storage, limit: 100)

  let assert Ok(run) =
    durable.reconnect(persistence, storage, id: "checkout:o-10")
  let assert Ok(execution.Completed(_)) =
    durable.drive(run, timeout: duration.seconds(30))
  let assert Ok([]) = durable.unfinished(storage, limit: 100)
  memory.stop(store)
  Nil
}

/// The closed kinds an application branches on.
pub fn run_error_kinds() -> Nil {
  let assert "retry later" = durable_run.action_for(durable.RunnerLost)
  let assert "retry later" = durable_run.action_for(durable.DriveTimedOut)
  let assert "the execution was saved with a different input" =
    durable_run.action_for(durable.InputMismatch)
  Nil
}
