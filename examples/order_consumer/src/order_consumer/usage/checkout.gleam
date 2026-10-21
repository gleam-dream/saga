//// Define a checkout workflow, run it locally, configure the run, and observe
//// it with telemetry. Each `run_*` function asserts what the guide states, so
//// the documented common path cannot drift from the API.

// snips-start: checkout
import gleam/time/duration
import saga
import saga/execution
import saga/telemetry
import sinal
import sinal/correlation

pub type CheckoutError {
  OutOfStock
  Declined
  MaybeCharged
}

pub fn reserve(order: String) -> Result(String, CheckoutError) {
  Ok("hold-" <> order)
}

pub fn release(_hold: String) -> Result(Nil, CheckoutError) {
  Ok(Nil)
}

fn charge(
  reservation: String,
  idempotency_key key: String,
) -> Result(String, CheckoutError) {
  case reservation {
    "hold-declined" -> Error(Declined)
    _ -> Ok("receipt-" <> key)
  }
}

/// Two dependent steps: a reservation that can be undone and a payment whose
/// outcome may be unknown, with a compensation policy.
pub fn checkout() {
  let reserve =
    saga.step("reserve_inventory", reserve)
    |> saga.undo(fn(undo) { release(undo.output) })
  let charge =
    saga.effect("charge_payment", fn(reservation, key) {
      // `saga.idempotency_key(key)` is the same for every attempt of this step.
      charge(reservation, idempotency_key: saga.idempotency_key(key))
    })
    |> saga.unknown_when(fn(error) { error == MaybeCharged })
    |> saga.compensate(max_attempts: 3, with: fn(failed) {
      case failed.failure {
        saga.Returned(MaybeCharged) ->
          saga.RetryAfter(duration.milliseconds(500))
        saga.Returned(error) -> saga.Abort(error)
        saga.Crashed(_) | saga.TimedOut -> saga.Hold(MaybeCharged)
      }
    })
  saga.define("checkout", fn(order) {
    order |> saga.perform(reserve) |> saga.perform(charge)
  })
}

// snips-end: checkout
/// The caller's own failure type keeps both reports intact.
pub type Failure(o, e, u) {
  NotStarted(execution.RunError)
  Unfinished(execution.Outcome(o, e, u))
}

/// Run to a result: the receipt, or the run error or outcome unchanged.
pub fn run_checkout(workflow, order) {
  case execution.run(workflow, order, execution.config()) {
    Ok(execution.Completed(receipt)) -> Ok(receipt)
    Ok(outcome) -> Error(Unfinished(outcome))
    Error(run_error) -> Error(NotStarted(run_error))
  }
}

/// The common path: success returns the receipt; a business failure
/// compensates, keeps the typed cause and reports no unknown effects; a
/// refused configuration keeps its run error.
pub fn run_common_path() -> Nil {
  let workflow = checkout()
  let assert Ok("receipt-" <> _) = run_checkout(workflow, "o-1")
  let assert Error(Unfinished(declined)) = run_checkout(workflow, "declined")
  let assert execution.Failed(
    cause: execution.StepFailed(error: Declined, ..),
    ..,
  ) = declined
  let assert [] = execution.unknown_effects(declined)
  let refused = execution.config() |> execution.with_max_concurrency(0)
  let assert Error(execution.InvalidConfig([
    execution.MaxConcurrencyNotPositive(0),
  ])) = execution.run(workflow, "o-1", refused)
  Nil
}

// snips-start: configure-run
/// Configure concurrency, deadline and correlation, then start, watch and
/// await a run instead of blocking in `run`.
pub fn run_configuration() -> Nil {
  let workflow = checkout()
  let config =
    execution.config()
    |> execution.with_max_concurrency(4)
    |> execution.with_deadline(execution.After(duration.seconds(30)))
    |> execution.with_correlation(correlation.unique())
  let assert Ok(exec) = execution.start(workflow, "o-2", config)
  let _ = execution.progress(exec, timeout: duration.seconds(1))
  let assert Ok(execution.Completed(_)) =
    execution.await(exec, timeout: duration.seconds(10))
  Nil
}

// snips-end: configure-run

/// Attach to the run-stopped telemetry event and read its correlation,
/// execution and outcome.
pub fn run_telemetry() -> Nil {
  let workflow = checkout()
  let attachment =
    sinal.observe(telemetry.run_stopped(), fn(_measurements, metadata) {
      let _ = #(metadata.correlation, metadata.execution, metadata.outcome)
      Nil
    })
  let assert Ok(_) = run_checkout(workflow, "o-3")
  let _ = sinal.detach(attachment)
  Nil
}

pub fn describe_checkout_error(error: CheckoutError) -> String {
  case error {
    OutOfStock -> "out of stock"
    Declined -> "declined"
    MaybeCharged -> "the charge may have been taken"
  }
}
