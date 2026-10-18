//// Runs every documented usage example under `src/order_consumer/usage/`, so
//// the examples the README and design point to cannot drift from the API.

import order_consumer/usage/checkout
import order_consumer/usage/committed_effect
import order_consumer/usage/correlation
import order_consumer/usage/durable_run
import order_consumer/usage/operations
import order_consumer/usage/unknown_effect

pub fn common_path_test() {
  checkout.run_common_path()
}

pub fn configuration_test() {
  checkout.run_configuration()
}

pub fn telemetry_test() {
  checkout.run_telemetry()
}

pub fn unknown_effect_test() {
  unknown_effect.run()
}

pub fn step_correlation_test() {
  correlation.run()
}

pub fn durable_run_test() {
  durable_run.run()
}

pub fn stable_id_test() {
  operations.run_stable_id()
}

pub fn discover_unfinished_test() {
  operations.run_discover_unfinished()
}

pub fn error_kinds_test() {
  operations.run_error_kinds()
}

pub fn committed_local_effect_test() {
  committed_effect.run()
}
