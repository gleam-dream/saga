//// The bound on every duration that reaches an Erlang timer.
////
//// `erlang:send_after/3` and `receive ... after` accept at most
//// 4_294_967_295 milliseconds (2^32 - 1, about 49.7 days) and raise
//// `badarg` or `timeout_value` above it. A caller-supplied duration that
//// reaches a timer is checked against this bound where it is configured, so
//// an over-long value is a typed configuration error and never a raise
//// inside a running coordinator.

import gleam/time/duration.{type Duration}

/// The longest timer Erlang accepts, in milliseconds.
pub const max_milliseconds = 4_294_967_295

/// Whether `value` is a usable positive timer bound: at least 1 millisecond
/// and at most `max_milliseconds`, in whole milliseconds.
pub fn valid_timer_duration(value: Duration) -> Bool {
  let milliseconds = duration.to_milliseconds(value)
  milliseconds >= 1 && milliseconds <= max_milliseconds
}

/// Whether `value` is longer than any Erlang timer can hold.
pub fn too_long(value: Duration) -> Bool {
  duration.to_milliseconds(value) > max_milliseconds
}
