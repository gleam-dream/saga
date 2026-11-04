//// The bound on every duration that reaches an Erlang timer.
////
//// `receive ... after` accepts at most 4_294_967_295 milliseconds
//// (2^32 - 1, about 49.7 days) and raises `timeout_value` above it.
//// `erlang:send_after/3` accepts longer delays on OTP 29 (it raises
//// `badarg` only beyond about 292 years), but every caller-supplied
//// duration is held to the receive limit, the bound `saga/reporting`
//// already applied, so one ceiling covers every timer and wait. It is
//// checked where the duration is configured, so an over-long value is a
//// typed configuration error and never a raise in a running coordinator or
//// caller.

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
