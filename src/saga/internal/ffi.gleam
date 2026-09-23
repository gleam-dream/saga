/// Thin typed bindings onto `saga_ffi.erl`. Every native escape hatch used by
/// the coordinator and its tasks lives behind this module so the rest of the
/// implementation stays in plain Gleam types, with no `Dynamic` and no
/// unsafe coercion anywhere in this file or its callers.
pub type CrashClass {
  ErrorClass
  ExitClass
  ThrowClass
}

pub type RescueResult(value) {
  Rescued(value: value)
  Raised(class: CrashClass, reason: String)
}

@external(erlang, "saga_ffi", "rescue")
fn rescue_ffi(
  body: fn() -> value,
  on_ok: fn(value) -> RescueResult(value),
  on_error: fn(String) -> RescueResult(value),
  on_exit: fn(String) -> RescueResult(value),
  on_throw: fn(String) -> RescueResult(value),
) -> RescueResult(value)

/// Runs `body`, catching any raised error/exit/throw and reifying it as
/// `Raised`. Nothing above this function ever lets a native exception escape.
pub fn rescue(body: fn() -> value) -> RescueResult(value) {
  rescue_ffi(
    body,
    fn(value) { Rescued(value) },
    fn(reason) { Raised(ErrorClass, reason) },
    fn(reason) { Raised(ExitClass, reason) },
    fn(reason) { Raised(ThrowClass, reason) },
  )
}

/// Number of Erlang schedulers online, used as the default concurrency bound.
@external(erlang, "saga_ffi", "schedulers_online")
pub fn schedulers_online() -> Int

/// A fresh, monotonically increasing positive integer. Used for node ids and
/// per-attempt correlation sequences.
@external(erlang, "saga_ffi", "unique_integer")
pub fn unique_integer() -> Int

/// Native monotonic time in milliseconds, for duration measurements.
@external(erlang, "saga_ffi", "monotonic_time")
pub fn monotonic_time() -> Int
