/// Monotonic microsecond clock from bench_native. The benchmark uses its
/// own native binding because Saga's internal FFI is outside the public API.
@external(erlang, "bench_native", "monotonic_time")
pub fn monotonic_time() -> Int
