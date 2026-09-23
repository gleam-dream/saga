/// Thin typed binding onto `bench_ffi.erl`'s native monotonic clock. See
/// that file for why the bench harness needs its own tiny native surface
/// (saga's own internal ffi module is off limits; `gleam_erlang` exposes no
/// public monotonic clock).
@external(erlang, "bench_native", "monotonic_time")
pub fn monotonic_time() -> Int
