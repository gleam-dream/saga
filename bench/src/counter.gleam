/// A concurrency-safe counter, used only to count how many times a
/// workflow's build function actually runs across a bench shape's many
/// runs. The build function is evaluated once at `define` time and (before
/// the build-once refactor) once more per run *inside that run's own
/// coordinator process* -- never the bench harness's own process -- so a
/// single-process mailbox cell (as `saga/internal/cell` uses internally)
/// cannot safely count it: many coordinator processes increment
/// concurrently. Backed by an Erlang atomics array, which exists precisely
/// for this.
pub type Counter

@external(erlang, "bench_native", "new_counter")
pub fn new() -> Counter

@external(erlang, "bench_native", "counter_increment")
pub fn increment(counter: Counter) -> Nil

@external(erlang, "bench_native", "counter_read")
pub fn read(counter: Counter) -> Int
