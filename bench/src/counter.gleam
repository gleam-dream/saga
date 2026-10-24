/// Counts builder invocations across definition and execution. The atomic
/// counter supports increments from separate processes, so it also measures
/// older revisions whose coordinators reevaluate the builder.
pub type Counter

@external(erlang, "bench_native", "new_counter")
pub fn new() -> Counter

@external(erlang, "bench_native", "counter_increment")
pub fn increment(counter: Counter) -> Nil

@external(erlang, "bench_native", "counter_read")
pub fn read(counter: Counter) -> Int
