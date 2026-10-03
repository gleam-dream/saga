/// A small try/after cleanup helper, independent of saga's own internal
/// test-support module (this package must only use saga's public API and
/// cannot import `saga/internal/*` or saga's own `test/support`). Ensures a
/// started `Execution` is always cancelled and awaited, even if the test
/// body's assertion panics, so a blocked coordinator never leaks between
/// tests.
import gleam/time/duration
import saga/execution.{type Execution}

pub fn with_execution(
  execution: Execution(o, e, u),
  use_execution: fn() -> a,
) -> a {
  ensure(use_execution, fn() {
    execution.cancel(execution)
    let _ = execution.await(execution, duration.seconds(2))
    Nil
  })
}

@external(erlang, "cleanup_ffi", "ensure")
fn ensure(body: fn() -> a, after: fn() -> Nil) -> a
