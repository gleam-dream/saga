// Positive control for fixtures/negative/continue_output_type.gleam: the
// exact same shape (a step with a `compensate` decider returning
// `Continue`), but with the replacement output correctly typed as `Int`
// instead of `String`. This must compile. Its purpose is to prove the
// negative fixture fails for the reason its .expect file claims (a type
// mismatch on the `Continue` replacement value) and not for some unrelated
// mistake in how the fixture is written — if this control ever stopped
// compiling, that would mean the negative fixture's failure reason can no
// longer be trusted either.
import saga

pub type Err {
  Err
}

pub fn valid() {
  saga.step("s", fn(x: Int) -> Result(Int, Err) { Ok(x) })
  |> saga.compensate(max_attempts: 2, with: fn(_input, _failure, _attempt) {
    saga.Continue(42, saga.NoUndo)
  })
}
