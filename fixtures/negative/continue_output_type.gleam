// Negative fixture: a `compensate` decider's `Continue` replacement output
// must match the step's declared output type. saga must not let a recovery
// decision silently swap in a value of the wrong type.
import saga

pub type Err {
  Err
}

pub fn invalid() {
  saga.step("s", fn(x: Int) -> Result(Int, Err) { Ok(x) })
  |> saga.compensate(max_attempts: 2, with: fn(_input, _failure, _attempt) {
    saga.Continue("not an int", saga.NoUndo)
  })
}
