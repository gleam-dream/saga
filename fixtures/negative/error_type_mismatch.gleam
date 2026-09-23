// Negative fixture: two steps with different error types cannot be
// combined with `saga.both` without first unifying them through
// `saga.map_step_errors`. This proves saga does not erase a step's error
// type into something dynamic that could paper over a real mismatch.
import saga

pub type ErrA {
  ErrA
}

pub type ErrB {
  ErrB
}

pub fn invalid() {
  saga.define("mixed", fn(input) {
    let a =
      input
      |> saga.perform(saga.step("a", fn(x: Int) -> Result(Int, ErrA) { Ok(x) }))
    let b =
      input
      |> saga.perform(saga.step("b", fn(x: Int) -> Result(Int, ErrB) { Ok(x) }))
    saga.both(a, b)
  })
}
