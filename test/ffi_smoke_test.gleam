import gleeunit/should
import saga/internal/ffi

pub fn rescue_returns_ok_value_test() {
  ffi.rescue(fn() { 1 + 1 })
  |> should.equal(ffi.Rescued(2))
}

pub fn rescue_catches_panic_test() {
  case ffi.rescue(fn() { panic as "boom" }) {
    ffi.Raised(ffi.ErrorClass, _reason) -> Nil
    _other -> panic as "expected Raised(ErrorClass, _), got something else"
  }
}

pub fn unique_integer_is_monotonic_test() {
  let a = ffi.unique_integer()
  let b = ffi.unique_integer()
  { b > a } |> should.be_true
}

pub fn schedulers_online_is_positive_test() {
  { ffi.schedulers_online() > 0 } |> should.be_true
}
