import gleeunit
import gleeunit/should
import saga

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn version_test() {
  saga.version()
  |> should.equal("0.1.0")
}
