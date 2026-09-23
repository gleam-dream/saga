// Negative fixture: `Port` is opaque outside saga's own module. An external
// consumer must go through `saga.define`'s builder input, never construct or
// pattern-match a `Port` value directly.
import saga

pub fn invalid() {
  saga.Port
}
