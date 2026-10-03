// Negative fixture: `EffectKey` is opaque outside `saga`. A consumer reads a
// key it receives with `saga.idempotency_key` and the other accessors, and
// never constructs or pattern-matches one.
import saga

pub fn invalid() {
  saga.EffectKey
}
