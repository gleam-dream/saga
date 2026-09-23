// Negative fixture: `Execution` is opaque outside `saga/execution`. An
// external consumer must obtain one only from `start`/`run`, never
// construct or pattern-match it directly.
import saga/execution

pub fn invalid() {
  execution.Execution
}
