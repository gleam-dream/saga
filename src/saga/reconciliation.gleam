//// Names an external action whose outcome a durable run must establish
//// before it can continue.
////
//// `saga/durable` suspends a recovered run with
//// `RecoveryRequired(Required(step, action, key))` when a step's recovery
//// callback (`saga.recoverable`, `saga.reconcile_undo` or
//// `saga.reconcile_compensation`) reports the effect of a step attempt, a
//// compensation or an undo as unknown. Establish the effect of the action
//// that `key` identifies, so that the callback can report it, then `drive`
//// the run again.

/// Which kind of action has an unknown outcome.
pub type Action {
  Activity
  Compensation
  Undo
}

/// The step address, the action, and the key that identifies the exact
/// attempt whose outcome must be established.
pub type Required {
  Required(step: String, action: Action, key: String)
}
