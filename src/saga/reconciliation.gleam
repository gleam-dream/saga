/// Identifies an external action whose outcome must be established before
/// the workflow can safely advance. The key identifies the exact attempt.
pub type Action {
  Activity
  Compensation
  Undo
}

pub type Required {
  Required(step: String, action: Action, key: String)
}
