/// Failures crossing the coordinator's optional persistence boundary.
import saga/reconciliation
import saga/storage

pub type Failure {
  StorageFailure(storage.Error)
  CodecFailure(String)
  InvalidState(String)
  Uncertain(reconciliation.Required)
}
