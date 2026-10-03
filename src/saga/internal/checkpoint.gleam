/// Failures crossing the coordinator's optional persistence boundary, in
/// saga's internal vocabulary. `saga/durable` converts each to its public
/// `Error`. These values are saved inside checkpoints (a suspension's
/// reason), so `saga_checkpoint.erl`'s schema mirrors every constructor.
import saga/codec.{type CodecError}
import saga/storage

pub type Failure {
  StorageFailure(storage.Error)
  CodecFailure(boundary: Boundary, error: CodecError)
  InvalidState(Problem)
  Uncertain(Required)
  TooLarge(bytes: Int, limit: Int)
}

/// A step address. Node closures report the address they were authored
/// with; the coordinator replaces it with the resolved one (`at`).
pub type Address {
  Address(scope: List(String), name: String, occurrence: Int)
}

pub type Boundary {
  RunInput
  RunOutput
  RunError
  RunUndoError
  StepInput(step: Address)
  StepOutput(step: Address)
}

pub type Problem {
  Malformed
  ForeignExecution(saved: String)
  GraphMismatch
  ConcurrencyBelowInFlight(in_flight: Int, max_concurrency: Int)
  UndoNotRestorable(step: Address)
  CompensationInputMissing(step: Address)
  DeciderMissingAfterMapping(step: Address)
}

pub type Action {
  AttemptAction(attempt: Int)
  CompensationAction(attempt: Int)
  UndoAction
}

pub type Key {
  Key(idempotency: String, attempt: Int, attempt_key: String)
}

pub type Required {
  Required(step: Address, action: Action, key: Key)
}

/// Why a workflow cannot be persisted, found before any effect.
pub type DefinitionProblem {
  EmptyWorkflowVersion
  MissingRecoverable(step: Address)
  EmptyStepVersion(step: Address)
  EmptyStepCodecVersion(boundary: Boundary)
  MissingRestoreUndo(step: Address)
}

/// Re-addresses a failure a node reported to the node's resolved address.
pub fn at(failure: Failure, address: Address) -> Failure {
  case failure {
    CodecFailure(StepInput(_), error) -> CodecFailure(StepInput(address), error)
    CodecFailure(StepOutput(_), error) ->
      CodecFailure(StepOutput(address), error)
    InvalidState(UndoNotRestorable(_)) ->
      InvalidState(UndoNotRestorable(address))
    InvalidState(CompensationInputMissing(_)) ->
      InvalidState(CompensationInputMissing(address))
    InvalidState(DeciderMissingAfterMapping(_)) ->
      InvalidState(DeciderMissingAfterMapping(address))
    Uncertain(Required(_, action, key)) ->
      Uncertain(Required(address, action, key))
    other -> other
  }
}
