//// Defines the storage contract that `saga/durable` saves executions
//// through, and the values an adapter builds.
////
//// Use this module to write a storage adapter, or to name its errors. One
//// `Storage` value serves a whole store, such as a database pool, and every
//// operation names the execution it acts on:
////
//// - `create(id, bytes)` saves an execution's first checkpoint, once;
//// - `load(id)` reads its latest checkpoint;
//// - `claim(id)` takes exclusive ownership and advances the ownership
////   generation, returning a `Claim`;
//// - `commit(claim, commit)` saves new bytes only while `claim` is the
////   current owner, the expected revision matches and the caller has seen
////   the current cancellation flag;
//// - `release(claim)` gives ownership up;
//// - `cancel(id)` records cancellation without overwriting progress;
//// - `unfinished(limit)` lists executions that are pending or suspended and
////   that no live claim holds, for a future driver to pick up.
////
//// Ownership is the `Claim` value, not the calling process: any process
//// holding the current claim may commit or release, and a claim rebuilt
//// with the right generation but another token is refused. How an adapter
//// notices that an owner is gone is its own choice, within the window it
//// declares to `saga/storage/conformance`: the memory adapter watches the
//// claiming process, and a database adapter uses a lease that
//// `with_renewal` keeps alive while the runner lives.
////
//// An adapter must apply each operation atomically; no workflow rules live
//// here. `saga/storage/memory`, `saga/storage/file` and the `saga_postgres`
//// package implement the contract, and `saga/storage/conformance` checks
//// an adapter against it.

import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/time/duration.{type Duration}

/// Why a storage operation failed. Later releases may add variants:
/// describe them with `describe_error`.
pub type Error {
  /// No execution has this id.
  NotFound
  /// `create` found an execution with this id already.
  AlreadyExists
  /// Another live claim owns the execution.
  Busy
  /// The expected revision is not the current one.
  Conflict
  /// The claim is no longer the current owner: it was released, or a
  /// later claim replaced it.
  StaleOwner
  /// The execution was cancelled since the committer last looked.
  CancellationChanged
  /// The saved record is unreadable.
  Corrupt
  /// The store could not be reached or refused the operation; `detail` is
  /// for logs only.
  Unavailable(detail: String)
  /// The operation did not finish within the storage's call timeout.
  TimedOut
}

/// Describes a storage error for logs.
pub fn describe_error(error: Error) -> String {
  case error {
    NotFound -> "no execution has this id"
    AlreadyExists -> "an execution with this id already exists"
    Busy -> "another runner owns the execution"
    Conflict -> "the checkpoint revision changed"
    StaleOwner -> "the claim no longer owns the execution"
    CancellationChanged -> "the execution was cancelled meanwhile"
    Corrupt -> "the saved record is unreadable"
    Unavailable(detail) -> "the store is unavailable: " <> detail
    TimedOut -> "the storage operation timed out"
  }
}

/// Ownership of one execution, returned by a successful claim. `token` is
/// the adapter's proof of this claim, such as a lease id: a claim with the
/// right generation but another token is refused.
pub opaque type Claim {
  Claim(id: String, generation: Int, token: String)
}

/// Builds the claim an adapter returns from `claim`.
pub fn claim(
  id id: String,
  generation generation: Int,
  token token: String,
) -> Claim {
  Claim(id:, generation:, token:)
}

/// The execution a claim owns.
pub fn claim_id(claim: Claim) -> String {
  claim.id
}

/// The ownership generation a claim holds.
pub fn claim_generation(claim: Claim) -> Int {
  claim.generation
}

/// The adapter's token for a claim.
pub fn claim_token(claim: Claim) -> String {
  claim.token
}

/// One saved execution, as an adapter returns it: its revision (the number
/// of commits), its ownership generation (the number of claims), its
/// cancellation flag and its checkpoint bytes.
pub opaque type Stored {
  Stored(revision: Int, generation: Int, cancelled: Bool, data: BitArray)
}

/// Builds the record an adapter returns.
pub fn stored(
  revision revision: Int,
  generation generation: Int,
  cancelled cancelled: Bool,
  data data: BitArray,
) -> Stored {
  Stored(revision:, generation:, cancelled:, data:)
}

/// The number of commits saved.
pub fn revision(stored: Stored) -> Int {
  stored.revision
}

/// The number of claims taken.
pub fn generation(stored: Stored) -> Int {
  stored.generation
}

/// Whether the execution was cancelled.
pub fn cancelled(stored: Stored) -> Bool {
  stored.cancelled
}

/// The checkpoint bytes.
pub fn data(stored: Stored) -> BitArray {
  stored.data
}

/// Where an execution stands, for `unfinished`: still running or waiting to
/// run, suspended until an operator establishes an unknown effect, or
/// finished. The union is closed.
pub type Phase {
  Pending
  Suspended
  Finished
}

/// One write: the revision the committer read, the cancellation flag it
/// observed, the execution's phase after the write, and the new bytes.
/// Read fields by label; later releases may add fields.
pub type Commit {
  Commit(
    expected_revision: Int,
    observed_cancelled: Bool,
    phase: Phase,
    data: BitArray,
  )
}

/// A store's operations, built by an adapter with `new`.
pub opaque type Storage {
  Storage(
    create: fn(String, BitArray) -> Result(Stored, Error),
    load: fn(String) -> Result(Stored, Error),
    claim: fn(String) -> Result(#(Claim, Stored), Error),
    commit: fn(Claim, Commit) -> Result(Stored, Error),
    release: fn(Claim) -> Result(Nil, Error),
    cancel: fn(String) -> Result(Nil, Error),
    unfinished: fn(Int) -> Result(List(String), Error),
    renewal: Option(#(Duration, fn(Claim) -> Result(Nil, Error))),
    call_timeout: Duration,
  )
}

/// Builds a storage from an adapter's operations:
///
/// - `create` saves revision 0, generation 0, not cancelled, phase
///   `Pending`, or fails with `AlreadyExists`;
/// - `load` fails with `NotFound` for an unknown id;
/// - `claim` fails with `Busy` while another live claim owns the
///   execution, and otherwise advances the generation;
/// - `commit` succeeds only for the current claim, the current revision
///   and the current cancellation flag, increments the revision and keeps
///   ownership; it fails with `StaleOwner`, `CancellationChanged` or
///   `Conflict`, in that order of precedence;
/// - `release` fails with `StaleOwner` unless `claim` is the current owner;
/// - `cancel` sets the flag idempotently, keeping revision and bytes;
/// - `unfinished(limit)` returns up to `limit` ids whose phase is `Pending`
///   or `Suspended` and that no live claim holds, oldest first where the
///   store can tell.
///
/// Each operation must finish within the call timeout (5 seconds by
/// default, `with_call_timeout`); saga gives up on a slower one with
/// `TimedOut`.
pub fn new(
  create create: fn(String, BitArray) -> Result(Stored, Error),
  load load: fn(String) -> Result(Stored, Error),
  claim claim: fn(String) -> Result(#(Claim, Stored), Error),
  commit commit: fn(Claim, Commit) -> Result(Stored, Error),
  release release: fn(Claim) -> Result(Nil, Error),
  cancel cancel: fn(String) -> Result(Nil, Error),
  unfinished unfinished: fn(Int) -> Result(List(String), Error),
) -> Storage {
  Storage(
    create:,
    load:,
    claim:,
    commit:,
    release:,
    cancel:,
    unfinished:,
    renewal: None,
    call_timeout: duration.seconds(5),
  )
}

/// Declares that claims expire unless renewed: while a runner owns an
/// execution, saga calls `renew(claim)` every `every`, from a
/// process linked to the runner, so a live runner keeps its claim however
/// long a step takes, and a lost one stops renewing. `renew` fails with
/// `StaleOwner` once the claim was taken over, which stops the runner.
/// Choose `every` well inside the lease, a third of it for example. A renewal
/// that fails with `Unavailable` or `TimedOut` is tried again at the next
/// interval; writes stay fenced by the claim if the lease expires meanwhile.
/// `new` starts without renewal, so a wrapper that rebuilds an adapter's
/// storage with `new` must declare the adapter's renewal again.
pub fn with_renewal(
  storage: Storage,
  every every: Duration,
  renew renew: fn(Claim) -> Result(Nil, Error),
) -> Storage {
  Storage(..storage, renewal: Some(#(every, renew)))
}

/// Bounds each storage operation saga performs (default 5 seconds). A slower
/// operation fails with `TimedOut`; a runner whose storage call times out
/// stops and keeps its last committed checkpoint. A bound below 1
/// millisecond is raised to 1 millisecond.
pub fn with_call_timeout(storage: Storage, timeout: Duration) -> Storage {
  let milliseconds = int.max(1, duration.to_milliseconds(timeout))
  Storage(..storage, call_timeout: duration.milliseconds(milliseconds))
}

// The operations, for `saga/durable` and `saga/storage/conformance`.

@internal
pub fn do_create(storage: Storage, id: String, data: BitArray) {
  storage.create(id, data)
}

@internal
pub fn do_load(storage: Storage, id: String) {
  storage.load(id)
}

@internal
pub fn do_claim(storage: Storage, id: String) {
  storage.claim(id)
}

@internal
pub fn do_commit(storage: Storage, claim: Claim, commit: Commit) {
  storage.commit(claim, commit)
}

@internal
pub fn do_release(storage: Storage, claim: Claim) {
  storage.release(claim)
}

@internal
pub fn do_cancel(storage: Storage, id: String) {
  storage.cancel(id)
}

@internal
pub fn do_unfinished(storage: Storage, limit: Int) {
  storage.unfinished(limit)
}

@internal
pub fn renewal(
  storage: Storage,
) -> Option(#(Duration, fn(Claim) -> Result(Nil, Error))) {
  storage.renewal
}

@internal
pub fn call_timeout(storage: Storage) -> Duration {
  storage.call_timeout
}
