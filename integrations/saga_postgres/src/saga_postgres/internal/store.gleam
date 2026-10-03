//// The `saga/storage` operations over one table, `saga_executions`.
////
//// Every write is one conditional statement, committed on its own. A
//// statement that changes no row is diagnosed by reading the row back:
//// the conditions only move one way (revision and generation grow, the
//// cancellation flag only turns on, a token is replaced by a claim or
//// cleared by a release), so the row read afterwards names the reason.
//// Lease expiry is judged by the database's clock alone
//// (`clock_timestamp()`). A live claim is a row whose `owner_token` is set
//// and whose `lease_until` lies ahead of that clock. A claim whose lease
//// expired stays the current claim, and may still commit, release or
//// renew, until another claim replaces it.
////
//// The statements are written for READ COMMITTED, PostgreSQL's default. A
//// write that fails to serialise under REPEATABLE READ or SERIALIZABLE is
//// retried a few times.

import gleam/dynamic/decode
import gleam/int
import gleam/option
import gleam/result
import gleam/string
import gleam/time/duration
import pog
import saga/storage.{type Claim, type Storage, type Stored}

/// How long a query may take, below saga's default call timeout of
/// 5 000 ms, so that a slow query fails here with a typed error first.
pub const query_timeout = 4500

/// The SQL of one store, built once for its table.
type Statements {
  Statements(
    create: String,
    load: String,
    owner: String,
    claim: String,
    commit: String,
    release: String,
    renew: String,
    cancel: String,
    unfinished: String,
  )
}

fn statements(table: String) -> Statements {
  let lease_until = "clock_timestamp() + $2::bigint * interval '1 millisecond'"
  Statements(
    create: "INSERT INTO "
      <> table
      <> " (id, revision, generation, cancelled, phase, data) VALUES ($1, 0, 0, false, 'pending', $2) ON CONFLICT (id) DO NOTHING RETURNING revision, generation, cancelled, data",
    load: "SELECT revision, generation, cancelled, data FROM "
      <> table
      <> " WHERE id = $1",
    owner: "SELECT generation, cancelled, owner_token FROM "
      <> table
      <> " WHERE id = $1",
    claim: "UPDATE "
      <> table
      <> " SET generation = generation + 1, owner_token = gen_random_uuid()::text, lease_until = "
      <> lease_until
      <> ", updated_at = clock_timestamp() WHERE id = $1 AND (owner_token IS NULL OR lease_until <= clock_timestamp()) RETURNING owner_token, revision, generation, cancelled, data",
    commit: "UPDATE "
      <> table
      <> " SET revision = revision + 1, phase = $6, data = $7, lease_until = "
      <> lease_until
      <> ", updated_at = clock_timestamp() WHERE id = $1 AND generation = $3 AND owner_token = $4 AND revision = $5 AND cancelled = $8 RETURNING revision, generation, cancelled, data",
    release: "UPDATE "
      <> table
      <> " SET owner_token = NULL, lease_until = NULL, updated_at = clock_timestamp() WHERE id = $1 AND generation = $2 AND owner_token = $3",
    renew: "UPDATE "
      <> table
      <> " SET lease_until = "
      <> lease_until
      <> " WHERE id = $1 AND generation = $3 AND owner_token = $4",
    cancel: "UPDATE "
      <> table
      <> " SET cancelled = true, updated_at = clock_timestamp() WHERE id = $1",
    unfinished: "SELECT id FROM "
      <> table
      <> " WHERE phase <> 'finished' AND (owner_token IS NULL OR lease_until <= clock_timestamp()) ORDER BY created_at, id LIMIT $1",
  )
}

/// The storage over `table` (a quoted, schema-qualified name), whose
/// claims last `lease` milliseconds after their last claim, commit or
/// renewal, renewed every third of it. The lease is in milliseconds because
/// it is a SQL parameter; the public `saga_postgres.with_lease` takes a
/// `Duration`.
pub fn new(connection: pog.Connection, table: String, lease: Int) -> Storage {
  let sql = statements(table)
  storage.new(
    create: fn(id, data) { create(connection, sql, id, data) },
    load: fn(id) { load(connection, sql, id) },
    claim: fn(id) { claim(connection, sql, lease, id) },
    commit: fn(claim, change) { commit(connection, sql, lease, claim, change) },
    release: fn(claim) { release(connection, sql, claim) },
    cancel: fn(id) { cancel(connection, sql, id) },
    unfinished: fn(limit) { unfinished(connection, sql, limit) },
  )
  |> storage.with_renewal(
    every: duration.milliseconds(int.max(1, lease / 3)),
    renew: fn(claim) { renew(connection, sql, lease, claim) },
  )
}

fn stored_decoder(from: Int) -> decode.Decoder(Stored) {
  use revision <- decode.field(from, decode.int)
  use generation <- decode.field(from + 1, decode.int)
  use cancelled <- decode.field(from + 2, decode.bool)
  use data <- decode.field(from + 3, decode.bit_array)
  decode.success(storage.stored(revision:, generation:, cancelled:, data:))
}

fn create(
  connection: pog.Connection,
  sql: Statements,
  id: String,
  data: BitArray,
) -> Result(Stored, storage.Error) {
  use rows <- result.try(
    pog.query(sql.create)
    |> pog.parameter(pog.text(id))
    |> pog.parameter(pog.bytea(data))
    |> pog.returning(stored_decoder(0))
    |> execute(connection),
  )
  case rows {
    [stored] -> Ok(stored)
    _ -> Error(storage.AlreadyExists)
  }
}

fn load(
  connection: pog.Connection,
  sql: Statements,
  id: String,
) -> Result(Stored, storage.Error) {
  use rows <- result.try(
    pog.query(sql.load)
    |> pog.parameter(pog.text(id))
    |> pog.returning(stored_decoder(0))
    |> execute(connection),
  )
  case rows {
    [stored] -> Ok(stored)
    _ -> Error(storage.NotFound)
  }
}

/// A row's ownership state, read to diagnose a refused write.
type Owner {
  Owner(generation: Int, cancelled: Bool, token: String)
}

fn owner(
  connection: pog.Connection,
  sql: Statements,
  id: String,
) -> Result(Owner, storage.Error) {
  use rows <- result.try(
    pog.query(sql.owner)
    |> pog.parameter(pog.text(id))
    |> pog.returning({
      use generation <- decode.field(0, decode.int)
      use cancelled <- decode.field(1, decode.bool)
      use token <- decode.field(2, decode.optional(decode.string))
      decode.success(Owner(
        generation:,
        cancelled:,
        token: option.unwrap(token, ""),
      ))
    })
    |> execute(connection),
  )
  case rows {
    [owner] -> Ok(owner)
    _ -> Error(storage.NotFound)
  }
}

fn owns(owner: Owner, claim: Claim) -> Bool {
  owner.token != ""
  && owner.token == storage.claim_token(claim)
  && owner.generation == storage.claim_generation(claim)
}

fn claim(
  connection: pog.Connection,
  sql: Statements,
  lease: Int,
  id: String,
) -> Result(#(Claim, Stored), storage.Error) {
  use rows <- result.try(
    pog.query(sql.claim)
    |> pog.parameter(pog.text(id))
    |> pog.parameter(pog.int(lease))
    |> pog.returning({
      use token <- decode.field(0, decode.string)
      use stored <- decode.then(stored_decoder(1))
      decode.success(#(token, stored))
    })
    |> execute(connection),
  )
  case rows {
    [#(token, stored)] ->
      Ok(#(
        storage.claim(id:, generation: storage.generation(stored), token:),
        stored,
      ))
    _ -> owner(connection, sql, id) |> result.try(fn(_) { Error(storage.Busy) })
  }
}

fn commit(
  connection: pog.Connection,
  sql: Statements,
  lease: Int,
  claim: Claim,
  commit: storage.Commit,
) -> Result(Stored, storage.Error) {
  let id = storage.claim_id(claim)
  use rows <- result.try(
    pog.query(sql.commit)
    |> pog.parameter(pog.text(id))
    |> pog.parameter(pog.int(lease))
    |> pog.parameter(pog.int(storage.claim_generation(claim)))
    |> pog.parameter(pog.text(storage.claim_token(claim)))
    |> pog.parameter(pog.int(commit.expected_revision))
    |> pog.parameter(pog.text(phase(commit.phase)))
    |> pog.parameter(pog.bytea(commit.data))
    |> pog.parameter(pog.bool(commit.observed_cancelled))
    |> pog.returning(stored_decoder(0))
    |> execute(connection),
  )
  case rows {
    [stored] -> Ok(stored)
    _ -> {
      use current <- result.try(owner(connection, sql, id))
      Error(case owns(current, claim) {
        False -> storage.StaleOwner
        True ->
          case current.cancelled == commit.observed_cancelled {
            False -> storage.CancellationChanged
            // A row that matches every condition when read back changed
            // in between; the committer reloads either way.
            True -> storage.Conflict
          }
      })
    }
  }
}

fn release(
  connection: pog.Connection,
  sql: Statements,
  claim: Claim,
) -> Result(Nil, storage.Error) {
  let id = storage.claim_id(claim)
  use count <- result.try(
    pog.query(sql.release)
    |> pog.parameter(pog.text(id))
    |> pog.parameter(pog.int(storage.claim_generation(claim)))
    |> pog.parameter(pog.text(storage.claim_token(claim)))
    |> count(connection),
  )
  case count {
    0 ->
      owner(connection, sql, id)
      |> result.try(fn(_) { Error(storage.StaleOwner) })
    _ -> Ok(Nil)
  }
}

fn renew(
  connection: pog.Connection,
  sql: Statements,
  lease: Int,
  claim: Claim,
) -> Result(Nil, storage.Error) {
  let id = storage.claim_id(claim)
  use count <- result.try(
    pog.query(sql.renew)
    |> pog.parameter(pog.text(id))
    |> pog.parameter(pog.int(lease))
    |> pog.parameter(pog.int(storage.claim_generation(claim)))
    |> pog.parameter(pog.text(storage.claim_token(claim)))
    |> count(connection),
  )
  case count {
    0 ->
      owner(connection, sql, id)
      |> result.try(fn(_) { Error(storage.StaleOwner) })
    _ -> Ok(Nil)
  }
}

fn cancel(
  connection: pog.Connection,
  sql: Statements,
  id: String,
) -> Result(Nil, storage.Error) {
  use count <- result.try(
    pog.query(sql.cancel)
    |> pog.parameter(pog.text(id))
    |> count(connection),
  )
  case count {
    0 -> Error(storage.NotFound)
    _ -> Ok(Nil)
  }
}

fn unfinished(
  connection: pog.Connection,
  sql: Statements,
  limit: Int,
) -> Result(List(String), storage.Error) {
  case limit > 0 {
    False -> Ok([])
    True ->
      pog.query(sql.unfinished)
      |> pog.parameter(pog.int(limit))
      |> pog.returning(decode.field(0, decode.string, decode.success))
      |> execute(connection)
  }
}

fn phase(phase: storage.Phase) -> String {
  case phase {
    storage.Pending -> "pending"
    storage.Suspended -> "suspended"
    storage.Finished -> "finished"
  }
}

fn execute(
  query: pog.Query(a),
  connection: pog.Connection,
) -> Result(List(a), storage.Error) {
  run(query, connection, 3) |> result.map(fn(returned) { returned.rows })
}

fn count(
  query: pog.Query(a),
  connection: pog.Connection,
) -> Result(Int, storage.Error) {
  run(query, connection, 3) |> result.map(fn(returned) { returned.count })
}

fn run(
  query: pog.Query(a),
  connection: pog.Connection,
  attempts: Int,
) -> Result(pog.Returned(a), storage.Error) {
  case pog.timeout(query, query_timeout) |> pog.execute(connection) {
    Ok(returned) -> Ok(returned)
    // A serialization failure or a deadlock changed nothing: try again.
    Error(pog.PostgresqlError(code:, ..))
      if attempts > 1 && { code == "40001" || code == "40P01" }
    -> run(query, connection, attempts - 1)
    Error(error) -> Error(from_query_error(error))
  }
}

fn from_query_error(error: pog.QueryError) -> storage.Error {
  case error {
    pog.QueryTimeout -> storage.TimedOut
    pog.UnexpectedResultType(_) -> storage.Corrupt
    _ -> storage.Unavailable(describe(error))
  }
}

/// A query failure as text, for logs. It names no credential: pog's errors
/// carry none.
pub fn describe(error: pog.QueryError) -> String {
  case error {
    pog.ConnectionUnavailable -> "no database connection is available"
    pog.QueryTimeout -> "the query timed out"
    pog.PostgresqlError(code, name, message) ->
      "PostgreSQL " <> code <> " " <> name <> ": " <> message
    pog.ConstraintViolated(message, constraint, _) ->
      "constraint " <> constraint <> " violated: " <> message
    pog.UnexpectedArgumentCount(expected, got) ->
      "expected "
      <> int.to_string(expected)
      <> " query arguments, got "
      <> int.to_string(got)
    pog.UnexpectedArgumentType(expected, got) ->
      "expected a query argument of type " <> expected <> ", got " <> got
    pog.UnexpectedResultType(errors) ->
      "unexpected result: " <> string.inspect(errors)
  }
}
