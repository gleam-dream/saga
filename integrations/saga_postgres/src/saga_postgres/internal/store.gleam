//// The `saga/storage` operations over two tables: `saga_executions`, one row
//// per execution with its revision, ownership, cancellation and phase, and
//// `saga_records`, one row per checkpoint record.
////
//// Every write but a claim is one conditional statement, committed on its
//// own: a commit updates the execution row and upserts its records in one
//// statement, so a refused commit writes no record. A claim updates the
//// execution row and reads its records in one READ COMMITTED transaction,
//// so the records it returns belong to its own generation and a failed
//// read takes no ownership. Every other read of records is one statement,
//// so it sees one committed revision. A statement that
//// changes no row is diagnosed by a later read, which can observe further
//// concurrent changes. Commit diagnosis checks missing
//// execution, then ownership, cancellation and progress disagreements.
//// Lease expiry is judged by the database's clock alone
//// (`clock_timestamp()`). A live claim is a row whose `owner_token` is set
//// and whose `lease_until` lies ahead of that clock. A claim whose lease
//// expired stays the current claim, and may still commit, release or
//// renew, until another claim replaces it.
////
//// The statements are written for READ COMMITTED, PostgreSQL's default. A
//// write that fails to serialise under REPEATABLE READ or SERIALIZABLE is
//// retried a few times.

import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/duration
import pog
import saga/storage.{type Claim, type Storage, type Stored}

/// The pool-backed query attempt budget in milliseconds. Each retry and
/// refusal-diagnosis query receives its own budget. Saga's default call wait
/// is 5 seconds; this budget is not a total adapter-operation deadline.
/// Already checked-out transaction connections use the driver's timing.
pub const query_timeout = 4500

/// The SQL of one store, built once for its tables.
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
    list: String,
  )
}

fn statements(schema: String) -> Statements {
  let executions = schema <> ".saga_executions"
  let records = schema <> ".saga_records"
  let lease_until = "clock_timestamp() + $2::bigint * interval '1 millisecond'"
  Statements(
    create: "WITH created AS (INSERT INTO "
      <> executions
      <> " (id, revision, generation, cancelled, phase) VALUES ($1, 0, 0, false, 'pending') ON CONFLICT (id) DO NOTHING RETURNING id), written AS (INSERT INTO "
      <> records
      <> " (execution_id, key, data) SELECT created.id, r.key, r.data FROM created, unnest($2::text[], $3::bytea[]) AS r(key, data)) SELECT count(*) FROM created",
    load: "SELECT e.revision, e.generation, e.cancelled, r.key, r.data FROM "
      <> executions
      <> " e LEFT JOIN "
      <> records
      <> " r ON r.execution_id = e.id WHERE e.id = $1",
    owner: "SELECT generation, cancelled, owner_token FROM "
      <> executions
      <> " WHERE id = $1",
    claim: "UPDATE "
      <> executions
      <> " SET generation = generation + 1, owner_token = gen_random_uuid()::text, lease_until = "
      <> lease_until
      <> ", updated_at = clock_timestamp() WHERE id = $1 AND (owner_token IS NULL OR lease_until <= clock_timestamp()) RETURNING owner_token, generation",
    commit: "WITH updated AS (UPDATE "
      <> executions
      <> " SET revision = revision + 1, phase = $6, lease_until = "
      <> lease_until
      <> ", updated_at = clock_timestamp() WHERE id = $1 AND generation = $3 AND owner_token = $4 AND revision = $5 AND cancelled = $7 RETURNING id), written AS (INSERT INTO "
      <> records
      <> " (execution_id, key, data) SELECT updated.id, r.key, r.data FROM updated, unnest($8::text[], $9::bytea[]) AS r(key, data) ON CONFLICT (execution_id, key) DO UPDATE SET data = EXCLUDED.data) SELECT count(*) FROM updated",
    release: "UPDATE "
      <> executions
      <> " SET owner_token = NULL, lease_until = NULL, updated_at = clock_timestamp() WHERE id = $1 AND generation = $2 AND owner_token = $3",
    renew: "UPDATE "
      <> executions
      <> " SET lease_until = "
      <> lease_until
      <> " WHERE id = $1 AND generation = $3 AND owner_token = $4",
    cancel: "UPDATE "
      <> executions
      <> " SET cancelled = true, updated_at = clock_timestamp() WHERE id = $1",
    // Ids compare byte by byte (`COLLATE "C"`), the order the storage
    // contract states, which the `saga_executions_listing` index serves.
    list: "SELECT id FROM "
      <> executions
      <> " WHERE phase = $1 AND ($2::text IS NULL OR id COLLATE \"C\" > $2::text) AND (owner_token IS NULL OR lease_until <= clock_timestamp()) ORDER BY id COLLATE \"C\" LIMIT $3",
  )
}

/// The storage over the tables in `schema`, a quoted name. Accepted
/// claims, commits and renewals refresh the database-clock lease by `lease`
/// milliseconds. Saga owns renewal every third of that duration. The SQL
/// parameter uses milliseconds; public `saga_postgres.with_lease` takes a
/// `Duration`.
pub fn new(connection: pog.Connection, schema: String, lease: Int) -> Storage {
  let sql = statements(schema)
  storage.new(
    create: fn(id, records) { create(connection, sql, id, records) },
    load: fn(id) { load(connection, sql, id) },
    claim: fn(id) { claim(connection, sql, lease, id) },
    commit: fn(claim, change) { commit(connection, sql, lease, claim, change) },
    release: fn(claim) { release(connection, sql, claim) },
    cancel: fn(id) { cancel(connection, sql, id) },
    list: fn(listing) { list(connection, sql, listing) },
  )
  |> storage.with_renewal(
    every: duration.milliseconds(int.max(1, lease / 3)),
    renew: fn(claim) { renew(connection, sql, lease, claim) },
  )
}

/// One row of `load`: the execution's state and one of its records, if it
/// has any.
type Row {
  Row(
    revision: Int,
    generation: Int,
    cancelled: Bool,
    record: Option(#(String, BitArray)),
  )
}

fn row_decoder() -> decode.Decoder(Row) {
  use revision <- decode.field(0, decode.int)
  use generation <- decode.field(1, decode.int)
  use cancelled <- decode.field(2, decode.bool)
  use key <- decode.field(3, decode.optional(decode.string))
  use data <- decode.field(4, decode.optional(decode.bit_array))
  decode.success(
    Row(revision:, generation:, cancelled:, record: case key, data {
      Some(key), Some(data) -> Some(#(key, data))
      _, _ -> None
    }),
  )
}

/// Parameters for a set of records: their keys and their bytes, in the
/// same order.
fn record_parameters(
  query: pog.Query(a),
  records: Dict(String, BitArray),
) -> pog.Query(a) {
  let pairs = dict.to_list(records)
  query
  |> pog.parameter(pog.array(pog.text, list.map(pairs, fn(pair) { pair.0 })))
  |> pog.parameter(pog.array(pog.bytea, list.map(pairs, fn(pair) { pair.1 })))
}

fn create(
  connection: pog.Connection,
  sql: Statements,
  id: String,
  records: Dict(String, BitArray),
) -> Result(Nil, storage.Error) {
  use rows <- result.try(
    pog.query(sql.create)
    |> pog.parameter(pog.text(id))
    |> record_parameters(records)
    |> pog.returning(decode.field(0, decode.int, decode.success))
    |> execute(connection),
  )
  case rows {
    [1] -> Ok(Nil)
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
    |> pog.returning(row_decoder())
    |> execute(connection),
  )
  stored_from_rows(rows)
}

fn stored_from_rows(rows: List(Row)) -> Result(Stored, storage.Error) {
  case rows {
    [] -> Error(storage.NotFound)
    [first, ..] ->
      Ok(storage.stored(
        revision: first.revision,
        generation: first.generation,
        cancelled: first.cancelled,
        records: list.filter_map(rows, fn(row) {
          option.to_result(row.record, Nil)
        })
          |> dict.from_list,
      ))
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

/// What one claim transaction established: a claim with the records of
/// its generation, or that a live claim holds the execution (or it is
/// absent).
type Claimed {
  Claimed(Claim, List(Row))
  NotClaimed
}

fn claim(
  connection: pog.Connection,
  sql: Statements,
  lease: Int,
  id: String,
) -> Result(#(Claim, Stored), storage.Error) {
  claim_attempt(connection, sql, lease, id, 3)
}

/// Takes the claim and reads the records in one READ COMMITTED
/// transaction. The claim's update holds the execution row's lock until the
/// transaction ends, so no commit can change the records between the two
/// statements, and the read sees every commit made before the claim. A read
/// that fails rolls the claim back, so a failed claim takes no ownership.
fn claim_attempt(
  connection: pog.Connection,
  sql: Statements,
  lease: Int,
  id: String,
  attempts: Int,
) -> Result(#(Claim, Stored), storage.Error) {
  let claimed =
    pog.transaction(connection, fn(connection) {
      use _ <- result.try(
        pog.query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        |> pog.timeout(query_timeout)
        |> pog.execute(connection),
      )
      use claimed <- result.try(
        pog.query(sql.claim)
        |> pog.parameter(pog.text(id))
        |> pog.parameter(pog.int(lease))
        |> pog.returning({
          use token <- decode.field(0, decode.string)
          use generation <- decode.field(1, decode.int)
          decode.success(#(token, generation))
        })
        |> pog.timeout(query_timeout)
        |> pog.execute(connection),
      )
      case claimed.rows {
        [#(token, generation)] -> {
          use loaded <- result.try(
            pog.query(sql.load)
            |> pog.parameter(pog.text(id))
            |> pog.returning(row_decoder())
            |> pog.timeout(query_timeout)
            |> pog.execute(connection),
          )
          Ok(Claimed(storage.claim(id:, generation:, token:), loaded.rows))
        }
        _ -> Ok(NotClaimed)
      }
    })
  case claimed {
    Ok(Claimed(claim, rows)) ->
      stored_from_rows(rows) |> result.map(fn(stored) { #(claim, stored) })
    Ok(NotClaimed) ->
      owner(connection, sql, id) |> result.try(fn(_) { Error(storage.Busy) })
    Error(pog.TransactionQueryError(error))
    | Error(pog.TransactionRolledBack(error)) ->
      case error {
        // A serialization failure or a deadlock rolled everything back.
        pog.PostgresqlError(code:, ..)
          if attempts > 1 && { code == "40001" || code == "40P01" }
        -> claim_attempt(connection, sql, lease, id, attempts - 1)
        _ -> Error(from_query_error(error))
      }
  }
}

fn commit(
  connection: pog.Connection,
  sql: Statements,
  lease: Int,
  claim: Claim,
  commit: storage.Commit,
) -> Result(Nil, storage.Error) {
  let id = storage.claim_id(claim)
  use rows <- result.try(
    pog.query(sql.commit)
    |> pog.parameter(pog.text(id))
    |> pog.parameter(pog.int(lease))
    |> pog.parameter(pog.int(storage.claim_generation(claim)))
    |> pog.parameter(pog.text(storage.claim_token(claim)))
    |> pog.parameter(pog.int(commit.expected_revision))
    |> pog.parameter(pog.text(phase(commit.phase)))
    |> pog.parameter(pog.bool(commit.observed_cancelled))
    |> record_parameters(commit.records)
    |> pog.returning(decode.field(0, decode.int, decode.success))
    |> execute(connection),
  )
  case rows {
    [1] -> Ok(Nil)
    _ -> {
      use current <- result.try(owner(connection, sql, id))
      Error(case owns(current, claim) {
        False -> storage.StaleOwner
        True ->
          case current.cancelled == commit.observed_cancelled {
            False -> storage.CancellationChanged
            // Ownership and cancellation still match. The caller reloads
            // the checkpoint before trying another revision.
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

fn list(
  connection: pog.Connection,
  sql: Statements,
  listing: storage.Listing,
) -> Result(List(String), storage.Error) {
  case listing.limit > 0 {
    False -> Ok([])
    True ->
      pog.query(sql.list)
      |> pog.parameter(pog.text(phase(listing.phase)))
      |> pog.parameter(pog.nullable(pog.text, listing.after))
      |> pog.parameter(pog.int(listing.limit))
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

/// A query failure as text for logs. It includes no pool configuration,
/// but server messages and decoded argument detail are passed through.
/// This is diagnostic rendering, not a general redaction boundary.
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
