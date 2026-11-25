//// Stores executions' checkpoints in memory, in one actor process.
////
//// Use this adapter for tests, and for `saga/durable` runs that must survive
//// the loss of their runner or caller but not of the VM. One store holds
//// every execution, each under its own id. Use `saga/storage/file` when a
//// run must survive VM shutdown, or the `saga_postgres` package to share
//// runs across VMs.
////
//// ```gleam
//// import saga/durable
//// import saga/storage/memory
////
//// let assert Ok(store) = memory.start()
//// let assert Ok(run) =
////   durable.start_or_reconnect(persistence, memory.storage(store), id: "order-1", input: order)
//// // ...
//// memory.stop(store)
//// ```
////
//// Under a supervisor, name the store and find it by name; the handle stays
//// valid across restarts, which lose every saved execution:
////
//// ```gleam
//// let name = process.new_name("checkouts")
//// let child = memory.supervised(name)
//// // ... after the supervisor started:
//// let storage = memory.storage(memory.named(name))
//// ```
////
//// A claim ends when it is released or when the process that took it exits;
//// a runner's claim therefore ends with its runner. Each operation waits
//// at most 5 seconds for the store, then fails with `storage.TimedOut`.

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/string
import saga/internal/ffi
import saga/storage.{type Claim, type Storage, type Stored}

/// A running memory store.
pub opaque type Memory {
  Memory(subject: Subject(Message))
}

/// The store's messages; only this module sends them.
pub opaque type Message {
  Create(
    id: String,
    records: Dict(String, BitArray),
    reply: Subject(Result(Nil, Error)),
  )
  Load(id: String, reply: Subject(Result(Stored, Error)))
  Take(id: String, from: Pid, reply: Subject(Result(#(Claim, Stored), Error)))
  Commit(
    claim: Claim,
    commit: storage.Commit,
    reply: Subject(Result(Nil, Error)),
  )
  Release(claim: Claim, reply: Subject(Result(Nil, Error)))
  Cancel(id: String, reply: Subject(Result(Nil, Error)))
  List(listing: storage.Listing, reply: Subject(Result(List(String), Error)))
  OwnerDown(down: process.Down)
  Shutdown
}

type Error =
  storage.Error

type Owner {
  Owner(token: String, monitor: process.Monitor)
}

type Entry {
  Entry(
    revision: Int,
    generation: Int,
    cancelled: Bool,
    phase: storage.Phase,
    records: Dict(String, BitArray),
    owner: Option(Owner),
  )
}

type State {
  State(entries: Dict(String, Entry))
}

/// Starts a store linked to the calling process.
pub fn start() -> Result(Memory, actor.StartError) {
  builder(None) |> actor.start |> result.map(fn(started) { started.data })
}

/// A child specification that starts a store registered under `name`. Find
/// it with `named(name)`.
pub fn supervised(
  name: process.Name(Message),
) -> supervision.ChildSpecification(Memory) {
  supervision.worker(fn() { builder(Some(name)) |> actor.start })
}

/// The store registered under `name`, whether or not it runs yet.
pub fn named(name: process.Name(Message)) -> Memory {
  Memory(process.named_subject(name))
}

/// Stops the store and discards its executions.
pub fn stop(memory: Memory) -> Nil {
  process.send(memory.subject, Shutdown)
}

fn builder(
  name: Option(process.Name(Message)),
) -> actor.Builder(State, Message, Memory) {
  let builder =
    actor.new_with_initialiser(5000, fn(subject) {
      let selector =
        process.new_selector()
        |> process.select(subject)
        |> process.select_monitors(OwnerDown)
      actor.initialised(State(dict.new()))
      |> actor.selecting(selector)
      |> actor.returning(Memory(subject))
      |> Ok
    })
    |> actor.on_message(handle)
  case name {
    Some(name) -> actor.named(builder, name)
    None -> builder
  }
}

/// The `Storage` operations of this store.
pub fn storage(memory: Memory) -> Storage {
  storage.new(
    create: fn(id, records) { request(memory, Create(id, records, _)) },
    load: fn(id) { request(memory, Load(id, _)) },
    claim: fn(id) { request(memory, Take(id, process.self(), _)) },
    commit: fn(claim, commit) { request(memory, Commit(claim, commit, _)) },
    release: fn(claim) { request(memory, Release(claim, _)) },
    cancel: fn(id) { request(memory, Cancel(id, _)) },
    list: fn(listing) { request(memory, List(listing, _)) },
  )
}

fn request(
  memory: Memory,
  make: fn(Subject(Result(a, Error))) -> Message,
) -> Result(a, Error) {
  case process.subject_owner(memory.subject) {
    Error(Nil) -> Error(storage.Unavailable("the memory store is not running"))
    Ok(pid) -> {
      let reply = process.new_subject()
      let monitor = process.monitor(pid)
      process.send(memory.subject, make(reply))
      let selector =
        process.new_selector()
        |> process.select(reply)
        |> process.select_specific_monitor(monitor, fn(_) {
          Error(storage.Unavailable("the memory store stopped"))
        })
      let result = case process.selector_receive(selector, 5000) {
        Ok(result) -> result
        Error(Nil) -> Error(storage.TimedOut)
      }
      process.demonitor_process(monitor)
      result
    }
  }
}

fn stored(entry: Entry) -> Stored {
  storage.stored(
    revision: entry.revision,
    generation: entry.generation,
    cancelled: entry.cancelled,
    records: entry.records,
  )
}

fn handle(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    Create(id, records, reply) ->
      case dict.get(state.entries, id) {
        Ok(_) -> {
          process.send(reply, Error(storage.AlreadyExists))
          actor.continue(state)
        }
        Error(Nil) -> {
          let entry = Entry(0, 0, False, storage.Pending, records, None)
          process.send(reply, Ok(Nil))
          actor.continue(put(state, id, entry))
        }
      }
    Load(id, reply) -> {
      process.send(reply, existing(state, id) |> result.map(stored))
      actor.continue(state)
    }
    Take(id, from, reply) ->
      case existing(state, id) {
        Error(error) -> {
          process.send(reply, Error(error))
          actor.continue(state)
        }
        Ok(Entry(owner: Some(_), ..)) -> {
          process.send(reply, Error(storage.Busy))
          actor.continue(state)
        }
        Ok(entry) -> {
          let token = "memory-" <> int.to_string(ffi.unique_integer())
          let entry =
            Entry(
              ..entry,
              generation: entry.generation + 1,
              owner: Some(Owner(token, process.monitor(from))),
            )
          let claim =
            storage.claim(id: id, generation: entry.generation, token: token)
          process.send(reply, Ok(#(claim, stored(entry))))
          actor.continue(put(state, id, entry))
        }
      }
    Commit(claim, commit, reply) ->
      case owned(state, claim) {
        Error(error) -> {
          process.send(reply, Error(error))
          actor.continue(state)
        }
        Ok(entry) ->
          case
            commit.observed_cancelled == entry.cancelled,
            commit.expected_revision == entry.revision
          {
            False, _ -> {
              process.send(reply, Error(storage.CancellationChanged))
              actor.continue(state)
            }
            True, False -> {
              process.send(reply, Error(storage.Conflict))
              actor.continue(state)
            }
            True, True -> {
              let entry =
                Entry(
                  ..entry,
                  revision: entry.revision + 1,
                  phase: commit.phase,
                  records: dict.merge(entry.records, commit.records),
                )
              process.send(reply, Ok(Nil))
              actor.continue(put(state, storage.claim_id(claim), entry))
            }
          }
      }
    Release(claim, reply) ->
      case owned(state, claim) {
        Error(error) -> {
          process.send(reply, Error(error))
          actor.continue(state)
        }
        Ok(entry) -> {
          option.map(entry.owner, fn(owner) {
            process.demonitor_process(owner.monitor)
          })
          process.send(reply, Ok(Nil))
          actor.continue(put(
            state,
            storage.claim_id(claim),
            Entry(..entry, owner: None),
          ))
        }
      }
    Cancel(id, reply) ->
      case existing(state, id) {
        Error(error) -> {
          process.send(reply, Error(error))
          actor.continue(state)
        }
        Ok(entry) -> {
          process.send(reply, Ok(Nil))
          actor.continue(put(state, id, Entry(..entry, cancelled: True)))
        }
      }
    List(storage.Listing(phase:, after:, limit:), reply) -> {
      let ids =
        dict.to_list(state.entries)
        |> list.filter_map(fn(pair) {
          let #(id, entry) = pair
          let later = case after {
            Some(after) -> string.compare(id, after) == order.Gt
            None -> True
          }
          case later && entry.owner == None && entry.phase == phase {
            True -> Ok(id)
            False -> Error(Nil)
          }
        })
        |> list.sort(string.compare)
        |> list.take(limit)
      process.send(reply, Ok(ids))
      actor.continue(state)
    }
    OwnerDown(process.ProcessDown(monitor:, ..)) -> {
      let entries =
        dict.map_values(state.entries, fn(_, entry) {
          case entry.owner {
            Some(owner) if owner.monitor == monitor ->
              Entry(..entry, owner: None)
            _ -> entry
          }
        })
      actor.continue(State(entries: entries))
    }
    OwnerDown(process.PortDown(..)) -> actor.continue(state)
    Shutdown -> actor.stop()
  }
}

fn existing(state: State, id: String) -> Result(Entry, Error) {
  dict.get(state.entries, id) |> result.replace_error(storage.NotFound)
}

fn owned(state: State, claim: Claim) -> Result(Entry, Error) {
  use entry <- result.try(existing(state, storage.claim_id(claim)))
  let token = storage.claim_token(claim)
  let generation = storage.claim_generation(claim)
  case entry.owner {
    Some(owner) if owner.token == token && entry.generation == generation ->
      Ok(entry)
    _ -> Error(storage.StaleOwner)
  }
}

fn put(state: State, id: String, entry: Entry) -> State {
  State(entries: dict.insert(state.entries, id, entry))
}
