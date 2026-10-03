import gleam/list
import gleam/string
import gleeunit/should

@external(erlang, "saga_module_docs_ffi", "public_sources")
fn public_sources() -> List(#(String, String))

/// `gleam docs` renders a module doc only from `////` lines; a `///` comment
/// before the imports is a doc for the first definition and renders nothing
/// at module level.
pub fn every_public_module_starts_with_a_module_doc_test() {
  let sources = public_sources()
  list.length(sources) |> should.equal(10)
  sources
  |> list.filter(fn(source) { !string.starts_with(source.1, "//// ") })
  |> list.map(fn(source) { source.0 })
  |> should.equal([])
}
