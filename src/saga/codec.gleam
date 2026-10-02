//// Converts saved values to and from text with a version, for
//// `saga/durable` checkpoints.
////
//// Use this module only with `saga/durable`: local runs with
//// `saga/execution` need no codecs. `durable.prepare` takes a codec for a
//// workflow's input, output, error and undo error, and `saga.recoverable`
//// takes one for a step's input and output. The version of every codec
//// enters the workflow's compatibility stamp, so changing a codec's version
//// makes saved checkpoints incompatible instead of misread.
////
//// ```gleam
//// import gleam/int
//// import saga/codec
////
//// let amount =
////   codec.new("amount-1", fn(n) { Ok(int.to_string(n)) }, fn(text) {
////     case int.parse(text) {
////       Ok(n) -> Ok(n)
////       Error(Nil) -> Error("invalid amount")
////     }
////   })
//// ```

import gleam/result
import saga/internal/ffi

/// A versioned pair of conversions between a value and saved text.
pub opaque type Codec(a) {
  Codec(
    version: String,
    encode: fn(a) -> Result(String, String),
    decode: fn(String) -> Result(a, String),
  )
}

/// Builds a codec from a version and two conversions. `decode` must accept
/// every text `encode` produces. `durable.prepare` rejects an empty version.
pub fn new(
  version: String,
  encode: fn(a) -> Result(String, String),
  decode: fn(String) -> Result(a, String),
) -> Codec(a) {
  Codec(version, encode, decode)
}

/// Returns the codec's version.
pub fn version(codec: Codec(a)) -> String {
  codec.version
}

/// Encodes `value`, then decodes the text once to check that it round-trips.
/// A raised exception in either conversion becomes `Error(reason)`.
pub fn encode(codec: Codec(a), value: a) -> Result(String, String) {
  case
    ffi.rescue(fn() {
      use text <- result.try(codec.encode(value))
      use _ <- result.try(codec.decode(text))
      Ok(text)
    })
  {
    ffi.Rescued(result) -> result
    ffi.Raised(_, reason) -> Error(reason)
  }
}

/// Decodes `text`. A raised exception becomes `Error(reason)`.
pub fn decode(codec: Codec(a), text: String) -> Result(a, String) {
  case ffi.rescue(fn() { codec.decode(text) }) {
    ffi.Rescued(result) -> result
    ffi.Raised(_, reason) -> Error(reason)
  }
}

/// The identity codec for `String`, version `"text-1"`.
pub fn text() -> Codec(String) {
  new("text-1", Ok, Ok)
}

/// A codec for `Bool` as `"true"`/`"false"`, version `"bool-1"`.
pub fn bool() -> Codec(Bool) {
  new(
    "bool-1",
    fn(value) {
      Ok(case value {
        True -> "true"
        False -> "false"
      })
    },
    fn(value) {
      case value {
        "true" -> Ok(True)
        "false" -> Ok(False)
        _ -> Error("invalid Bool")
      }
    },
  )
}
