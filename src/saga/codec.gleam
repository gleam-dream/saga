//// Converts saved values to and from text with a version, for
//// `saga/durable` checkpoints.
////
//// Use this module only with `saga/durable`: local runs with
//// `saga/execution` need no codecs. `durable.new` takes a codec for a
//// workflow's input, output, error and undo error, and
//// `durable.recoverable` takes one for a step's input and output. The
//// version of every codec enters the workflow's compatibility stamp, so
//// changing a codec's version makes saved checkpoints incompatible instead
//// of misread.
////
//// `json` is the ordinary constructor: it takes the `gleam/json` encoder and
//// the `gleam/dynamic/decode` decoder an application already has. The
//// encoder may refuse a value with a message, so a JSON Blueprint codec
//// bridges with `result.map_error`:
////
//// ```gleam
//// import gleam/result
//// import json/blueprint/codec as blueprint
//// import saga/codec
////
//// let order =
////   codec.json(
////     "order-1",
////     fn(order) {
////       blueprint.to_json(order_codec(), order)
////       |> result.map_error(blueprint.describe_encode_error)
////     },
////     blueprint.decoder(order_codec()),
////   )
//// ```
////
//// A plain encoder that cannot fail is wrapped in `Ok`:
//// `codec.json("amount-1", fn(n) { Ok(json.int(n)) }, decode.int)`.

import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/result
import gleam/string
import saga/internal/ffi

/// A versioned pair of conversions between a value and saved text.
pub opaque type Codec(a) {
  Codec(
    version: String,
    encode: fn(a) -> Result(String, CodecError),
    decode: fn(String) -> Result(a, CodecError),
  )
}

/// Why a codec could not save or restore a value. The union is closed.
pub type CodecError {
  /// The encoder refused the value; `detail` is its message, for logs.
  EncodeFailed(detail: String)
  /// A `new` codec's decoder refused the saved text; `detail` is its
  /// message, for logs.
  DecodeFailed(detail: String)
  /// A `json` codec's saved text was not JSON, or its decoder refused it.
  JsonDecodeFailed(error: json.DecodeError)
  /// The text the encoder produced did not decode back; `error` is the
  /// decoding failure. Saga checks this before every effect.
  RoundTripFailed(error: CodecError)
  /// A conversion raised; `reason` is the formatted exception, for logs.
  CodecRaised(reason: String)
}

/// A codec over `gleam/json`: `encode` builds the JSON of a value or refuses
/// it with a message, and `decoder` reads it back. `durable.new` panics on an
/// empty version.
pub fn json(
  version: String,
  encode: fn(a) -> Result(json.Json, String),
  decoder: decode.Decoder(a),
) -> Codec(a) {
  Codec(
    version,
    fn(value) {
      encode(value)
      |> result.map(json.to_string)
      |> result.map_error(EncodeFailed)
    },
    fn(text) { json.parse(text, decoder) |> result.map_error(JsonDecodeFailed) },
  )
}

/// A codec from a version and two text conversions, for a format other
/// than JSON. `decode` must accept every text `encode` produces.
/// `durable.new` panics on an empty version.
pub fn new(
  version: String,
  encode: fn(a) -> Result(String, String),
  decode: fn(String) -> Result(a, String),
) -> Codec(a) {
  Codec(
    version,
    fn(value) { encode(value) |> result.map_error(EncodeFailed) },
    fn(text) { decode(text) |> result.map_error(DecodeFailed) },
  )
}

/// The identity codec for `String`, version `"text-1"`.
pub fn text() -> Codec(String) {
  Codec("text-1", Ok, Ok)
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
        _ -> Error("not a Bool")
      }
    },
  )
}

/// A codec for `Int` as its decimal text, version `"int-1"`.
pub fn int() -> Codec(Int) {
  new("int-1", fn(value) { Ok(int.to_string(value)) }, fn(text) {
    int.parse(text) |> result.replace_error("not an Int")
  })
}

/// Describes a codec error for logs.
pub fn describe_error(error: CodecError) -> String {
  case error {
    EncodeFailed(detail) -> "the encoder refused the value: " <> detail
    DecodeFailed(detail) -> "the decoder refused the saved text: " <> detail
    JsonDecodeFailed(json.UnexpectedEndOfInput) ->
      "the saved JSON ended unexpectedly"
    JsonDecodeFailed(json.UnexpectedByte(byte)) ->
      "the saved JSON has an unexpected byte " <> byte
    JsonDecodeFailed(json.UnexpectedSequence(sequence)) ->
      "the saved JSON has an unexpected sequence " <> sequence
    JsonDecodeFailed(json.UnableToDecode(errors)) ->
      "the saved JSON does not match the decoder: "
      <> string.join(
        list.map(errors, fn(error) {
          "expected "
          <> error.expected
          <> ", found "
          <> error.found
          <> " at "
          <> string.join(error.path, ".")
        }),
        "; ",
      )
    RoundTripFailed(error) ->
      "the encoded value does not decode back: " <> describe_error(error)
    CodecRaised(reason) -> "a conversion raised: " <> reason
  }
}

/// Returns the codec's version.
@internal
pub fn version(codec: Codec(a)) -> String {
  codec.version
}

/// Encodes `value`, then decodes the text once to check that it round-trips.
/// A raised exception in either conversion becomes `CodecRaised`.
@internal
pub fn encode(codec: Codec(a), value: a) -> Result(String, CodecError) {
  case
    ffi.rescue(fn() {
      use text <- result.try(codec.encode(value))
      use _ <- result.try(
        codec.decode(text) |> result.map_error(RoundTripFailed),
      )
      Ok(text)
    })
  {
    ffi.Rescued(result) -> result
    ffi.Raised(_, reason) -> Error(CodecRaised(reason))
  }
}

/// Decodes `text`. A raised exception becomes `CodecRaised`.
@internal
pub fn decode(codec: Codec(a), text: String) -> Result(a, CodecError) {
  case ffi.rescue(fn() { codec.decode(text) }) {
    ffi.Rescued(result) -> result
    ffi.Raised(_, reason) -> Error(CodecRaised(reason))
  }
}
