/// Versioned, checked conversion at the optional persistence boundary.
import gleam/result
import saga/internal/ffi

pub opaque type Codec(a) {
  Codec(
    version: String,
    encode: fn(a) -> Result(String, String),
    decode: fn(String) -> Result(a, String),
  )
}

pub fn new(
  version: String,
  encode: fn(a) -> Result(String, String),
  decode: fn(String) -> Result(a, String),
) -> Codec(a) {
  Codec(version, encode, decode)
}

pub fn version(codec: Codec(a)) -> String {
  codec.version
}

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

pub fn decode(codec: Codec(a), text: String) -> Result(a, String) {
  case ffi.rescue(fn() { codec.decode(text) }) {
    ffi.Rescued(result) -> result
    ffi.Raised(_, reason) -> Error(reason)
  }
}

pub fn text() -> Codec(String) {
  new("text-1", Ok, Ok)
}

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
