import saga

pub fn bad() {
  saga.define("choice", fn(input) {
    saga.choose(
      input,
      "route",
      saga.map(input, fn(_) { True }),
      fn(port) { saga.map(port, fn(_) { "text" }) },
      fn(port) { saga.map(port, fn(_) { 42 }) },
    )
  })
}
