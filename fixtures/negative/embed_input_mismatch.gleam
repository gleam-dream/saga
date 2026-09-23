// Negative fixture: `embed` requires the embedded workflow's declared input
// type to match the enclosing port's value type. A String-input outer port
// cannot embed an Int-input inner workflow.
import saga

pub type Err {
  Err
}

fn inner_workflow() -> saga.Workflow(Int, Int, Err, Nil) {
  let assert Ok(workflow) =
    saga.define("inner", fn(input) {
      input
      |> saga.perform(saga.step("s", fn(x: Int) -> Result(Int, Err) { Ok(x) }))
    })
  workflow
}

pub fn invalid() {
  saga.define("outer", fn(input: saga.Port(String, Err, Nil)) {
    input |> saga.embed(inner_workflow())
  })
}
