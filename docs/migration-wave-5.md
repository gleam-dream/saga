# Migrating to the wave 5 saga API

Wave 5 lets a step read its execution's correlation, so the step's own HTTP
client can be correlated without threading the value by hand. A durable
execution is always correlated and keeps one correlation across drives.
`EffectKey` becomes opaque. The only breaking change is the one that
`EffectKey` makes: field reads become accessor calls.

## `saga`

### `EffectKey` is opaque, with accessors

```gleam
// before
pub type EffectKey {
  EffectKey(idempotency: String, attempt: Int, attempt_key: String)
}
key.idempotency
key.attempt
key.attempt_key
// after
pub opaque type EffectKey
pub fn idempotency_key(key: EffectKey) -> String
pub fn attempt_number(key: EffectKey) -> Int
pub fn attempt_key(key: EffectKey) -> String
pub fn correlation_of(key: EffectKey) -> Correlation

saga.idempotency_key(key)
saga.attempt_number(key)
saga.attempt_key(key)
saga.correlation_of(key)
```

Callers only receive an `EffectKey`, and the next field would have broken
positional code again, so a later release can now add to it freely. An
`EffectKey` can still be compared with `==` and stored in a tuple.
`undo.key` and `failed.key` are `EffectKey`s too:
`saga.idempotency_key(undo.key)`.

**Other records stay public.** `UndoRequest`, `FailedAttempt`,
`durable.Required`, `StepAddress`, `Crash`, `StepDescriptor`,
`execution.Settlement`, `execution.Progress` and the telemetry metadata
records are read by label in every dependent (`undo.output`,
`failed.failure`, `UndoRequest(output:, ..)`), and a labelled read or a
labelled pattern with `..` keeps compiling when a field is added. Only a
positional construction or pattern breaks, and their docs say not to write
one. Opaque accessors would turn the common callback `fn(undo) {
release(undo.output) }` into `saga.undo_output(undo)` in every undo.
`StepAddress` and `Settlement` are also built by callers, to compare against
or to fake an outcome in a test, and `StepAddress` is an identity (scope,
name, occurrence) that does not grow.

Dependents that read the key (searched `saga`, `fabric` including
`fabric_saga`, experiments and consumers, and `oversight/apps`):

| File                                                 | Before                                    | After                                               |
| ---------------------------------------------------- | ----------------------------------------- | --------------------------------------------------- |
| `oversight/apps/research_agent/.../publish.gleam:55` | `[#("idempotency-key", key.idempotency)]` | `[#("idempotency-key", saga.idempotency_key(key))]` |
| `oversight/apps/research_agent/.../publish.gleam:76` | `"/drafts?key=" <> key.idempotency`       | `"/drafts?key=" <> saga.idempotency_key(key)`       |
| `oversight/apps/checkout/.../workflow.gleam:6`       | doc comment: `EffectKey.idempotency`      | `saga.idempotency_key`                              |

`fabric`, `fabric_saga`, the experiments and consumers, `support_desk`,
`saga_postgres` and `bench` bind a key but read no field. In saga itself,
`test/`, `examples/order_consumer`, README and DURABILITY.md are migrated.

### A step reads its run's correlation

`saga.effect`, `saga.undo` (`undo.key`), `saga.compensate` (`failed.key`) and
the durable resolvers (`durable.recoverable`, `durable.resolve_undo`,
`durable.resolve_compensation`) receive an `EffectKey`, and
`saga.correlation_of(key)` is the correlation of the run. `saga.step`
receives only the step's input, as before: change it to `saga.effect` to read
the context.

```gleam
// before: the client is correlated when the workflow is built or started
saga.step("refund_payment", fn(refund: Refund) {
  shop.refund(refund.shop, refund.request)
})

// after: the step correlates its own client from the run
saga.effect("refund_payment", fn(refund: Refund, key) {
  let shop = shop.correlated(shop, saga.correlation_of(key))
  shop.refund(shop, refund.request)
})
```

| Run                                                     | `saga.correlation_of(key)`                      |
| ------------------------------------------------------- | ----------------------------------------------- |
| local, `execution.with_correlation(c)`                  | `c`                                             |
| local, none set                                         | a fresh `correlation.unique()`, chosen at start |
| durable, first drive with `durable.with_correlation(c)` | `c`, on every drive of the execution            |
| durable, first drive with none set                      | `correlation.from_key(id)`, on every drive      |
| durable, saved before this release (format 1)           | `correlation.from_key(id)`                      |

Every run has a correlation, so there is no `None` arm to write. The
`unique()` value is per run: it is the same in the run's steps, undos,
deciders and events, and a second `execution.run` gets another.

### Telemetry correlation is never optional

`correlation: Option(Correlation)` becomes `correlation: Correlation` in six
records: `RunMetadata`, `RunStopMetadata`, `StepMetadata`,
`StepStopMetadata`, `CompensationMetadata` and `UndoMetadata`.

```gleam
// before
sinal.observe(telemetry.run_stopped(), fn(_m, metadata) {
  case metadata.correlation {
    Some(c) -> log(c)
    None -> log_unjoined()
  }
})
// after
sinal.observe(telemetry.run_stopped(), fn(_m, metadata) {
  log(metadata.correlation)
})
```

The wire encoding is unchanged, so an Erlang or Elixir handler is
unaffected. Call sites that change (a `None` arm or an `Option` parameter that
receives `d.correlation` / `m.correlation` of a saga event, or
`saga.correlation_of(key)`; the apps are migrated by other agents):

| Dependent                                                                  | Site                                                                                      |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `oversight/apps/checkout/src/checkout/workflow.gleam:60`                   | `case saga.correlation_of(key) { Some(c) -> .. None -> .. }` becomes the plain value      |
| `oversight/apps/checkout/src/checkout/telemetry.gleam:119-160`             | six saga observers pass `d.correlation` to `record`, which takes an `Option`              |
| `oversight/apps/support_desk/src/support_desk/telemetry.gleam:224-261`     | six saga observers pass `m.correlation` to `saga(..)`                                     |
| `oversight/apps/research_agent/src/research_agent/telemetry.gleam:231-270` | four saga observers read `m.correlation`                                                  |
| `fabric/integrations/fabric_saga/test/book_trip_test.gleam:478`            | `process.send(seen, metadata.correlation)` is a `Correlation`; the assertion drops `Some` |

### A durable execution keeps one correlation

```gleam
// before
durable.start_or_reconnect(persistence, storage, id: "refund-7", input: i)
// events: correlation None unless the handle called with_correlation, and
// each drive used whatever its own handle carried
// after: same call
// the first drive saves its correlation (the handle's, or from_key("refund-7"))
// and every later drive reads it back, whatever its handle carries
```

- **Set `durable.with_correlation` before the first `drive`.** The first
  drive saves the value it uses with the checkpoint, before it dispatches
  anything. A `with_correlation` on a later handle is ignored.
- **Old records.** The checkpoint format is now 2 and holds the correlation.
  A format 1 record still reads; it means `from_key(id)`, whatever a handle
  supplies, and its next commit saves it in format 2. A binary that reads
  format 2 is the only one that can read it back: an older saga cannot read
  a record that this release has written. Saga is unpublished, so no stored
  record outside tests and the apps is affected.
- **One extra commit.** The first drive of an execution commits once to save
  the correlation, so storage-failure injection that counts commits sees one
  more on a first drive. Later drives are unchanged.
- **Handlers.** A handler that counted events with `correlation: None` as
  uncorrelated no longer sees any.

Dependents that start durable executions (`checkout/jobs.gleam:86`,
`research_agent/publish.gleam:174`) call `durable.with_correlation` on the
handle before `drive` and are unchanged.

## Dependents

No dependent needs more than the accessor change above.

| Dependent                         | Effect                                                                                                                                                        |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fabric/integrations/fabric_saga` | Runs the saga with `execution.with_correlation(config, call.correlation)`; a step of a fabric tool's saga can now read the run's correlation from its key.    |
| `oversight/apps/support_desk`     | `Refund(shop, request)` carries a per-ticket correlated shop only because a step had no run context. The wrapper can go: `saga.effect` plus `correlation_of`. |
| `oversight/apps/research_agent`   | `upload_body` and `publish` can become `saga.effect` and correlate their client from the key; the job's by-hand correlation of the client goes.               |
| `oversight/apps/checkout`         | Steps can correlate from the key instead of `Deps.client_for(order_id)`; optional.                                                                            |
| `saga/examples/order_consumer`    | `readme_step_correlation_test` shows the step reading its correlation.                                                                                        |

These app edits are optional simplifications; the apps are migrated after this
wave by other agents.

## Telemetry completeness

`saga/telemetry` has six events (`run_started`, `run_stopped`,
`step_started`, `step_stopped`, `compensation_stopped`, `undo_stopped`). Each
metadata record carries `correlation`, filled from one field of the run state
for every event, including the events a restarted execution emits for work it
resumes. The gaps found are fixed:

- A durable handle without `with_correlation` emitted `None`: now `from_key(id)`.
- A drive on a handle that forgot `with_correlation` after a restart
  reported a different value than the first drive: now every drive reports
  the saved one.
- A step could not read the value that its own events carry: now
  `saga.correlation_of(key)`.

Not changed: the durable layer emits no event of its own (claim, release,
lease loss, checkpoint write). Those would be new events, not a correlation
gap.
