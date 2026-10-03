# Migrating to the wave 5 saga API

Wave 5 lets a step read its execution's correlation, so the step's own HTTP
client can be correlated without threading the value by hand, and makes a
durable execution always correlated. The change is additive for every
dependent in the ecosystem: one record gains a field, and one default changes
from "no correlation" to the execution id.

## `saga`

### `EffectKey` gains `correlation`

```gleam
// before
pub type EffectKey {
  EffectKey(idempotency: String, attempt: Int, attempt_key: String)
}
// after
pub type EffectKey {
  EffectKey(
    idempotency: String,
    attempt: Int,
    attempt_key: String,
    correlation: Option(Correlation),
  )
}
```

Saga builds an `EffectKey`; callers read it by label. Source that reads
`key.idempotency`, `key.attempt` or `key.attempt_key` compiles unchanged. A
positional construction or pattern, `EffectKey(a, b, c)`, no longer compiles.
No dependent has one (searched `saga`, `fabric`, `oversight/apps`).

`saga.effect`, `saga.undo` (`undo.key`), `saga.compensate` (`failed.key`) and
the durable resolvers (`durable.recoverable`, `durable.resolve_undo`,
`durable.resolve_compensation`) receive an `EffectKey`, so each one reads the
correlation. `saga.step` receives only the step's input, as before: change it
to `saga.effect` to read the context.

`correlation` is the value of `execution.with_correlation` or
`durable.with_correlation`, the same value that the run's `saga/telemetry`
events carry in `metadata.correlation`.

| Run                                    | `key.correlation`                       |
| -------------------------------------- | --------------------------------------- |
| local, `execution.with_correlation(c)` | `Some(c)`                               |
| local, none set                        | `None`                                  |
| durable, `durable.with_correlation(c)` | `Some(c)`, on every drive of the handle |
| durable, none set                      | `Some(correlation.from_key(id))`        |

The correlation is not saved in the checkpoint. A resolver, a restored undo
and `durable.Required.key` after a restart carry the correlation of the handle
that drives after the restart.

```gleam
// before: the client is correlated when the workflow is built or started
let workflow = publish.workflow(correlated_client, base)

saga.step("refund_payment", fn(refund: Refund) {
  shop.refund(refund.shop, refund.request)
})

// after: the step correlates its own client from the run
saga.effect("refund_payment", fn(refund: Refund, key) {
  let shop = case key.correlation {
    Some(correlation) -> shop.correlated(shop, correlation)
    None -> shop
  }
  shop.refund(shop, refund.request)
})
```

### A durable execution without `with_correlation` is correlated by its id

```gleam
// before
let run = durable.start_or_reconnect(persistence, storage, id: "refund-7", input: i)
// events: correlation: None; steps: no correlation
// after: same call
// events: correlation: Some(correlation.from_key("refund-7")); steps: the same
```

`from_key` is stable, so every drive and every VM that reconnects to the
execution reports the same value without calling `with_correlation`. A
handle that calls `durable.with_correlation` is unchanged, and so are local
runs. A handler that counted durable events with `correlation: None` as
"uncorrelated" now sees `Some`.

## Dependents

Searched `/code/gleam-dream/*/src`, `*/test`, `*/integrations`, `*/consumers`,
`*/examples` and `/code/gleam-dream/oversight/apps`. No dependent breaks, and
none needs a change to build.

| Dependent                                                         | Use of the changed items                                                                                                                                         | Effect                                                                                                                                                                                       |
| ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fabric/integrations/fabric_saga`                                 | `saga.effect` in tests; runs the saga with `execution.with_correlation(config, call.correlation)`                                                                | A step of a fabric tool's saga reads the fabric run's correlation from its `EffectKey`.                                                                                                      |
| `fabric/experiments/workflow_composition`, `fabric/consumers/app` | `saga.step`, `saga.effect`                                                                                                                                       | None.                                                                                                                                                                                        |
| `saga/integrations/saga_postgres`                                 | `saga.step`, `saga.effect` in tests; storage conformance                                                                                                         | None.                                                                                                                                                                                        |
| `oversight/apps/support_desk`                                     | `refund.gleam`: `Refund(shop, request)` carries a per-ticket `shop.correlated(shop, call.correlation)` because a step has no run context                         | The wrapper can go: build the workflow with one shop and let `refund_payment`, `write_ledger` and `notify_customer` use `saga.effect` and correlate from `key.correlation` (SD-2 residue).   |
| `oversight/apps/research_agent`                                   | `publish.gleam`: `workflow(client, base)` takes a client that the job correlates by hand (`jobs.handle`); `durable.with_correlation(correlation)` is set already | `create_draft` already uses `saga.effect`; `upload_body` and `publish` become `saga.effect` and call `http_gun.with_correlation(client, c)` from `key.correlation`; the job's one line goes. |
| `oversight/apps/checkout`                                         | `workflow.gleam`: `Deps.client_for(order_id)` builds a correlated http_gun client per order; `durable.with_correlation` and `execution.with_correlation` are set | Steps can correlate from `key.correlation` instead of `client_for(order_id)`; optional.                                                                                                      |
| `saga/examples/order_consumer`                                    | `readme_test.readme_step_correlation_test`                                                                                                                       | New example of the step reading its correlation.                                                                                                                                             |

These app edits are optional simplifications; the apps are migrated after
this wave by other agents.

## Telemetry completeness

`saga/telemetry` has six events (`run_started`, `run_stopped`,
`step_started`, `step_stopped`, `compensation_stopped`, `undo_stopped`). Each
metadata record carries `correlation`, and the coordinator fills it from one
field of the run state for every event, including the events a restarted
execution emits for work that it resumes (undo after restart, compensation
resolution). The gaps found:

- A durable handle that forgot `with_correlation` after a restart emitted
  `None`. Fixed by the default above.
- A step could not read the value that its own events carry. Fixed by
  `EffectKey.correlation`.
- Not changed: the correlation is not saved in the checkpoint, so a drive
  with `with_correlation` and a later drive without it report different
  values for the same execution (`c` and `from_key(id)`). Keep one source of
  truth per application, as research_agent does with the research id.
- Not changed: the durable layer emits no event of its own (claim, release,
  lease loss, checkpoint write). If the owner wants them, they are new
  events, not a correlation gap.
