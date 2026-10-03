# Migrating to the wave 4 saga API

Wave 4 makes every timeout, deadline, interval and lease in saga's public API
a `gleam/time/duration` `Duration`, and makes unbounded an explicit
`Infinity`. No public function, constructor field or setter takes milliseconds
as an `Int` any more, matching grind and http_gun. Defaults and behavior are
unchanged.

New dependency: `gleam_time >= 1.11.0 and < 2.0.0`, in `saga`,
`saga_postgres` and any package that builds a `Duration` (every dependent
below). Import it as `import gleam/time/duration`.

Two rules cover most call sites:

- A literal `5000` becomes `duration.seconds(5)`; a literal `250` becomes
  `duration.milliseconds(250)`.
- A bound is refused below 1 millisecond, as before. A `Duration` has finer
  resolution than saga's timers, so one under a millisecond truncates to 0
  ms and is refused wherever 0 was refused. `duration.to_milliseconds`
  truncates the same way.

## `saga`

### `timeout`, `RetryAfter`, `InvalidTimeout`, `StepDescriptor`

```gleam
// before
saga.step("charge", charge) |> saga.timeout(10_000)
saga.RetryAfter(500)
saga.InvalidTimeout(step, value)      // value: Int, in ms
StepDescriptor(timeout: Some(10_000)) // Option(Int)
// after
saga.step("charge", charge) |> saga.timeout(duration.seconds(10))
saga.RetryAfter(duration.milliseconds(500))
saga.InvalidTimeout(step, value)      // value: Duration
StepDescriptor(timeout: Some(duration.seconds(10))) // Option(Duration)
```

`RetryAfter`'s field is now `delay`, was `milliseconds`.

## `saga/execution`

### New: `Timeout`

```gleam
pub type Timeout {
  After(Duration)
  Infinity
}
```

The run deadline and the per-attempt default may be lifted, so they take a
`Timeout`. `Infinity` must be chosen explicitly.

### `with_deadline` and `with_step_timeout` take a `Timeout`

```gleam
// before
config
|> execution.with_deadline(30_000)
|> execution.with_step_timeout(10_000)
// after
config
|> execution.with_deadline(execution.After(duration.seconds(30)))
|> execution.with_step_timeout(execution.After(duration.seconds(10)))
```

`execution.config()` still has no deadline: its default is `Infinity`.

### `without_step_timeout` is removed

```gleam
// before
config |> execution.without_step_timeout
// after
config |> execution.with_step_timeout(execution.Infinity)
```

### Settle, cleanup and retry-cap setters take a `Duration`

```gleam
// before
config
|> execution.with_settle_timeout(0)
|> execution.with_cleanup_timeout(200)
|> execution.with_max_retry_delay(60_000)
// after
config
|> execution.with_settle_timeout(duration.seconds(0))
|> execution.with_cleanup_timeout(duration.milliseconds(200))
|> execution.with_max_retry_delay(duration.seconds(60))
```

A zero settle timeout still kills in-flight work at once. The settle timeout
and the retry cap may be zero but not negative; the cleanup timeout must be
at least 1 millisecond.

### `await` and `progress` take a `Duration`

```gleam
// before
execution.await(exec, 10_000)
execution.progress(exec, timeout: 1000)
// after
execution.await(exec, duration.seconds(10))
execution.progress(exec, timeout: duration.seconds(1))
```

### `ConfigError` carries a `Duration`

`DeadlineNotPositive`, `StepTimeoutNotPositive`, `SettleTimeoutNegative`,
`CleanupTimeoutNotPositive` and `MaxRetryDelayNegative` hold the offending
`Duration` (was `Int`). `describe_config_error` renders it in milliseconds,
for example `got -3 ms` (was `got -3`).

```gleam
// before
execution.MaxRetryDelayNegative(-3)
// after
execution.MaxRetryDelayNegative(duration.milliseconds(-3))
```

## `saga/durable`

```gleam
// before
durable.drive(run, timeout: 30_000)
Error(durable.InvalidTimeout(0))        // milliseconds: Int
// after
durable.drive(run, timeout: duration.seconds(30))
Error(durable.InvalidTimeout(duration.milliseconds(0))) // timeout: Duration
```

A caller that holds a grind `Duration` passes it straight to `drive`; the
conversion to milliseconds is gone.

## `saga/storage`

```gleam
// before
storage.with_call_timeout(backend, 100)
storage.with_renewal(backend, every: 20, renew: renew)
// after
storage.with_call_timeout(backend, duration.milliseconds(100))
storage.with_renewal(backend, every: duration.milliseconds(20), renew: renew)
```

The call timeout is still raised to at least 1 millisecond. The `@internal`
readers `storage.call_timeout(storage)` and `storage.renewal(storage)` return
a `Duration` (the renewal as `Option(#(Duration, fn(Claim) -> ..))`), which an
adapter wrapper passes back unchanged:

```gleam
let rebuilt = storage.new(..) |> storage.with_call_timeout(storage.call_timeout(s))
case storage.renewal(s) {
  Some(#(every, renew)) -> storage.with_renewal(rebuilt, every:, renew:)
  None -> rebuilt
}
```

## `saga/storage/conformance`

```gleam
// before
conformance.run(fresh, timeout: 5000, owner_loss_within: lease + 500)
// after
conformance.run(
  fresh,
  timeout: duration.seconds(5),
  owner_loss_within: duration.add(lease, duration.milliseconds(500)),
)
```

`conformance.InvalidTimeout` still reports either argument below 1
millisecond.

## `saga/testing`

```gleam
// before
testing.wait_until(exec, matching: ready, within: 10_000)
// after
testing.wait_until(exec, matching: ready, within: duration.seconds(10))
```

## `saga/telemetry`

`CompensationMetadata.retry_delay` is `Option(Duration)` (was
`Option(Int)` milliseconds). The event's map still carries whole
milliseconds under `"retry_delay"`, so handlers that read the map, and the
Sinal codec round trip, are unchanged.

```gleam
// before
let assert Some(300_000) = metadata.retry_delay
// after
let assert Some(delay) = metadata.retry_delay
delay |> should.equal(duration.seconds(300))
```

The measurements stay numeric, as telemetry events are: `duration` and
`system_time` are native-unit integers (milliseconds), not configured
bounds.

## `saga_postgres`

```gleam
// before
saga_postgres.config(db) |> saga_postgres.with_lease(30_000)
// after
saga_postgres.config(db) |> saga_postgres.with_lease(duration.seconds(30))
```

A lease below 100 milliseconds is still raised to 100 milliseconds; renewal
is still every third of it. Add `gleam_time` to the package's dependencies.

## Not configurable, so unchanged

These have no setter and no public signature. They stay fixed internal
constants and are listed in the README defaults table: the memory adapter's
5 second call bound, the file adapter's 5 second mutation lock wait
(`storage.Busy` after it), the coordinator's 5 second start handshake and
`saga_postgres`'s 4.5 second query bound.

## Dependents

Each dependent needs `gleam_time` in its `gleam.toml` and the sites below.
`fabric.await(handle, ms)` and `queue`/`grind` calls are those packages' own
API and are not part of this change.

### fabric/integrations/fabric_saga (light, tests only)

`src/` calls no changed function. In `test/`:

| File                                | Before                      | After                                            |
| ----------------------------------- | --------------------------- | ------------------------------------------------ |
| `test/unknown_effect_test.gleam:72` | `with_settle_timeout(5000)` | `with_settle_timeout(duration.seconds(5))`       |
| `test/book_trip_test.gleam:132`     | `saga.RetryAfter(60_000)`   | `saga.RetryAfter(duration.seconds(60))`          |
| `test/book_trip_test.gleam:172`     | `with_settle_timeout(50)`   | `with_settle_timeout(duration.milliseconds(50))` |

### oversight/apps/checkout (light; deletes a conversion)

| File                               | Before                                                                                         | After                                                                                                                                       |
| ---------------------------------- | ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/checkout/app.gleam:89`        | `saga_postgres.with_lease(config.saga_lease_ms)`                                               | `saga_postgres.with_lease(config.saga_lease)`, `saga_lease: Duration`                                                                       |
| `src/checkout/app.gleam:133`       | `execution.with_step_timeout(10_000)`                                                          | `execution.with_step_timeout(execution.After(duration.seconds(10)))`                                                                        |
| `src/checkout/jobs.gleam:126-145`  | `drive_until_cancelled(.., timeout: Int)` calls `durable.drive(run, timeout:)`                 | takes grind's `Duration`, passes it to `durable.drive` and adds `duration.seconds(1)` to the selector wait                                  |
| `src/checkout/orders.gleam:76`     | `execution.await(run, 60_000)`                                                                 | `execution.await(run, duration.seconds(60))`                                                                                                |
| `test/checkout_test.gleam:470-473` | `with_lease(lease)`, `conformance.run(fresh, timeout: 10_000, owner_loss_within: lease + 500)` | `lease = duration.milliseconds(300)`, `timeout: duration.seconds(10)`, `owner_loss_within: duration.add(lease, duration.milliseconds(500))` |

`Config.saga_lease_ms` and `grind_lease_ms` can become `Duration` fields, so
`busy: duration.milliseconds(config.saga_lease_ms)` (app.gleam:150) and
`queue.with_lease(duration.milliseconds(config.grind_lease_ms))` (:157) lose
their wrappers. The deadline conversion between grind and saga that
checkout's FEEDBACK.md records is gone.

### oversight/apps/support_desk (none)

Its saga use (`refund.gleam`) calls no changed function, and its tests wait
through `fabric.await`. It rebuilds against the new saga and fabric_saga
without a source change.

### oversight/apps/research_agent (light)

| File                                       | Before                                                                 | After                                                                                                                               |
| ------------------------------------------ | ---------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `src/research_agent/app.gleam:123`         | `saga_postgres.with_lease(10_000)`                                     | `saga_postgres.with_lease(duration.seconds(10))`                                                                                    |
| `src/research_agent/publish.gleam:163`     | `execution.with_step_timeout(5000)`                                    | `execution.with_step_timeout(execution.After(duration.seconds(5)))`                                                                 |
| `src/research_agent/publish.gleam:149-173` | `publish.run(.., timeout: Int)` forwards to `durable.drive`            | `timeout: Duration`; `jobs.gleam:128` passes `duration.seconds(60)`                                                                 |
| `test/research_agent_test.gleam:400-418`   | `with_lease(lease)`, `timeout: 5000`, `owner_loss_within: lease + 500` | `lease = duration.seconds(1)`, `timeout: duration.seconds(5)`, `owner_loss_within: duration.add(lease, duration.milliseconds(500))` |

### Other packages

`examples/order_consumer` is migrated in this change: its
`workflows.bounded_config` takes an `execution.Timeout` deadline (was
`Option(Int)`). `bench` and the negative fixtures use none of the changed
functions.
