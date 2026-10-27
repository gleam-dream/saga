# order_consumer

[![CI](https://github.com/gleam-dream/saga/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/gleam-dream/saga/actions/workflows/ci.yml)
[![saga on Hex](https://img.shields.io/hexpm/v/saga?label=Hex%3A%20saga)](https://hex.pm/packages/saga)
[![Gleam](https://img.shields.io/badge/Gleam-FFAFF3?logo=gleam&logoColor=000000)](https://gleam.run/)

A separate Gleam package that depends on Saga by path and imports only its public
modules. It holds Saga's usage examples as compiled, runnable code and is the
external acceptance test for the facade, with fake application services in place
of providers.

Run from this directory inside the repository dev shell:

```sh
gleam test   # every example and scenario, with assertions
gleam run    # the scenarios and the usage examples, with a printed trace
```

The full gate (`bash scripts/test.sh`) runs both, so an API change that breaks an
example fails the build.

| Topic                                              | Example                                                     |
| -------------------------------------------------- | ----------------------------------------------------------- |
| Define, run, configure and observe a checkout      | `src/order_consumer/usage/checkout.gleam`                   |
| Unknown effects and held reconciliation            | `src/order_consumer/usage/unknown_effect.gleam`             |
| Step correlation from the `EffectKey`              | `src/order_consumer/usage/correlation.gleam`                |
| Durable run, resolvers and error kinds             | `src/order_consumer/usage/durable_run.gleam`                |
| Stable id, reconnect, read and discover unfinished | `src/order_consumer/usage/operations.gleam`                 |
| Recover a committed local effect                   | `src/order_consumer/usage/committed_effect.gleam`           |
| Caller-owned durable codecs, errors and choices    | `test/durable_consumer_test.gleam`                          |
| Owned reporting and operational causes             | `test/reporting_consumer_test.gleam`                        |
| Blueprint codec bridge                             | `test/blueprint_codec_test.gleam`                           |
| Cancellation with `execution.progress` and a gate  | `test/order_consumer_test.gleam`, `test/support/gate.gleam` |

`test/usage_test.gleam` runs each usage module. The workflows for the scenarios
live in `src/order_consumer/workflows.gleam`. The design's
[testing section](../../docs/design/design-layer.pdf)
states the rules for waiting on a run and for writing a step gate.
