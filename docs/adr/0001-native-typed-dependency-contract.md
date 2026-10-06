# Native typed dependencies define the workflow contract

<a id="adr-0001"></a>

## Decision

- Saga owns one native typed Workflow and opaque Port connections. Step names identify occurrences and diagnostics rather than carrying native results.
- Native input, output, business-error, and undo-error types belong to callers. Explicit error mapping adapts a workflow into application vocabulary.
- Reject unreachable effect steps at definition time; assign embedded occurrences scoped addresses so repeated embeds do not collide.
- Local and durable execution use the same definition. Closure serialization and arbitrary heterogeneous runtime-native lookup are excluded.

## Rationale and alternatives

- A Reactor wrapper would retain dynamic named maps behind a second typed representation. The native contract directly preserves producer-to-consumer typing and admits adversarial compiler fixtures.
- Separate local/durable definitions would duplicate authoring and change composition meaning. Optional checked persistence preserves the local common path.
- Reactor 1.0.6 is a scoped behavior oracle, not the durable or runtime-schema graph contract.

## Evidence and history

- Native authoring: `6285bb53e78e12b1269143d12313ad9431ee9499`, 2026-09-23. External consumer/compiler negatives: `907e4b806cbb799130ee9ff802d6885dc6010fdc`, same date.
- Current evidence: `src/saga.gleam`, `examples/order_consumer`, `fixtures/negative`.
- Prior scope: Oversight `saga-design.md` core/DAG sections; `PUBLIC-API.md`; `API-COVERAGE.md` Saga rows; `research/workflow-boundaries.md`; interface-lab unified/shared/choice consumers. Laboratory names remain evidence, not extra current APIs.
