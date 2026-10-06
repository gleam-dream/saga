# Changelog

## Unreleased

Saga is not yet published to Hex. Its current capabilities are:

- Native typed workflows, reusable definitions, scoped embedding, explicit error mapping, and parallel dependency scheduling.
- Owner-controlled local execution with configuration, Duration bounds, progress, cancellation, retries, reverse-completion rollback, and retained compensation evidence.
- Explicit unknown-effect classification, stable action idempotency keys, and Hold for application-marked uncertainty unless rollback is authorized.
- Optional checked persistence on the same workflow, durable handles, claim-fenced storage, reconciliation, memory and file adapters, and a separate PostgreSQL adapter.
- Mandatory action and event correlation, typed outcomes, complete payload-free summaries, and independent owned reporting that preserves the full typed report.
- External public consumers, compiler-negative fixtures, fault and restart checks, adapter conformance, and a pinned Reactor differential oracle.

The [native design](docs/design/design.typ) defines the complete intended scope and its pending contracts. The [architecture decision records](docs/adr/) retain the rationale, alternatives, and evidenced history of these unpublished changes. [DURABILITY.md](DURABILITY.md) contains durable-execution operating guidance.
