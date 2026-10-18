# Usage is compiled examples, and oracle and benchmark references stay as documents

<a id="adr-0014"></a>

## Decision

- Usage lives as compiled, runnable code in `examples/order_consumer`: one module per topic under `src/order_consumer/usage/`, each asserting what it shows, run by `gleam test` and `gleam run`. The full gate runs both, so an API change that breaks an example fails the build.
- The README keeps a short usage section that mirrors the examples and links to them. No standalone prose usage guide exists, and operating guidance for durable runs is expressed as examples (stable id and reconnect, error kinds, discovering unfinished runs, recovering a committed local effect) plus the design's durability contracts.
- [docs/ORACLE.md](../ORACLE.md) and [docs/BENCHMARKS.md](../BENCHMARKS.md) are retained documents. They link each other, the design verification section and the ADRs, and `README.md` and `AGENTS.md` link both.
- The package keeps no changelog before its first publication (ADR 0013).

## Rationale and alternatives

- A prose guide cannot fail when the API changes, so it drifts. A compiled example fails the gate at the moment it stops being true.
- The oracle pin, the adapted-test mapping, the deliberate differences and the benchmark method are the evidence a reader needs to judge Saga's behavior claims. The design layer is a stateless statement of contracts and an ADR records a decision, so neither is the home for a retained reference table; a document beside them is.
- Folding the references into the design was rejected because the mapping table and the dated results would bloat a stateless document with evidence that changes with each capture.
- Keeping prose usage guides next to examples was rejected because two descriptions of the same usage diverge.

## Evidence and history

- Examples: `examples/order_consumer/src/order_consumer/usage/` and `test/usage_test.gleam`; gate wiring in `scripts/check.py` (`examples/order_consumer` tests and `external-consumer-run`).
- ADR 0010 and ADR 0011 record why the oracle is scoped and why benchmarks are observational.
