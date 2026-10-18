# Reactor 1.0.6 is a scoped differential oracle with checked deliberate differences

<a id="adr-0010"></a>

## Decision

- Reactor `v1.0.6` (commit `e4ddc9e438f08562e9fb31b08255a86f4bade8ef`, hex-locked in the oracle project) is the behavioral oracle for scheduling and rollback. It is MIT-licensed, and its notice is retained in `THIRD-PARTY-NOTICES.md`. Saga's native contracts define the product; Reactor's API does not.
- Seven scenarios run against live Reactor and against Saga. Both sides print `normalized.dN.*` lines in one language-neutral format, and `scripts/oracle.sh` compares them mechanically. A claimed match must produce an empty difference. A claimed deliberate difference must byte-match the checked-in `oracle/differences/dN.diff`.
- The deliberate differences are undo order (D2: Saga undoes in reverse completion order, Reactor undoes forward) and sibling settlement (D3: Saga settles siblings and undoes a late completion once it is known, Reactor leaves it after returning).
- Adapted upstream tests carry one of three classifications. A faithfully adapted test has the upstream scenario shape and expected outcome in Saga's API. A differential comparison runs the live scenario. An inspired test keeps the upstream motivation while Saga's outcome, mechanism, or scope differs.
- Behaviors not compared are halt and resume, abandoned steps, whole-run undo, dynamic steps, the DSL transformer suite, and the graph exporter.

## Rationale and alternatives

- A hand-written summary of a difference can drift silently from behavior. A checked-in difference that the script recomputes fails loudly when either side changes, so every match or difference claim is re-reviewed before it is trusted again.
- Raw captured Reactor output stays as separate evidence of the Reactor side and is verified against a live run. It is not the comparison input, because the normalized form isolates behavior from Erlang term formatting.
- D3 samples `slow_undone` at the same logical point on both sides: fully settled after Reactor's fixed 500 ms observation sleep, and at the return of the synchronous run on the Saga side. The difference then reflects whether an orphaned effect is ever undone, not when each side samples state.
- Exact Reactor parity was rejected because the retained contract requires reverse-order undo and sibling settlement (ADR 0003).

## Adapted upstream tests

The mapping of each adapted upstream test to its Saga test, with classification and deliberate difference, is retained in [docs/ORACLE.md](../ORACLE.md).

## Evidence and history

- Oracle scenarios `oracle/reactor/scenarios/d*.exs`, Saga scenarios `test/oracle_test.gleam`, comparison `scripts/oracle.sh`, and checked differences `oracle/differences/d2.diff` and `d3.diff`.
- [docs/ORACLE.md](../ORACLE.md) retains the pin, the adapted-test mapping, every deliberate difference and the limits of the claim; [docs/BENCHMARKS.md](../BENCHMARKS.md) is its sibling for run-cost evidence.
- The oracle proves only its seven scoped scenarios. It is not general Reactor or Temporal parity.
