# Checkpoints are saved as records, and listings page by phase

<a id="adr-0017"></a>

Accepted 2026-11-25 by owner ruling. Extends [0005-storage-claims-as-values.md](0005-storage-claims-as-values.md): claims, fencing and refusal order are unchanged.

## Decision

- A checkpoint is a set of named records. The identity (id and stamp) and the input are written once; the run header and one record per step are written when they change. `commit` replaces the records it names, keeps the others, and changes revision and phase in the same atomic step.
- `create` takes the first records and `commit` returns nothing; `load` and `claim` return every record.
- `unfinished(limit)` becomes `list(Listing(phase, after, limit))`: the unowned executions of one phase, in id byte order, after a cursor id.
- Saved checkpoints of the earlier single-record format are not read. Drain unfinished executions before upgrading.

## Rationale and alternatives

- Every commit rewrote the whole checkpoint, so committed bytes grew with the square of the step count: about 93 MB for a 200-step chain with 1 KB values. Records bring that to about 0.7 MB.
- An append-only event log would make every commit constant in size, but recovery would replay the log and the coordinator's state would need a second, event-based model. Records keep the one snapshot model; the run header still grows with the completion journal.
- `unfinished` returned suspended executions first with no cursor, so executions that stay suspended could fill every page. Listing by phase lets a driver page pending executions alone; one phase per call keeps the listing a single ordered range.
- Both changes break adapters. They are made together so an adapter changes once.

## Evidence

- Durable tests bound committed bytes per step and forbid rewriting an unchanged record; the conformance suite checks record merging, refused commits and paged listing on the memory, file and PostgreSQL adapters. `bench/src/durable_writes.gleam` measures commits and bytes.
