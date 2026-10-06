# Distinguish execution budgets from total return deadlines

<a id="adr-0009"></a>

## Status and evidence

- This is an observed contract boundary and open ruling; no runtime semantic change is approved.
- Source revision: `2a6bc4bf380145e62015e66e15d216f9a6d7d1d4`. A public gated slow-release observation on 2026-10-06 used a 20 ms drive budget: the release callback blocked on a gate, the caller still had no report 100 ms later, and opening the gate returned DriveTimedOut. This isolates return timing; it makes no adapter conformance claim.

## Current boundary

- Drive's Duration bounds execution waiting before runner stop. Synchronous drain and possible bounded claim release precede return.
- Drain uses resettable five-second receive windows. Abnormal exit with known claim performs separately bounded release.
- Local startup also has separate internal readiness and public control-handshake waits; a single five-second statement is not a total-start bound.

## Open alternatives

- Retain synchronous cleanup and state its return margin; cleanup remains drive-owned.
- Add a total deadline or surviving asynchronous cleanup owner. Required decisions include worker stop, stale-owner fencing, temporary Busy, cleanup retry and lifetime.
- No change to cleanup ownership has been approved. Driver interruption preserves resumable state and does not record cancellation.

## Related executable example

- README and execution module examples reduce noncompletion to unknown-effects lists and admission errors to Error([]); the readme consumer expects that list for declined payment.
- The library already preserves native Outcome. A coordinated example/assertion change must retain business failure, admission failure, undo failure, and uncertainty distinctly; the existing consumer assertion currently expects the erased value.
