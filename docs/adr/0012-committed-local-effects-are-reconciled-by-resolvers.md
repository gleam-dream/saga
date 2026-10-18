# A committed local database effect is reconciled by an application resolver

<a id="adr-0012"></a>

## Decision

- An application SQL write and Saga's later checkpoint are separate transactions, even when both use the same database or pool. Saga does not offer a shared transaction between them.
- The application keeps a stable business operation id and an authoritative posting record inside its own transaction, and makes the effect idempotent and discoverable under that identity.
- The attempt resolver returns Completed only when the application record establishes the result. A failed lookup or inconclusive absence stays MaybeSent.
- A missing checkpoint is never evidence that the SQL write rolled back, and a delivery system that redelivers the coordinator never authorizes resubmitting the effect.

## Rationale and alternatives

- A runner can die after the posting transaction commits and before Saga saves the completion. Only the application's own record can show that the posting happened, so recovery reads that record instead of repeating the callback.
- Sharing a pool saves connections but does not merge transaction boundaries. The adapter's commit boundaries keep Saga writes separate from application writes, and its design states them.
- Treating an absent checkpoint as an absent effect would repeat a committed posting. The resolver contract exists to prevent that.

## Evidence and history

- The financial recovery consumer in the [Oversight repository](https://github.com/gleam-dream/oversight) exercises the sequence through public APIs, disposable PostgreSQL, a scripted provider, and controlled runner termination. It also shows Grind redelivering the coordinator without resubmitting the payment.
- Durability design: operating a durable execution, committed local database effect.
