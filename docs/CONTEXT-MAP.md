# Saga context map

## Contexts

- [Execution](design/execution/CONTEXT.typ) — typed workflows, dependency scheduling, retry, compensation, undo, outcome, settlement, local ownership, telemetry, and the intended composition and runtime-graph extensions.
- [Durability](design/durability/CONTEXT.typ) — saved executions, checkpoints, compatibility, recovery resolvers, claims, the storage port, reference adapters, and delivery obligations.

## Relationships

- Durability is the **customer** and Execution the **supplier**. Execution supplies the Workflow, EffectKey, and Outcome values; Durability attaches persistence to the same Workflow and never defines a second graph.
- A term shared across the contexts has one owning context. Execution owns correlation, effect key, idempotency key, outcome, and settlement. Durability owns checkpoint, claim, storage, resolver, and drive, and cites the Execution slugs without redefining them.
- The PostgreSQL storage adapter is a nested project with its own layer under `integrations/saga_postgres`. It is a **conformist** to the Durability storage port: Durability defines storage values and workflow permission, and the adapter enforces database mutations.
