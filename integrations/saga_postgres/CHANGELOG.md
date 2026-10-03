# Changelog

## Unreleased

- Initial package: a PostgreSQL storage for saga's durable runs over one
  table, `saga_executions`, on the application's own `pog.Connection`.
- `config`, `with_lease` (default 30 000 ms, at least 100 ms) and
  `with_schema` (default `public`).
- `storage` implements the `saga/storage` contract with leased claims,
  renewed every lease / 3, and passes `saga/storage/conformance`. Lease
  expiry is judged by the database's clock. Each query is bounded at
  4 500 ms.
- `migrate` applies forward-only numbered migrations in one transaction
  under a per-schema advisory lock, recorded in `saga_schema_migrations`.
  The same SQL ships in `priv/migrations/`.
- A runner killed while `drive`'s caller lives no longer holds its claim
  for the whole lease: saga releases it at once. The lease remains the
  fallback when the runner's node is lost, and the tests cover both paths.
- `pog` and `pgo` are pinned to the minor ranges grind uses, so one
  application pool serves the application, grind and saga.
