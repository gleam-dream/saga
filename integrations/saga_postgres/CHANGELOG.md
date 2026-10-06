# Changelog

## Unreleased

- Initial PostgreSQL storage adapter for Saga durable executions on the application's pool, with schema configuration, leased generation/token claims, conditional checkpoint writes, cancellation intent, and unfinished discovery.
- Forward transaction-locked migrations and equivalent packaged SQL support application migration tooling.
- Adapter and public conformance tests exercise the disposable PostgreSQL 16 cluster and shared-pool consumer.
- [Design decisions](docs/adr) preserve package, ownership, migration, lease, deadline, and retained-scope rationale; [the standing design](docs/design/design.typ) states the actual contracts and unresolved limits.
