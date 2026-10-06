# Saga PostgreSQL adapter instructions

- This is a nested package in [Saga](../../AGENTS.md). Use the parent dev shell, design-gate pin, and `gleam-dream/saga` tracker; this package has no independent flake, gate copy, or tracker.
- The standing adapter contract is [docs/design/design.typ](docs/design/design.typ), its vocabulary is [docs/design/CONTEXT.typ](docs/design/CONTEXT.typ), its generated document is [docs/design/design-layer.pdf](docs/design/design-layer.pdf), and its rationale is [docs/adr](docs/adr). [docs/COVERAGE.md](docs/COVERAGE.md) records the source/tooling and intended-extension boundaries.
- Parent [storage contracts](../../docs/design/design.typ#storage-and-claim-lifetime) own Claim, Stored, Commit, workflow checkpoint meaning, and recovery permission. This adapter owns SQL, schema installation, database-clock leases, and borrowed-pool semantics.
- Keep executable SQL and its internal statement list equivalent. Do not infer external-effect replay safety from lease or revision fencing.
- The application owns the connection pool. Do not add pool startup, shutdown, an ambient scheduler, or a shared ecosystem durability runtime to this adapter.

Run documentation checks from the Saga root:

```sh
nix run .#design-gate-render -- integrations/saga_postgres/docs/design integrations/saga_postgres/docs/design/design-layer.pdf
nix run .#design-gate-check -- docs/design . --nested-project integrations/saga_postgres
nix run .#design-gate-context -- integrations/saga_postgres/docs/design --estimate
nix run .#design-gate-context -- integrations/saga_postgres/docs/design --manifest
```

- For a selected context subtree, pair `--section PATH` with the current manifest's `--expect-digest DIGEST`. Context output is ephemeral and is not another documentation source.
- Run design apps sequentially for each layer because they share its generated `.render` directory.
- Run runtime checks only when the change requires them, within the parent Nix shell. Run the command below from `integrations/saga_postgres`; `scripts/test-postgres.sh` starts and removes its own PostgreSQL 16 cluster.
- Plain `gleam test` fails without the harness's script-provided test URL.

```sh
nix develop ../.. --command bash -c 'gleam format --check src test && gleam build --warnings-as-errors && scripts/test-postgres.sh'
```
