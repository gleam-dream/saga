# Agent Instructions

## About this repo

`saga` — A strongly-typed saga/DAG orchestrator for Gleam: typed dependency graphs instead of dynamic step maps.

Ports/wraps: Reactor (Elixir). Design: [gleam-dream/oversight](https://github.com/gleam-dream/oversight)/saga-design.md.

## Tooling

- `nix develop` (or direnv): dev shell with `gleam`, Erlang/OTP 28, `rebar3`, `lefthook`, and PostgreSQL 16 for `integrations/saga_postgres`.
- `nix fmt`: formats the whole repo via treefmt (`gleam format`, `nixfmt`, `prettier`).
- `lefthook`: pre-commit hook formats staged files and re-stages them.
- `nix flake check`: fails iff the tree is not formatted (plus any existing checks).

## Gates

Run from the repo root, inside `nix develop`:

```sh
gleam format --check src test
gleam build --warnings-as-errors
gleam test
(cd examples/order_consumer && gleam format --check src test && gleam build --warnings-as-errors && gleam test && gleam run)
(cd bench && gleam format --check src && gleam build --warnings-as-errors)
(cd integrations/saga_postgres && gleam format --check src test && gleam build --warnings-as-errors && scripts/test-postgres.sh)
scripts/check_negative.sh
scripts/check_durable_restart.sh
nix fmt
nix flake check
```

`examples/order_consumer` is a separate Gleam package (path dependency on
saga) that only imports saga's public modules — it is the external
acceptance test for saga's facade, not part of saga's own build.
`integrations/saga_postgres` is the PostgreSQL storage adapter, a separate
package so that saga never depends on pog. `scripts/test-postgres.sh` runs its
tests, including saga's storage conformance suite, against a throwaway
PostgreSQL 16 cluster that the script creates and removes.
`scripts/check_negative.sh` proves the compiler-negative fixtures under
`fixtures/negative/` still fail to compile, from that same external point
of view.
`scripts/check_durable_restart.sh` kills an Erlang VM after two concurrent
external-effect probes but before saga saves their outputs, then recovers
the same DAG in a fresh VM through the file storage adapter. A second probe
interrupts two compensation callbacks and resolves their saved decisions in a
fresh VM without repeating either callback.
