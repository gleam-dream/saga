# Agent Instructions

## About this repo

`saga` — A strongly-typed saga/DAG orchestrator for Gleam: typed dependency graphs instead of dynamic step maps.

Ports/wraps: Reactor (Elixir). Design: [gleam-dream/oversight](https://github.com/gleam-dream/oversight)/saga-design.md.

## Tooling

- `nix develop` (or direnv): dev shell with `gleam`, Erlang/OTP 28, `rebar3`, `lefthook`.
- `nix fmt`: formats the whole repo via treefmt (`gleam format`, `nixfmt`, `prettier`).
- `lefthook`: pre-commit hook formats staged files and re-stages them.
- `nix flake check`: fails iff the tree is not formatted (plus any existing checks).
- `gleam test`: runs the test suite.
