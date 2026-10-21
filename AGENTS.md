# Agent Instructions

## About this repo

`saga` runs typed workflows of dependent steps in Gleam. It is a native typed
implementation; Reactor 1.0.6 is a scoped behavioral oracle. The design layer
holds everything else: `docs/design/` (two contexts, execution and durability),
`docs/CONTEXT-MAP.md`, `docs/adr/` for rationale, and `docs/COVERAGE.md`, which
maps repository parts to their design owners.

## Tooling

- `nix develop` (or direnv): dev shell with `gleam`, Erlang/OTP 29, `rebar3`, `lefthook`, and PostgreSQL 16 for `integrations/saga_postgres`.
- `nix fmt`: formats the whole repo via treefmt (`gleam format`, `nixfmt`, `prettier`).
- `lefthook`: pre-commit hook formats staged files and re-stages them.
- `nix flake check`: fails iff the tree is not formatted (plus any existing checks).
- Dependencies: the unpublished sibling packages (`sinal`, `json_blueprint`) are git dependencies with `ref = "main"`, and each `manifest.toml` holds the resolved commit. Move one by hand with `gleam update <package>` in the package directory. The Reactor oracle fetches its sources with mix into the gitignored `oracle/reactor/deps/`; nothing third-party is committed.

## Validation

Use the shared tools directly, then run application tests:

```sh
nix flake check
nix run .#design-gate-check -- docs/design . --nested-project integrations/saga_postgres
nix develop --command bash scripts/test.sh
```

For the core edit loop, run `nix develop --command gleam test`.
`scripts/test.sh` compiles and tests the retained packages and service fixtures;
it does not wrap formatting or design validation.

- `examples/order_consumer` is a separate package (path dependency on saga) that imports only public modules. Run `gleam test` inside it.
- `integrations/saga_postgres` is a separate package so that saga never depends on pog. `scripts/test-postgres.sh` runs its tests, including the storage conformance suite, against a throwaway PostgreSQL 16 cluster.
- `scripts/check_negative.sh` proves the compiler-negative fixtures under `fixtures/negative/` still fail to compile. `scripts/check_durable_restart.sh` kills a VM during concurrent effect and compensation work and recovers in a fresh VM.
- `nix develop .#oracle --command scripts/oracle.sh` runs the Reactor differential oracle; recompute `oracle/differences/*.diff` only after a reviewed behavior change.
- Run the benchmark with `cd bench && gleam run`.

## References

- [docs/ORACLE.md](docs/ORACLE.md) pins Reactor 1.0.6 (`v1.0.6`, commit `e4ddc9e`) as the behavioral oracle, lists the adapted tests and every deliberate difference, and states the limits of the claim. `scripts/oracle.sh` runs it; recompute `oracle/differences/*.diff` only after a reviewed behavior change.
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) records the run-cost method, retained results and limits. Results are observational and never gate a change; `cd bench && gleam run` runs the harness.
- `examples/order_consumer` holds the usage examples as compiled, runnable code (`gleam test`, `gleam run`). Add or change an example in the same change as the API it shows.

<!-- agent-skills:begin -->
<!-- framework-commit: cab7c0590036edaa66d8430cc5016399a9fd2c71 origin: git@github.com:lostbean/skills.git -->

(Updated for the repository validation policy.)

## Agent skills

**Design layer** — `docs/design/design.typ` describes the design (the layer
renders to one `docs/design/design-layer.pdf`), each context's `CONTEXT.typ`
under `docs/design/execution/` and `docs/design/durability/` defines its
vocabulary, and `docs/adr/` records decision rationale. `docs/CONTEXT-MAP.md`
indexes the design documents. `docs/COVERAGE.md` maps repository parts to their
design owners.

**Tracker** — local markdown under `issues/`: one directory per feature
(`issues/<feature-slug>/`), the PRD at `issues/<feature-slug>/PRD.md`, issue
files at `issues/<feature-slug>/issues/<NN>-<slug>.md` numbered from 01. Each
issue file carries two header lines near the top — `Status: <state>`,
`Category: <category>`. Conversation appends to the bottom
of the file under a `## Comments` heading. Labels (canonical strings, adopted
verbatim): states `needs-triage`, `needs-info`, `ready-for-agent`,
`ready-for-human`, `in-progress`, `done`, `wontfix`; categories `bug`,
`enhancement`.

**AI disclaimer** — AI-authored tracker comments start with
`AI-assisted contribution.`

**Shared design validation** — `nix run .#design-gate-check -- docs/design . --nested-project integrations/saga_postgres`
uses the pinned design-layer implementation for render freshness, vocabulary and
layer integrity. `nix run .#design-gate-render -- docs/design docs/design/design-layer.pdf`
updates the rendered document; `nix run .#design-gate-context -- docs/design` exports
context. Run these tools sequentially because they share `.render`.

<!-- agent-skills:end -->

## README snippets

Keep README Gleam examples in compiled example or consumer source. Use named
`snips-start` / `snips-end` regions (or include a whole example module), and retain
Prettier ignore ranges around generated README blocks. Format the source first,
then run `nix run .#snips -- --commands deny README.md integrations/saga_postgres/README.md`. CI runs
`nix run .#snips -- --check --commands deny README.md integrations/saga_postgres/README.md` using the shared pinned
package. Snips checks synchronization; existing example tests check behavior.
