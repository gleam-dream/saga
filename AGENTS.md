# Agent Instructions

## About this repo

`saga` runs typed workflows of dependent steps in Gleam.

Saga is a native typed implementation. Reactor 1.0.6 is a scoped behavioral oracle.
Design: [docs/design/design.typ](docs/design/design.typ); vocabulary:
[docs/design/CONTEXT.typ](docs/design/CONTEXT.typ); rationale: [docs/adr](docs/adr).
The [coverage map](docs/COVERAGE.md) records runtime, tooling, and retained intent.

## Tooling

- `nix develop` (or direnv): dev shell with `gleam`, Erlang/OTP 28, `rebar3`, `lefthook`, and PostgreSQL 16 for `integrations/saga_postgres`.
- `nix fmt`: formats the whole repo via treefmt (`gleam format`, `nixfmt`, `prettier`).
- `lefthook`: pre-commit hook formats staged files and re-stages them.
- `nix flake check`: fails iff the tree is not formatted (plus any existing checks).

## Gates

Run the authoritative registry from the repository root:

```sh
nix develop --command python3 -B scripts/check.py fast
nix develop --command python3 -B scripts/check.py full
nix develop .#oracle --command python3 -B scripts/check.py oracle
nix develop --command python3 -B scripts/check.py benchmark
```

`full` covers every deterministic runtime obligation and both native design
layers. CI splits the same registry into `ci` and `design`; every mandatory
result must succeed. The retained oracle runs on relevant source changes,
weekly and manually; observational benchmarks run weekly and manually. See README's development
inventory. Gate commands validate the existing tree without formatting it.

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

<!-- agent-skills:begin -->
<!-- framework-commit: cab7c0590036edaa66d8430cc5016399a9fd2c71 origin: git@github.com:lostbean/skills.git -->

(machine-owned; do not edit inside this fence — re-run setup to refresh)

## Agent skills

**Design layer** — `docs/design/design.typ` describes the design,
`docs/design/CONTEXT.typ` defines its vocabulary, and `docs/adr/` records
decision rationale. The rendered document is `docs/design/design-layer.pdf`.
`docs/COVERAGE.md` maps repository parts to their design owners.

**Tracker** — GitHub issues in `gleam-dream/saga`, accessed with
`gh issue list --repo gleam-dream/saga` and `gh issue view NUMBER --repo gleam-dream/saga`.
Labels bind roles as follows: `needs-triage` → `needs-triage`,
`needs-info` → `question`, `ready-for-agent` → `ready-for-agent`,
`ready-for-human` → `ready-for-human`, `in-progress` → `in-progress`,
`done` → `done`, `wontfix` → `wontfix`, `bug` → `bug`,
and `enhancement` → `enhancement`.

**AI disclaimer** — AI-authored tracker comments start with
`AI-assisted contribution.`

**Design gate** — `nix run .#design-gate-check -- docs/design . --nested-project integrations/saga_postgres` checks render freshness,
vocabulary references and layer integrity (exit 0 clean, 1 violation, 2 error).
The gate is supplied by the pinned `design-layer` flake input.
`nix run .#design-gate-render -- docs/design docs/design/design-layer.pdf`
rebuilds the rendered document. `nix run .#design-gate-context -- docs/design --estimate`
estimates agent context; the same command without `--estimate` emits ephemeral
Markdown. `--manifest`, `--preview`, and `--section PATH --expect-digest DIGEST`
support loading selected sections. A bare Typst compilation does not run the gate.

**Context verification** — use native semantic blocks for lists, tables,
models and behavior. After authoring, verify context estimation and a selected
section export as well as rendering and the design gate.
Run these commands sequentially for each layer; they share its generated
`.render` workspace.

**Staleness** — source changes since the design last changed require a
conformance review before the layer is treated as current.

<!-- agent-skills:end -->
