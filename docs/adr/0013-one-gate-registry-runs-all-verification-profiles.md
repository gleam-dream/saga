# One gate registry runs the fast, ci, design, full, oracle, and benchmark profiles

<a id="adr-0013"></a>

## Decision

- `scripts/check.py` is the single registry of obligations. The fast, ci, design, full, oracle, and benchmark profiles select from it, and CI splits the same registry into ci and design.
- Gate commands check the tree and never repair it. A profile stops at its first failure and retains per-check logs, nonempty result entries, dependency revisions, and environment and lock metadata under `.artifacts/PROFILE`.
- Hosted verification builds from the committed lock files, never persists credentials, and runs fork pull requests under the same checks. Unpublished sibling packages are git dependencies on `main`; each manifest locks the resolved commit, and a developer updates it by hand with `gleam update`.
- Every package build treats Gleam warnings as errors and compiles authored Erlang with `erlc -Werror`. Authored scripts are formatted and linted; generated dependency code and retained oracle and fixture bytes are excluded.
- The package keeps no changelog before its first publication. Decisions and their history live in ADRs, and the design records the current contracts.

## Rationale and alternatives

- One registry prevents a profile from silently omitting an obligation that another profile runs, and the nonempty-result check turns a vacuous pass into a failure.
- Ruff checks syntax, imports, and undefined names only; it is not a Python type checker, so no claim of script type safety is made.
- The database harness disables fsync and synchronous commit. Passing runner and VM recovery tests therefore establish no database power-loss durability.
- A changelog duplicated ADR content and described changes to an unpublished package. A reader needing history reads the ADRs.

## Evidence and history

- Registry and regression tests: `scripts/check.py`, `scripts/test_check.py`. Workflows: `.github/workflows/ci.yml` and `extended.yml`.
- The oracle runs on relevant source changes, weekly, and manually. Benchmarks run weekly and manually.
