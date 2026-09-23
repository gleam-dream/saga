#!/usr/bin/env bash
# Runs the Reactor 1.0.6 differential oracle: replays every scenario in
# oracle/reactor/scenarios/*.exs against real Reactor and diffs the output
# against the recorded oracle/reactor/expected/d*.txt (proving the recorded
# oracle behavior still holds), then runs the Gleam-side equivalents in
# test/oracle_test.gleam, which assert either an identical outcome or one
# of the deliberate, documented differences (see PROVENANCE.md).
#
# Requires Elixir/mix, which is NOT in the package's default dev shell.
# Run this from `nix develop .#oracle`, e.g.:
#
#   nix develop .#oracle --command scripts/oracle.sh
#
# or, if the oversight dev shell is already active (it carries Elixir too):
#
#   nix develop /code/gleam-dream/oversight -c scripts/oracle.sh
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
oracle_dir="$root_dir/oracle/reactor"

if ! command -v mix >/dev/null 2>&1; then
  echo "error: mix (Elixir) not found on PATH." >&2
  echo "Run this script from 'nix develop .#oracle' (saga's Elixir-equipped shell)" >&2
  echo "or 'nix develop /code/gleam-dream/oversight -c scripts/oracle.sh'." >&2
  exit 1
fi

echo "== Reactor 1.0.6 side =="
(
  cd "$oracle_dir"
  mix deps.get --only prod >/dev/null 2>&1 || mix deps.get
  mix compile --warnings-as-errors
)

fail=0
for scenario_path in "$oracle_dir"/scenarios/d*.exs; do
  scenario_file="$(basename "$scenario_path")"
  id="$(echo "$scenario_file" | sed -E 's/^(d[0-9]+)_.*/\1/')"
  expected_file="$oracle_dir/expected/$id.txt"
  actual="$(cd "$oracle_dir" && mix run "scenarios/$scenario_file" 2>&1)"

  if [ ! -f "$expected_file" ]; then
    echo "MISSING expected file for $scenario_file: $expected_file" >&2
    fail=1
    continue
  fi

  if diff -u "$expected_file" <(printf '%s\n' "$actual") >/tmp/oracle-diff-"$id".txt 2>&1; then
    echo "  $id ($scenario_file): matches recorded Reactor behavior"
  else
    echo "  $id ($scenario_file): DRIFTED from recorded Reactor behavior" >&2
    cat /tmp/oracle-diff-"$id".txt >&2
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "error: one or more Reactor scenarios drifted from expected/. The" >&2
  echo "recorded oracle evidence no longer matches upstream Reactor 1.0.6 —" >&2
  echo "investigate before trusting PROVENANCE.md's deliberate-difference claims." >&2
  exit 1
fi

echo
echo "== Saga side (test/oracle_test.gleam, part of the full suite) =="
(cd "$root_dir" && gleam test)

echo
echo "Oracle comparison summary:"
echo "  D1 sequential dependency order         -> match"
echo "  D2 undo order + multiple undo failures -> DELIBERATE DIFFERENCE (reverse vs forward undo order)"
echo "  D3 failure with an active sibling      -> DELIBERATE DIFFERENCE (Saga settles; Reactor orphans)"
echo "  D4 retry limits                        -> match (Saga max_attempts = Reactor max_retries + 1)"
echo "  D5 compensate continue                 -> match"
echo "  D6 max_concurrency bound               -> match"
echo "  D7 shared step executes once           -> match (native Saga scenario, compared differentially)"
echo
echo "See PROVENANCE.md for the full upstream-test-to-Saga-test mapping."
