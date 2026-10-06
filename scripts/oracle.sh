#!/usr/bin/env bash
# Runs the Reactor 1.0.6 differential oracle as a genuine mechanical
# comparison, not a hand-written summary:
#
#   1. Replays every scenario in oracle/reactor/scenarios/*.exs against
#      real Reactor and diffs its RAW output against the recorded
#      oracle/reactor/expected/d*.txt (proving the recorded oracle
#      evidence still holds).
#   2. Runs test/oracle_test.gleam's main() (via `gleam run -m
#      oracle_test`), which re-executes the SAME scenario functions
#      `gleam test`'s oracle_dN_..._test assertions use, and prints each
#      scenario's outcome as normalized.dN.* lines — a text format shared
#      byte-for-byte with the Reactor side's own normalized.dN.* lines
#      (see oracle/reactor/lib/oracle/trace.ex's print_normalized/2).
#   3. For each scenario, computes the actual `diff` between the two
#      sides' normalized.dN.* lines and checks it against
#      oracle/differences/dN.diff: a scenario with no such file must diff
#      to nothing (a genuine "match"); a scenario with a checked-in
#      dN.diff must produce that exact diff (a genuine, checked
#      "deliberate difference") — never an echoed claim.
#   4. Runs the full `gleam test` suite (the assertions themselves).
#
# See oracle/differences/README.md for the diff file format and
# PROVENANCE.md for what each scenario proves.
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
diffs_dir="$root_dir/oracle/differences"

if ! command -v mix >/dev/null 2>&1; then
  echo "error: mix (Elixir) not found on PATH." >&2
  echo "Run this script from 'nix develop .#oracle' (saga's Elixir-equipped shell)" >&2
  echo "or 'nix develop /code/gleam-dream/oversight -c scripts/oracle.sh'." >&2
  exit 1
fi

if [[ -n ${SAGA_ORACLE_EVIDENCE_DIR:-} ]]; then
  work_dir="$SAGA_ORACLE_EVIDENCE_DIR"
  mkdir -p "$work_dir"
else
  work_dir="$(mktemp -d)"
  trap 'rm -rf "$work_dir"' EXIT
fi

echo "== Reactor 1.0.6 side: replaying scenarios against real Reactor =="
(
  cd "$oracle_dir"
  mix deps.get --only prod >/dev/null 2>&1 || mix deps.get
  mix compile --warnings-as-errors
)

fail=0
: >"$work_dir/reactor_normalized.txt"
for scenario_path in "$oracle_dir"/scenarios/d*.exs; do
  scenario_file="$(basename "$scenario_path")"
  id="$(echo "$scenario_file" | sed -E 's/^(d[0-9]+)_.*/\1/')"
  expected_file="$oracle_dir/expected/$id.txt"
  actual="$(cd "$oracle_dir" && mix run "scenarios/$scenario_file" 2>&1)"
  printf '%s\n' "$actual" >"$work_dir/reactor-raw-$id.txt"

  if [ ! -f "$expected_file" ]; then
    echo "MISSING expected file for $scenario_file: $expected_file" >&2
    fail=1
    continue
  fi

  # The literal captured-evidence lines (oracle/reactor/expected/d*.txt):
  # proves the recorded Reactor behavior hasn't drifted.
  if diff -u "$expected_file" <(printf '%s\n' "$actual" | grep -v '^normalized\.') \
    >"$work_dir/evidence-diff-$id.txt" 2>&1; then
    echo "  $id ($scenario_file): matches recorded Reactor behavior"
  else
    echo "  $id ($scenario_file): DRIFTED from recorded Reactor behavior" >&2
    cat "$work_dir/evidence-diff-$id.txt" >&2
    fail=1
  fi

  # The normalized.dN.* lines: the input to the mechanical Saga
  # comparison below.
  printf '%s\n' "$actual" | grep '^normalized\.' >>"$work_dir/reactor_normalized.txt"
done

if [ "$fail" -ne 0 ]; then
  echo "error: one or more Reactor scenarios drifted from expected/. The" >&2
  echo "recorded oracle evidence no longer matches upstream Reactor 1.0.6 —" >&2
  echo "investigate before trusting PROVENANCE.md's deliberate-difference claims." >&2
  exit 1
fi

echo
echo "== Saga side: gleam run -m oracle_test (same scenarios as gleam test) =="
(cd "$root_dir" && gleam run -m oracle_test 2>&1 | tee "$work_dir/saga_raw.txt")
grep '^normalized\.' "$work_dir/saga_raw.txt" >"$work_dir/saga_normalized.txt"

echo
echo "== Mechanical comparison: reactor vs saga, per scenario =="
compare_fail=0
for scenario_path in "$oracle_dir"/scenarios/d*.exs; do
  scenario_file="$(basename "$scenario_path")"
  id="$(echo "$scenario_file" | sed -E 's/^(d[0-9]+)_.*/\1/')"

  grep "^normalized\.$id\." "$work_dir/reactor_normalized.txt" >"$work_dir/reactor_$id.txt" || true
  grep "^normalized\.$id\." "$work_dir/saga_normalized.txt" >"$work_dir/saga_$id.txt" || true

  if [ ! -s "$work_dir/reactor_$id.txt" ] || [ ! -s "$work_dir/saga_$id.txt" ]; then
    echo "  $id: MISSING normalized output on one side (reactor or saga printed nothing for $id)" >&2
    compare_fail=1
    continue
  fi

  actual_diff="$(diff -u --label reactor --label saga "$work_dir/reactor_$id.txt" "$work_dir/saga_$id.txt" || true)"
  expected_diff_file="$diffs_dir/$id.diff"

  if [ -f "$expected_diff_file" ]; then
    expected_diff="$(cat "$expected_diff_file")"
    if [ "$actual_diff" = "$expected_diff" ]; then
      echo "  $id: DELIBERATE DIFFERENCE, matches checked oracle/differences/$id.diff"
    else
      echo "  $id: deliberate-difference claim in oracle/differences/$id.diff is STALE" >&2
      echo "       recomputed diff does not match the checked-in file:" >&2
      diff -u "$expected_diff_file" <(printf '%s\n' "$actual_diff") >&2 || true
      compare_fail=1
    fi
  else
    if [ -z "$actual_diff" ]; then
      echo "  $id: match (identical normalized output, no oracle/differences/$id.diff)"
    else
      echo "  $id: UNEXPECTED DIFFERENCE (no oracle/differences/$id.diff is checked in for this scenario)" >&2
      printf '%s\n' "$actual_diff" >&2
      compare_fail=1
    fi
  fi
done

if [ "$compare_fail" -ne 0 ]; then
  echo >&2
  echo "error: the mechanical reactor-vs-saga comparison did not match its" >&2
  echo "recorded classification for at least one scenario. Either Saga's" >&2
  echo "behavior changed, or oracle/differences/*.diff is stale — update" >&2
  echo "PROVENANCE.md and the checked diff together, deliberately, never" >&2
  echo "silently." >&2
  exit 1
fi

echo
echo "== Saga side: full gleam test suite (the actual assertions) =="
(cd "$root_dir" && gleam test)

echo
echo "Oracle comparison summary (mechanically verified above, not asserted here):"
for scenario_path in "$oracle_dir"/scenarios/d*.exs; do
  scenario_file="$(basename "$scenario_path")"
  id="$(echo "$scenario_file" | sed -E 's/^(d[0-9]+)_.*/\1/')"
  if [ -f "$diffs_dir/$id.diff" ]; then
    echo "  $id -> deliberate difference (see oracle/differences/$id.diff, PROVENANCE.md)"
  else
    echo "  $id -> match"
  fi
done
echo
echo "See PROVENANCE.md for the full upstream-test-to-Saga-test mapping and"
echo "oracle/differences/README.md for the diff-checking mechanism above."
