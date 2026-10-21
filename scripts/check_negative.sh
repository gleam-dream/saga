#!/usr/bin/env bash
# Checks that saga's negative compiler fixtures fail to compile with their
# expected diagnostics, from the external consumer's own point of view: each
# fixture is copied into examples/order_consumer/src (a package that only
# imports saga's public modules) and checked with `gleam check` there. This
# is a bash port of relay's scripts/check_negative_fixtures.py, adapted to
# probe from a separate consumer package rather than saga's own tree.
#
# Also checks (see below):
#   - fixtures/positive/*.gleam: the same-shaped POSITIVE control fixtures
#     must compile successfully, proving a paired negative fixture fails
#     for the reason its .expect claims, not for an unrelated mistake.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURES_DIR="$ROOT/fixtures/negative"
POSITIVE_DIR="$ROOT/fixtures/positive"
CONSUMER_DIR="$ROOT/examples/order_consumer"
PROBE_FILE="$CONSUMER_DIR/src/negative_probe.gleam"

cleanup() {
  rm -f "$PROBE_FILE"
}
trap cleanup EXIT

if [ ! -d "$FIXTURES_DIR" ] || [ -z "$(ls -A "$FIXTURES_DIR"/*.gleam 2>/dev/null)" ]; then
  echo "FAIL: No negative fixtures found in $FIXTURES_DIR" >&2
  exit 1
fi

echo "== positive control fixtures (must compile) =="
if [ ! -d "$POSITIVE_DIR" ] || [ -z "$(ls -A "$POSITIVE_DIR"/*.gleam 2>/dev/null)" ]; then
  echo "FAIL: No positive control fixtures found in $POSITIVE_DIR" >&2
  exit 1
fi

positive_passed=0
for fixture in "$POSITIVE_DIR"/*.gleam; do
  name="$(basename "$fixture" .gleam)"
  cp "$fixture" "$PROBE_FILE"

  set +e
  output="$(cd "$CONSUMER_DIR" && gleam check 2>&1)"
  status=$?
  set -e

  rm -f "$PROBE_FILE"

  if [ "$status" -ne 0 ]; then
    echo "FAIL: positive control $name.gleam was expected to compile but did not:" >&2
    echo "$output" >&2
    exit 1
  fi

  echo "PASS: positive control $name compiled as expected"
  positive_passed=$((positive_passed + 1))
done
echo "All $positive_passed positive control fixtures compiled as expected."
echo

echo "== negative fixtures (must fail with their expected diagnostic) =="
passed=0
for fixture in "$FIXTURES_DIR"/*.gleam; do
  name="$(basename "$fixture" .gleam)"
  expect_file="$FIXTURES_DIR/$name.expect"

  if [ ! -f "$expect_file" ]; then
    echo "FAIL: Missing .expect for $name.gleam" >&2
    exit 1
  fi

  cp "$fixture" "$PROBE_FILE"

  set +e
  output="$(cd "$CONSUMER_DIR" && gleam check 2>&1)"
  status=$?
  set -e

  rm -f "$PROBE_FILE"

  if [ "$status" -eq 0 ]; then
    echo "FAIL: $name.gleam compiled successfully but was expected to fail!" >&2
    exit 1
  fi

  while IFS= read -r fragment; do
    # Skip blank lines and comment lines in the .expect file.
    case "$fragment" in
    "" | "#"*) continue ;;
    esac
    if ! grep -qF "$fragment" <<<"$output"; then
      echo "FAIL: $name.gleam did not contain expected diagnostic fragment: $fragment" >&2
      echo "$output" >&2
      exit 1
    fi
  done <"$expect_file"

  echo "PASS: $name rejected at compile time with expected diagnostics"
  passed=$((passed + 1))
done

echo "All $passed negative compiler fixtures rejected as expected."
