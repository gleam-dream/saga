#!/usr/bin/env bash
# Run application tests in the repository's Nix development shell.
set -euo pipefail
cd "$(dirname "$0")/.."
native_output="$(mktemp -d)"
trap 'rm -rf "$native_output"' EXIT
for package in . examples/order_consumer integrations/saga_postgres; do (
  cd "$package"
  gleam build --warnings-as-errors
  includes=()
  for directory in build/dev/erlang/*/include; do includes+=(-I "$directory"); done
  ERL_LIBS="$PWD/build/dev/erlang" find src test -name '*.erl' -exec erlc -Werror "${includes[@]}" -o "$native_output" {} +
  case "$package" in
  integrations/saga_postgres) bash scripts/test-postgres.sh ;;
  examples/order_consumer) gleam test && gleam run ;;
  *) gleam test ;;
  esac
) done
(cd bench && gleam build --warnings-as-errors)
bash scripts/check_negative.sh
bash scripts/check_durable_restart.sh
