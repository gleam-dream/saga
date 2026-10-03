#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
probe_dir="$(mktemp -d "${TMPDIR:-/tmp}/saga-durable-vm.XXXXXX")"
probe_pid=""
cleanup() {
  if [ -n "$probe_pid" ]; then
    kill -9 "$probe_pid" 2>/dev/null || true
    wait "$probe_pid" 2>/dev/null || true
  fi
  rm -rf "$probe_dir"
}
trap cleanup EXIT

gleam test >/dev/null
for probe_module in recovery_probe compensation_recovery_probe; do
rm -rf "$probe_dir" && mkdir -p "$probe_dir"
SAGA_PROBE_MODE=prepare SAGA_PROBE_PATH="$probe_dir" \
  erl -noshell -pa build/dev/erlang/*/ebin -s "$probe_module" main -s init stop >/dev/null 2>&1 &
probe_pid=$!

for _ in $(seq 1 500); do
  if ls "$probe_dir"/*.saga >/dev/null 2>&1 && [ -f "$probe_dir/a.ledger" ] && [ -f "$probe_dir/b.ledger" ]; then
    break
  fi
  if ! kill -0 "$probe_pid" 2>/dev/null; then
    echo "prepare VM exited before persisting the effect" >&2
    exit 1
  fi
  sleep 0.01
done

if ! ls "$probe_dir"/*.saga >/dev/null 2>&1 || [ ! -f "$probe_dir/a.ledger" ] || [ ! -f "$probe_dir/b.ledger" ]; then
  echo "prepare VM did not reach admitted effect" >&2
  exit 1
fi

kill -9 "$probe_pid"
wait "$probe_pid" 2>/dev/null || true
probe_pid=""

output="$(SAGA_PROBE_MODE=recover SAGA_PROBE_PATH="$probe_dir" \
  erl -noshell -pa build/dev/erlang/*/ebin -s "$probe_module" main -s init stop 2>&1)"
if [[ "$output" != *RECOVERED* ]]; then
  echo "$output" >&2
  exit 1
fi
echo "PASS: fresh Erlang VM recovered $probe_module"
done
