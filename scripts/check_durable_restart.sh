#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
probe_file="/tmp/saga-durable-vm-$$"
probe_pid=""
cleanup() {
  if [ -n "$probe_pid" ]; then
    kill -9 "$probe_pid" 2>/dev/null || true
    wait "$probe_pid" 2>/dev/null || true
  fi
  rm -f "$probe_file" "$probe_file.cancel" "$probe_file.a.ledger" "$probe_file.b.ledger" "$probe_file".*.tmp
}
trap cleanup EXIT

gleam test >/dev/null
for probe_module in recovery_probe compensation_recovery_probe; do
SAGA_PROBE_MODE=prepare SAGA_PROBE_PATH="$probe_file" \
  erl -noshell -pa build/dev/erlang/*/ebin -s "$probe_module" main -s init stop >/dev/null 2>&1 &
probe_pid=$!

for _ in $(seq 1 500); do
  if [ -f "$probe_file" ] && [ -f "$probe_file.a.ledger" ] && [ -f "$probe_file.b.ledger" ]; then
    break
  fi
  if ! kill -0 "$probe_pid" 2>/dev/null; then
    echo "prepare VM exited before persisting the effect" >&2
    exit 1
  fi
  sleep 0.01
done

if [ ! -f "$probe_file" ] || [ ! -f "$probe_file.a.ledger" ] || [ ! -f "$probe_file.b.ledger" ]; then
  echo "prepare VM did not reach admitted effect" >&2
  exit 1
fi

kill -9 "$probe_pid"
wait "$probe_pid" 2>/dev/null || true
probe_pid=""

output="$(SAGA_PROBE_MODE=recover SAGA_PROBE_PATH="$probe_file" \
  erl -noshell -pa build/dev/erlang/*/ebin -s "$probe_module" main -s init stop 2>&1)"
if [[ "$output" != *RECOVERED* ]]; then
  echo "$output" >&2
  exit 1
fi
echo "PASS: fresh Erlang VM recovered $probe_module"
cleanup
done
