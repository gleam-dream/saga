#!/usr/bin/env bash
# Runs this package's tests against a throwaway PostgreSQL cluster, and only
# against it: a fresh `initdb` in a temporary directory, listening on
# 127.0.0.1 at a random free port with trust authentication, stopped and
# removed on exit. It ignores every PG* variable, so it never reaches
# another server, and gives up rather than share a port something already
# listens on. The tests read the cluster's URL from
# SAGA_TEST_DATABASE_URL, which only this script sets.
#
# Usage (from the saga dev shell, which provides PostgreSQL 16):
#   integrations/saga_postgres/scripts/test-postgres.sh [gleam test args]
set -euo pipefail
cd "$(dirname "$0")/.."
for name in $(env | sed -n 's/^\(PG[A-Za-z0-9_]*\)=.*/\1/p'); do
  unset "$name"
done
unset SAGA_TEST_DATABASE_URL
for tool in initdb pg_ctl pg_isready createdb postgres; do
  command -v "$tool" >/dev/null || {
    echo "$tool not found: run this in the dev shell (nix develop)" >&2
    exit 1
  }
done
root="$(mktemp -d "${TMPDIR:-/tmp}/saga-postgres.XXXXXX")"
cluster="$root/data"
started=0
# Invoked indirectly by the EXIT trap, including explicit exit branches.
# shellcheck disable=SC2329
cleanup() {
  if [[ $started == 1 ]]; then
    pg_ctl -D "$cluster" -m immediate stop >/dev/null 2>&1 || true
  fi
  rm -rf "$root"
}
trap 'cleanup' EXIT
initdb -D "$cluster" --username=saga --auth-local=trust --auth-host=trust \
  --no-sync >/dev/null
for _ in 1 2 3 4 5; do
  port=$((20000 + RANDOM % 20000))
  # Something already answers there: never start next to it.
  if pg_isready -h 127.0.0.1 -p "$port" -t 1 >/dev/null 2>&1; then continue; fi
  if pg_ctl -D "$cluster" -l "$root/postgres.log" -w start -o \
    "-h 127.0.0.1 -p $port -k $root -c fsync=off -c synchronous_commit=off -c max_connections=300" \
    >/dev/null; then
    started=1
    break
  fi
done
if [[ $started != 1 ]]; then
  echo "could not start a cluster on a free port; see $root/postgres.log" >&2
  cat "$root/postgres.log" >&2 || true
  exit 1
fi
createdb -h 127.0.0.1 -p "$port" -U saga saga_test
export SAGA_TEST_DATABASE_URL="postgres://saga@127.0.0.1:$port/saga_test"
echo "throwaway cluster on 127.0.0.1:$port ($(postgres --version))"
if gleam test "$@"; then
  exit 0
else
  status=$?
  # The cluster is removed on exit; keep server-side failure evidence in the
  # gate log so connection limits and SQL errors remain diagnosable.
  tail -80 "$root/postgres.log" >&2
  exit "$status"
fi
