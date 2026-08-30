#!/usr/bin/env bash
# Claim-mode load test against real server processes (design-doc load plan, option-2 fidelity).
#
# Launches N `lockers-server` JVMs in LOCKERS_ROOM_OWNERSHIP=claim mode sharing one Postgres
# (locker storage AND claim tables — the shared-substrate configuration production runs), drives
# the target write load through the redirect-following driver, then repeats the same load against
# a single monolith-mode JVM as the baseline and prints both reports side by side.
#
# Usage:
#   LOCKERS_DB_URL='jdbc:postgresql://localhost:5432/lockers?user=lockers&password=lockers' \
#     scripts/claim-load.sh
#
# Knobs (defaults in parentheses): LOCKERS_LOAD_NODES (2), LOCKERS_LOAD_ROOMS (1000),
# LOCKERS_LOAD_WPS (50), LOCKERS_LOAD_DURATION_SEC (30; the doc's full soak is 600).
# Keep wps × duration ≥ rooms so every room is actually exercised.
set -euo pipefail
cd "$(dirname "$0")/.."

: "${LOCKERS_DB_URL:?set LOCKERS_DB_URL to a Postgres JDBC URL}"

NODES="${LOCKERS_LOAD_NODES:-2}"
BASE_PORT=18091
PORT_MONO=$((BASE_PORT + 2 * NODES)) ADMIN_MONO=$((BASE_PORT + 2 * NODES + 1))
LOG_DIR="$(mktemp -d /tmp/claim-load.XXXXXX)"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

echo "== building server distribution =="
./gradlew -q :server:run:installDist
BIN=server/run/build/install/run/bin/run

wait_ready() {
  local port=$1 name=$2
  for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:${port}/readyz" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  echo "FATAL: ${name} (port ${port}) never became ready; log tail:" >&2
  tail -20 "${LOG_DIR}/${name}.log" >&2
  exit 1
}

start_claim_node() {
  local name=$1 port=$2 admin=$3
  LOCKERS_ROOM_OWNERSHIP=claim \
  LOCKERS_NODE_ID="$name" \
  LOCKERS_ADVERTISE_ADDR="127.0.0.1:${port}" \
  LOCKERS_HTTP_PORT="$port" \
  LOCKERS_ADMIN_PORT="$admin" \
  LOCKERS_DB_URL="$LOCKERS_DB_URL" \
  "$BIN" >"${LOG_DIR}/${name}.log" 2>&1 &
  PIDS+=($!)
}

run_driver() {
  local label=$1 targets=$2 pg_url=${3:-}
  LOCKERS_LOAD_TARGETS="$targets" \
  LOCKERS_LOAD_LABEL="$label" \
  LOCKERS_TEST_PG_URL="$pg_url" \
    ./gradlew :server:test --rerun --tests '*ExternalClaimLoadDriverTest' -q ||
    { echo "driver failed for ${label}; server log tails:" >&2; tail -20 "${LOG_DIR}"/*.log >&2; exit 1; }
  # Surface the driver's printed report from the test's stdout capture.
  find server/build/test-results/test -name '*ExternalClaimLoadDriverTest.xml' \
    -exec sh -c 'python3 -c "
import sys, xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
out = root.find(\"system-out\")
print(out.text if out is not None and out.text else \"(no driver output captured)\")
" "$1"' _ {} \;
}

echo "== phase 1: ${NODES}-node claim cluster =="
# Staggered boot (like a rolling deploy): simultaneous first-boot against a FRESH database can
# race ktstore's CREATE TABLE statements (Postgres's pg_type race hits even IF NOT EXISTS).
TARGETS=""
for i in $(seq 1 "$NODES"); do
  port=$((BASE_PORT + 2 * (i - 1)))
  admin=$((port + 1))
  start_claim_node "node${i}" "$port" "$admin"
  wait_ready "$port" "node${i}"
  TARGETS="${TARGETS:+${TARGETS},}127.0.0.1:${port}"
done
run_driver "claim ${NODES}-node" "$TARGETS" "$LOCKERS_DB_URL"

echo "== draining claim nodes =="
kill "${PIDS[@]}" 2>/dev/null || true
wait 2>/dev/null || true
PIDS=()

echo "== phase 2: monolith baseline =="
LOCKERS_ROOM_OWNERSHIP=local \
LOCKERS_HTTP_PORT="$PORT_MONO" \
LOCKERS_ADMIN_PORT="$ADMIN_MONO" \
LOCKERS_DB_URL="$LOCKERS_DB_URL" \
"$BIN" >"${LOG_DIR}/monolith.log" 2>&1 &
PIDS+=($!)
wait_ready "$PORT_MONO" monolith
run_driver "monolith baseline" "127.0.0.1:${PORT_MONO}"

echo "== done; server logs in ${LOG_DIR} =="
