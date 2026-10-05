#!/usr/bin/env bash
# M1-M3 demo: room listed -> join accepted -> relayed traffic verified ->
# both spectators print the same input-log hash as the host.
#
# Requires the server and client-sim to be built:
#   cargo build --release --bin fighter-server --bin client-sim
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/release"
PORT="${PORT:-18080}"
UDP_PORT="${UDP_PORT:-17780}"
TMP="$(mktemp -d)"
trap 'kill $SERVER $HOST $JOIN $SPEC1 $SPEC2 2>/dev/null || true; rm -rf "$TMP"' EXIT

export FIGHTER__SERVER__HTTP_BIND="127.0.0.1:$PORT"
export FIGHTER__SERVER__UDP_BIND="127.0.0.1:$UDP_PORT"
export FIGHTER__SERVER__PUBLIC_UDP_HOST="127.0.0.1"
export FIGHTER__SERVER__LOG_FORMAT="pretty"
export FIGHTER__LIMITS__MAX_CONNECTIONS_PER_IP=5000

"$BIN/fighter-server" >"$TMP/server.log" 2>&1 &
SERVER=$!
sleep 1

URL="ws://127.0.0.1:$PORT/v1/ws"

"$BIN/client-sim" host --url "$URL" --name Lino --room Demo \
  --spectators --ticks 300 --code-out "$TMP/code.txt" >"$TMP/host.log" 2>&1 &
HOST=$!

for _ in $(seq 1 40); do
  [ -s "$TMP/code.txt" ] && break
  sleep 0.25
done
CODE="$(cat "$TMP/code.txt")"
echo "room code: $CODE"

"$BIN/client-sim" join --url "$URL" --name Lufi --code "$CODE" --duration 30 \
  >"$TMP/join.log" 2>&1 &
JOIN=$!

"$BIN/client-sim" spectate --url "$URL" --name Early --code "$CODE" \
  >"$TMP/spec1.log" 2>&1 &
SPEC1=$!
sleep 2
"$BIN/client-sim" spectate --url "$URL" --name Late --code "$CODE" \
  >"$TMP/spec2.log" 2>&1 &
SPEC2=$!

wait "$HOST" || true
sleep 8
kill "$SPEC1" "$SPEC2" 2>/dev/null || true
wait "$SPEC1" "$SPEC2" 2>/dev/null || true

HOST_HASH="$(grep 'host input-log sha256' "$TMP/host.log" | awk '{print $NF}')"
S1_HASH="$(grep 'spectator input-log sha256' "$TMP/spec1.log" | awk '{print $NF}')"
S2_HASH="$(grep 'spectator input-log sha256' "$TMP/spec2.log" | awk '{print $NF}')"

echo "host:  $HOST_HASH"
echo "early: $S1_HASH"
echo "late:  $S2_HASH"

if [ -n "$HOST_HASH" ] && [ "$HOST_HASH" = "$S1_HASH" ] && [ "$HOST_HASH" = "$S2_HASH" ]; then
  echo "DEMO OK: all input-log hashes match"
else
  echo "DEMO FAILED"
  exit 1
fi
