# Load test

Method and measured results for the client-sim load tool. Numbers are indicative
and were taken on a Windows development machine (not the 2 vCPU / 2 GB target
host), so treat the scaling figures as a floor rather than a certification.

## Relay latency

Goal: server-side relay forwarding under 1 ms at the 99th percentile (section
7.4). The server records every forwarded datagram in the Prometheus histogram
`fighter_relay_latency_seconds`, measured from UDP receive to send completion.

Run:

```sh
# terminal 1
FIGHTER__SERVER__PUBLIC_UDP_HOST=127.0.0.1 cargo run --release --bin fighter-server
# terminal 2 (writes the room code to code.txt)
cargo run --release --bin client-sim -- host --name Lino --room Relay \
  --spectators --ticks 600 --code-out code.txt
# terminal 3
cargo run --release --bin client-sim -- join --name Lufi --code "$(cat code.txt)" --duration 30
# terminal 4
curl -s localhost:8080/metrics | grep relay_latency
```

Result (1200 forwarded datagrams: 600 host -> guest, 600 guest echo):

| metric | value |
|---|---|
| samples | 1200 |
| mean | 0.106 ms |
| p99 | < 0.5 ms |
| max | < 0.5 ms (every sample fell in the `le="0.0005"` bucket) |

## Lobby, feed fan-out and spectators

Goal (section 11): 1,000 concurrent matches with relay plus 5,000 spectators on
2 vCPU / 2 GB. The target host was not available, so a scaled run was used:

```sh
FIGHTER__LIMITS__MAX_CONNECTIONS_PER_IP=5000 \
FIGHTER__LIMITS__MAX_CONNECTIONS=20000 \
cargo run --release --bin fighter-server
cargo run --release --bin client-sim -- load \
  --matches 100 --spectators 200 --duration 10
```

Result:

| metric | value |
|---|---|
| matches | 100 completed |
| spectator tasks | 200 (100 attached, 100 waiting for a free room) |
| WebSocket connections | 300 accepted |
| feed frames | 10,000 INPUTS, 100 MATCH_START, 100 MATCH_END |
| rooms | 100 `full` |
| server peak working set | 50.5 MB |
| wall time | 11.7 s |

Extrapolating linearly, 1,000 matches and 5,000 spectators would be roughly
10x the rooms and 50x the spectators; the dominant cost is per-connection
buffers, not the room log, so the 2 GB ceiling is comfortable. This is an
estimate, not a measured result on the target hardware.

Note: `max_connections_per_ip` defaults to 8, so a localhost load test must
raise `FIGHTER__LIMITS__MAX_CONNECTIONS_PER_IP` first.

## Reproducing in CI

The M1-M4 integration tests (`cargo test --all`) cover correctness. The load
numbers above require a machine with spare cores and are not run in CI.
