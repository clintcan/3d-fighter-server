# 3D Fighter online server

A standalone Rust service that the game [3D Fighter](https://github.com/clintcan/3d-fighter)
connects to for internet play: a lobby and room browser, UDP connection
brokering (hole punching and relay), and delayed spectator feeds.

The full specification lives in [`AGENTS.md`](AGENTS.md). This repository
implements it milestone by milestone (section 12).

## Status

- **M1 — Lobby and rooms:** implemented.
  `hello`/`welcome` with version gating, `ping`, `list_rooms` (filters,
  pagination), `create_room` (public/unlisted/password), room codes,
  `join_room` by id or code, `join_request`/`answer_join`/`join_declined`/
  `cancel_join` with timeout and decline cooldown, `kick`, `leave_room`,
  `room_update` → coalesced `room_state`, host-leave closes the room, idle
  room expiry, `/healthz`, `/metrics`, `/v1/rooms`, config file + env
  overrides, structured logging, Docker image.
- **M2 — Connection brokering:** implemented. UDP `BIND`/`BOUND` with endpoint
  tracking, keepalive and 60 s expiry; `peer_endpoints` with `punch_at` and
  same-public-IP LAN detection; relay with source-address check, per-player
  200 datagram/s and 64 KiB/s budgets; relay-only mode; UDP `PING`/`PONG` and
  `connection_report` metrics.
- **M3 — Spectators:** implemented. Host feed publishing with validation, a
  per-room match log, 20 ms delayed fan-out (no per-spectator sleeps), late-join
  catch-up re-batched into 600-tick frames, `CHECKSUM`/`MATCH_END` pass-through,
  `FEED_RESET` on gaps, backpressure-based slow-spectator drop, spectator counts
  in `room_state`, and `react`/`reaction`.
- **M4 — Hardening:** implemented. All section 10.2 limits, temporary bans with
  exponential backoff, configurable name blocklist, constant-time password
  comparison, graceful shutdown with a match-drain window, reconnection with
  single-use `resume_token` (30 s grace), cargo-fuzz targets for every parser,
  and the `client-sim` host/join/spectate/load tools with
  [`docs/LOAD-TEST.md`](docs/LOAD-TEST.md).
- **M5 — Extras:** implemented.
  - Quick match (`queue_join`/`queue_leave`) pairs compatible clients, first in
    queue hosts.
  - Replays: finished matches stored under `storage.replay_dir`, listed at
    `GET /v1/replays` and downloaded at `GET /v1/replays/{id}`.
  - Feed verification: the guest may publish too; disagreements mark the room
    `feed_verified: false` and increment a metric.
  - Admin API (`POST /admin/notice`, `GET /admin/rooms`,
    `POST /admin/rooms/{id}/close`, `POST /admin/ban`) behind a bearer token.
  - Room stats (`matches`, `rematches`, `spectator_peak`) in `room_state`.
  - Region discovery at `GET /v1/regions`.

## Layout

```
crates/protocol/    wire codecs and validation (no I/O)
crates/server/      lobby, HTTP/WebSocket, config, metrics
crates/client-sim/  test/demo client (M2 onward)
config/             example configuration
docs/               decisions and generated protocol vectors
```

## Build and test

Requires a recent stable Rust toolchain (MSVC toolchain on Windows).

```sh
cargo build --release
cargo test --all
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

`cargo test -p fighter-protocol` regenerates
[`docs/PROTOCOL-VECTORS.md`](docs/PROTOCOL-VECTORS.md).

## Run

```sh
cp config/server.example.toml config/server.toml   # then edit
cargo run --release --bin fighter-server
```

The server reads `FIGHTER_CONFIG` (default `config/server.toml` when present).
Every setting can be overridden by an environment variable named
`FIGHTER__SECTION__KEY`, for example `FIGHTER__SERVER__REGION=eu` or
`FIGHTER__LIMITS__MAX_CONNECTIONS=20000`.

It listens for WebSocket + HTTP on `server.http_bind` (default
`0.0.0.0:8080`). Put TLS in front (Caddy/nginx) or configure `[tls]` later.
The server enforces HTTP header-read timeouts (`http_header_timeout_ms`, default
10 s) and caps raw TCP connections (`max_http_connections`), so incomplete
requests cannot pile up; a proxy is still recommended for TLS. When TLS
terminates at a proxy, list it in `server.trusted_proxies` so bans and per-IP
limits see the real client.
The UDP listener (rendezvous, relay, ping) binds `server.udp_bind` (default
`0.0.0.0:7780`); clients are told `server.public_udp_host` and that port in
`welcome.udp`.

Endpoints:

| Path | Purpose |
|---|---|
| `GET /v1/ws` | Lobby WebSocket (subprotocol `3dfighter.lobby.v1`) |
| `GET /healthz` | `200 ok`, or `503` while shutting down |
| `GET /metrics` | Prometheus text |
| `GET /v1/rooms` | Public room list as `{"rooms":[...]}`, cached 2 s, no codes |

## Docker

```sh
docker build -t fighter-server .
docker run -p 8080:8080 -p 7780:7780/udp fighter-server
```

## Demo and load test

Build the tools, then run the M1-M3 demo (host + join + two spectators, one
late; all three input-log hashes must match):

```sh
cargo build --release --bin fighter-server --bin client-sim
scripts/demo.sh
```

Load test (see [`docs/LOAD-TEST.md`](docs/LOAD-TEST.md)):

```sh
FIGHTER__LIMITS__MAX_CONNECTIONS_PER_IP=5000 \
  cargo run --release --bin fighter-server &
cargo run --release --bin client-sim -- load --matches 100 --spectators 200 --duration 10
```

Fuzzing (requires nightly and `cargo-fuzz`):

```sh
cargo install cargo-fuzz
for t in json_message binary_frame udp_datagram text_name; do
  cargo +nightly fuzz run "$t" -- -max_total_time=60
done
```

## Configuration

See [`config/server.example.toml`](config/server.example.toml). Secrets such as
the admin token are read from the environment (`admin.token_env`, default
`FIGHTER_ADMIN_TOKEN`); nothing sensitive belongs in the file or the repository.

## Decisions

Every judgement call the spec left open is recorded in
[`docs/DECISIONS.md`](docs/DECISIONS.md).
