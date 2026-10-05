# AGENTS.md: 3D Fighter Online Server

This file is the complete specification and working guide for the **3D Fighter online server**, a standalone Rust service that the game [3D Fighter](https://github.com/clintcan/3d-fighter) (Godot 4, MIT) connects to for internet play. Read all of it before writing code. If something here is ambiguous, choose the simplest behaviour that satisfies the stated requirements, write the decision down in `docs/DECISIONS.md`, and keep going.

The game client side is **not** part of this repository. It is maintained in the `3d-fighter` repository and will be built against this document, so the wire protocol in sections 6–9 is a contract: implement it exactly, byte for byte.

---

## 1. What the server is for

3D Fighter already plays online peer-to-peer: two game instances exchange inputs over UDP using rollback netcode, with their own end-to-end encrypted protocol. That works on a LAN, and over the internet only if the host forwards UDP port 7777 on their router. This server removes that limitation and adds the social layer around matches:

1. **Lobby and room browser.** Hosts create rooms; players list available rooms (open, in a match, spectatable), filter them, and join by clicking or by a short room code.
2. **Connection brokering.** The server helps the two players reach each other directly (UDP hole punching), and **relays** their packets when a direct path is impossible. The players' traffic stays end-to-end encrypted; the server forwards opaque bytes and never needs to read them.
3. **Spectators.** Anyone can watch a match in a room that allows it. The host publishes the match setup and the confirmed inputs of both players; the server fans them out to spectators after a short delay. Each spectator's game re-simulates the match locally. The simulation is deterministic, so the inputs alone reproduce the fight exactly, at a cost of about 240 bytes per second per match.
4. **More around matches:** room codes, passwords, quick match, reconnection, reactions, replays, server notices and update prompts, plus metrics and admin tools. The full list, with priorities, is in section 12.

### Non-goals
- The server **never simulates the game** and has no game logic. It knows nothing about hit boxes or health. It relays bytes, keeps room state, and stores input logs.
- No user accounts or passwords in v1. Identity is an anonymous client id plus a display name.
- No free-text chat in v1 (moderation burden). Reactions are a fixed set of emotes.
- No web front end beyond a JSON room list and metrics (a website can be built on top later).

---

## 2. How the game works (what you need to know)

You will not run the game, but these facts shape the protocol.

| Fact | Value |
|---|---|
| Simulation | Deterministic, fixed **60 ticks per second**. Same inputs + same seed + same build = identical match on every machine (verified on Windows and Linux). |
| One player's input per tick | A `u16`: bits 0–3 = numpad direction **1–9** (5 = neutral, 6 = right, 4 = left, 8 = up, 2 = down; screen-relative), bits 4–8 = buttons: LP = `0x10`, HP = `0x20`, LK = `0x40`, HK = `0x80`, SIDESTEP = `0x100`. Bits 9–15 are always 0. |
| Match setup | Stage index (u8), P1 fighter index (u8), P2 fighter index (u8), seed (u32). The host is always P1. |
| Roster (v0.4.1), index → id | 0 `kenji`, 1 `rhea`, 2 `brutus`, 3 `valka`, 4 `jin` |
| Stages (v0.4.1), index → id | 0 `ring` (Boxing Ring), 1 `dojo`, 2 `rooftop`, 3 `temple`, 4 `beach` |
| Version identity | `game_version` string (e.g. `"0.4.1"`) **and** `content_hash`, a u32 hash of all character and move data. Two clients can only play or watch together if both match exactly. The server must compare both. |
| Match flow | Best of 3 rounds (up to 5 on draws), 99-second rounds, an intro of 110 ticks before each round. A full match is typically 1–4 minutes (3,600–14,400 ticks). |
| State checksums | Every 60 ticks, each game hashes its confirmed state into a u32; peers compare them to detect desyncs. The host can include them in the spectator feed. |
| Peer-to-peer protocol | UDP, default port 7777, NetPeer protocol version **3**. First byte of every datagram is a type in the range `0x01`–`0x34` (1, 10–16, 20, 21, 30, 40, 41, 50, 51, 52; 52 is `T_PUNCH`, a tiny hole-punch datagram the server may relay like any other). Datagrams are at most ~600 bytes (the largest is the handshake WELCOME carrying an RSA public key). The handshake includes a cookie challenge, an RSA-2048 key exchange and a 6-digit security code; after it, every datagram is AES-256-CBC + HMAC-SHA256. **The server relays these datagrams unchanged and must never parse them.** |
| Player names | 1–24 characters, with control, zero-width and bidirectional-override characters removed (see section 10.4). |

All multi-byte integers in this document are **little-endian**, matching the game's `StreamPeerBuffer` default.

---

## 3. Architecture

One binary, `fighter-server`, with three listeners:

```
                         ┌─────────────────────────── fighter-server ───────────────────────────┐
 game (host)  ── wss ──► │ Lobby (WebSocket, JSON + binary frames)  ── rooms, joins, spectator feed │
 game (guest) ── wss ──► │                                                                          │
 spectators   ── wss ──► │ HTTP: /healthz  /metrics  /v1/rooms  /v1/replays  /admin/*               │
                         │                                                                          │
 game (host)  ── udp ──► │ Rendezvous + Relay (UDP)  ── endpoint discovery, hole punch, relay       │
 game (guest) ── udp ──► │                                                                          │
                         └──────────────────────────────────────────────────────────────────────────┘
 host ◄──────────── direct UDP (hole punched) or via the relay ────────────► guest
```

- **Lobby**: a WebSocket endpoint (`/v1/ws`), behind TLS in production (`wss://`). It carries JSON text frames for control messages and binary frames for the spectator feed. One connection per game client, kept open for the whole session.
- **HTTP**: health, Prometheus metrics, a public JSON room list (for a website), replay downloads and an authenticated admin API. Same port as the WebSocket.
- **UDP**: a single socket (default port `7780`) for endpoint discovery (`BIND`), hole-punch coordination, relaying and ping.
- **State**: in memory in v1 (rooms, sessions, live match logs). Replays and simple stats go to SQLite (optional, milestone 5). A restart drops live rooms; clients reconnect and see a server notice.

The server is a single process. Scale-out by region (one instance per region, e.g. `asia`, `eu`, `us`) is enough; clients pick a region. Instances do not talk to each other.

---

## 4. Technology and repository rules

### Stack
- **Rust stable** (edition 2021, MSRV = latest stable minus 2 releases). `#![forbid(unsafe_code)]` in every crate.
- Async runtime: `tokio`. HTTP + WebSocket: `axum` (with its `ws` feature). Serialization: `serde` / `serde_json`. Config: `serde` + `toml`. Logging: `tracing` + `tracing-subscriber` (JSON output option). Metrics: `prometheus` or `metrics` + an exporter. Randomness: `rand` with `OsRng` for tokens. Optional persistence: `sqlx` with SQLite. Tests: `tokio::test`, plus `proptest` and `cargo-fuzz` for parsers.
- Add a dependency only when it removes real work, and note why in `docs/DECISIONS.md`. No dependencies with known security advisories (`cargo audit` must pass).

### Suggested layout
```
3DFighter-Server/
├── AGENTS.md                 (this file)
├── README.md                 (how to run, configure, deploy)
├── Cargo.toml                (workspace)
├── crates/
│   ├── protocol/             (message types, binary codecs, validation; no I/O; fully unit tested)
│   ├── server/               (lobby, rooms, UDP rendezvous/relay, spectator fan-out, HTTP, config)
│   └── client-sim/           (CLI that imitates game clients for tests and demos; see section 13.3)
├── config/server.example.toml
├── docs/
│   ├── DECISIONS.md          (every judgement call, one paragraph each)
│   ├── PROTOCOL-VECTORS.md   (hex examples of every binary message, generated by tests)
│   └── LOAD-TEST.md          (load test method and results)
├── fuzz/                     (cargo-fuzz targets for every parser)
├── Dockerfile
└── .github/workflows/ci.yml  (fmt, clippy, test, audit, fuzz smoke)
```

### Working rules for agents
1. Before every commit: `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test --all`. All three must pass. CI repeats them and also runs `cargo audit` and a short fuzz pass.
2. Small, focused commits with clear messages. One feature or fix per commit.
3. Every protocol message gets a codec unit test, a round-trip property test, and a fuzz target. Every rule in section 10 gets a test that proves it.
4. **Never change the wire protocol silently.** Any change bumps `protocol` (section 6.1), updates this file and `docs/PROTOCOL-VECTORS.md`, and is listed in the changelog. Keep accepting the previous protocol version for one release if you can.
5. No panics on untrusted input. Every parser returns `Result`, and malformed input is dropped or answered with an `error`, never `unwrap()`ed.
6. No secrets in the repository (TLS keys, admin tokens). Configuration comes from a file and environment variables.
7. Log in a structured way, without personal data beyond what is needed: no IP addresses in logs by default (config flag `log_ips = false`).

---

## 5. Flows (overview)

### 5.1 Hosting and joining
```
host                                   server                                  guest
 │── hello ─────────────────────────────►│◄──────────────────────────── hello ──│
 │◄──────────────────────────── welcome ─│─ welcome ───────────────────────────►│
 │── create_room ───────────────────────►│                                      │
 │◄──────────────────────── room_created │                                      │
 │                                       │◄─────────────────────── list_rooms ──│
 │                                       │─ rooms ─────────────────────────────►│
 │                                       │◄──────────────────────── join_room ──│
 │◄────────────────────────join_request ─│─ join_pending ──────────────────────►│
 │── answer_join (accept) ──────────────►│                                      │
 │◄─────────────────────── match_session │─ match_session ─────────────────────►│
 │== UDP BIND (session token) ==========►│◄========== UDP BIND (session token) ==│
 │◄========================== UDP BOUND ═│═ UDP BOUND ========================►│
 │◄────────────────────── peer_endpoints │─ peer_endpoints ────────────────────►│
 │◄═════════ game handshake + match traffic: direct (punched) or via RELAY ═════►│
 │── connection_report (direct|relay) ──►│◄────── connection_report ────────────│
 │── room_update (character select…) ───►│─ room_state (to members, spectators) │
```

### 5.2 Spectating
```
host                                   server                                spectator
 │── [bin] MATCH_START ─────────────────►│                                      │
 │── [bin] INPUTS (batches, every ~100ms)►│ stores the match log                 │
 │                                       │◄──────────────────────── spectate ───│
 │                                       │─ spectate_started (delay 3000 ms) ──►│
 │                                       │─ [bin] MATCH_START + INPUTS from tick 0 up to (now - delay), then live ►│
 │── [bin] CHECKSUM (every 60 ticks) ───►│─ [bin] CHECKSUM (delayed) ──────────►│
 │── [bin] MATCH_END ───────────────────►│─ [bin] MATCH_END (delayed) ─────────►│
```

---

## 6. Lobby protocol (WebSocket)

### 6.1 Connection
- Endpoint: `GET /v1/ws`, WebSocket subprotocol `3dfighter.lobby.v1`. A request without it gets HTTP 400.
- Text frames: one JSON object per frame, UTF-8, max **8 KiB**. Binary frames: section 8, max **64 KiB**.
- Every JSON message has a `"type"` string. A client may add `"rid"` (request id, a string of at most 32 characters); the server echoes it as `"rid"` in the direct response or `error`.
- The first client message must be `hello` within **10 s**, otherwise the server closes the connection (close code 4000).
- The server sends a WebSocket ping every 20 s; a client silent for **45 s** is disconnected. Clients may also send `ping` messages for latency display.
- Unknown `type` values: reply `error` with `unknown_type`; do not disconnect (forward compatibility). Malformed JSON: reply `error` with `bad_message`; disconnect after 10 malformed messages in a minute.

### 6.2 Client → server messages

| type | Fields | Who | Notes |
|---|---|---|---|
| `hello` | `protocol: 1`, `game_version: string`, `content_hash: u32`, `client_id: uuid string`, `name: string`, `resume_token?: string`, `relay_only?: bool`, `region?: string` | anyone, first | `client_id` is random per installation, stored by the game. `relay_only: true` means never reveal this client's IP to peers (section 7.4). |
| `ping` | `t: u64` (client ms) | anyone | Server replies `pong`. |
| `list_rooms` | `status?: "open" \| "in_match" \| "any"` (default `"any"`), `spectatable?: bool`, `compatible_only?: bool` (default true), `cursor?: string`, `limit?: 1–100` (default 50) | anyone | Never lists `unlisted` rooms. |
| `create_room` | `name?: string` (default `"<name>'s room"`), `visibility: "public" \| "unlisted"`, `password?: string` (4–32 chars), `allow_spectators: bool`, `spectator_delay_ms?: 0–10000` (default 3000), `max_spectators?: 0–200` (default 50) | not in a room | The creator becomes host (P1). Returns `room_created`. |
| `join_room` | `room: string` (room id **or** room code), `password?: string` | not in a room | Version and content hash must match the room's. Returns `join_pending`, then `match_session` or `join_declined`. |
| `cancel_join` | — | pending guest | Withdraws the request; the host gets `join_cancelled`. |
| `answer_join` | `request_id: string`, `accept: bool` | host | Decline = optional `reason` string (max 64 characters). |
| `kick` | `reason?: string` | host | Removes the guest; the room reopens. |
| `leave_room` | — | host, guest or spectator | A host leaving closes the room (`room_closed`). A guest leaving reopens it. |
| `room_update` | `phase: "lobby" \| "character_select" \| "stage_select" \| "in_match" \| "results"`, `fighters?: [string\|null, string\|null]` (P1, P2 ids), `stage?: string`, `round?: u8`, `wins?: [u8, u8]`, `timer?: u8` | host | Updates the room listing; broadcast to members and spectators as `room_state`. At most 4 per second (coalesce, keep the newest). |
| `connection_report` | `path: "direct" \| "relay"`, `rtt_ms?: u16` | host, guest | Recorded for metrics and shown in `room_state`. |
| `spectate` | `room: string` (id or code), `password?: string` | not in a room | Room must allow spectators and have room. Returns `spectate_started`, then binary feed frames. |
| `stop_spectating` | — | spectator | |
| `react` | `emote: "clap" \| "fire" \| "wow" \| "laugh" \| "gg" \| "ouch"` | members and spectators | Rate limit 1 per 2 s per client; broadcast as `reaction` to everyone in the room. |
| `queue_join` | `mode: "casual"`, `region?: string` | not in a room | Quick match (milestone 5). |
| `queue_leave` | — | queued | |

### 6.3 Server → client messages

| type | Fields | Notes |
|---|---|---|
| `welcome` | `protocol: 1`, `session_id`, `resume_token`, `server_time: u64` (ms), `region`, `udp: {host, port}`, `limits: {...}` (section 10.2), `motd?: string`, `latest_game_version?: string`, `update_url?: string` | If the client's version is older than `latest_game_version`, the game shows an "update available" prompt. If below `min_game_version`, the server sends `error` with `version_unsupported` and closes instead. |
| `pong` | `t` (echoed), `server_time` | |
| `rooms` | `rooms: [Room]`, `next_cursor?: string` | Room summary objects, section 6.4. |
| `room_created` | `room: Room`, `code: string` | `code` is 6 characters from `ABCDEFGHJKMNPQRSTUVWXYZ23456789` (no 0/O/1/I/L), unique among live rooms. |
| `join_pending` | `room_id` | To the guest while the host decides. |
| `join_request` | `request_id`, `name`, `client_id_hash` (first 8 hex characters of SHA-256 of the client id, for "same person again" hints without revealing the id) | To the host. Only one pending request per room; others get `error` with `room_busy`. A request expires after **30 s** (`join_declined` with reason `timeout`). |
| `join_cancelled` | `request_id` | To the host. |
| `join_declined` | `room_id`, `reason` | To the guest. Reasons: `declined`, `timeout`, `room_closed`, `room_full`. After a decline the same `client_id` cannot request that room again for **30 s**. |
| `match_session` | `room_id`, `role: "host" \| "guest"`, `session_token` (32 hex characters = 16 bytes), `relay_key` (16 hex characters = 8 bytes, unique per player), `peer: {name}`, `udp: {host, port}` | Both players get this when the host accepts. The game now does UDP `BIND` (section 7). |
| `peer_endpoints` | `room_id`, `candidates: [{ip, port, kind: "public" \| "local"}]`, `punch_at: u64` (server ms) | Sent to each player when **both** have bound. Empty `candidates` means relay only. See section 7.3. |
| `room_state` | `room: Room` | To members and spectators on every change. |
| `player_left` | `room_id`, `role`, `reason: "left" \| "disconnected" \| "kicked"` | |
| `room_closed` | `room_id`, `reason: "host_left" \| "host_disconnected" \| "expired" \| "admin"` | Also ends any spectating. |
| `spectate_started` | `room_id`, `delay_ms`, `match_live: bool` | Binary feed frames follow (section 8). |
| `spectate_ended` | `room_id`, `reason` | |
| `reaction` | `room_id`, `from: "host" \| "guest" \| "spectator"`, `name`, `emote` | |
| `queue_matched` | then `match_session` follows | Milestone 5. |
| `server_notice` | `message`, `severity: "info" \| "warning"` | For example "Server restarts in 5 minutes". |
| `error` | `code`, `message`, `rid?` | Codes in section 6.5. |

### 6.4 Room object

```json
{
  "id": "r_7f3a9c2e",
  "code": "KX7Q2M",
  "name": "Lino's room",
  "host_name": "Lino",
  "visibility": "public",
  "has_password": false,
  "status": "in_match",
  "phase": "in_match",
  "game_version": "0.4.1",
  "content_hash": 2874412290,
  "compatible": true,
  "players": [ {"role": "host", "name": "Lino", "fighter": "jin"}, {"role": "guest", "name": "Lufi", "fighter": "valka"} ],
  "stage": "beach",
  "round": 3,
  "wins": [1, 1],
  "spectators": 4,
  "allow_spectators": true,
  "max_spectators": 50,
  "spectator_delay_ms": 3000,
  "connection": "direct",
  "region": "asia",
  "created_at": 1791200000000
}
```
- `status` is computed by the server: `open` (host only, accepting joins), `full` (two players, not in a match), `in_match` (host reported `in_match`).
- `code` is included only for members of the room and for `public` rooms. For `unlisted` rooms the code is the only way in, and only the host sees it.
- `compatible` is `true` when the room's `game_version` and `content_hash` equal the requesting client's.
- Never include IP addresses or `client_id`s in a room object.

### 6.5 Error codes
`bad_message`, `unknown_type`, `not_allowed` (wrong role or state), `version_unsupported`, `version_mismatch` (room needs a different game version or data), `room_not_found`, `room_full`, `room_busy` (a join request is already pending), `wrong_password`, `spectating_disabled`, `spectators_full`, `rate_limited`, `name_invalid`, `already_in_room`, `server_full`, `internal`.

### 6.6 Reconnection
If a WebSocket drops, the server keeps the client's session (room membership, role, spectating) for **30 s**. A new connection whose `hello` carries the old `resume_token` takes it over and gets `welcome` with the same `session_id`, followed by the current `room_state`. A host reconnecting mid-match keeps publishing the feed from the next tick it has; a gap in the feed is handled as in section 8.3. After 30 s the session ends as a `disconnected` leave.

---

## 7. Rendezvous and relay (UDP)

One UDP socket, default port **7780**. Every datagram starts with a type byte in the range **`0xF0`–`0xF7`**, which never collides with game datagrams (`0x01`–`0x34`). The server **drops silently** anything malformed, unknown, unauthenticated or over a rate limit, and never sends a reply larger than the request (no amplification).

### 7.1 Datagram formats

| Type | Direction | Layout (little-endian) | Size |
|---|---|---|---|
| `0xF0 BIND` | client → server | `u8 0xF0`, `[16] session_token`, `u8 role` (0 = host, 1 = guest), `u8 n` (0–4), then `n` × (`[4] ipv4`, `u16 port`) local candidates | 19 + 6n (max 43) |
| `0xF1 BOUND` | server → client | `u8 0xF1`, `[4] observed_ipv4`, `u16 observed_port` | 7 |
| `0xF2 RELAY` | client → server | `u8 0xF2`, `[8] relay_key`, `payload` (1–1200 bytes) | 10–1209 |
| `0xF3 RELAYED` | server → client | `u8 0xF3`, `payload` | 2–1201 |
| `0xF4 PING` | client → server | `u8 0xF4`, `u32 nonce`, `u32 client_time_ms` | 9 |
| `0xF5 PONG` | server → client | `u8 0xF5`, `u32 nonce`, `u32 client_time_ms` | 9 |

IPv6: v1 is IPv4 only. Reserve `0xF6`/`0xF7` for IPv6 variants of BIND/BOUND.

### 7.2 Binding
- After `match_session`, each player sends `BIND` with its session token and role from the game's UDP socket. That is the socket it will use for the match, normally the one bound to port 7777. It resends every **250 ms** until it gets `BOUND`, then every **15 s** as a keepalive (NAT mappings expire).
- The server checks the token and role, records the **source address** as that player's public endpoint plus the local candidates, and replies `BOUND` with the source address it saw. A re-BIND from a new address updates the endpoint (the NAT rebound).
- A binding expires after **60 s** without any BIND or RELAY from it. A session's tokens are invalidated when the room closes, the guest leaves, or after **6 hours**.

### 7.3 Hole punching
- When both players are bound, the server sends `peer_endpoints` to each over WebSocket. The list contains the other player's public endpoint, plus their local candidates if both players have the same public IP (the same LAN). It also gives `punch_at`, a time **300 ms** in the future, so both sides start together.
- At `punch_at`, both games start their normal peer-to-peer handshake, sending the guest's HELLO directly to the candidates. Packets in both directions open the NAT mappings. The game handles this; the server only coordinates the timing.
- If the game gets no direct reply within **3 s**, it switches to the relay. It reports the outcome with `connection_report`.

### 7.4 Relay
- A player sends `RELAY` with its own `relay_key` and the game datagram as payload. The server checks that the key exists and that the **source address equals that player's bound endpoint**. It then sends `RELAYED` with the same payload to the **other** player's bound endpoint. No other processing happens: no parsing, no reordering, no buffering beyond the socket.
- The server never relays to an address that hasn't proven itself with BIND, so it can't be used to send traffic to arbitrary hosts.
- **Relay-only mode:** if either player said `relay_only: true` in `hello`, or the room is configured so, `peer_endpoints.candidates` is empty and neither player ever learns the other's IP address. The relay is then the only path.
- **Budgets per player:** at most 200 datagrams per second and 64 KiB/s. A match normally uses about 70 datagrams per second and 6 KiB/s per player. Excess traffic is dropped and counted in metrics.
- **Latency target:** forwarding adds under 1 ms at the 99th percentile on the server itself (measure it in the load test).

### 7.5 Ping
`PING`/`PONG` lets the game show the latency to each region before choosing a server, and to estimate a room's connection quality. Limit: 10 per second per source address. A PONG is the same size as a PING.

---

## 8. Spectator feed (binary WebSocket frames)

The **host** publishes the feed on its lobby WebSocket as binary frames. The server validates it, stores the match log, and sends the same frames to spectators after `spectator_delay_ms`. The delay stops anyone in the stream from giving live hints to a player. The frame layouts are identical in both directions.

### 8.1 Frame formats (little-endian)

| Type | Layout |
|---|---|
| `0x01 MATCH_START` | `u8 0x01`, `u16 feed_version` (= 1), `u32 match_id` (the match seed), `u8 stage_index`, `u8 p1_fighter_index`, `u8 p2_fighter_index`, then six short strings, each `u8 length` + UTF-8 (max 64 bytes): `game_version`, `stage_id`, `p1_fighter_id`, `p2_fighter_id`, `p1_name`, `p2_name`. |
| `0x02 INPUTS` | `u8 0x02`, `u32 match_id`, `u32 first_tick`, `u16 count` (1–600), then `count` × (`u16 p1_input`, `u16 p2_input`) |
| `0x03 CHECKSUM` | `u8 0x03`, `u32 match_id`, `u32 tick`, `u32 checksum` |
| `0x04 MATCH_END` | `u8 0x04`, `u32 match_id`, `u32 final_tick`, `u8 result` (0 = P1 won, 1 = P2 won, 2 = draw, 3 = aborted), `u8 p1_wins`, `u8 p2_wins` |
| `0x05 FEED_RESET` | `u8 0x05`, `u32 match_id` (server → spectators only: the match log was lost, for example after a host reconnect with a gap; the spectator shows "feed interrupted" and waits for the next MATCH_START) |

Input values follow section 2: only bits 0–8 may be set, and the direction must be 1–9. The server rejects frames that break this (`error` with `bad_message` to the host, and the frame is dropped).

### 8.2 Rules the server enforces
- Only the room's current host may publish, and only while the room has a guest.
- A new `MATCH_START` begins a new match log and ends the previous one (a rematch starts a new match). `match_id` must differ from the previous match in the room.
- `INPUTS.first_tick` must equal the number of ticks received so far for that match: contiguous from tick 0, no gaps or overlaps. On a violation, the server ends the feed with `FEED_RESET` and replies `error` with `bad_message`. The host will start a new feed at the next match.
- `CHECKSUM.tick` must be a multiple of 60 and not ahead of the ticks received.
- A match log is capped at **2 hours** of ticks (432,000); after that the feed stops.
- The host sends `INPUTS` batches about every 100 ms (6 ticks or more each). The server must not assume a batch size.

### 8.3 Delivery to spectators
- On `spectate`, the server sends `spectate_started`. If a match is live, it then sends `MATCH_START` and, **as fast as the connection allows**, every stored `INPUTS` frame up to `now - delay` (re-batched into frames of up to 600 ticks). After that it streams live, delayed by `delay_ms`. The spectator's game fast-forwards through the backlog: simulating costs about 0.25 ms per tick, so a three-minute match catches up in about 3 s.
- Delay is measured from the server's receive time of each frame. Use a per-room queue and timers, never `sleep` per spectator.
- A slow spectator, whose send buffer is over 1 MiB or more than 30 s behind, is disconnected with `spectate_ended` reason `too_slow`. It must never slow the host or other spectators.
- When the match ends, spectators get `MATCH_END` after the delay and stay in the room for the next match. They can see `room_state` (results, rematch) in the meantime.

### 8.4 Integrity (milestone 5)
In an internet room **both players** publish the same feed, so verification is always active. The server compares the host's and guest's inputs and checksums per tick. On disagreement it keeps forwarding the host's feed but marks the room `feed_verified: false` in `room_state` and increments a metric. This detects a tampered or desynced host feed without trusting either side.

The host sends `MATCH_END` once the result is confirmed (no rollback can change it). A player who leaves early sends `MATCH_END` with `result` 3 (aborted).

---

## 9. HTTP endpoints

| Method and path | Auth | Response |
|---|---|---|
| `GET /healthz` | none | `200 ok` when listeners are up; `503` while shutting down. |
| `GET /metrics` | none, or a bearer token if configured | Prometheus text: connections, rooms by status, joins (accepted, declined, timed out), relay packets and bytes (forwarded and dropped), spectators, feed frames, rate-limit drops, matches started and finished, path direct vs relay. |
| `GET /v1/rooms` | none | Public room list (same `Room` objects, without codes), for a website. Cached for 2 s. |
| `GET /v1/replays?limit=&cursor=` | none | Milestone 5: recent finished matches `{id, stage, fighters, names, result, duration_ticks, finished_at, game_version, content_hash}`. |
| `GET /v1/replays/{id}` | none | Milestone 5: the match as a binary file: `MATCH_START`, all `INPUTS` (re-batched), all `CHECKSUM`, `MATCH_END` frames, concatenated, each prefixed with `u32 length`. Content type `application/vnd.3dfighter.replay`. |
| `POST /admin/notice` | bearer | `{message, severity}` → `server_notice` to all clients. |
| `POST /admin/rooms/{id}/close` | bearer | Closes a room (`room_closed` reason `admin`). |
| `POST /admin/ban` | bearer | `{client_id_hash?, ip?, minutes}`: rejects matching `hello`s with `not_allowed`. |
| `GET /admin/rooms` | bearer | Full room list including codes, connection paths and spectator counts. |

Admin endpoints are disabled unless `admin_token` is configured. Compare the token in constant time.

---

## 10. Security, privacy and limits

### 10.1 Principles
- **Never trust the client.** Validate every field: type, length, range, UTF-8. Unknown JSON fields are ignored; missing required fields cause `bad_message`.
- **No amplification.** No UDP reply is larger than the datagram that caused it, and unauthenticated UDP gets no reply except `BOUND` (7 bytes) to a valid BIND (19+ bytes) and `PONG` (equal size).
- **End-to-end encryption stays intact.** The relay forwards opaque game datagrams. Never try to decrypt, inspect or modify them.
- **Tokens:** session tokens are 128-bit and relay keys 64-bit, from a CSPRNG. Compare them in constant time. Resume tokens are 128-bit and single-use.
- **Privacy:** no IP addresses in room objects, logs (by default) or responses, except `peer_endpoints` to the matched peer, which relay-only mode prevents. `client_id` is never shown to other clients; only an 8-hex-character hash is.

### 10.2 Limits (configurable; defaults)

| Limit | Default |
|---|---|
| WebSocket connections per IP | 8 |
| Total WebSocket connections | 10,000 |
| JSON messages per connection | 20 per second (burst 40) |
| `list_rooms` | 2 per second |
| `create_room` / `join_room` | 6 per minute |
| Rooms per client | 1 (host or member) |
| Live rooms | 5,000 |
| Spectators per room | `max_spectators`, at most 200 |
| Feed publish rate | 30 binary frames per second per host |
| UDP datagrams per source IP (unauthenticated) | 20 per second |
| Relay per player | 200 datagrams per second, 64 KiB/s |
| Idle room (host only, no activity) | closed after 30 minutes |
| Pending join request | 30 s |

`welcome.limits` tells the client the values it needs: `max_room_name`, `max_spectators`, `reaction_interval_ms`.

### 10.3 Abuse handling
- Repeated malformed traffic or rate-limit violations: temporary ban of that `client_id` and IP for 10 minutes (exponential up to 24 h). Count it in metrics.
- Host-side controls: decline joins, `kick`, room passwords, unlisted rooms.
- The same client declined by a host cannot re-request that room for 30 s (mirrors the game's own rule).

### 10.4 Names and text
Apply the same cleaning as the game to every name and room name: remove characters below U+0020, U+007F–U+009F, U+200B–U+200F, U+2028–U+202E, U+2060–U+206F and U+FEFF; trim; limit names to 24 characters and room names to 32 (Unicode scalar values). Empty after cleaning → `"Player"` for names, `"<name>'s room"` for room names. An optional, configurable blocklist of words rejects a name with `name_invalid`. There is no free text anywhere else.

---

## 11. Configuration and operations

`config/server.example.toml`:
```toml
[server]
region = "asia"
http_bind = "0.0.0.0:8080"     # WebSocket + HTTP; put TLS in front (Caddy/nginx) or set [tls]
udp_bind = "0.0.0.0:7780"
public_udp_host = "fighter-asia.example.com"   # what clients are told in welcome.udp
log_format = "json"            # or "pretty"
log_ips = false

[tls]                          # optional: terminate TLS in-process
cert = "/etc/fighter/cert.pem"
key = "/etc/fighter/key.pem"

[game]
min_game_version = "0.4.1"     # older clients are refused
latest_game_version = "0.4.1"  # newer releases trigger an update prompt
update_url = "https://github.com/clintcan/3d-fighter/releases/latest"
motd = "Welcome to 3D Fighter online!"

[limits]
max_connections = 10000
max_connections_per_ip = 8
max_rooms = 5000
default_spectator_delay_ms = 3000

[admin]
token_env = "FIGHTER_ADMIN_TOKEN"   # admin API enabled only if this variable is set

[storage]                      # milestone 5
replays = true
database = "/var/lib/fighter/fighter.db"
replay_retention_days = 7
```
- Every setting can be overridden by an environment variable `FIGHTER__SECTION__KEY` (for example `FIGHTER__SERVER__REGION=eu`).
- **Graceful shutdown** on SIGTERM: send `server_notice`, stop accepting rooms, wait up to 60 s for matches to finish (configurable), then close.
- **Docker:** a multi-stage `Dockerfile` producing a small image that runs as a non-root user, exposing 8080/tcp and 7780/udp.
- **Resources:** must run 1,000 concurrent matches (2,000 players with relay) plus 5,000 spectators on 2 vCPU and 2 GB RAM. Document the measured numbers in `docs/LOAD-TEST.md`.

---

## 12. Features and milestones

Each milestone is done when its acceptance tests pass in CI and the demo in section 13.3 works.

### M1: Lobby and rooms (must have)
- `hello`/`welcome` with version gating, `ping`, `list_rooms` (filters, pagination), `create_room` (public, unlisted, password), room codes, `join_room` by id or code, `join_request` / `answer_join` / `join_declined` / `cancel_join`, request timeout, decline cooldown, `kick`, `leave_room`, `room_update` → `room_state`, room status computation, host-leave closes the room, idle room expiry.
- `/healthz`, `/metrics`, `/v1/rooms`, config file + env overrides, structured logging, Docker image.
- **Acceptance:**
  - two simulated clients can create, list, find by code, request, accept and decline;
  - mismatched `game_version` or `content_hash` cannot join and gets `version_mismatch`;
  - an unlisted room never appears in `list_rooms`;
  - a wrong password is refused;
  - every error code in section 6.5 that applies to M1 has a test.

### M2: Connection brokering (must have)
- UDP `BIND`/`BOUND`, keepalive and expiry, `peer_endpoints` with `punch_at`, LAN detection (same public IP → include local candidates), relay with source-address check and budgets, relay-only mode, `PING`/`PONG`, `connection_report`.
- **Acceptance:**
  - the client simulator exchanges 10,000 datagrams through the relay with none altered;
  - a relay datagram from an unbound address or with a wrong key is dropped;
  - BOUND and PONG are never larger than their requests (property test);
  - in relay-only mode `peer_endpoints.candidates` is empty;
  - a re-BIND from a new port moves the endpoint.

### M3: Spectators (must have)
- Feed publishing with validation, match log storage, delayed fan-out, late-join catch-up from tick 0, `CHECKSUM` pass-through, `MATCH_END`, `FEED_RESET`, slow-spectator disconnect, spectator counts in `room_state`, `react` / `reaction`.
- **Acceptance:**
  - a spectator joining mid-match receives exactly the same sequence of (tick, p1, p2) inputs as one who joined at the start (byte-identical after re-batching to per-tick form);
  - no spectator receives a frame earlier than its server receive time plus `delay_ms`, within 50 ms;
  - a feed gap triggers `FEED_RESET`;
  - a stalled spectator is dropped without delaying the others (test with a spectator that never reads).

### M4: Hardening (must have)
- All limits in section 10.2, temporary bans, name cleaning and blocklist, constant-time token comparisons, graceful shutdown, reconnection with `resume_token` (section 6.6), fuzz targets for every parser (JSON messages, binary frames, UDP datagrams), load test tool and results.
- **Acceptance:**
  - fuzzing each parser for 10 minutes finds no panic;
  - the load test sustains the targets in section 11 with relay p99 under 1 ms server-side;
  - a reconnect within 30 s keeps the room and role;
  - after 30 s the guest is reported as `disconnected`.

### M5: Extras (should have; pick in this order)
1. **Quick match:** `queue_join` pairs two compatible clients in the same region (first in queue hosts), creates a room automatically and sends `queue_matched` + `match_session`. Fairness: first come, first served; no rating needed.
2. **Replays:** store finished match logs (SQLite), the `/v1/replays` endpoints, retention cleanup.
3. **Feed verification:** both players publish, server compares (section 8.4).
4. **Admin API:** notices, close room, bans, full room list.
5. **Room stats:** matches played per room, rematch count, spectator peak, shown in `room_state`.
6. **Region discovery:** `GET /v1/regions` (configured list of `{region, ws_url, udp_host, udp_port}`) so the game can ping every region and pick the closest.

### Ideas for later (not in scope; document them if you think of more)
Accounts and ratings, leaderboards, tournaments and brackets, a spectator web viewer (needs the game compiled to WebAssembly), TURN/ICE compatibility, IPv6, cross-region relays, commentary audio.

---

## 13. Testing

### 13.1 Unit and property tests (crate `protocol`)
- Encode/decode every JSON message and binary frame; round-trip property tests with `proptest`.
- Validation of every field (lengths, ranges, input bit layout, UTF-8, name cleaning cases, including the bidi override U+202E).
- **Protocol vectors:** a test that writes the hex encoding of one example of every binary frame and UDP datagram into `docs/PROTOCOL-VECTORS.md`. The game team uses these to check its encoder. Example to match exactly:
  - `PING` with nonce `0x01020304` and time `1000`: `f4 04 03 02 01 e8 03 00 00`
  - `INPUTS` for match `7`, first tick `0`, two ticks where P1 holds right + LP (`0x0016`) and P2 is neutral (`0x0005`): `02 07 00 00 00 00 00 00 00 02 00 16 00 05 00 16 00 05 00`

### 13.2 Integration tests (crate `server`)
Start the server on random ports inside the test, connect simulated clients (WebSocket + UDP on 127.0.0.1), and drive whole flows: host/join/accept/decline/cancel/kick/leave, version mismatch, passwords, codes, bind/punch/relay, relay-only, feed publishing, late-join spectating, delays, resets, slow spectators, reconnection, limits, shutdown. Use a controllable clock (inject a `Clock` trait) for timeouts and delays instead of real sleeping, so tests run in milliseconds.

### 13.3 Client simulator and demo (crate `client-sim`)
A CLI that behaves like the game on the wire, for testing without Godot:
- `client-sim host --name Lino --room "Lino's room" [--spectators] [--password x]`: creates a room, accepts the first join, binds UDP, then runs a fake match. It sends a random-but-plausible input per tick (valid directions and buttons) through the relay to the guest, and publishes the spectator feed: MATCH_START, INPUTS every 100 ms, CHECKSUM every 60 ticks with a fake value, MATCH_END after N seconds.
- `client-sim join --name Lufi --code KX7Q2M` / `--first-open`: joins, binds, echoes the host's relayed datagrams back, and verifies that every byte arrived intact.
- `client-sim spectate --code KX7Q2M --late 10s`: spectates, re-assembles the per-tick input log, and prints a SHA-256 of it at the end. It must equal the hash the host prints, whether the spectator joined at once or late.
- `client-sim load --matches 1000 --spectators 5000 --duration 120s`: the load test.

**The demo for each milestone** runs `host`, `join` and two `spectate` processes (one late) against a local server and shows: room listed → join accepted → relayed traffic verified → both spectators print the same input-log hash as the host.

### 13.4 CI
GitHub Actions: `fmt --check`, `clippy -D warnings`, `test`, `cargo audit`, a 60-second fuzz run per target, and the M1–M3 demo script. Pin actions to commit SHAs and give the workflow token read-only permissions.

---

## 14. What the game client will do (for reference; implemented in the 3d-fighter repo)
So you know what to expect on the wire:
1. **Online menu:** a "Play online" section with a region picker (pings each region over UDP `PING`), a room browser (`list_rooms`, refreshing every 3 s while open), Create Room, Join by Code, and Spectate.
2. **Hosting:** `create_room`, then the existing accept/decline prompt is driven by `join_request`. On accept, the game's existing peer-to-peer handshake runs over the punched or relayed path. Because the server already gated the join, the game auto-accepts at the peer level for that session.
3. **Playing:**
   - The game's existing rollback netcode runs unchanged.
   - In relay mode, each outgoing game datagram is wrapped in `RELAY`, and each `RELAYED` payload is unwrapped and fed to the existing receive path. The relay session counts as the peer's address.
   - The host publishes the spectator feed from its confirmed inputs, which are final at that point (rollback can no longer change them).
   - It sends `room_update` on phase changes, rounds and results.
4. **Spectating:**
   - The game loads the fight with the stage and fighters from `MATCH_START` and the seed as `match_id`.
   - It feeds both players' inputs into the simulation tick by tick, fast-forwarding through the backlog, then plays in real time.
   - It shows "LIVE (3 s delay)", the spectator count, reactions and the round state.
   - It compares `CHECKSUM` frames with its own state hashes to detect divergence.
5. **Security:** the game keeps its own end-to-end encryption and security code. The server is never trusted with game traffic.

---

## 15. Glossary
- **Host**: the player who created the room; P1 in the match.
- **Guest**: the player who joined; P2.
- **Tick**: one 1/60 s simulation step.
- **Confirmed inputs**: inputs of both players for a tick, known for certain, so rollback can no longer change them. Only these are published to spectators.
- **Rollback netcode**: the technique the game uses: predict the remote player's input, simulate ahead, and re-simulate when the real input arrives.
- **Hole punching**: both players send UDP to each other's public address at about the same time, so their routers let the replies through.
- **Relay**: the server forwards datagrams between the two players when hole punching fails.
- **Spectator delay**: how far behind real time spectators watch (default 3 s), so they can't feed live information to a player.
