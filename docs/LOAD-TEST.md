# 3D Fighter server: load and stress test report

**Dates:** 2026-10-07 and 2026-10-08
**Server builds tested:** `2796357` (baseline), `55136eb` (fixes for #17–#21), `c59521a` (#22 drain), `c8a4364` (#23 bounded drain); `e85f9bf` reverts the drain and behaves like `55136eb`.
**Production today:** `55136eb`, with `net.core.rmem_max` 8 MB and `max_connections_per_ip = 32`.

This is the authoritative load and stress report for the server. It was produced
with a purpose-built Rust load generator (kept with the game project) that speaks
the real lobby and UDP protocol; the in-repo `client-sim load` command is a much
lighter smoke tool and is described at the end.

## Summary

- **CPU runs out first, not memory.** On the production droplet (1 vCPU, 512 MB) the limit is about **400 simultaneous direct matches** or about **110 relayed matches**, each with lobby browsers and spectators alongside (roughly 1,400 or 400 people online).
- **Caddy is now the main cost for WebSocket traffic.** Its TLS work takes about as much CPU as fighter-server for lobby and feed traffic. Relay traffic (UDP) doesn't go through it.
- **The #17–#21 fixes roughly doubled direct-match capacity** (about 250 → 400 matches live). They also halved server memory, cut it about 5× per idle connection, and removed kernel UDP drops. Dead connections now close in 60–90 s instead of 15+ minutes.
- **The relay's per-packet cost on the droplet didn't change.** It's about 30 µs of process CPU per forwarded datagram, so relayed capacity stays at about 110 matches. The zero-copy rewrite helped on a desktop core, but on the KVM guest the remaining cost appears to be the syscalls. The per-wake-up drain (#22/#23) gave no gain in either environment and was reverted.
- **Under overload the server sheds relay traffic and keeps the lobby working** (`55136eb` / `e85f9bf`). That's the right priority; the unbounded drain (`c59521a`) briefly inverted it.

## Method

### Load generator

The load generator is a purpose-built Rust tool that speaks the real protocol. It uses a copy of `crates/protocol` from `2796357`, and TLS through rustls for the live runs.

| Simulated role | Behaviour |
|---|---|
| **Match (2 players)** | `hello` → host `create_room` → guest `join_room` → `answer_join` → both receive `match_session`. **Relayed** matches then `BIND` over UDP and each player sends **70 RELAY datagrams/s** with an 88-byte payload, like the game's sealed INPUT packets, with a re-BIND every 15 s. **Direct** matches skip the relay, as a hole-punched match would. **Both players publish the spectator feed,** as the game does: MATCH_START, then INPUTS (6 ticks every 100 ms) and a CHECKSUM every second. |
| **Lobby browser** | Connected and idle in the lobby: `list_rooms` (limit 50) every 3 s and a JSON `ping` every 15 s, like the game's room browser. |
| **Spectator** | `spectate` on a running match, then reads the feed. Sends a JSON `ping` every 15 s, like the game. |

**What it measures:**
- **Relay:** one-way latency (send time stamped in the payload, same clock) and loss, inside the measurement window only.
- **Lobby:** `list_rooms` round trip.
- **Feed:** INPUTS frames published per second and frames delivered to spectators.
- **Errors:** setup and connection failures.

**How a step runs:**
1. Matches are set up at 25 per second locally and 2–4 per second live.
2. Browsers and spectators are added.
3. A 5 s warm-up.
4. A 20 s measurement window (30 s for the live A/B runs).

**What a spectator should receive:** about 11 frames/s each (10 INPUTS plus 1 CHECKSUM), so the expected delivery is 11 × spectators.

### Local environment

- **Server:** fighter-server (release build) in WSL Ubuntu 22.04, confined like the droplet with a `systemd-run` scope: `AllowedCPUs=0`, `CPUQuota=100%`, `MemoryMax=512M`, `MemorySwapMax=1G`, `LimitNOFILE=65536`. `net.core.rmem_max` was 8 MB for the `55136eb` and later runs.
- **Config:** production `[limits]` (`max_connections_per_ip = 32`, `max_connections = 10000`, `max_rooms = 5000`, replay storage on), JSON logs to a file.
- **Client addresses:** each simulated player connects from its **own loopback address** (127.x.y.z), so per-IP limits, including the 20/s unauthenticated-UDP limit, behave as with real players.
- **CPU and memory figures:** from the scope's cgroup (`cpu.stat` usage, `memory.current` peak). The load generator ran on the other 15 cores.
- **No Caddy or TLS** locally (plain `ws://`).

### Live environment

- **Server:** the production droplet (DigitalOcean sgp1, 1 vCPU, 512 MB plus 1 GB swap, Ubuntu 24.04) behind Caddy (TLS on 443), with **no real users online**.
- **Client:** one PC in the Philippines over a home connection: `wss://` through Caddy and UDP to 7780, all from one public IP. `max_connections_per_ip` was raised to 10,000 for the test and restored to 32 afterwards.
- **Server-side numbers:** a read-only SSH snapshot at the start and end of each window: `utime + stime` of fighter-server and caddy from `/proc/<pid>/stat`, RSS, `MemAvailable`, kernel UDP counters from `/proc/net/snmp`, and relay counters from `/metrics`.
- **Latency figures:** live relay latency is **PC → Singapore → PC** (about 65–70 ms of that is the network). Live `list_rooms` times include about 65 ms of network round trip.
- **Limits of the live tests:** relay steps stopped at 40 matches (5,600 datagrams/s) because of the client's home upload. Direct steps went up to 400 matches plus 400 browsers and 200 spectators (1,400 WebSocket connections).

## Local results (1 core, 512 MB)

"Mix" means one lobby browser per match and one spectator per two matches.

### Baseline `2796357`, host-only feed

| Scenario | CPU | Memory | Relay loss | Relay latency | `list_rooms` | Notes |
|---|---|---|---|---|---|---|
| 50 relayed + mix | 38% | 33 MB | 0% | p99 ≤ 1.5 ms | 2.8 ms | |
| 100 relayed + mix | 58% | 56 MB | 0% | p99 ≤ 2 ms | 3.1 ms | |
| 120 relayed + mix | 72% | 70 MB | 0.45% | p99 ≤ 10 ms, p99.9 ≤ 50 ms | 3.9 ms | loss well below 100% CPU |
| 140 relayed + mix | 76% | 77 MB | 0% | p99 ≤ 3 ms | 3.5 ms | |
| 160 relayed + mix | 87% | 87 MB | 0.79% | p99 ≤ 15 ms | 4.2 ms | |
| 180 relayed + mix | 99% | 98 MB | 7.0% | p99 ≤ 20 ms | 6.0 ms | |
| 200 relayed + mix | 100% | 108 MB | 8.9% | p99 ≤ 20 ms | 6.0 ms | |
| 400 relayed + mix | 100% | 216 MB | 88% | p99 ≤ 100 ms | 55 ms (max 358) | |
| 600 relayed + mix | 100% | 334 MB | 99% | p99 ≤ 500 ms | 2.2 s | 7 matches failed to bind |
| 800 relayed + mix | 100% | 388 MB | ~100% | > 1 s | 1.7 s | 67 matches and 377 spectators failed; 1,287 `list_rooms` timeouts |
| 150 relayed, nothing else | 77% | 47 MB | 0.43% | p99 ≤ 10 ms | — | about 37 µs CPU per datagram |
| 1,000 browsers, no matches | 12% | 137 MB | — | — | 0.6 ms | |
| 3,000 browsers, no matches | 30% | **405 MB** | — | — | 0.6 ms | about 135 KB per connection |
| 400 matches, 25% relayed + mix | 100% | 216 MB | 55% | p99 ≤ 100 ms | 62 ms | saturated by the non-relay work |

### Baseline `2796357`, both players publishing the feed

| Scenario | CPU | Memory | Notes |
|---|---|---|---|
| 100 direct, nothing else | 20% | 37 MB | |
| 200 direct, nothing else | 36% | 66 MB | |
| 400 direct, nothing else | 73% | 131 MB | about 0.18% per match, all of it feed ingest; grew with room count (O(rooms) lookup, #19) |
| 200 direct + 200 browsers | 49% | 99 MB | `list_rooms` 3.4 ms; about +0.065% per browser |
| 200 direct + 100 spectators | 42% | 80 MB | about +0.06% per spectator; delivery 967 of ~1,100 frames/s |
| 80 relayed + mix | 59% | 47 MB | relay loss 0.32%, p99 ≤ 7.5 ms |
| 100 relayed + mix | 73% | 58 MB | relay loss 0.80%, p99 ≤ 15 ms |
| 120 relayed + mix | 81% | 69 MB | relay loss 1.24%, p99 ≤ 15 ms |

### `55136eb`: fixes for #17–#21

| Scenario | CPU | Memory | Relay loss | Relay latency | `list_rooms` | Spectator delivery |
|---|---|---|---|---|---|---|
| 150 relayed, nothing else | **64%** | 21 MB | **0%** | p99 ≤ 1.5 ms | — | — |
| 100 relayed + mix | **53%** | 19 MB | **0%** | p99 ≤ 1.5 ms | 2.9 ms (max 4.5) | 479 / ~550 |
| 200 relayed + mix | 98% | 40 MB | **0%** | p99 ≤ 200 ms (mean 14 ms) | 5.2 ms (max 18) | 1,100 / ~1,100 |
| 300 relayed + mix | 100% | 61 MB | 51% | p99 ≤ 500 ms | 7.2 ms (max 27) | 1,434 / ~1,650 |
| 400 relayed + mix | 100% | 80 MB | 80% | p99 ≤ 1 s | 40 ms (max 186) | 1,945 / ~2,200 |
| 400 direct, nothing else | **35%** | 42 MB | — | — | — | — |
| 800 direct, nothing else | 63% | 86 MB | — | — | — | — |
| 3,000 browsers, no matches | 23% | **75 MB** | — | — | 0.5 ms | — |
| 400 matches, 25% relayed + mix | 100% | 80 MB | 13% | p99 ≤ 1 s | 26 ms | 2,201 / ~2,200 |
| 800 matches, 25% relayed + mix | 100% | 183 MB | ~100% | > 1 s | 3.4 s | collapse |

### The drain experiments (#22, #23), relay scenarios on all three builds

| Scenario | `55136eb` | `c59521a` (unbounded drain) | `c8a4364` (bounded drain) |
|---|---|---|---|
| 150 relayed, nothing else: CPU | 64% | 67% | 71% |
| 150 relayed: relay latency | mean 0.45 ms, p99 ≤ 1.5 ms | mean 0.85 ms, p99 ≤ 5 ms | mean 1.9 ms, p99 ≤ 7.5 ms |
| 100 relayed + mix: `list_rooms` max | 4.5 ms | 71 ms | 7.5 ms |
| 200 relayed + mix | **0% loss**, `list_rooms` 5 ms (max 18) | 0% loss, `list_rooms` 23 ms (max 215) | 7% loss, `list_rooms` 5 ms (max 15) |
| 250 relayed + mix | — | `list_rooms` **1.4 s** (max 4.3 s); spectators 283 / ~1,375 frames/s | 36% loss, `list_rooms` 5 ms; spectators 1,307 / ~1,375 |
| 300 relayed + mix (overloaded) | 51% loss, `list_rooms` 7 ms | 7% loss, but WebSocket handshakes time out, `list_rooms` 1.8 s, **spectators 0** | 58% loss, `list_rooms` 6 ms |

The unbounded drain never yielded on a 1-worker runtime, so WebSocket tasks starved under relay load (#23). Bounding it fixed the starvation, but neither variant reduced CPU.

## Live results: production droplet

All rows have zero relay loss. Relay latency includes the PC ↔ Singapore round trip, about 65–70 ms.

### Baseline `2796357`

| Scenario | fighter-server CPU | Caddy CPU | Memory available | Notes |
|---|---|---|---|---|
| 10 relayed | 10% | 4% | 277 MB | relay mean 68 ms |
| 20 relayed | 15% | 5% | 299 MB | |
| 40 relayed (5,400 datagrams/s) | 23% | 9% | 286 MB | **265 kernel `RcvbufErrors`** in 23 s (208 KB default buffer) |
| 50 direct + 50 browsers + 25 spectators | 11% | 12% | 257 MB | `list_rooms` 134 ms |
| 100 direct + mix | 20% | 20% | 199 MB | `list_rooms` 138 ms |
| 200 direct + mix | 41% | 32% | 100 MB | `list_rooms` 153 ms (max 706); RSS fighter 126 MB, Caddy 99 MB |
| 300 direct + mix | 52% | 40% | **78 MB** | `list_rooms` 238 ms (max 1 s); spectators **1,068 / ~1,650** frames/s; RSS fighter 165 MB |
| After the test | — | — | — | 877 orphaned WebSocket connections still open after 13 min, 621 after 15 min (half-open behind Caddy, #21) |

### `55136eb`

| Scenario | fighter-server CPU | Caddy CPU | Memory available | Notes |
|---|---|---|---|---|
| 10 relayed | 9% | 3% | 298 MB | |
| 20 relayed | 14% | 6% | 295 MB | |
| 40 relayed (5,600 datagrams/s) | 22% | 9% | 287 MB | **0 kernel UDP drops**; 8 MB buffer granted |
| 50 direct + mix | 9% | 13% | 270 MB | `list_rooms` 132 ms |
| 100 direct + mix | 15% | 20% | 231 MB | `list_rooms` 135 ms |
| 200 direct + mix | 27% | 31% | 165 MB | `list_rooms` 138 ms (max 251); RSS fighter 50 MB, Caddy 121 MB |
| 300 direct + mix | **42%** | 40% | 112 MB | `list_rooms` 154 ms (max 630); RSS fighter **83 MB** |
| 400 direct + mix (1,400 connections) | 45% | 44% | 88 MB | `list_rooms` 508 ms (max 3 s); at the limit |
| After the test | — | — | — | 919 orphaned connections → **0 within 60–90 s** (#21 fixed) |

### Relay A/B on the droplet: `55136eb` vs `c8a4364`

40 relayed matches (about 5,600 datagrams/s), two 30 s windows per build, back to back from the same client:

| Build | fighter-server CPU | Caddy CPU | Loss / kernel drops |
|---|---|---|---|
| `55136eb` | 21%, 20% | 9%, 9% | 0 / 0 |
| `c8a4364` | 21%, 20% | 9%, 9% | 0 / 0 |

## Costs per unit and capacity (`55136eb`)

| Unit | Local (1 desktop core) | Live (droplet, server + Caddy) |
|---|---|---|
| Relay, per forwarded datagram | about 24 µs process CPU | about 30 µs |
| Relayed match (relay + feed) | about 0.43% of the core | about 0.55% server + 0.2% Caddy ≈ **0.75%** |
| Direct match (feed only) | about 0.09% | about 0.1% server + Caddy's TLS share |
| Lobby browser (`list_rooms` every 3 s with many rooms) | about 0.065% | plus Caddy |
| Spectator | about 0.06% | plus Caddy |
| Idle WebSocket connection, memory | about 25 KB (was ~135 KB) | fighter RSS about 80 KB per connection at 400 matches + mix (including match state) |

**Capacity on the current droplet**, budgeting about 85–90% of the core for server plus Caddy:

| Mix | Simultaneous matches | People online (with browsers and spectators) |
|---|---|---|
| All direct | about **400** | about 1,400 |
| All relayed | about **110** | about 400 |
| A quarter relayed (a guess at the real mix) | about **240** | about 850 |

**What limits each case:**
- **Direct matches:** CPU, with Caddy's TLS taking about half; memory comes next (88 MB left at 400 matches).
- **Relayed matches:** the relay's syscall cost per datagram.

**Bandwidth:** each relayed match sends about 60 MB per hour out of the droplet, so the plan's transfer allowance covers roughly 8,000 relayed match-hours a month.

## Issue history

| Issue | Finding | Outcome | Verified |
|---|---|---|---|
| #17 | Relay about 35–40 µs CPU per datagram (copies, label lookups, per-packet histogram) | Zero-copy forward, dedicated counters, latency sampled 1/64 (`55136eb`) | Locally −17% CPU and no loss up to saturation; **no change on the droplet** (syscall-bound) |
| #18 | UDP socket on the 208 KB default buffer; kernel drops at 23% CPU | `socket2` buffers `udp_recv_buffer` / `udp_send_buffer` (4 MB / 1 MB), granted size logged (`55136eb`); droplet `rmem_max` 8 MB | 8 MB granted; **0 kernel drops** at every load |
| #19 | Feed ingest about 0.18% CPU per match, growing with room count | O(1) room lookup through the session (`55136eb`) | Feed cost about halved (400 direct: 73% → 35% locally); spectators fully delivered live at 300 matches |
| #20 | About 135 KB per idle WebSocket connection | 16 KB read/write buffers, 256 KB write cap (`55136eb`) | 3,000 browsers: 405 → 75 MB locally; server RSS halved live |
| #21 | Half-open sockets linger behind Caddy (15+ min) | Close after `ws_pong_timeout_ms` (60 s) without a client frame (`55136eb`) | 919 orphaned connections closed in 60–90 s live |
| #22 | Remaining relay cost: syscalls, readiness overhead | Per-wake-up drain (`c59521a`) | No CPU gain; starved the lobby under load (#23) |
| #23 | Unbounded drain never yields on one worker | Bounded drain (`c8a4364`), then **reverted** (`e85f9bf`), keeping the `lobby_stays_responsive_during_relay_burst` test | Bounded: lobby fine, still no gain (live A/B identical, locally slightly worse at saturation) |

## Recommendations

1. **Nothing urgent.** Current capacity is far beyond current use.
2. **If relayed play becomes the bottleneck:** a 2-vCPU host plus `SO_REUSEPORT` sharding is the practical path. Syscall batching (`recvmmsg`/`sendmmsg`) is the other lever, but needs `unsafe` or a wrapper crate such as `quinn-udp`. Profile on the droplet first (`perf top` under relay load).
3. **If direct or lobby traffic becomes the bottleneck:** Caddy's TLS is about half the CPU for WebSocket traffic. Options:
   - terminate TLS in-process (the server's `[tls]` option) to save the proxy hop;
   - a larger droplet;
   - the room list is the next server-side cost (`list_rooms` with 50 rooms every 3 s per browser); a cached or delta room list would help at scale.
4. **Keep `max_connections_per_ip` at 32** now that `trusted_proxies` makes per-IP limits see real client addresses. Raise it only if real players turn out to share carrier-grade NAT addresses.
5. **Load-test hygiene:** a single-IP live test needs the per-IP limit raised temporarily and restored. The tooling and droplet scripts used here are kept with the game project and can be shared.

## Limitations

- **Hardware differences:**
  - A desktop core is faster than the droplet's shared vCPU, and WSL's kernel differs from the droplet's KVM guest; the relay results show how much that matters.
  - Local runs had no TLS or Caddy.
- **Single client:**
  - The live client was one machine on one home connection: one source IP, and relay steps capped at 40 matches by upload bandwidth.
  - Real players come from many addresses, through different NATs.
- **Single runs:** most points were measured once over a 20 s window; repeated points agreed within about ±3–5% CPU. The live A/B used two 30 s windows per build.
- **Synthetic traffic:**
  - Real matches also have handshake bursts, rematches and room churn.
  - Payloads are synthetic but sized and paced like the game's.
  - The feed content isn't validated by spectators (only counted).

## In-repo smoke tool

`cargo run --release --bin client-sim -- load --matches N --spectators M --duration S`
exercises the lobby, feed fan-out and spectator paths against a local server; it
is a quick sanity check, not a capacity benchmark. It needs
`FIGHTER__LIMITS__MAX_CONNECTIONS_PER_IP` raised when many simulated clients share
`127.0.0.1`. The full load and stress numbers above come from the separate
generator described under **Method**.

## Reproducing in CI

The integration tests (`cargo test --all`) cover correctness. The load numbers
above require a machine with spare cores and are not run in CI.
