//! UDP listener: rendezvous, relay and ping (section 7).
//!
//! One socket receives every datagram. Relay is handled on the raw buffer with no
//! per-packet allocation: a RELAY is `[0xF2][key 8][payload]` and a RELAYED is
//! `[0xF3][payload]`, so forwarding rewrites `buf[8]` to `0xF3` and sends
//! `&buf[8..n]` (issue #17). After each wake-up the socket is drained with
//! `try_recv_from` until `WouldBlock`, and replies use `try_send_to` with an
//! awaited fallback, so the epoll wake-up and task scheduling are amortised over
//! every datagram queued at that moment (issue #22). Relay and ping never touch
//! the lobby lock (issue #11); only BIND takes it, briefly, to notify peers. No
//! reply is ever larger than the datagram that caused it, so the server cannot be
//! used for amplification.
//!
//! Batching the syscalls themselves (`recvmmsg`/`sendmmsg`) would need `unsafe`
//! or a wrapper crate, and this crate is `#![forbid(unsafe_code)]`; the safe
//! drain below captures most of the benefit. Multi-core sharding with
//! `SO_REUSEPORT` is a future option (issue #22, option 3).

use std::net::SocketAddr;

use anyhow::{Context, Result};
use fighter_protocol::udp::{UdpDatagram, TYPE_RELAY, TYPE_RELAYED};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

use crate::app::AppState;
use crate::config::Config;
use crate::relay::{BindOutcome, RelayOutcome};

const MAX_DATAGRAM: usize = 2048;

/// Bind the UDP socket with explicit receive/send buffers (issue #18). Returns
/// the tokio socket and logs the buffer sizes the kernel actually granted.
pub fn bind_socket(config: &Config) -> Result<UdpSocket> {
    let addr: SocketAddr = config
        .server
        .udp_bind
        .parse()
        .with_context(|| format!("invalid udp_bind {}", config.server.udp_bind))?;
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    let _ = socket.set_recv_buffer_size(config.server.udp_recv_buffer);
    let _ = socket.set_send_buffer_size(config.server.udp_send_buffer);
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    tracing::info!(
        recv_buffer = socket.recv_buffer_size().unwrap_or(0),
        send_buffer = socket.send_buffer_size().unwrap_or(0),
        requested_recv = config.server.udp_recv_buffer,
        "UDP socket buffers"
    );
    let std_socket: std::net::UdpSocket = socket.into();
    Ok(UdpSocket::from_std(std_socket)?)
}

/// Run the receive loop on an already-bound socket.
pub async fn serve_socket(socket: UdpSocket, state: AppState) -> Result<()> {
    let local = socket.local_addr().ok();
    tracing::info!(?local, "UDP listener up");
    let mut buf = vec![0u8; MAX_DATAGRAM];
    let mut sample: u64 = 0;
    loop {
        // Wait for the first datagram (this is where we park on epoll).
        let (n, from) = match socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "udp recv error");
                continue;
            }
        };
        process(&socket, &state, &mut buf, n, from, &mut sample).await;
        // Drain everything already queued without re-arming epoll (issue #22).
        loop {
            match socket.try_recv_from(&mut buf) {
                Ok((n, from)) => process(&socket, &state, &mut buf, n, from, &mut sample).await,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    tracing::debug!(error = %e, "udp drain error");
                    break;
                }
            }
        }
    }
}

/// Handle one datagram from `buf[..n]`.
async fn process(
    socket: &UdpSocket,
    state: &AppState,
    buf: &mut [u8],
    n: usize,
    from: SocketAddr,
    sample: &mut u64,
) {
    if n == 0 {
        return;
    }
    let now = state.clock.now_ms();
    let limits = &state.config.limits;

    // Fast path: RELAY, forwarded with no allocation.
    if buf[0] == TYPE_RELAY {
        if n < 10 {
            return; // malformed: type + key + at least one payload byte
        }
        let key: [u8; 8] = match buf[1..9].try_into() {
            Ok(k) => k,
            Err(_) => return,
        };
        let payload_len = n - 9;
        let started = std::time::Instant::now();
        let outcome = state
            .bindings
            .lock()
            .expect("bindings")
            .relay(&key, from, payload_len, now);
        match outcome {
            RelayOutcome::Forward { to } => {
                buf[8] = TYPE_RELAYED;
                if send_bytes(socket, &buf[8..n], to).await {
                    state.metrics.relay_forwarded.inc();
                    state
                        .metrics
                        .relay_forwarded_bytes
                        .inc_by(payload_len as u64);
                    // Sample 1 in 64 for a meaningful, cheap histogram (#17).
                    *sample = sample.wrapping_add(1);
                    if *sample % 64 == 0 {
                        state
                            .metrics
                            .relay_latency
                            .observe(started.elapsed().as_secs_f64());
                    }
                }
            }
            RelayOutcome::RateLimited => state.metrics.relay_dropped.inc(),
            RelayOutcome::NoPeer | RelayOutcome::Unknown | RelayOutcome::WrongSource => {
                state.bindings.lock().expect("bindings").unauth_allowed(
                    from.ip(),
                    now,
                    limits.udp_unauth_per_second,
                );
            }
        }
        return;
    }

    // Everything else is rare; decode it.
    let datagram = match UdpDatagram::decode(&buf[..n]) {
        Ok(d) => d,
        Err(_) => return,
    };
    if let Some((to, bytes)) = handle(state, from, datagram, now) {
        if bytes.len() <= n {
            send_bytes(socket, &bytes, to).await;
        } else {
            tracing::warn!(request = n, reply = bytes.len(), "refused to amplify");
        }
    }
}

/// Send without parking when the socket is writable; await only on `WouldBlock`.
async fn send_bytes(socket: &UdpSocket, bytes: &[u8], to: SocketAddr) -> bool {
    match socket.try_send_to(bytes, to) {
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            socket.send_to(bytes, to).await.is_ok()
        }
        Err(e) => {
            tracing::debug!(error = %e, "udp send error");
            false
        }
    }
}

/// Handle a decoded non-RELAY datagram, returning an address and bytes to send.
fn handle(
    state: &AppState,
    from: SocketAddr,
    datagram: UdpDatagram,
    now: u64,
) -> Option<(SocketAddr, Vec<u8>)> {
    let limits = &state.config.limits;
    match datagram {
        UdpDatagram::Bind {
            session_token,
            role,
            candidates,
        } => {
            let outcome = {
                let mut bindings = state.bindings.lock().expect("bindings");
                if !bindings.unauth_allowed(from.ip(), now, limits.udp_unauth_per_second) {
                    return None;
                }
                bindings.bind(&session_token, role, from, candidates, now)
            };
            match outcome {
                BindOutcome::Bound { room_id, .. } => {
                    let SocketAddr::V4(observed) = from else {
                        return None; // v1 is IPv4 only
                    };
                    // Lock order is lobby -> bindings; the bindings lock above
                    // has already been released.
                    let ctx = state.ctx();
                    state
                        .lobby
                        .lock()
                        .expect("lobby lock")
                        .try_notify_peers(&room_id, now, &ctx);
                    Some((from, UdpDatagram::Bound { observed }.encode()))
                }
                BindOutcome::Invalid => None,
            }
        }
        UdpDatagram::Ping {
            nonce,
            client_time_ms,
        } => {
            let allowed = {
                let mut bindings = state.bindings.lock().expect("bindings");
                bindings.unauth_allowed(from.ip(), now, limits.udp_unauth_per_second)
                    && bindings.ping_allowed(from.ip(), now, limits.udp_ping_per_second)
            };
            if !allowed {
                return None;
            }
            Some((
                from,
                UdpDatagram::Pong {
                    nonce,
                    client_time_ms,
                }
                .encode(),
            ))
        }
        // RELAY is handled on the fast path; clients never send the rest.
        UdpDatagram::Relay { .. }
        | UdpDatagram::Bound { .. }
        | UdpDatagram::Relayed { .. }
        | UdpDatagram::Pong { .. } => None,
    }
}

/// Bind `addr` and run the receive loop.
pub async fn run(state: AppState) -> Result<()> {
    let socket = bind_socket(&state.config)?;
    serve_socket(socket, state).await
}
