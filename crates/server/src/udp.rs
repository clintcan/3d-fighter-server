//! UDP listener: rendezvous, relay and ping (section 7).
//!
//! One socket receives every datagram. It is decoded before any lock is taken,
//! so malformed and irrelevant datagrams cost nothing. Relay and ping are
//! handled entirely behind the `Bindings` lock and never touch the lobby lock
//! (issue #11); only BIND takes the lobby lock, briefly, to notify peers. No
//! reply is ever larger than the datagram that caused it, so the server cannot
//! be used for amplification.

use std::net::SocketAddr;

use anyhow::Result;
use fighter_protocol::udp::{UdpDatagram, TYPE_RELAYED};
use tokio::net::UdpSocket;

use crate::app::AppState;
use crate::relay::{BindOutcome, RelayOutcome};

const MAX_DATAGRAM: usize = 2048;

/// Run the receive loop on an already-bound socket.
pub async fn serve_socket(socket: UdpSocket, state: AppState) -> Result<()> {
    let local = socket.local_addr().ok();
    tracing::info!(?local, "UDP listener up");
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let (n, from) = match socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "udp recv error");
                continue;
            }
        };
        // Cheap check before any lock.
        let datagram = match UdpDatagram::decode(&buf[..n]) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let now = state.clock.now_ms();
        let started = std::time::Instant::now();
        if let Some((to, bytes)) = handle(&state, from, datagram, now) {
            // No amplification: BOUND <= BIND, PONG == PING, RELAYED < RELAY.
            if bytes.len() <= n {
                if let Err(e) = socket.send_to(&bytes, to).await {
                    tracing::debug!(error = %e, "udp send error");
                }
                if bytes.first() == Some(&TYPE_RELAYED) {
                    state
                        .metrics
                        .relay_latency
                        .observe(started.elapsed().as_secs_f64());
                }
            } else {
                tracing::warn!(request = n, reply = bytes.len(), "refused to amplify");
            }
        }
    }
}

/// Handle one decoded datagram, returning an address and bytes to send.
fn handle(
    state: &AppState,
    from: SocketAddr,
    datagram: UdpDatagram,
    now: u64,
) -> Option<(SocketAddr, Vec<u8>)> {
    let limits = &state.config.limits;
    match datagram {
        UdpDatagram::Relay { relay_key, payload } => {
            let payload_len = payload.len();
            let outcome =
                state
                    .bindings
                    .lock()
                    .expect("bindings")
                    .relay(&relay_key, from, payload_len, now);
            match outcome {
                RelayOutcome::Forward { to } => {
                    state
                        .metrics
                        .relay_packets
                        .with_label_values(&["forwarded"])
                        .inc();
                    state
                        .metrics
                        .relay_bytes
                        .with_label_values(&["forwarded"])
                        .inc_by(payload_len as u64);
                    Some((to, UdpDatagram::Relayed { payload }.encode()))
                }
                RelayOutcome::RateLimited => {
                    state
                        .metrics
                        .relay_packets
                        .with_label_values(&["dropped"])
                        .inc();
                    None
                }
                RelayOutcome::NoPeer | RelayOutcome::Unknown | RelayOutcome::WrongSource => {
                    // Unauthenticated or unbound: charge the source limit.
                    state.bindings.lock().expect("bindings").unauth_allowed(
                        from.ip(),
                        now,
                        limits.udp_unauth_per_second,
                    );
                    None
                }
            }
        }
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
        // Clients never send these; drop silently.
        UdpDatagram::Bound { .. } | UdpDatagram::Relayed { .. } | UdpDatagram::Pong { .. } => None,
    }
}

/// Bind `addr` and run the receive loop.
pub async fn run(state: AppState) -> Result<()> {
    let socket = UdpSocket::bind(&state.config.server.udp_bind).await?;
    serve_socket(socket, state).await
}
