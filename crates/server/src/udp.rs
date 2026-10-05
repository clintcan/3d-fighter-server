//! UDP listener: rendezvous, relay and ping (section 7).
//!
//! One socket receives every datagram, hands it to the lobby for validation and
//! endpoint bookkeeping, then performs the (single) send outside the lock. No
//! reply is ever larger than the datagram that caused it, so the server cannot
//! be used for amplification.

use anyhow::Result;
use tokio::net::UdpSocket;

use crate::app::AppState;

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
        let now = state.clock.now_ms();
        let started = std::time::Instant::now();
        let reply = {
            let ctx = state.ctx();
            let mut lobby = state.lobby.lock().expect("lobby lock");
            lobby.handle_udp(from, &buf[..n], &ctx, now)
        };
        if let Some((to, bytes)) = reply {
            // No amplification: BOUND <= BIND, PONG == PING, RELAYED < RELAY.
            if bytes.len() <= n {
                if let Err(e) = socket.send_to(&bytes, to).await {
                    tracing::debug!(error = %e, "udp send error");
                }
                if bytes.first() == Some(&fighter_protocol::udp::TYPE_RELAYED) {
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

/// Bind `addr` and run the receive loop.
pub async fn run(state: AppState) -> Result<()> {
    let socket = UdpSocket::bind(&state.config.server.udp_bind).await?;
    serve_socket(socket, state).await
}
