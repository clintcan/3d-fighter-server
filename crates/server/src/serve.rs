//! Server bootstrap shared by `main` and the integration tests.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use fighter_protocol::clock::Clock;
use fighter_protocol::json::Severity;
use tokio::task::JoinHandle;

use crate::app::AppState;
use crate::config::Config;

/// A running server instance bound to a port.
pub struct Running {
    pub addr: SocketAddr,
    pub udp_addr: SocketAddr,
    pub state: AppState,
    pub handle: JoinHandle<()>,
}

impl Running {
    pub fn http_url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    pub fn ws_url(&self) -> String {
        format!("ws://{}/v1/ws", self.addr)
    }
}

/// Bind and start the HTTP/WebSocket listener, the UDP listener and the
/// janitor task.
pub async fn start(config: Config, clock: Arc<dyn Clock>) -> Result<Running> {
    let started_ms = clock.now_ms();
    let state = AppState::new(config, clock, started_ms);
    let listener = tokio::net::TcpListener::bind(&state.config.server.http_bind).await?;
    let addr = listener.local_addr()?;
    let udp_socket = tokio::net::UdpSocket::bind(&state.config.server.udp_bind).await?;
    let udp_addr = udp_socket.local_addr()?;
    let router = crate::http::router(state.clone());

    let udp_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = crate::udp::serve_socket(udp_socket, udp_state).await {
            tracing::error!(error = %e, "UDP listener stopped");
        }
    });

    let task_state = state.clone();
    let handle = tokio::spawn(async move {
        let janitor = tokio::spawn(janitor(task_state.clone()));
        let spectate = tokio::spawn(spectate_tick(task_state.clone()));
        let _ = axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown(task_state))
        .await;
        janitor.abort();
        spectate.abort();
    });

    Ok(Running {
        addr,
        udp_addr,
        state,
        handle,
    })
}

/// Run until a shutdown signal, as `main` does.
pub async fn run(config: Config, clock: Arc<dyn Clock>) -> Result<()> {
    let running = start(config, clock).await?;
    tracing::info!(addr = %running.addr, "fighter-server listening");
    let _ = running.handle.await;
    tracing::info!("fighter-server stopped");
    Ok(())
}

/// Sweep expiries and coalesced broadcasts every 100 ms.
async fn janitor(state: AppState) {
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let ctx = state.ctx();
        let mut lobby = state.lobby.lock().expect("lobby lock");
        lobby.sweep(&ctx);
    }
}

/// Fan out spectator frames every 20 ms, within the 50 ms delay tolerance.
async fn spectate_tick(state: AppState) {
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let ctx = state.ctx();
        let mut lobby = state.lobby.lock().expect("lobby lock");
        lobby.flush_spectators(&ctx);
    }
}

async fn shutdown(state: AppState) {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
    state.shutting_down.store(true, Ordering::SeqCst);

    // Tell everyone and stop accepting rooms, then wait for matches to finish.
    {
        let lobby = state.lobby.lock().expect("lobby lock");
        lobby.broadcast_notice("Server is shutting down", Severity::Warning);
    }
    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(state.config.limits.shutdown_grace_ms);
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let active = state.lobby.lock().expect("lobby lock").active_matches();
        if active == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    tracing::info!("graceful shutdown complete");
}
