//! Server bootstrap shared by `main` and the integration tests.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use axum::extract::Request;
use fighter_protocol::clock::Clock;
use fighter_protocol::json::Severity;
use hyper::body::Incoming;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tower::{Service, ServiceExt};

use crate::app::AppState;
use crate::config::Config;
use crate::replays::{ReplayJob, ReplayStore};

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
    let udp_socket = crate::udp::bind_socket(&state.config)?;
    let udp_addr = udp_socket.local_addr()?;
    let router = crate::http::router(state.clone());

    let udp_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = crate::udp::serve_socket(udp_socket, udp_state).await {
            tracing::error!(error = %e, "UDP listener stopped");
        }
    });

    // Replay writes run on a background task, off the lobby lock.
    if let Some(rx) = state.take_replay_receiver() {
        tokio::spawn(replay_writer(state.replays.clone(), rx));
    }

    let task_state = state.clone();
    let handle = tokio::spawn(async move {
        let janitor = tokio::spawn(janitor(task_state.clone()));
        let spectate = tokio::spawn(spectate_tick(task_state.clone()));

        // Serve with hyper's HTTP/1 builder directly (issue #13/#14). The
        // hyper-util auto builder always does version detection in
        // `serve_connection_with_upgrades` and would still serve cleartext
        // HTTP/2, where the header-read timeout does not apply. HTTP/1 is all we
        // need: the WebSocket upgrade is HTTP/1.1 and the JSON endpoints are tiny.
        let mut make_service = router.into_make_service_with_connect_info::<SocketAddr>();
        let mut http1 = hyper::server::conn::http1::Builder::new();
        http1
            .timer(TokioTimer::new())
            .header_read_timeout(Duration::from_millis(
                task_state.config.limits.http_header_timeout_ms,
            ));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let shutdown = shutdown(task_state.clone());
        tokio::pin!(shutdown);

        // Back off after accept errors (for example EMFILE) instead of spinning.
        let mut accept_backoff = Duration::ZERO;
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, remote) = match accepted {
                        Ok(v) => {
                            accept_backoff = Duration::ZERO;
                            v
                        }
                        Err(e) => {
                            if accept_backoff.is_zero() {
                                tracing::warn!(error = %e, "accept error; backing off");
                            } else {
                                tracing::debug!(error = %e, "accept error");
                            }
                            accept_backoff = if accept_backoff.is_zero() {
                                Duration::from_millis(100)
                            } else {
                                (accept_backoff * 2).min(Duration::from_secs(1))
                            };
                            tokio::time::sleep(accept_backoff).await;
                            continue;
                        }
                    };
                    // Bound raw connections before any request is read.
                    let permit = match task_state.try_acquire_http() {
                        Some(p) => p,
                        None => {
                            task_state
                                .metrics
                                .connections_rejected
                                .with_label_values(&["raw"])
                                .inc();
                            drop(stream);
                            continue;
                        }
                    };
                    let tower_service = match make_service.call(remote).await {
                        Ok(s) => s,
                        Err(infallible) => match infallible {},
                    };
                    let http1 = http1.clone();
                    active.fetch_add(1, Ordering::SeqCst);
                    let active = active.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let io = TokioIo::new(stream);
                        let hyper_service = hyper::service::service_fn(
                            move |request: Request<Incoming>| {
                                tower_service.clone().oneshot(request)
                            },
                        );
                        let conn = http1.serve_connection(io, hyper_service).with_upgrades();
                        if let Err(e) = conn.await {
                            tracing::debug!(error = %e, "connection error");
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                    });
                }
                _ = &mut shutdown => break,
            }
        }
        drop(listener);
        // Let in-flight connections finish, bounded by the shutdown grace.
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(task_state.config.limits.shutdown_grace_ms);
        while active.load(Ordering::SeqCst) > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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

/// Write queued replays on a blocking thread, off the lobby lock.
async fn replay_writer(
    replays: Arc<Mutex<ReplayStore>>,
    mut rx: mpsc::UnboundedReceiver<ReplayJob>,
) {
    while let Some((meta, body)) = rx.recv().await {
        let store = replays.clone();
        let _ = tokio::task::spawn_blocking(move || {
            store.lock().expect("replays").save(meta, &body);
        })
        .await;
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
