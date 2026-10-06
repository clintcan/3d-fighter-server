//! Shared application state.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use fighter_protocol::clock::Clock;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

use crate::config::Config;
use crate::lobby::{Ctx, Lobby};
use crate::metrics::Metrics;
use crate::moderation::Bans;
use crate::proxy::{parse_trusted, IpAllow};
use crate::relay::Bindings;
use crate::replays::{ReplayJob, ReplayStore};

#[derive(Clone)]
pub struct AppState {
    pub lobby: Arc<Mutex<Lobby>>,
    pub config: Arc<Config>,
    pub metrics: Arc<Metrics>,
    pub clock: Arc<dyn Clock>,
    pub blocklist: Arc<Vec<String>>,
    pub admin_token: Option<String>,
    pub trusted_proxies: Arc<Vec<IpAllow>>,
    pub replays: Arc<Mutex<ReplayStore>>,
    /// UDP rendezvous/relay state, behind its own lock so the relay path never
    /// contends with the lobby (issue #11).
    pub bindings: Arc<Mutex<Bindings>>,
    /// Bounds raw TCP connections before any request is read (issue #13).
    pub http_semaphore: Arc<Semaphore>,
    pub shutting_down: Arc<AtomicBool>,
    pub rooms_cache: Arc<Mutex<Option<(u64, String)>>>,
    conn_total: Arc<AtomicUsize>,
    conn_per_ip: Arc<Mutex<HashMap<IpAddr, usize>>>,
    replay_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<ReplayJob>>>>,
}

impl AppState {
    pub fn new(config: Config, clock: Arc<dyn Clock>, started_ms: u64) -> Self {
        let region = config.server.region.clone();
        let blocklist = Arc::new(config.moderation.blocklist.clone());
        let bans = Bans::new(config.limits.ban_base_ms, config.limits.ban_max_ms);
        let replays = Arc::new(Mutex::new(ReplayStore::new(
            PathBuf::from(&config.storage.replay_dir),
            config.storage.replays,
            config.limits.max_replays,
            config.limits.max_replay_bytes,
        )));
        let (replay_tx, replay_rx) = mpsc::unbounded_channel::<ReplayJob>();
        let trusted_proxies = Arc::new(parse_trusted(&config.server.trusted_proxies));
        let bindings = Arc::new(Mutex::new(Bindings::default()));
        let http_semaphore = Arc::new(Semaphore::new(config.limits.max_http_connections));
        let admin_token = config
            .admin
            .token
            .clone()
            .or_else(|| std::env::var(&config.admin.token_env).ok())
            .filter(|t| !t.is_empty());
        Self {
            lobby: Arc::new(Mutex::new(Lobby::new(
                started_ms,
                region,
                bans,
                replays.clone(),
                replay_tx,
                bindings.clone(),
            ))),
            config: Arc::new(config),
            metrics: Arc::new(Metrics::new()),
            clock,
            blocklist,
            admin_token,
            trusted_proxies,
            replays,
            bindings,
            http_semaphore,
            shutting_down: Arc::new(AtomicBool::new(false)),
            rooms_cache: Arc::new(Mutex::new(None)),
            conn_total: Arc::new(AtomicUsize::new(0)),
            conn_per_ip: Arc::new(Mutex::new(HashMap::new())),
            replay_rx: Arc::new(Mutex::new(Some(replay_rx))),
        }
    }

    /// Take the replay writer receiver once, so `serve` can spawn the writer.
    pub fn take_replay_receiver(&self) -> Option<mpsc::UnboundedReceiver<ReplayJob>> {
        self.replay_rx.lock().expect("replay rx").take()
    }

    /// Reserve a raw TCP connection slot (issue #13). The permit is held for the
    /// life of the connection, so incomplete requests count too.
    pub fn try_acquire_http(&self) -> Option<OwnedSemaphorePermit> {
        self.http_semaphore.clone().try_acquire_owned().ok()
    }

    /// Borrowed handler context.
    pub fn ctx(&self) -> Ctx<'_> {
        Ctx {
            config: &self.config,
            metrics: &self.metrics,
            clock: self.clock.as_ref(),
            blocklist: self.blocklist.as_slice(),
        }
    }

    /// Reserve a connection slot, or return false if a limit is reached.
    /// `lingering_total` / `lingering_ip` are disconnected sessions kept for
    /// reconnection, which still count against the limits (issue #12).
    pub fn try_acquire_conn(
        &self,
        ip: IpAddr,
        lingering_total: usize,
        lingering_ip: usize,
    ) -> bool {
        if self.shutting_down.load(Ordering::SeqCst) {
            return false;
        }
        if self.conn_total.load(Ordering::SeqCst) + lingering_total
            >= self.config.limits.max_connections
        {
            return false;
        }
        let mut per_ip = self.conn_per_ip.lock().expect("conn map");
        let entry = per_ip.entry(ip).or_insert(0);
        if *entry + lingering_ip >= self.config.limits.max_connections_per_ip {
            return false;
        }
        *entry += 1;
        drop(per_ip);
        self.conn_total.fetch_add(1, Ordering::SeqCst);
        self.metrics
            .connections
            .set(self.conn_total.load(Ordering::SeqCst) as i64);
        true
    }

    pub fn release_conn(&self, ip: IpAddr) {
        let mut per_ip = self.conn_per_ip.lock().expect("conn map");
        if let Some(entry) = per_ip.get_mut(&ip) {
            *entry = entry.saturating_sub(1);
            if *entry == 0 {
                per_ip.remove(&ip);
            }
        }
        drop(per_ip);
        let prev = self.conn_total.fetch_sub(1, Ordering::SeqCst);
        self.metrics.connections.set(prev.saturating_sub(1) as i64);
    }
}
