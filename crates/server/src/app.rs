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
    admin_failures: Arc<Mutex<HashMap<IpAddr, AdminFail>>>,
}

/// Failed admin-auth attempts for one address (issue #27).
#[derive(Debug, Clone, Copy)]
struct AdminFail {
    count: u32,
    window_start: u64,
    locked_until: u64,
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
            admin_failures: Arc::new(Mutex::new(HashMap::new())),
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

    /// Whether admin auth from `ip` is currently locked out (issue #27).
    pub fn admin_locked(&self, ip: IpAddr, now: u64) -> bool {
        self.admin_failures
            .lock()
            .expect("admin failures")
            .get(&ip)
            .is_some_and(|f| f.locked_until > now)
    }

    /// Clear failures after a successful admin auth.
    pub fn admin_success(&self, ip: IpAddr) {
        self.admin_failures
            .lock()
            .expect("admin failures")
            .remove(&ip);
    }

    /// Record a failed admin auth. Returns true if this attempt locks the
    /// address out.
    pub fn note_admin_failure(&self, ip: IpAddr, now: u64) -> bool {
        let max = self.config.limits.admin_max_failures.max(1);
        let lock = self.config.limits.admin_lockout_ms;
        let mut map = self.admin_failures.lock().expect("admin failures");
        let entry = map.entry(ip).or_insert(AdminFail {
            count: 0,
            window_start: now,
            locked_until: 0,
        });
        if now.saturating_sub(entry.window_start) > 60_000 {
            entry.count = 0;
            entry.window_start = now;
        }
        entry.count += 1;
        if entry.count >= max {
            entry.locked_until = now.saturating_add(lock);
            entry.count = 0;
            true
        } else {
            false
        }
    }

    /// Drop expired/old admin-auth entries so the map stays bounded (issue #28).
    pub fn sweep_admin_failures(&self, now: u64) {
        let cap = self.config.limits.max_rate_limit_sources;
        let mut map = self.admin_failures.lock().expect("admin failures");
        map.retain(|_, f| f.locked_until > now || now.saturating_sub(f.window_start) <= 60_000);
        while map.len() > cap {
            let Some(oldest) = map
                .iter()
                .min_by_key(|(_, f)| f.window_start)
                .map(|(k, _)| *k)
            else {
                break;
            };
            map.remove(&oldest);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use fighter_protocol::clock::TestClock;

    #[test]
    fn admin_failures_are_swept() {
        let mut config = Config::default();
        config.admin.token = Some("t".into());
        let clock = Arc::new(TestClock::new(1_000_000));
        let state = AppState::new(config, clock, 0);
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        state.note_admin_failure(ip, 1_000_000);
        assert!(state.admin_failures.lock().expect("map").contains_key(&ip));
        state.sweep_admin_failures(1_061_000);
        assert!(!state.admin_failures.lock().expect("map").contains_key(&ip));
    }
}
