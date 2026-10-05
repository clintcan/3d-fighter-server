//! UDP rendezvous bindings, relay budgets and endpoint bookkeeping (section 7).
//!
//! Bindings live next to the lobby but are keyed by session token and relay key
//! so the datagram path never has to walk room state. A slot is registered when
//! a match session is accepted and removed when the room closes, the guest
//! leaves, the binding expires or the session is gone.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, SocketAddrV4};

use fighter_protocol::json::Candidate;
use fighter_protocol::ratelimit::TokenBucket;
use fighter_protocol::udp::BindRole;

use crate::config::LimitsConfig;

/// Per-player binding and budgets.
pub struct Slot {
    pub session_id: String,
    pub room_id: String,
    pub role: BindRole,
    pub peer: String,
    pub relay_only: bool,
    pub token: [u8; 16],
    pub key: [u8; 8],
    /// Public endpoint observed by the server, once the player has bound.
    pub endpoint: Option<SocketAddr>,
    pub candidates: Vec<SocketAddrV4>,
    pub last_seen_ms: u64,
    pub registered_ms: u64,
    pub notified: bool,
    dgram_bucket: TokenBucket,
    byte_bucket: TokenBucket,
}

impl Slot {
    fn same_public_ip(&self, other: &Slot) -> bool {
        match (self.endpoint, other.endpoint) {
            (Some(a), Some(b)) => a.ip() == b.ip(),
            _ => false,
        }
    }
}

/// Candidates a viewer should be told about its peer.
pub fn candidates_for(viewer: &Slot, peer: &Slot) -> Vec<Candidate> {
    if viewer.relay_only || peer.relay_only {
        return Vec::new();
    }
    let Some(endpoint) = peer.endpoint else {
        return Vec::new();
    };
    let mut out = vec![Candidate {
        ip: endpoint.ip().to_string(),
        port: endpoint.port(),
        kind: fighter_protocol::json::CandidateKind::Public,
    }];
    if viewer.same_public_ip(peer) {
        for c in &peer.candidates {
            out.push(Candidate {
                ip: c.ip().to_string(),
                port: c.port(),
                kind: fighter_protocol::json::CandidateKind::Local,
            });
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindOutcome {
    /// The token and role are valid; `changed` is true when the public endpoint
    /// moved (a NAT rebind), so peers must be told again.
    Bound {
        session_id: String,
        room_id: String,
        changed: bool,
    },
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayOutcome {
    Forward { to: SocketAddr },
    NoPeer,
    Unknown,
    WrongSource,
    RateLimited,
}

#[derive(Default)]
pub struct Bindings {
    slots: HashMap<String, Slot>,
    by_token: HashMap<[u8; 16], String>,
    by_key: HashMap<[u8; 8], String>,
    ping_buckets: HashMap<IpAddr, (TokenBucket, u64)>,
    unauth_buckets: HashMap<IpAddr, (TokenBucket, u64)>,
}

impl Bindings {
    /// Register a slot for a player whose match session was just accepted.
    #[allow(clippy::too_many_arguments)]
    pub fn register(
        &mut self,
        session_id: String,
        room_id: String,
        role: BindRole,
        peer: String,
        relay_only: bool,
        token: [u8; 16],
        key: [u8; 8],
        now: u64,
        limits: &LimitsConfig,
    ) {
        self.unregister(&session_id);
        let slot = Slot {
            session_id: session_id.clone(),
            room_id,
            role,
            peer,
            relay_only,
            token,
            key,
            endpoint: None,
            candidates: Vec::new(),
            last_seen_ms: now,
            registered_ms: now,
            notified: false,
            dgram_bucket: TokenBucket::new(
                limits.relay_datagrams_per_second,
                limits.relay_datagrams_per_second as f64,
            ),
            byte_bucket: TokenBucket::new(
                limits.relay_bytes_per_second,
                limits.relay_bytes_per_second as f64,
            ),
        };
        self.by_token.insert(token, session_id.clone());
        self.by_key.insert(key, session_id.clone());
        self.slots.insert(session_id, slot);
    }

    pub fn unregister(&mut self, session_id: &str) {
        if let Some(slot) = self.slots.remove(session_id) {
            self.by_token.remove(&slot.token);
            self.by_key.remove(&slot.key);
        }
    }

    pub fn unregister_room(&mut self, room_id: &str) {
        let ids: Vec<String> = self
            .slots
            .values()
            .filter(|s| s.room_id == room_id)
            .map(|s| s.session_id.clone())
            .collect();
        for id in ids {
            self.unregister(&id);
        }
    }

    pub fn get(&self, session_id: &str) -> Option<&Slot> {
        self.slots.get(session_id)
    }

    pub fn mark_notified(&mut self, session_id: &str) {
        if let Some(slot) = self.slots.get_mut(session_id) {
            slot.notified = true;
        }
    }

    /// Record a BIND. Returns the session and whether the endpoint moved.
    pub fn bind(
        &mut self,
        token: &[u8; 16],
        role: BindRole,
        from: SocketAddr,
        candidates: Vec<SocketAddrV4>,
        now: u64,
    ) -> BindOutcome {
        let Some(session_id) = self.by_token.get(token).cloned() else {
            return BindOutcome::Invalid;
        };
        let Some(slot) = self.slots.get_mut(&session_id) else {
            return BindOutcome::Invalid;
        };
        if slot.role != role {
            return BindOutcome::Invalid;
        }
        let changed = slot.endpoint != Some(from);
        slot.endpoint = Some(from);
        slot.candidates = candidates;
        slot.last_seen_ms = now;
        if changed {
            slot.notified = false;
        }
        BindOutcome::Bound {
            session_id,
            room_id: slot.room_id.clone(),
            changed,
        }
    }

    /// Validate and budget a RELAY, then resolve the peer endpoint.
    pub fn relay(
        &mut self,
        key: &[u8; 8],
        from: SocketAddr,
        payload_len: usize,
        now: u64,
    ) -> RelayOutcome {
        let Some(session_id) = self.by_key.get(key).cloned() else {
            return RelayOutcome::Unknown;
        };
        let peer_id = {
            let Some(slot) = self.slots.get_mut(&session_id) else {
                return RelayOutcome::Unknown;
            };
            if slot.endpoint != Some(from) {
                return RelayOutcome::WrongSource;
            }
            if !slot.dgram_bucket.try_acquire(now) {
                return RelayOutcome::RateLimited;
            }
            if !slot.byte_bucket.try_acquire_n(now, payload_len as f64) {
                return RelayOutcome::RateLimited;
            }
            slot.last_seen_ms = now;
            slot.peer.clone()
        };
        match self.slots.get(&peer_id).and_then(|p| p.endpoint) {
            Some(to) => RelayOutcome::Forward { to },
            None => RelayOutcome::NoPeer,
        }
    }

    /// Per-source limit for unauthenticated datagrams (BIND, PING, failed RELAY).
    pub fn unauth_allowed(&mut self, ip: IpAddr, now: u64, per_sec: u32) -> bool {
        allow(&mut self.unauth_buckets, ip, now, per_sec)
    }

    /// Per-source limit for PING/PONG.
    pub fn ping_allowed(&mut self, ip: IpAddr, now: u64, per_sec: u32) -> bool {
        allow(&mut self.ping_buckets, ip, now, per_sec)
    }

    /// Drop expired bindings and prune rate-limit buckets.
    pub fn sweep(&mut self, now: u64, expiry_ms: u64, max_age_ms: u64) {
        let expired: Vec<String> = self
            .slots
            .values()
            .filter(|s| {
                now.saturating_sub(s.last_seen_ms) > expiry_ms
                    || now.saturating_sub(s.registered_ms) > max_age_ms
            })
            .map(|s| s.session_id.clone())
            .collect();
        for id in expired {
            self.unregister(&id);
        }
        let cutoff = now.saturating_sub(60_000);
        self.ping_buckets.retain(|_, (_, last)| *last >= cutoff);
        self.unauth_buckets.retain(|_, (_, last)| *last >= cutoff);
    }
}

fn allow(
    buckets: &mut HashMap<IpAddr, (TokenBucket, u64)>,
    ip: IpAddr,
    now: u64,
    per_sec: u32,
) -> bool {
    let per_sec = per_sec.max(1);
    let entry = buckets.entry(ip).or_insert_with(|| {
        (
            TokenBucket::new(per_sec.saturating_mul(2), per_sec as f64),
            now,
        )
    });
    entry.1 = now;
    entry.0.try_acquire(now)
}
