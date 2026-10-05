//! Temporary bans for repeated abuse (section 10.3).
//!
//! A client id or IP accumulates strikes. Reaching the threshold issues a ban
//! of `base * 2^(bans-1)` milliseconds, capped at `max`. Strikes decay after a
//! quiet hour, and an expired ban is forgotten.

use std::collections::HashMap;
use std::net::IpAddr;

const STRIKE_THRESHOLD: u32 = 10;
const DECAY_MS: u64 = 60 * 60 * 1_000;

#[derive(Debug, Clone, Copy)]
struct Entry {
    strikes: u32,
    bans: u32,
    banned_until: u64,
    last_ms: u64,
}

#[derive(Debug, Default)]
pub struct Bans {
    by_client: HashMap<String, Entry>,
    by_ip: HashMap<IpAddr, Entry>,
    base_ms: u64,
    max_ms: u64,
}

impl Bans {
    pub fn new(base_ms: u64, max_ms: u64) -> Self {
        Self {
            by_client: HashMap::new(),
            by_ip: HashMap::new(),
            base_ms,
            max_ms,
        }
    }

    pub fn is_banned_client(&self, client_id_hash: &str, now: u64) -> bool {
        self.by_client
            .get(client_id_hash)
            .is_some_and(|e| e.banned_until > now)
    }

    pub fn is_banned_ip(&self, ip: IpAddr, now: u64) -> bool {
        self.by_ip.get(&ip).is_some_and(|e| e.banned_until > now)
    }

    /// Record one violation for a client and/or IP. Returns true if a ban is
    /// now in force.
    pub fn record(&mut self, client_id_hash: Option<&str>, ip: Option<IpAddr>, now: u64) -> bool {
        let mut banned = false;
        if let Some(hash) = client_id_hash {
            banned |= record_entry(
                &mut self.by_client,
                hash.to_string(),
                now,
                self.base_ms,
                self.max_ms,
            );
        }
        if let Some(ip) = ip {
            banned |= record_entry(&mut self.by_ip, ip, now, self.base_ms, self.max_ms);
        }
        banned
    }

    /// Issue an explicit ban (admin API), independent of the strike counter.
    pub fn ban_for(
        &mut self,
        client_id_hash: Option<&str>,
        ip: Option<IpAddr>,
        minutes: u64,
        now: u64,
    ) {
        let until = now.saturating_add(minutes.saturating_mul(60_000));
        if let Some(hash) = client_id_hash {
            let entry = self.by_client.entry(hash.to_string()).or_insert(Entry {
                strikes: 0,
                bans: 0,
                banned_until: 0,
                last_ms: now,
            });
            entry.banned_until = until;
            entry.last_ms = now;
        }
        if let Some(ip) = ip {
            let entry = self.by_ip.entry(ip).or_insert(Entry {
                strikes: 0,
                bans: 0,
                banned_until: 0,
                last_ms: now,
            });
            entry.banned_until = until;
            entry.last_ms = now;
        }
    }

    /// Drop expired bans and old entries.
    pub fn sweep(&mut self, now: u64) {
        self.by_client
            .retain(|_, e| e.banned_until > now || now.saturating_sub(e.last_ms) < DECAY_MS);
        self.by_ip
            .retain(|_, e| e.banned_until > now || now.saturating_sub(e.last_ms) < DECAY_MS);
    }
}

fn record_entry<K: std::hash::Hash + Eq>(
    map: &mut HashMap<K, Entry>,
    key: K,
    now: u64,
    base_ms: u64,
    max_ms: u64,
) -> bool {
    let entry = map.entry(key).or_insert(Entry {
        strikes: 0,
        bans: 0,
        banned_until: 0,
        last_ms: now,
    });
    if now.saturating_sub(entry.last_ms) > DECAY_MS {
        entry.strikes = 0;
    }
    entry.strikes += 1;
    entry.last_ms = now;
    if entry.strikes >= STRIKE_THRESHOLD {
        entry.strikes = 0;
        entry.bans += 1;
        let shift = (entry.bans - 1).min(20);
        let duration = base_ms.saturating_mul(1u64 << shift).min(max_ms);
        entry.banned_until = now.saturating_add(duration);
        return true;
    }
    entry.banned_until > now
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bans_after_threshold_and_grows() {
        let mut bans = Bans::new(600_000, 24 * 3_600_000);
        for _ in 0..STRIKE_THRESHOLD - 1 {
            assert!(!bans.record(Some("abc"), None, 1000));
        }
        assert!(bans.record(Some("abc"), None, 1000));
        assert!(bans.is_banned_client("abc", 1000));
        // First ban lasts 10 minutes.
        assert!(bans.is_banned_client("abc", 1000 + 599_000));
        assert!(!bans.is_banned_client("abc", 1000 + 601_000));
    }
}
