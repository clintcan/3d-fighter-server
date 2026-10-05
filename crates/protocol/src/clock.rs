//! Injectable time source so tests never sleep.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch, monotonic enough for our purposes.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// Wall clock used in production.
#[derive(Debug, Default, Clone, Copy)]
pub struct RealClock;

impl Clock for RealClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// Controllable clock for tests. Advance it instead of sleeping.
#[derive(Debug, Default)]
pub struct TestClock {
    ms: AtomicU64,
}

impl TestClock {
    pub fn new(start_ms: u64) -> Self {
        Self {
            ms: AtomicU64::new(start_ms),
        }
    }

    pub fn set(&self, ms: u64) {
        self.ms.store(ms, Ordering::SeqCst);
    }

    pub fn advance(&self, ms: u64) {
        self.ms.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_ms(&self) -> u64 {
        self.ms.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_advances() {
        let c = TestClock::new(1000);
        assert_eq!(c.now_ms(), 1000);
        c.advance(250);
        assert_eq!(c.now_ms(), 1250);
        c.set(7);
        assert_eq!(c.now_ms(), 7);
    }
}
