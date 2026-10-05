//! Token-bucket rate limiter used for the section 10.2 message limits.

/// A token bucket. `capacity` is the burst size; `refill_per_sec` is the steady
/// rate. Time is supplied by the caller so tests can drive it.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_ms: f64,
    last_ms: u64,
}

impl TokenBucket {
    /// `capacity` tokens, refilled at `refill_per_sec` tokens per second.
    pub fn new(capacity: u32, refill_per_sec: f64) -> Self {
        let capacity = capacity.max(1) as f64;
        Self {
            capacity,
            tokens: capacity,
            refill_per_ms: refill_per_sec / 1000.0,
            last_ms: 0,
        }
    }

    /// Refill up to now, then try to take one token.
    pub fn try_acquire(&mut self, now_ms: u64) -> bool {
        self.try_acquire_n(now_ms, 1.0)
    }

    /// Refill up to now, then try to take `n` tokens.
    pub fn try_acquire_n(&mut self, now_ms: u64, n: f64) -> bool {
        if now_ms > self.last_ms {
            let elapsed = (now_ms - self.last_ms) as f64;
            self.tokens = (self.tokens + elapsed * self.refill_per_ms).min(self.capacity);
            self.last_ms = now_ms;
        } else if now_ms < self.last_ms {
            // Clock moved backwards; treat as no time passing.
            self.last_ms = now_ms;
        }
        if self.tokens >= n {
            self.tokens -= n;
            true
        } else {
            false
        }
    }

    /// Seconds until one token is available, for `Retry-After` style hints.
    pub fn retry_after_secs(&self) -> u64 {
        if self.refill_per_ms <= 0.0 || self.tokens >= 1.0 {
            return 0;
        }
        let needed = 1.0 - self.tokens;
        (needed / self.refill_per_ms / 1000.0).ceil() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_then_refill() {
        let mut b = TokenBucket::new(4, 10.0);
        for _ in 0..4 {
            assert!(b.try_acquire(0));
        }
        assert!(!b.try_acquire(0));
        // 100 ms at 10/s = 1 token.
        assert!(b.try_acquire(100));
        assert!(!b.try_acquire(100));
        // Full refill after a long wait, capped at capacity.
        assert!(b.try_acquire(10_000));
        assert!(b.try_acquire(10_000));
        assert!(b.try_acquire(10_000));
        assert!(b.try_acquire(10_000));
        assert!(!b.try_acquire(10_000));
    }

    #[test]
    fn clock_going_backwards_is_safe() {
        let mut b = TokenBucket::new(1, 1.0);
        assert!(b.try_acquire(1000));
        assert!(!b.try_acquire(500));
    }
}
