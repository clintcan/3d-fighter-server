//! Spectator match log and per-spectator cursors (section 8).
//!
//! The server stores the host's feed as a per-tick input log plus batch receive
//! times. Delivery is pull-based: a 20 ms task computes how many ticks are
//! visible (`receive + delay <= now`) for a room and pushes any not-yet-delivered
//! inputs to each spectator, re-batched into frames of up to 600 ticks. A late
//! joiner starts at tick 0 and catches up naturally; a live spectator advances a
//! little at a time. This also avoids one timer per spectator.

use fighter_protocol::feed::{MatchEnd, MatchStart};

/// A match log is capped at two hours of ticks (section 8.2).
pub const MAX_FEED_TICKS: usize = 432_000;

/// Receive time of one INPUTS batch, with the cumulative tick count at its end.
#[derive(Debug, Clone, Copy)]
struct Batch {
    end_tick: u32,
    recv_ms: u64,
}

#[derive(Debug, Default)]
pub struct MatchLog {
    pub match_id: Option<u32>,
    pub start: Option<MatchStart>,
    pub inputs: Vec<(u16, u16)>,
    batches: Vec<Batch>,
    pub checksums: Vec<(u32, u32, u64)>,
    pub end: Option<(MatchEnd, u64)>,
    /// Server time when this match began, for the real-time feed limit.
    pub started_ms: u64,
}

impl MatchLog {
    pub fn is_live(&self) -> bool {
        self.start.is_some() && self.end.is_none()
    }

    pub fn tick_count(&self) -> usize {
        self.inputs.len()
    }

    /// Approximate heap use, for the global match-log budget.
    pub fn byte_size(&self) -> usize {
        let strings = self
            .start
            .as_ref()
            .map(|m| {
                m.game_version.len()
                    + m.stage_id.len()
                    + m.p1_fighter_id.len()
                    + m.p2_fighter_id.len()
                    + m.p1_name.len()
                    + m.p2_name.len()
                    + 16
            })
            .unwrap_or(0);
        self.inputs.len() * 4 + self.checksums.len() * 12 + strings + 64
    }

    /// Begin a new match, discarding any previous log.
    pub fn begin(&mut self, start: MatchStart, now: u64) {
        self.match_id = Some(start.match_id);
        self.start = Some(start);
        self.inputs.clear();
        self.batches.clear();
        self.checksums.clear();
        self.end = None;
        self.started_ms = now;
    }

    /// Discard the log (a feed gap or reset).
    pub fn clear(&mut self) {
        self.match_id = None;
        self.start = None;
        self.inputs.clear();
        self.batches.clear();
        self.checksums.clear();
        self.end = None;
    }

    /// Append a contiguous INPUTS batch received at `recv_ms`.
    pub fn push_inputs(&mut self, inputs: &[(u16, u16)], recv_ms: u64) {
        self.inputs.extend_from_slice(inputs);
        self.batches.push(Batch {
            end_tick: self.inputs.len() as u32,
            recv_ms,
        });
    }

    pub fn push_checksum(&mut self, tick: u32, checksum: u32, recv_ms: u64) {
        self.checksums.push((tick, checksum, recv_ms));
    }

    /// How many ticks are allowed to be visible at `now` with `delay_ms`.
    pub fn visible_ticks(&self, now: u64, delay_ms: u64) -> u32 {
        let idx = self
            .batches
            .partition_point(|b| b.recv_ms.saturating_add(delay_ms) <= now);
        if idx == 0 {
            0
        } else {
            self.batches[idx - 1].end_tick
        }
    }
}

/// Delivery cursor for one spectator.
#[derive(Debug, Clone, Default)]
pub struct SpectatorState {
    pub delivered_ticks: u32,
    pub checksum_cursor: usize,
    pub end_sent: bool,
    /// Consecutive flushes where the send buffer was full. Used to drop a
    /// spectator that never drains.
    pub stall_flushes: u32,
}

impl SpectatorState {
    pub fn reset(&mut self) {
        self.delivered_ticks = 0;
        self.checksum_cursor = 0;
        self.end_sent = false;
        self.stall_flushes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start() -> MatchStart {
        MatchStart {
            feed_version: 1,
            match_id: 7,
            stage_index: 0,
            p1_fighter_index: 0,
            p2_fighter_index: 1,
            game_version: "0.4.1".into(),
            stage_id: "ring".into(),
            p1_fighter_id: "kenji".into(),
            p2_fighter_id: "rhea".into(),
            p1_name: "A".into(),
            p2_name: "B".into(),
        }
    }

    #[test]
    fn visible_ticks_respects_delay() {
        let mut log = MatchLog::default();
        log.begin(start(), 0);
        log.push_inputs(&[(5, 5); 6], 1000);
        log.push_inputs(&[(5, 5); 6], 1100);

        assert_eq!(log.visible_ticks(1000, 3000), 0);
        assert_eq!(log.visible_ticks(4000, 3000), 6);
        assert_eq!(log.visible_ticks(4100, 3000), 12);
        assert_eq!(log.visible_ticks(9999, 3000), 12);
    }

    #[test]
    fn begin_clears_previous_match() {
        let mut log = MatchLog::default();
        log.begin(start(), 0);
        log.push_inputs(&[(5, 5); 3], 0);
        let mut next = start();
        next.match_id = 8;
        log.begin(next, 0);
        assert_eq!(log.match_id, Some(8));
        assert_eq!(log.tick_count(), 0);
    }
}
