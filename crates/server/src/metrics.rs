//! Prometheus metrics (section 9).

use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry,
    TextEncoder,
};

#[derive(Debug)]
pub struct Metrics {
    pub registry: Registry,
    pub connections: IntGauge,
    pub connections_total: IntCounter,
    pub connections_rejected: IntCounterVec,
    pub rooms: IntGaugeVec,
    pub rooms_created_total: IntCounter,
    pub joins: IntCounterVec,
    pub messages: IntCounterVec,
    pub rate_limited: IntCounter,
    pub malformed: IntCounter,
    #[allow(dead_code)]
    pub bans: IntCounter,
    #[allow(dead_code)]
    pub feed_frames: IntCounterVec,
    #[allow(dead_code)]
    pub relay_packets: IntCounterVec,
    #[allow(dead_code)]
    pub relay_bytes: IntCounterVec,
    pub spectators: IntGauge,
    #[allow(dead_code)]
    pub matches: IntCounterVec,
    pub feed_mismatch: IntCounter,
    pub relay_latency: Histogram,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let connections =
            IntGauge::new("fighter_ws_connections", "Open WebSocket connections").expect("metric");
        let connections_total = IntCounter::new(
            "fighter_ws_connections_total",
            "WebSocket connections accepted",
        )
        .expect("metric");
        let connections_rejected = IntCounterVec::new(
            prometheus::Opts::new(
                "fighter_ws_connections_rejected_total",
                "Rejected connections",
            ),
            &["reason"],
        )
        .expect("metric");
        let rooms = IntGaugeVec::new(
            prometheus::Opts::new("fighter_rooms", "Live rooms by status"),
            &["status"],
        )
        .expect("metric");
        let rooms_created_total =
            IntCounter::new("fighter_rooms_created_total", "Rooms created").expect("metric");
        let joins = IntCounterVec::new(
            prometheus::Opts::new("fighter_joins_total", "Join outcomes"),
            &["result"],
        )
        .expect("metric");
        let messages = IntCounterVec::new(
            prometheus::Opts::new("fighter_messages_total", "Lobby messages by type"),
            &["type"],
        )
        .expect("metric");
        let rate_limited =
            IntCounter::new("fighter_rate_limited_total", "Rate limited requests").expect("metric");
        let malformed =
            IntCounter::new("fighter_malformed_total", "Malformed messages").expect("metric");
        let bans = IntCounter::new("fighter_bans_total", "Temporary bans issued").expect("metric");
        let feed_frames = IntCounterVec::new(
            prometheus::Opts::new("fighter_feed_frames_total", "Feed frames by type"),
            &["type"],
        )
        .expect("metric");
        let relay_packets = IntCounterVec::new(
            prometheus::Opts::new("fighter_relay_packets_total", "Relay datagrams"),
            &["direction"],
        )
        .expect("metric");
        let relay_bytes = IntCounterVec::new(
            prometheus::Opts::new("fighter_relay_bytes_total", "Relay bytes"),
            &["direction"],
        )
        .expect("metric");
        let spectators = IntGauge::new("fighter_spectators", "Active spectators").expect("metric");
        let matches = IntCounterVec::new(
            prometheus::Opts::new("fighter_matches_total", "Matches by outcome"),
            &["result"],
        )
        .expect("metric");
        let relay_latency = Histogram::with_opts(
            HistogramOpts::new(
                "fighter_relay_latency_seconds",
                "Server-side relay forwarding time",
            )
            .buckets(vec![
                0.00005, 0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01,
            ]),
        )
        .expect("metric");
        let feed_mismatch = IntCounter::new(
            "fighter_feed_mismatch_total",
            "Host/guest feed disagreements",
        )
        .expect("metric");

        for m in [
            Box::new(connections.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(connections_total.clone()),
            Box::new(connections_rejected.clone()),
            Box::new(rooms.clone()),
            Box::new(rooms_created_total.clone()),
            Box::new(joins.clone()),
            Box::new(messages.clone()),
            Box::new(rate_limited.clone()),
            Box::new(malformed.clone()),
            Box::new(bans.clone()),
            Box::new(feed_frames.clone()),
            Box::new(relay_packets.clone()),
            Box::new(relay_bytes.clone()),
            Box::new(spectators.clone()),
            Box::new(matches.clone()),
            Box::new(relay_latency.clone()),
            Box::new(feed_mismatch.clone()),
        ] {
            registry.register(m).expect("register metric");
        }

        Self {
            registry,
            connections,
            connections_total,
            connections_rejected,
            rooms,
            rooms_created_total,
            joins,
            messages,
            rate_limited,
            malformed,
            bans,
            feed_frames,
            relay_packets,
            relay_bytes,
            spectators,
            matches,
            relay_latency,
            feed_mismatch,
        }
    }

    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        if encoder.encode(&self.registry.gather(), &mut buf).is_err() {
            return String::new();
        }
        String::from_utf8(buf).unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
