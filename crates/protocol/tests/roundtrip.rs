//! Round-trip property tests for every binary codec.

use fighter_protocol::feed::{FeedFrame, MatchResult, MatchStart};
use fighter_protocol::udp::{BindRole, UdpDatagram};
use proptest::collection::vec;
use proptest::prelude::*;
use std::net::{Ipv4Addr, SocketAddrV4};

fn arb_ipv4() -> impl Strategy<Value = Ipv4Addr> {
    any::<[u8; 4]>().prop_map(Ipv4Addr::from)
}

fn arb_addr() -> impl Strategy<Value = SocketAddrV4> {
    (arb_ipv4(), any::<u16>()).prop_map(|(ip, port)| SocketAddrV4::new(ip, port))
}

fn arb_short_string() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-z]{0,20}").unwrap()
}

fn arb_input() -> impl Strategy<Value = u16> {
    (1u16..=9, 0u16..=31).prop_map(|(dir, buttons)| dir | (buttons << 4))
}

proptest! {
    #[test]
    fn udp_bind_round_trips(
        token in any::<[u8; 16]>(),
        role in prop_oneof![Just(BindRole::Host), Just(BindRole::Guest)],
        candidates in vec(arb_addr(), 0..=4),
    ) {
        let dg = UdpDatagram::Bind { session_token: token, role, candidates };
        let bytes = dg.encode();
        prop_assert_eq!(UdpDatagram::decode(&bytes).unwrap(), dg);
    }

    #[test]
    fn udp_bound_round_trips(addr in arb_addr()) {
        let dg = UdpDatagram::Bound { observed: addr };
        prop_assert_eq!(UdpDatagram::decode(&dg.encode()).unwrap(), dg);
    }

    #[test]
    fn udp_relay_round_trips(
        key in any::<[u8; 8]>(),
        payload in vec(any::<u8>(), 1..=1200),
    ) {
        let dg = UdpDatagram::Relay { relay_key: key, payload };
        prop_assert_eq!(UdpDatagram::decode(&dg.encode()).unwrap(), dg);
    }

    #[test]
    fn udp_relayed_round_trips(payload in vec(any::<u8>(), 1..=1200)) {
        let dg = UdpDatagram::Relayed { payload };
        prop_assert_eq!(UdpDatagram::decode(&dg.encode()).unwrap(), dg);
    }

    #[test]
    fn udp_ping_pong_round_trip(nonce in any::<u32>(), t in any::<u32>()) {
        let ping = UdpDatagram::Ping { nonce, client_time_ms: t };
        prop_assert_eq!(UdpDatagram::decode(&ping.encode()).unwrap(), ping);
        let pong = UdpDatagram::Pong { nonce, client_time_ms: t };
        prop_assert_eq!(UdpDatagram::decode(&pong.encode()).unwrap(), pong);
    }

    #[test]
    fn feed_inputs_round_trips(
        match_id in any::<u32>(),
        first_tick in any::<u32>(),
        inputs in vec((arb_input(), arb_input()), 1..=600),
    ) {
        let frame = FeedFrame::Inputs { match_id, first_tick, inputs };
        prop_assert_eq!(FeedFrame::decode(&frame.encode().unwrap()).unwrap(), frame);
    }

    #[test]
    fn feed_match_start_round_trips(
        match_id in any::<u32>(),
        stage_index in any::<u8>(),
        p1 in any::<u8>(),
        p2 in any::<u8>(),
        game_version in arb_short_string(),
        stage_id in arb_short_string(),
        p1_id in arb_short_string(),
        p2_id in arb_short_string(),
        p1_name in arb_short_string(),
        p2_name in arb_short_string(),
    ) {
        let frame = FeedFrame::MatchStart(MatchStart {
            feed_version: 1,
            match_id,
            stage_index,
            p1_fighter_index: p1,
            p2_fighter_index: p2,
            game_version,
            stage_id,
            p1_fighter_id: p1_id,
            p2_fighter_id: p2_id,
            p1_name,
            p2_name,
        });
        prop_assert_eq!(FeedFrame::decode(&frame.encode().unwrap()).unwrap(), frame);
    }

    #[test]
    fn feed_checksum_round_trips(match_id in any::<u32>(), tick in any::<u32>(), checksum in any::<u32>()) {
        let frame = FeedFrame::Checksum { match_id, tick, checksum };
        prop_assert_eq!(FeedFrame::decode(&frame.encode().unwrap()).unwrap(), frame);
    }

    #[test]
    fn feed_match_end_round_trips(
        match_id in any::<u32>(),
        final_tick in any::<u32>(),
        result in prop_oneof![
            Just(MatchResult::P1Won), Just(MatchResult::P2Won),
            Just(MatchResult::Draw), Just(MatchResult::Aborted)
        ],
        p1_wins in any::<u8>(),
        p2_wins in any::<u8>(),
    ) {
        let frame = FeedFrame::MatchEnd { match_id, final_tick, result, p1_wins, p2_wins };
        prop_assert_eq!(FeedFrame::decode(&frame.encode().unwrap()).unwrap(), frame);
    }

    #[test]
    fn feed_reset_round_trips(match_id in any::<u32>()) {
        let frame = FeedFrame::FeedReset { match_id };
        prop_assert_eq!(FeedFrame::decode(&frame.encode().unwrap()).unwrap(), frame);
    }

    #[test]
    fn decoding_arbitrary_bytes_never_panics(data in vec(any::<u8>(), 0..2048)) {
        let _ = UdpDatagram::decode(&data);
        let _ = FeedFrame::decode(&data);
    }

    #[test]
    fn no_amplification(
        token in any::<[u8; 16]>(),
        role in prop_oneof![Just(BindRole::Host), Just(BindRole::Guest)],
        candidates in vec(arb_addr(), 0..=4),
        addr in arb_addr(),
        nonce in any::<u32>(),
        t in any::<u32>(),
        payload in vec(any::<u8>(), 1..=1200),
        key in any::<[u8; 8]>(),
    ) {
        let bind = UdpDatagram::Bind { session_token: token, role, candidates };
        let bound = UdpDatagram::Bound { observed: addr };
        prop_assert!(bound.encode().len() <= bind.encode().len());

        let ping = UdpDatagram::Ping { nonce, client_time_ms: t };
        let pong = UdpDatagram::Pong { nonce, client_time_ms: t };
        prop_assert_eq!(pong.encode().len(), ping.encode().len());

        let relay = UdpDatagram::Relay { relay_key: key, payload: payload.clone() };
        let relayed = UdpDatagram::Relayed { payload };
        prop_assert!(relayed.encode().len() <= relay.encode().len());
    }
}
