//! M3 acceptance tests: feed publishing, delayed fan-out, late join, reset and
//! slow-spectator handling.

mod common;

use std::time::Duration;

use common::{pair_up, start_test, Client};
use fighter_protocol::feed::{FeedFrame, MatchResult, MatchStart};
use serde_json::json;
use tokio::time::timeout;

fn start_frame(match_id: u32) -> Vec<u8> {
    FeedFrame::MatchStart(MatchStart {
        feed_version: 1,
        match_id,
        stage_index: 0,
        p1_fighter_index: 0,
        p2_fighter_index: 1,
        game_version: "0.4.1".into(),
        stage_id: "ring".into(),
        p1_fighter_id: "kenji".into(),
        p2_fighter_id: "rhea".into(),
        p1_name: "Lino".into(),
        p2_name: "Lufi".into(),
    })
    .encode()
    .unwrap()
}

fn input_for(i: usize) -> (u16, u16) {
    let dir1 = ((i % 9) + 1) as u16;
    let buttons1 = ((i % 32) as u16) << 4;
    let dir2 = (((i * 3) % 9) + 1) as u16;
    let buttons2 = (((i * 5) % 32) as u16) << 4;
    (dir1 | buttons1, dir2 | buttons2)
}

fn inputs_frame(match_id: u32, first_tick: u32, count: usize) -> Vec<u8> {
    let inputs: Vec<(u16, u16)> = (first_tick as usize..first_tick as usize + count)
        .map(input_for)
        .collect();
    FeedFrame::Inputs {
        match_id,
        first_tick,
        inputs,
    }
    .encode()
    .unwrap()
}

fn checksum_frame(match_id: u32, tick: u32, checksum: u32) -> Vec<u8> {
    FeedFrame::Checksum {
        match_id,
        tick,
        checksum,
    }
    .encode()
    .unwrap()
}

fn end_frame(match_id: u32, final_tick: u32) -> Vec<u8> {
    FeedFrame::MatchEnd {
        match_id,
        final_tick,
        result: MatchResult::P1Won,
        p1_wins: 2,
        p2_wins: 0,
    }
    .encode()
    .unwrap()
}

struct Collected {
    inputs: Vec<(u16, u16)>,
    checksums: usize,
    final_tick: Option<u32>,
}

async fn collect_until_end(client: &mut Client) -> Collected {
    let mut out = Collected {
        inputs: Vec::new(),
        checksums: 0,
        final_tick: None,
    };
    loop {
        let bytes = client.recv_binary().await;
        match FeedFrame::decode(&bytes).expect("feed frame") {
            FeedFrame::MatchStart(_) => {}
            FeedFrame::Inputs {
                first_tick, inputs, ..
            } => {
                assert_eq!(first_tick as usize, out.inputs.len(), "contiguous ticks");
                out.inputs.extend(inputs);
            }
            FeedFrame::Checksum { .. } => out.checksums += 1,
            FeedFrame::MatchEnd { final_tick, .. } => {
                out.final_tick = Some(final_tick);
                return out;
            }
            FeedFrame::FeedReset { .. } => panic!("unexpected FEED_RESET"),
        }
    }
}

async fn spectate(url: &str, room: &str, name: &str) -> Client {
    let mut c = Client::connect(url, name, "0.4.1", 111).await;
    c.send(json!({"type":"spectate","room":room})).await;
    c.recv_type("spectate_started").await;
    c
}

#[tokio::test]
async fn late_and_early_spectators_see_identical_inputs() {
    let (running, clock) = start_test().await;
    let url = running.ws_url();
    let mut pair = pair_up(&url, false, false).await;

    let mut early = spectate(&url, &pair.code, "Early").await;

    pair.host.send_binary(start_frame(1)).await;
    for b in 0..10 {
        pair.host.send_binary(inputs_frame(1, b * 6, 6)).await;
    }

    let mut late = spectate(&url, &pair.code, "Late").await;

    for b in 10..20 {
        pair.host.send_binary(inputs_frame(1, b * 6, 6)).await;
    }
    pair.host.send_binary(checksum_frame(1, 60, 0xabcd)).await;
    pair.host.send_binary(checksum_frame(1, 120, 0x1234)).await;
    pair.host.send_binary(end_frame(1, 120)).await;

    // Let the server ingest every frame before advancing the virtual clock.
    tokio::time::sleep(Duration::from_millis(200)).await;
    clock.advance(300);

    let early_data = collect_until_end(&mut early).await;
    let late_data = collect_until_end(&mut late).await;

    assert_eq!(early_data.inputs.len(), 120);
    assert_eq!(early_data.inputs, late_data.inputs);
    assert_eq!(early_data.final_tick, Some(120));
    assert_eq!(late_data.final_tick, Some(120));
    assert_eq!(early_data.checksums, 2);
    assert_eq!(late_data.checksums, 2);
}

#[tokio::test]
async fn frames_are_not_delivered_before_the_delay() {
    let (running, clock) = start_test().await;
    let url = running.ws_url();
    let mut pair = pair_up(&url, false, false).await;
    let mut spec = spectate(&url, &pair.code, "Spec").await;

    pair.host.send_binary(start_frame(1)).await;
    pair.host.send_binary(inputs_frame(1, 0, 6)).await;

    // MATCH_START is immediate.
    let first = spec.recv_binary().await;
    assert!(matches!(
        FeedFrame::decode(&first).unwrap(),
        FeedFrame::MatchStart(_)
    ));

    // No INPUTS before the delay elapses.
    let early = timeout(Duration::from_millis(250), spec.recv_binary()).await;
    assert!(early.is_err(), "inputs delivered before the delay");

    tokio::time::sleep(Duration::from_millis(100)).await;
    clock.advance(300);
    let bytes = spec.recv_binary().await;
    match FeedFrame::decode(&bytes).unwrap() {
        FeedFrame::Inputs { first_tick, .. } => assert_eq!(first_tick, 0),
        other => panic!("expected INPUTS, got {other:?}"),
    }
}

#[tokio::test]
async fn feed_gap_triggers_reset() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let mut pair = pair_up(&url, false, false).await;
    let mut spec = spectate(&url, &pair.code, "Spec").await;

    pair.host.send_binary(start_frame(1)).await;
    pair.host.send_binary(inputs_frame(1, 0, 6)).await;
    // Skip ticks 6..12: a gap.
    pair.host.send_binary(inputs_frame(1, 12, 6)).await;

    let err = pair.host.recv_type("error").await;
    assert_eq!(err["code"], "bad_message");

    // Spectator: MATCH_START then FEED_RESET.
    let first = spec.recv_binary().await;
    assert!(matches!(
        FeedFrame::decode(&first).unwrap(),
        FeedFrame::MatchStart(_)
    ));
    let second = spec.recv_binary().await;
    assert!(matches!(
        FeedFrame::decode(&second).unwrap(),
        FeedFrame::FeedReset { match_id: 1 }
    ));
}

#[tokio::test]
async fn stalled_spectator_is_dropped_without_delaying_others() {
    let (running, clock) = start_test().await;
    let url = running.ws_url();
    let mut pair = pair_up(&url, false, false).await;

    let mut good = spectate(&url, &pair.code, "Good").await;
    let _bad = spectate(&url, &pair.code, "Bad").await; // never reads

    let total_ticks = 300_000usize;
    let reader = tokio::spawn(async move {
        let mut got = 0usize;
        loop {
            let bytes = good.recv_binary().await;
            match FeedFrame::decode(&bytes).unwrap() {
                FeedFrame::Inputs { inputs, .. } => got += inputs.len(),
                FeedFrame::MatchEnd { .. } | FeedFrame::FeedReset { .. } => break,
                _ => {}
            }
        }
        got
    });

    pair.host.send_binary(start_frame(1)).await;
    let mut first = 0usize;
    while first < total_ticks {
        let count = 600.min(total_ticks - first);
        pair.host
            .send_binary(inputs_frame(1, first as u32, count))
            .await;
        first += count;
    }
    pair.host
        .send_binary(end_frame(1, total_ticks as u32))
        .await;

    // Let the server ingest every frame, then release the delay.
    tokio::time::sleep(Duration::from_millis(400)).await;
    clock.advance(500);

    let got = timeout(Duration::from_secs(30), reader)
        .await
        .expect("reader finished")
        .unwrap();
    assert_eq!(got, total_ticks, "the reading spectator got every tick");

    // The host sees the stalled spectator removed (2 -> 1).
    let mut saw_one = false;
    for _ in 0..100 {
        let v = timeout(Duration::from_secs(2), pair.host.recv())
            .await
            .expect("room_state");
        if v["type"] == "room_state" && v["room"]["spectators"] == 1 {
            saw_one = true;
            break;
        }
    }
    assert!(saw_one, "stalled spectator should be dropped");
}
