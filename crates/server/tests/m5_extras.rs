//! M5 acceptance tests: quick match, feed verification, admin API, regions and
//! replays.

mod common;

use std::time::Duration;

use common::{http_request, pair_up, start_test, start_with, test_config, Client};
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
        p1_name: "A".into(),
        p2_name: "B".into(),
    })
    .encode()
    .unwrap()
}

fn inputs_frame(match_id: u32, first_tick: u32, p1: u16, p2: u16, count: usize) -> Vec<u8> {
    FeedFrame::Inputs {
        match_id,
        first_tick,
        inputs: vec![(p1, p2); count],
    }
    .encode()
    .unwrap()
}

#[tokio::test]
async fn quick_match_pairs_two_clients() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut a = Client::connect(&url, "A", "0.4.1", 111).await;
    a.send(json!({"type":"queue_join","mode":"casual"})).await;
    let mut b = Client::connect(&url, "B", "0.4.1", 111).await;
    b.send(json!({"type":"queue_join","mode":"casual"})).await;

    let ma = a.recv_type("queue_matched").await;
    let mb = b.recv_type("queue_matched").await;
    assert_eq!(ma["room_id"], mb["room_id"]);

    let sa = a.recv_type("match_session").await;
    let sb = b.recv_type("match_session").await;
    assert_eq!(sa["role"], "host"); // first in queue hosts
    assert_eq!(sb["role"], "guest");
}

#[tokio::test]
async fn feed_disagreement_marks_room_unverified() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let mut pair = pair_up(&url, false, false).await;

    pair.host.send_binary(start_frame(1)).await;
    pair.guest.send_binary(start_frame(1)).await;
    // Same ticks but different inputs.
    pair.host
        .send_binary(inputs_frame(1, 0, 0x0015, 0x0015, 6))
        .await;
    pair.guest
        .send_binary(inputs_frame(1, 0, 0x0016, 0x0016, 6))
        .await;

    // Force a fresh room_state so we read the current verification flag.
    pair.host
        .send(json!({"type":"room_update","phase":"in_match"}))
        .await;

    let mut host = pair.host;
    let found = timeout(Duration::from_secs(2), async {
        loop {
            let v = host.recv().await;
            if v["type"] == "room_state" && v["room"]["feed_verified"] == false {
                return true;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(found, "room should be marked feed_verified: false");
}

#[tokio::test]
async fn regions_endpoint_lists_a_region() {
    let (running, _clock) = start_test().await;
    let (status, body) = http_request(running.addr, "GET", "/v1/regions", &[], "").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"regions\""));
    assert!(body.contains("test"));
}

#[tokio::test]
async fn admin_api_requires_token_and_can_notice() {
    let mut config = test_config();
    config.admin.token = Some("secret-token".into());
    let (running, _clock) = start_with(config).await;

    let (status, _) = http_request(running.addr, "GET", "/admin/rooms", &[], "").await;
    assert_eq!(status, 401);

    let (status, _) = http_request(
        running.addr,
        "GET",
        "/admin/rooms",
        &[("Authorization", "Bearer secret-token")],
        "",
    )
    .await;
    assert_eq!(status, 200);

    let (status, _) = http_request(
        running.addr,
        "POST",
        "/admin/notice",
        &[
            ("Authorization", "Bearer secret-token"),
            ("Content-Type", "application/json"),
        ],
        r#"{"message":"hi","severity":"info"}"#,
    )
    .await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn finished_match_is_stored_and_downloadable() {
    let mut config = test_config();
    config.storage.replays = true;
    config.storage.replay_dir = std::env::temp_dir()
        .join(format!("fighter-replays-{}", std::process::id()))
        .to_string_lossy()
        .to_string();
    let _ = std::fs::remove_dir_all(&config.storage.replay_dir);
    let (running, _clock) = start_with(config).await;
    let url = running.ws_url();
    let mut pair = pair_up(&url, false, false).await;

    pair.host.send_binary(start_frame(1)).await;
    pair.host
        .send_binary(inputs_frame(1, 0, 0x0015, 0x0015, 60))
        .await;
    pair.host
        .send_binary(
            FeedFrame::MatchEnd {
                match_id: 1,
                final_tick: 60,
                result: MatchResult::P1Won,
                p1_wins: 2,
                p2_wins: 0,
            }
            .encode()
            .unwrap(),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let (status, body) = http_request(running.addr, "GET", "/v1/replays", &[], "").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"replays\""), "body: {body}");
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    let id = value["replays"][0]["id"].as_str().unwrap().to_string();

    let (status, body) =
        http_request(running.addr, "GET", &format!("/v1/replays/{id}"), &[], "").await;
    assert_eq!(status, 200);
    assert!(!body.is_empty());
}
