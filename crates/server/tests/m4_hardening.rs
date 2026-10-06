//! M4 acceptance tests: reconnection, disconnect timeout and temporary bans.

mod common;

use std::time::Duration;

use common::{pair_up, start_test, Client};
use futures_util::SinkExt;
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn reconnect_within_grace_keeps_room_and_role() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let pair = pair_up(&url, false, false).await;
    let guest_token = pair.guest.resume_token.clone();
    let guest_sid = pair.guest.session_id.clone();
    let mut host = pair.host;

    // The guest's socket drops.
    drop(pair.guest);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // A new connection presenting the resume token adopts the session.
    let mut guest = Client::hello_raw(
        &url,
        json!({
            "type":"hello","protocol":1,"game_version":"0.4.1","content_hash":111,
            "client_id":"resumer","name":"Lufi","resume_token":guest_token
        }),
    )
    .await;
    let welcome = guest.recv_type("welcome").await;
    assert_eq!(welcome["session_id"], guest_sid.as_str());
    let state = guest.recv_type("room_state").await;
    assert_eq!(state["room"]["players"].as_array().unwrap().len(), 2);

    // The host still has a guest.
    let _ = host.recv_type("room_state").await;
}

#[tokio::test]
async fn guest_reported_disconnected_after_grace() {
    let (running, clock) = start_test().await;
    let url = running.ws_url();
    let pair = pair_up(&url, false, false).await;
    let mut host = pair.host;

    drop(pair.guest);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Past the 30 s resume grace the session is torn down.
    clock.advance(31_000);

    let left = host.recv_type("player_left").await;
    assert_eq!(left["role"], "guest");
    assert_eq!(left["reason"], "disconnected");
}

#[tokio::test]
async fn repeated_malformed_messages_ban_the_source() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let hello = |cid: &str| {
        json!({
            "type":"hello","protocol":1,"game_version":"0.4.1","content_hash":111,
            "client_id":cid,"name":"Bad"
        })
    };

    let mut client = Client::hello_raw(&url, hello("repeat-id")).await;
    client.recv_type("welcome").await;
    for _ in 0..10 {
        client
            .ws
            .send(Message::Text("not json".into()))
            .await
            .unwrap();
        let _ = client.recv_type("error").await;
    }

    // The same client id is refused. (IP strikes are skipped for loopback peers,
    // so the ban is by client id here; see the proxy test for IP bans.)
    let mut fresh = Client::hello_raw(&url, hello("repeat-id")).await;
    let first = fresh.recv().await;
    assert_eq!(first["type"], "error");
    assert_eq!(first["code"], "not_allowed");
}

#[tokio::test]
async fn resume_token_is_single_use() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let pair = pair_up(&url, false, false).await;
    let token = pair.guest.resume_token.clone();
    let guest_sid = pair.guest.session_id.clone();
    drop(pair.guest);
    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut first = Client::hello_raw(
        &url,
        json!({
            "type":"hello","protocol":1,"game_version":"0.4.1","content_hash":111,
            "client_id":"a","name":"A","resume_token":token
        }),
    )
    .await;
    assert_eq!(
        first.recv_type("welcome").await["session_id"],
        guest_sid.as_str()
    );

    // The old token is rotated, so a second attempt starts a fresh session.
    let mut second = Client::hello_raw(
        &url,
        json!({
            "type":"hello","protocol":1,"game_version":"0.4.1","content_hash":111,
            "client_id":"b","name":"B","resume_token":token
        }),
    )
    .await;
    let welcome = second.recv_type("welcome").await;
    assert_ne!(welcome["session_id"], guest_sid.as_str());
}
