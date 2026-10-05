//! Regression tests for the reported issues.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{pair_up, start_test, Client, Ws};
use fighter_protocol::udp::{BindRole, UdpDatagram};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

async fn bind_socket(server: SocketAddr, token: [u8; 16], role: BindRole) -> UdpSocket {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dg = UdpDatagram::Bind {
        session_token: token,
        role,
        candidates: Vec::new(),
    }
    .encode();
    socket.send_to(&dg, server).await.unwrap();
    let mut buf = [0u8; 64];
    let (n, _) = timeout(Duration::from_secs(2), socket.recv_from(&mut buf))
        .await
        .expect("BOUND timeout")
        .unwrap();
    assert!(matches!(
        UdpDatagram::decode(&buf[..n]).unwrap(),
        UdpDatagram::Bound { .. }
    ));
    socket
}

fn relay_datagram(key: [u8; 8], payload: Vec<u8>) -> Vec<u8> {
    UdpDatagram::Relay {
        relay_key: key,
        payload,
    }
    .encode()
}

/// Read until a JSON `pong`, returning false on close/timeout.
async fn wait_pong(ws: &mut Ws) -> bool {
    loop {
        match timeout(Duration::from_secs(2), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(t.as_str()) {
                    if v["type"] == "pong" {
                        return true;
                    }
                }
            }
            Ok(Some(Ok(Message::Ping(_)))) | Ok(Some(Ok(Message::Pong(_)))) => continue,
            _ => return false,
        }
    }
}

// #1: a control frame may arrive before hello.
#[tokio::test]
async fn control_frame_before_hello_is_skipped() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let mut req = url.into_client_request().unwrap();
    req.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_static("3dfighter.lobby.v1"),
    );
    let (mut ws, _) = connect_async(req).await.unwrap();
    ws.send(Message::Ping(Vec::new().into())).await.unwrap();
    ws.send(Message::Pong(Vec::new().into())).await.unwrap();
    ws.send(Message::Text(
        json!({
            "type":"hello","protocol":1,"game_version":"0.4.1","content_hash":111,
            "client_id":"c","name":"Lino"
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    let mut got_welcome = false;
    for _ in 0..5 {
        match timeout(Duration::from_secs(2), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                if v["type"] == "welcome" {
                    got_welcome = true;
                    break;
                }
            }
            Ok(Some(Ok(_))) => continue,
            other => panic!("connection closed before welcome: {other:?}"),
        }
    }
    assert!(
        got_welcome,
        "expected welcome after a pre-hello control frame"
    );
}

// #2a: a spectator that only answers pings is not marked idle.
#[tokio::test]
async fn pongs_count_as_activity() {
    let (running, clock) = start_test().await;
    let url = running.ws_url();
    let pair = pair_up(&url, false, false).await;
    let mut spec = Client::connect(&url, "Spec", "0.4.1", 111).await;
    spec.send(json!({"type":"spectate","room":pair.code})).await;
    spec.recv_type("spectate_started").await;

    for _ in 0..4 {
        clock.advance(20_000);
        spec.ws
            .send(Message::Pong(Vec::new().into()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    spec.send(json!({"type":"ping","t":1})).await;
    assert!(
        wait_pong(&mut spec.ws).await,
        "spectator dropped despite answering pings"
    );
}

// #2b: a host that only publishes binary feed frames is not marked idle.
#[tokio::test]
async fn binary_frames_count_as_activity() {
    let (running, clock) = start_test().await;
    let url = running.ws_url();
    let mut pair = pair_up(&url, false, false).await;

    for i in 0..4u32 {
        clock.advance(20_000);
        let frame =
            fighter_protocol::feed::FeedFrame::MatchStart(fighter_protocol::feed::MatchStart {
                feed_version: 1,
                match_id: 100 + i,
                stage_index: 0,
                p1_fighter_index: 0,
                p2_fighter_index: 1,
                game_version: "0.4.1".into(),
                stage_id: "ring".into(),
                p1_fighter_id: "kenji".into(),
                p2_fighter_id: "rhea".into(),
                p1_name: "H".into(),
                p2_name: "G".into(),
            })
            .encode()
            .unwrap();
        pair.host.send_binary(frame).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    pair.host.send(json!({"type":"ping","t":1})).await;
    assert!(
        wait_pong(&mut pair.host.ws).await,
        "host dropped despite publishing binary frames"
    );
}

// #3: a kicked guest can no longer relay to the host.
#[tokio::test]
async fn kicked_guest_relay_is_dropped() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let pair = pair_up(&url, false, false).await;

    let host_sock = bind_socket(running.udp_addr, pair.host_token, BindRole::Host).await;
    let guest_sock = bind_socket(running.udp_addr, pair.guest_token, BindRole::Guest).await;

    let mut buf = [0u8; 128];
    guest_sock
        .send_to(
            &relay_datagram(pair.guest_key, vec![1, 2, 3]),
            running.udp_addr,
        )
        .await
        .unwrap();
    let (n, _) = timeout(Duration::from_secs(2), host_sock.recv_from(&mut buf))
        .await
        .expect("relay before kick")
        .unwrap();
    assert!(matches!(
        UdpDatagram::decode(&buf[..n]).unwrap(),
        UdpDatagram::Relayed { .. }
    ));

    let mut host = pair.host;
    let mut guest = pair.guest;
    host.send(json!({"type":"kick"})).await;
    guest.recv_type("player_left").await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    guest_sock
        .send_to(
            &relay_datagram(pair.guest_key, vec![4, 5, 6]),
            running.udp_addr,
        )
        .await
        .unwrap();
    let after = timeout(Duration::from_millis(400), host_sock.recv_from(&mut buf)).await;
    assert!(after.is_err(), "kicked guest's relay should be dropped");
}

// #4: spectating enforces version/content hash.
#[tokio::test]
async fn spectate_rejects_version_mismatch() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let pair = pair_up(&url, false, false).await;

    let mut spec = Client::connect(&url, "Spec", "0.4.1", 222).await;
    spec.send(json!({"type":"spectate","room":pair.code})).await;
    let err = spec.recv_type("error").await;
    assert_eq!(err["code"], "version_mismatch");
}

// #5: error and direct responses echo the request id.
#[tokio::test]
async fn rid_is_echoed() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut client = Client::connect(&url, "A", "0.4.1", 111).await;
    client.send(json!({"type":"kick","rid":"k1"})).await;
    let err = client.recv_type("error").await;
    assert_eq!(err["code"], "not_allowed");
    assert_eq!(err["rid"], "k1");

    let mut host = Client::connect(&url, "H", "0.4.1", 111).await;
    host.send(json!({
        "type":"create_room","visibility":"public","allow_spectators":true,"rid":"c1"
    }))
    .await;
    let created = host.recv_type("room_created").await;
    assert_eq!(created["rid"], "c1");
    let code = created["code"].as_str().unwrap().to_string();

    let mut spec = Client::connect(&url, "S", "0.4.1", 111).await;
    spec.send(json!({"type":"spectate","room":code,"rid":"s1"}))
        .await;
    let started = spec.recv_type("spectate_started").await;
    assert_eq!(started["rid"], "s1");
}
