//! Regression tests for the reported issues.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{http_request, pair_up, start_test, start_with, test_config, Client, Ws};
use fighter_protocol::feed::{FeedFrame, MatchStart};
use fighter_protocol::udp::{BindRole, UdpDatagram};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
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

fn inputs_frame(match_id: u32, first_tick: u32, count: usize) -> Vec<u8> {
    FeedFrame::Inputs {
        match_id,
        first_tick,
        inputs: vec![(0x0015, 0x0015); count],
    }
    .encode()
    .unwrap()
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

// A1: a feed far faster than real time is rejected.
#[tokio::test]
async fn feed_faster_than_real_time_is_rejected() {
    let mut config = test_config();
    config.limits.feed_max_backlog_ticks = 120; // production default
    let (running, _clock) = start_with(config).await;
    let url = running.ws_url();
    let mut pair = pair_up(&url, false, false).await;

    pair.host.send_binary(start_frame(1)).await;
    // 600 ticks in one frame at t=0 exceeds 60/s + 120 backlog.
    pair.host.send_binary(inputs_frame(1, 0, 600)).await;
    let err = pair.host.recv_type("error").await;
    assert_eq!(err["code"], "bad_message");
}

// A2: oversized text is rejected before parsing.
#[tokio::test]
async fn oversized_text_is_rejected() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let mut client = Client::connect(&url, "A", "0.4.1", 111).await;
    let big = "x".repeat(9 * 1024);
    client.ws.send(Message::Text(big.into())).await.unwrap();
    let err = client.recv_type("error").await;
    assert_eq!(err["code"], "bad_message");
}

// A2: an oversized binary frame closes the connection.
#[tokio::test]
async fn oversized_binary_closes_connection() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let mut client = Client::connect(&url, "A", "0.4.1", 111).await;
    let big = vec![0u8; 65 * 1024];
    let _ = client.ws.send(Message::Binary(big.into())).await;
    let closed = timeout(Duration::from_secs(2), async {
        loop {
            match client.ws.next().await {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return true,
                Some(Ok(_)) => continue,
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(closed, "server should close on an oversized binary frame");
}

// A3: behind a trusted proxy, bans apply per forwarded address.
#[tokio::test]
async fn trusted_proxy_bans_per_forwarded_address() {
    let mut config = test_config();
    config.server.trusted_proxies = vec!["127.0.0.1".into()];
    let (running, _clock) = start_with(config).await;
    let url = running.ws_url();
    let hello = |cid: &str| {
        json!({
            "type":"hello","protocol":1,"game_version":"0.4.1","content_hash":111,
            "client_id":cid,"name":"Bad"
        })
    };

    let mut a = Client::hello_raw_with(&url, hello("a"), &[("x-forwarded-for", "1.2.3.4")]).await;
    a.recv_type("welcome").await;
    for _ in 0..10 {
        a.ws.send(Message::Text("not json".into())).await.unwrap();
        let _ = a.recv_type("error").await;
    }

    // Same forwarded address is banned...
    let mut c = Client::hello_raw_with(&url, hello("c"), &[("x-forwarded-for", "1.2.3.4")]).await;
    let first = c.recv().await;
    assert_eq!(first["type"], "error");
    assert_eq!(first["code"], "not_allowed");

    // ...but a different forwarded address is not.
    let mut b = Client::hello_raw_with(&url, hello("b"), &[("x-forwarded-for", "5.6.7.8")]).await;
    assert_eq!(b.recv().await["type"], "welcome");
}

// #13: a slow, incomplete request is closed by the header-read timeout.
#[tokio::test]
async fn partial_http_request_is_timed_out() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut config = test_config();
    config.limits.http_header_timeout_ms = 300;
    let (running, _clock) = start_with(config).await;

    // Send a partial request and stop; the server must close the connection.
    let mut stream = tokio::net::TcpStream::connect(running.addr).await.unwrap();
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n")
        .await
        .unwrap();
    let closed = timeout(Duration::from_secs(3), async {
        let mut buf = [0u8; 64];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return true,
                Ok(_) => continue,
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        closed,
        "incomplete request should be closed by the header timeout"
    );

    // Normal HTTP and WebSocket upgrades still work.
    let (status, _) = http_request(running.addr, "GET", "/healthz", &[], "").await;
    assert_eq!(status, 200);
    let _ = Client::connect(&running.ws_url(), "A", "0.4.1", 111).await;
}

// #14: cleartext HTTP/2 (h2c) is refused; only HTTP/1 is served.
#[tokio::test]
async fn http2_prior_knowledge_is_refused() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (running, _clock) = start_test().await;
    let mut stream = tokio::net::TcpStream::connect(running.addr).await.unwrap();
    // HTTP/2 connection preface followed by an empty SETTINGS frame.
    let mut req = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    req.extend_from_slice(&[0, 0, 0, 4, 0, 0, 0, 0, 0]);
    stream.write_all(&req).await.unwrap();

    // A served HTTP/2 connection answers with a binary SETTINGS frame (9-byte
    // header, byte 3 = frame type 0x04). An HTTP/1-only server answers with an
    // HTTP/1 status line or closes.
    let mut buf = [0u8; 256];
    match timeout(Duration::from_secs(2), stream.read(&mut buf)).await {
        Ok(Ok(0)) => {}
        Ok(Ok(n)) => {
            assert!(
                buf[..n].starts_with(b"HTTP/1.1"),
                "expected an HTTP/1 rejection or close, got: {:02x?}",
                &buf[..n.min(24)]
            );
        }
        Ok(Err(_)) | Err(_) => {}
    }

    // HTTP/1 requests and WebSocket upgrades still work.
    let (status, _) = http_request(running.addr, "GET", "/healthz", &[], "").await;
    assert_eq!(status, 200);
    let _ = Client::connect(&running.ws_url(), "A", "0.4.1", 111).await;
}

// #16: match_session carries one shared pair_secret, different per match.
async fn matched_pair_secrets(url: &str) -> (String, String) {
    let mut host = Client::connect(url, "H", "0.4.1", 111).await;
    host.send(json!({"type":"create_room","visibility":"public","allow_spectators":true}))
        .await;
    let code = host.recv_type("room_created").await["code"]
        .as_str()
        .unwrap()
        .to_string();
    let mut guest = Client::connect(url, "G", "0.4.1", 111).await;
    guest.send(json!({"type":"join_room","room":code})).await;
    guest.recv_type("join_pending").await;
    let request = host.recv_type("join_request").await;
    host.send(json!({
        "type":"answer_join","request_id":request["request_id"],"accept":true
    }))
    .await;
    let host_session = host.recv_type("match_session").await;
    let guest_session = guest.recv_type("match_session").await;
    (
        host_session["pair_secret"].as_str().unwrap().to_string(),
        guest_session["pair_secret"].as_str().unwrap().to_string(),
    )
}

#[tokio::test]
async fn match_session_carries_a_shared_pair_secret() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let (host_secret, guest_secret) = matched_pair_secrets(&url).await;
    assert_eq!(host_secret.len(), 32);
    assert!(host_secret.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(host_secret, guest_secret, "both players share the secret");

    let (host2, guest2) = matched_pair_secrets(&url).await;
    assert_eq!(host2, guest2);
    assert_ne!(host_secret, host2, "a new match has a new secret");
}

// A burst of relay datagrams queued at once is all forwarded.
#[tokio::test]
async fn relay_burst_is_forwarded() {
    const N: u32 = 200; // within the default per-player burst budget

    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let pair = pair_up(&url, false, false).await;
    let host_sock = bind_socket(running.udp_addr, pair.host_token, BindRole::Host).await;
    let guest_sock = bind_socket(running.udp_addr, pair.guest_token, BindRole::Guest).await;
    let mut host = pair.host;
    let mut guest = pair.guest;
    host.recv_type("peer_endpoints").await;
    guest.recv_type("peer_endpoints").await;

    // Send a burst without waiting for each echo, so many are queued per wake-up.
    for i in 0..N {
        let payload = i.to_le_bytes().to_vec();
        guest_sock
            .send_to(&relay_datagram(pair.guest_key, payload), running.udp_addr)
            .await
            .unwrap();
    }

    let mut buf = [0u8; 128];
    let mut seen = vec![false; N as usize];
    for _ in 0..N {
        let (n, _) = timeout(Duration::from_secs(3), host_sock.recv_from(&mut buf))
            .await
            .expect("forwarded datagram")
            .unwrap();
        match UdpDatagram::decode(&buf[..n]).unwrap() {
            UdpDatagram::Relayed { payload } => {
                let idx = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                seen[idx] = true;
            }
            other => panic!("expected RELAYED, got {other:?}"),
        }
    }
    assert!(
        seen.iter().all(|s| *s),
        "every burst datagram was forwarded"
    );
}

// #23: the bounded drain must not starve the lobby under a relay burst.
#[tokio::test]
async fn lobby_stays_responsive_during_relay_burst() {
    let mut config = test_config();
    // Allow a large burst so the drain actually runs hot.
    config.limits.relay_datagrams_per_second = 1_000_000;
    config.limits.relay_bytes_per_second = 1_000_000_000;
    let (running, _clock) = start_with(config).await;
    let url = running.ws_url();
    let pair = pair_up(&url, false, false).await;
    let host_sock = bind_socket(running.udp_addr, pair.host_token, BindRole::Host).await;
    let guest_sock = bind_socket(running.udp_addr, pair.guest_token, BindRole::Guest).await;
    let mut host = pair.host;
    let mut guest = pair.guest;
    host.recv_type("peer_endpoints").await;
    guest.recv_type("peer_endpoints").await;

    // Keep the host socket drained so the server never blocks on a full send
    // buffer (which would hide the starvation this test targets).
    let drain = tokio::spawn(async move {
        let mut buf = [0u8; 256];
        while host_sock.recv_from(&mut buf).await.is_ok() {}
    });

    let guest_key = pair.guest_key;
    let udp_addr = running.udp_addr;
    let burst = tokio::spawn(async move {
        for i in 0..20_000u32 {
            let _ = guest_sock
                .send_to(
                    &relay_datagram(guest_key, i.to_le_bytes().to_vec()),
                    udp_addr,
                )
                .await;
        }
    });

    // A browser must still get a room list while the relay is saturated.
    let mut browser = Client::connect(&url, "B", "0.4.1", 111).await;
    browser.send(json!({"type":"list_rooms"})).await;
    let rooms = timeout(Duration::from_secs(10), browser.recv_type("rooms"))
        .await
        .expect("list_rooms answered during the relay burst");
    assert_eq!(rooms["type"], "rooms");

    burst.await.ok();
    drain.abort();
}

// #24: the aggregate online object tracks state across the lobby lifecycle.
#[tokio::test]
async fn online_counts_track_state() {
    let mut config = test_config();
    config.limits.list_rooms_per_second = 1000; // many list_rooms calls in the test
    let (running, clock) = start_with(config).await;
    let url = running.ws_url();
    let hello = |cid: &str| {
        json!({
            "type":"hello","protocol":1,"game_version":"0.4.1","content_hash":111,
            "client_id":cid,"name":"P"
        })
    };
    async fn online(c: &mut Client) -> Value {
        c.send(json!({"type":"list_rooms"})).await;
        c.recv_type("rooms").await["online"].clone()
    }

    let mut host = Client::hello_raw(&url, hello("h")).await;
    let welcome = host.recv_type("welcome").await;
    assert_eq!(welcome["online"]["players"], 1);
    assert_eq!(welcome["online"]["rooms"], 0);

    let mut guest = Client::hello_raw(&url, hello("g")).await;
    guest.recv_type("welcome").await;
    assert_eq!(online(&mut host).await["players"], 2);

    host.send(json!({"type":"create_room","visibility":"public","allow_spectators":true}))
        .await;
    let code = host.recv_type("room_created").await["code"]
        .as_str()
        .unwrap()
        .to_string();
    guest.send(json!({"type":"join_room","room":code})).await;
    guest.recv_type("join_pending").await;
    let request = host.recv_type("join_request").await;
    host.send(json!({
        "type":"answer_join","request_id":request["request_id"],"accept":true
    }))
    .await;
    host.recv_type("match_session").await;
    guest.recv_type("match_session").await;

    let o = online(&mut host).await;
    assert_eq!(o["rooms"], 1);
    assert_eq!(o["players"], 2);
    assert_eq!(o["in_match"], 0); // still in the lobby phase

    host.send(json!({"type":"room_update","phase":"in_match"}))
        .await;
    assert_eq!(online(&mut host).await["in_match"], 2);

    let mut spec = Client::hello_raw(&url, hello("s")).await;
    spec.recv_type("welcome").await;
    spec.send(json!({"type":"spectate","room":code})).await;
    spec.recv_type("spectate_started").await;
    let o = online(&mut host).await;
    assert_eq!(o["spectating"], 1);
    assert_eq!(o["players"], 3);

    // Guest's socket drops: players drops at once; in_match keeps the room
    // membership during the resume grace.
    drop(guest);
    tokio::time::sleep(Duration::from_millis(150)).await;
    let o = online(&mut host).await;
    assert_eq!(o["players"], 2);
    assert_eq!(o["in_match"], 2);

    // After the grace the guest is removed and the room returns to the lobby.
    clock.advance(31_000);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let o = online(&mut host).await;
    assert_eq!(o["in_match"], 0);
    assert_eq!(o["players"], 2);
    assert_eq!(o["spectating"], 1);
}

// #25: resuming over a still-open socket must not let the old handler tear down
// the resumed connection, and must not leak online.players.
#[tokio::test]
async fn resume_over_live_socket_takes_over() {
    let mut config = test_config();
    config.limits.list_rooms_per_second = 1000;
    let (running, clock) = start_with(config).await;
    let url = running.ws_url();

    async fn online_of(c: &mut Client) -> Value {
        c.send(json!({"type":"list_rooms"})).await;
        c.recv_type("rooms").await["online"].clone()
    }
    let hello = |cid: &str, resume: Option<&str>| {
        json!({
            "type":"hello","protocol":1,"game_version":"0.4.1","content_hash":111,
            "client_id":cid,"name":"P","resume_token":resume
        })
    };

    let mut watcher = Client::connect(&url, "W", "0.4.1", 111).await;
    let mut a = Client::hello_raw(&url, hello("a", None)).await;
    let wa = a.recv_type("welcome").await;
    let token = wa["resume_token"].as_str().unwrap().to_string();
    let a_sid = wa["session_id"].as_str().unwrap().to_string();

    // B resumes while A's socket is still open.
    let mut b = Client::hello_raw(&url, hello("b", Some(&token))).await;
    let wb = b.recv_type("welcome").await;
    assert_eq!(wb["session_id"], a_sid.as_str());

    // Two live people (watcher + the single session), not three.
    assert_eq!(online_of(&mut watcher).await["players"], 2);

    // A's old socket goes away; its stale handler must not close B.
    drop(a);
    tokio::time::sleep(Duration::from_millis(150)).await;
    b.send(json!({"type":"ping","t":1})).await;
    let pong = timeout(Duration::from_secs(2), b.recv_type("pong"))
        .await
        .expect("B stays connected after the old socket closes");
    assert_eq!(pong["t"], 1);
    assert_eq!(online_of(&mut watcher).await["players"], 2);

    // B disconnects; after the grace the count returns to the baseline.
    drop(b);
    clock.advance(31_000);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(online_of(&mut watcher).await["players"], 1);
}
