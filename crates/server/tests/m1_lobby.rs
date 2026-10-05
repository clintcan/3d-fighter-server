//! M1 acceptance tests: the whole lobby flow over a real WebSocket.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fighter_protocol::clock::TestClock;
use fighter_server::config::Config;
use fighter_server::serve::{self, Running};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

static CLIENT_SEQ: AtomicU64 = AtomicU64::new(1);

fn test_config() -> Config {
    let mut c = Config::default();
    c.server.http_bind = "127.0.0.1:0".into();
    c.server.udp_bind = "127.0.0.1:0".into();
    c.server.region = "test".into();
    c
}

async fn start_test() -> (Running, Arc<TestClock>) {
    let clock = Arc::new(TestClock::new(1_000_000));
    let running = serve::start(test_config(), clock.clone())
        .await
        .expect("start server");
    (running, clock)
}

struct Client {
    ws: Ws,
}

impl Client {
    async fn connect(url: &str, name: &str, version: &str, hash: u32) -> Self {
        let mut req = url.into_client_request().expect("request");
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static("3dfighter.lobby.v1"),
        );
        let (ws, _resp) = connect_async(req).await.expect("ws connect");
        let mut client = Client { ws };
        let seq = CLIENT_SEQ.fetch_add(1, Ordering::SeqCst);
        client
            .send(json!({
                "type": "hello",
                "protocol": 1,
                "game_version": version,
                "content_hash": hash,
                "client_id": format!("client-{seq}"),
                "name": name,
            }))
            .await;
        let welcome = client.recv_type("welcome").await;
        assert_eq!(welcome["protocol"], 1);
        assert_eq!(welcome["region"], "test");
        client
    }

    async fn send(&mut self, v: Value) {
        self.ws
            .send(Message::Text(v.to_string().into()))
            .await
            .expect("send");
    }

    async fn recv(&mut self) -> Value {
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(3), self.ws.next())
                .await
                .expect("recv timeout")
                .expect("socket closed")
                .expect("ws error");
            match msg {
                Message::Text(t) => return serde_json::from_str(t.as_str()).expect("json"),
                Message::Ping(_) | Message::Pong(_) => continue,
                Message::Close(_) => panic!("closed unexpectedly"),
                _ => continue,
            }
        }
    }

    async fn recv_type(&mut self, ty: &str) -> Value {
        loop {
            let v = self.recv().await;
            if v["type"] == ty {
                return v;
            }
        }
    }
}

async fn http_get(addr: SocketAddr, path: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("http connect");
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");
    String::from_utf8_lossy(&buf).to_string()
}

#[tokio::test]
async fn create_list_find_by_code_join_accept() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut host = Client::connect(&url, "Lino", "0.4.1", 111).await;
    host.send(json!({
        "type": "create_room",
        "visibility": "public",
        "allow_spectators": true
    }))
    .await;
    let created = host.recv_type("room_created").await;
    let code = created["code"].as_str().unwrap().to_string();
    assert_eq!(code.len(), 6);
    assert_eq!(created["room"]["status"], "open");
    assert_eq!(created["room"]["players"].as_array().unwrap().len(), 1);

    let mut guest = Client::connect(&url, "Lufi", "0.4.1", 111).await;
    guest.send(json!({"type": "list_rooms"})).await;
    let listed = guest.recv_type("rooms").await;
    assert_eq!(listed["rooms"].as_array().unwrap().len(), 1);

    guest.send(json!({"type": "join_room", "room": code})).await;
    guest.recv_type("join_pending").await;
    let request = host.recv_type("join_request").await;
    assert_eq!(request["name"], "Lufi");
    assert_eq!(request["client_id_hash"].as_str().unwrap().len(), 8);

    host.send(json!({
        "type": "answer_join",
        "request_id": request["request_id"],
        "accept": true
    }))
    .await;

    let host_session = host.recv_type("match_session").await;
    assert_eq!(host_session["role"], "host");
    assert_eq!(host_session["peer"]["name"], "Lufi");
    assert_eq!(host_session["session_token"].as_str().unwrap().len(), 32);
    assert_eq!(host_session["relay_key"].as_str().unwrap().len(), 16);

    let guest_session = guest.recv_type("match_session").await;
    assert_eq!(guest_session["role"], "guest");
    assert_eq!(guest_session["peer"]["name"], "Lino");

    let state = host.recv_type("room_state").await;
    assert_eq!(state["room"]["status"], "full");
    assert_eq!(state["room"]["players"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn version_mismatch_cannot_join() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut host = Client::connect(&url, "Lino", "0.4.1", 111).await;
    host.send(json!({"type":"create_room","visibility":"public","allow_spectators":false}))
        .await;
    let created = host.recv_type("room_created").await;
    let code = created["code"].as_str().unwrap().to_string();

    let mut guest = Client::connect(&url, "Lufi", "0.4.1", 222).await;
    guest.send(json!({"type":"join_room","room":code})).await;
    let err = guest.recv_type("error").await;
    assert_eq!(err["code"], "version_mismatch");
}

#[tokio::test]
async fn unlisted_room_never_listed() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut host = Client::connect(&url, "Lino", "0.4.1", 111).await;
    host.send(json!({"type":"create_room","visibility":"unlisted","allow_spectators":false}))
        .await;
    let created = host.recv_type("room_created").await;
    // The host (a member) sees the code; a public listing must not.
    assert!(created["room"]["code"].is_string());

    let mut guest = Client::connect(&url, "Lufi", "0.4.1", 111).await;
    guest.send(json!({"type":"list_rooms"})).await;
    let listed = guest.recv_type("rooms").await;
    assert!(listed["rooms"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn wrong_password_refused() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut host = Client::connect(&url, "Lino", "0.4.1", 111).await;
    host.send(json!({
        "type":"create_room","visibility":"public","allow_spectators":false,"password":"abcd"
    }))
    .await;
    let created = host.recv_type("room_created").await;
    assert!(created["room"]["has_password"].as_bool().unwrap());
    let code = created["code"].as_str().unwrap().to_string();

    let mut guest = Client::connect(&url, "Lufi", "0.4.1", 111).await;
    guest
        .send(json!({"type":"join_room","room":code,"password":"nope"}))
        .await;
    assert_eq!(guest.recv_type("error").await["code"], "wrong_password");

    guest
        .send(json!({"type":"join_room","room":code,"password":"abcd"}))
        .await;
    guest.recv_type("join_pending").await;
}

#[tokio::test]
async fn decline_sets_cooldown() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut host = Client::connect(&url, "Lino", "0.4.1", 111).await;
    host.send(json!({"type":"create_room","visibility":"public","allow_spectators":false}))
        .await;
    let code = host.recv_type("room_created").await["code"]
        .as_str()
        .unwrap()
        .to_string();

    let mut guest = Client::connect(&url, "Lufi", "0.4.1", 111).await;
    guest.send(json!({"type":"join_room","room":code})).await;
    guest.recv_type("join_pending").await;
    let request = host.recv_type("join_request").await;
    host.send(json!({
        "type":"answer_join","request_id":request["request_id"],"accept":false,"reason":"no"
    }))
    .await;
    let declined = guest.recv_type("join_declined").await;
    assert_eq!(declined["reason"], "no");

    // Same client cannot request again during the cooldown.
    guest.send(json!({"type":"join_room","room":code})).await;
    let declined = guest.recv_type("join_declined").await;
    assert_eq!(declined["reason"], "declined");
}

#[tokio::test]
async fn cancel_join_notifies_host() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut host = Client::connect(&url, "Lino", "0.4.1", 111).await;
    host.send(json!({"type":"create_room","visibility":"public","allow_spectators":false}))
        .await;
    let code = host.recv_type("room_created").await["code"]
        .as_str()
        .unwrap()
        .to_string();

    let mut guest = Client::connect(&url, "Lufi", "0.4.1", 111).await;
    guest.send(json!({"type":"join_room","room":code})).await;
    guest.recv_type("join_pending").await;
    host.recv_type("join_request").await;
    guest.send(json!({"type":"cancel_join"})).await;
    let cancelled = host.recv_type("join_cancelled").await;
    assert!(cancelled["request_id"].is_string());
}

#[tokio::test]
async fn kick_reopens_room() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let (mut host, mut guest, _code) = paired_room(&url).await;
    host.send(json!({"type":"kick","reason":"bye"})).await;
    let left = guest.recv_type("player_left").await;
    assert_eq!(left["reason"], "kicked");
    assert_eq!(left["role"], "guest");
    let state = host.recv_type("room_state").await;
    assert_eq!(state["room"]["status"], "open");
}

#[tokio::test]
async fn host_leave_closes_room() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let (mut host, mut guest, _code) = paired_room(&url).await;
    host.send(json!({"type":"leave_room"})).await;
    let closed = guest.recv_type("room_closed").await;
    assert_eq!(closed["reason"], "host_left");
}

#[tokio::test]
async fn unknown_type_and_bad_message() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut client = Client::connect(&url, "Lino", "0.4.1", 111).await;
    client.send(json!({"type":"teleport"})).await;
    assert_eq!(client.recv_type("error").await["code"], "unknown_type");

    client
        .ws
        .send(Message::Text("not json".into()))
        .await
        .unwrap();
    assert_eq!(client.recv_type("error").await["code"], "bad_message");
}

#[tokio::test]
async fn already_in_room_and_room_not_found() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut host = Client::connect(&url, "Lino", "0.4.1", 111).await;
    host.send(json!({"type":"create_room","visibility":"public","allow_spectators":false}))
        .await;
    host.recv_type("room_created").await;
    host.send(json!({"type":"create_room","visibility":"public","allow_spectators":false}))
        .await;
    assert_eq!(host.recv_type("error").await["code"], "already_in_room");

    let mut guest = Client::connect(&url, "Lufi", "0.4.1", 111).await;
    guest
        .send(json!({"type":"join_room","room":"ZZZZZZ"}))
        .await;
    assert_eq!(guest.recv_type("error").await["code"], "room_not_found");
}

#[tokio::test]
async fn room_busy_when_request_pending() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut host = Client::connect(&url, "Lino", "0.4.1", 111).await;
    host.send(json!({"type":"create_room","visibility":"public","allow_spectators":false}))
        .await;
    let code = host.recv_type("room_created").await["code"]
        .as_str()
        .unwrap()
        .to_string();

    let mut first = Client::connect(&url, "First", "0.4.1", 111).await;
    first.send(json!({"type":"join_room","room":code})).await;
    first.recv_type("join_pending").await;
    host.recv_type("join_request").await;

    let mut second = Client::connect(&url, "Second", "0.4.1", 111).await;
    second.send(json!({"type":"join_room","room":code})).await;
    assert_eq!(second.recv_type("error").await["code"], "room_busy");
}

#[tokio::test]
async fn join_request_times_out() {
    let (running, clock) = start_test().await;
    let url = running.ws_url();

    let mut host = Client::connect(&url, "Lino", "0.4.1", 111).await;
    host.send(json!({"type":"create_room","visibility":"public","allow_spectators":false}))
        .await;
    let code = host.recv_type("room_created").await["code"]
        .as_str()
        .unwrap()
        .to_string();

    let mut guest = Client::connect(&url, "Lufi", "0.4.1", 111).await;
    guest.send(json!({"type":"join_room","room":code})).await;
    guest.recv_type("join_pending").await;

    clock.advance(31_000);
    let declined = guest.recv_type("join_declined").await;
    assert_eq!(declined["reason"], "timeout");
}

#[tokio::test]
async fn ping_pong_echoes_request_id() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();

    let mut client = Client::connect(&url, "Lino", "0.4.1", 111).await;
    client.send(json!({"type":"ping","t":42,"rid":"abc"})).await;
    let pong = client.recv_type("pong").await;
    assert_eq!(pong["t"], 42);
    assert_eq!(pong["rid"], "abc");
}

#[tokio::test]
async fn http_endpoints_work() {
    let (running, _clock) = start_test().await;

    let health = http_get(running.addr, "/healthz").await;
    assert!(health.contains("200"), "health response: {health}");
    assert!(health.trim_end().ends_with("ok"));

    let metrics = http_get(running.addr, "/metrics").await;
    assert!(metrics.contains("fighter_ws_connections"));

    let rooms = http_get(running.addr, "/v1/rooms").await;
    assert!(rooms.contains("\"rooms\""));
}

#[tokio::test]
async fn missing_subprotocol_rejected() {
    let (running, _clock) = start_test().await;
    let url = running.ws_url();
    let req = url.into_client_request().unwrap();
    let result = connect_async(req).await;
    assert!(result.is_err(), "upgrade without subprotocol should fail");
}

async fn paired_room(url: &str) -> (Client, Client, String) {
    let mut host = Client::connect(url, "Lino", "0.4.1", 111).await;
    host.send(json!({"type":"create_room","visibility":"public","allow_spectators":false}))
        .await;
    let code = host.recv_type("room_created").await["code"]
        .as_str()
        .unwrap()
        .to_string();

    let mut guest = Client::connect(url, "Lufi", "0.4.1", 111).await;
    guest
        .send(json!({"type":"join_room","room":code.clone()}))
        .await;
    guest.recv_type("join_pending").await;
    let request = host.recv_type("join_request").await;
    host.send(json!({
        "type":"answer_join","request_id":request["request_id"],"accept":true
    }))
    .await;
    host.recv_type("match_session").await;
    guest.recv_type("match_session").await;
    // Drain the room_state broadcast that follows acceptance.
    host.recv_type("room_state").await;
    guest.recv_type("room_state").await;
    (host, guest, code)
}
