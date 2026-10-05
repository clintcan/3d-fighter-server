//! Shared helpers for the server integration tests.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fighter_protocol::clock::TestClock;
use fighter_server::config::Config;
use fighter_server::serve::{self, Running};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

pub type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

static CLIENT_SEQ: AtomicU64 = AtomicU64::new(1);

pub fn test_config() -> Config {
    let mut c = Config::default();
    c.server.http_bind = "127.0.0.1:0".into();
    c.server.udp_bind = "127.0.0.1:0".into();
    c.server.public_udp_host = "127.0.0.1".into();
    c.server.region = "test".into();
    // Tests drive the injected clock; keep long-lived state alive while they do.
    c.limits.session_silent_ms = 3_600_000;
    c.limits.binding_expiry_ms = 3_600_000;
    c.limits.binding_max_age_ms = 24 * 3_600_000;
    c.limits.default_spectator_delay_ms = 100;
    c.limits.spectator_max_queued_bytes = 65536;
    // Feed rate limiting is exercised separately; tests publish in bursts.
    c.limits.feed_frames_per_second = 1_000_000;
    c
}

pub async fn start_test() -> (Running, Arc<TestClock>) {
    start_with(test_config()).await
}

pub async fn start_with(config: Config) -> (Running, Arc<TestClock>) {
    let clock = Arc::new(TestClock::new(1_000_000));
    let running = serve::start(config, clock.clone())
        .await
        .expect("start server");
    (running, clock)
}

/// Minimal HTTP request helper for the admin/replay endpoints.
pub async fn http_request(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("http connect");
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(body);
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");
    let text = String::from_utf8_lossy(&buf).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

pub struct Client {
    pub ws: Ws,
    pub session_id: String,
    pub resume_token: String,
}

impl Client {
    pub async fn connect(url: &str, name: &str, version: &str, hash: u32) -> Self {
        Self::connect_opts(url, name, version, hash, false).await
    }

    pub async fn connect_opts(
        url: &str,
        name: &str,
        version: &str,
        hash: u32,
        relay_only: bool,
    ) -> Self {
        let mut req = url.into_client_request().expect("request");
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static("3dfighter.lobby.v1"),
        );
        let (ws, _resp) = connect_async(req).await.expect("ws connect");
        let mut client = Client {
            ws,
            session_id: String::new(),
            resume_token: String::new(),
        };
        let seq = CLIENT_SEQ.fetch_add(1, Ordering::SeqCst);
        client
            .send(json!({
                "type": "hello",
                "protocol": 1,
                "game_version": version,
                "content_hash": hash,
                "client_id": format!("client-{seq}"),
                "name": name,
                "relay_only": relay_only,
            }))
            .await;
        let welcome = client.recv_type("welcome").await;
        client.session_id = welcome["session_id"].as_str().unwrap().to_string();
        client.resume_token = welcome["resume_token"].as_str().unwrap().to_string();
        client
    }

    /// Open a connection and send `hello` without waiting for a welcome, so the
    /// caller can inspect the first server message.
    pub async fn hello_raw(url: &str, hello: Value) -> Self {
        let mut req = url.into_client_request().expect("request");
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static("3dfighter.lobby.v1"),
        );
        let (ws, _resp) = connect_async(req).await.expect("ws connect");
        let mut client = Client {
            ws,
            session_id: String::new(),
            resume_token: String::new(),
        };
        client.send(hello).await;
        client
    }

    pub async fn send(&mut self, v: Value) {
        self.ws
            .send(Message::Text(v.to_string().into()))
            .await
            .expect("send");
    }

    pub async fn send_binary(&mut self, bytes: Vec<u8>) {
        self.ws
            .send(Message::Binary(bytes.into()))
            .await
            .expect("send binary");
    }

    /// Read the next binary frame, skipping text control messages.
    pub async fn recv_binary(&mut self) -> Vec<u8> {
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(3), self.ws.next())
                .await
                .expect("recv timeout")
                .expect("socket closed")
                .expect("ws error");
            match msg {
                Message::Binary(b) => return b.to_vec(),
                Message::Text(_) | Message::Ping(_) | Message::Pong(_) => continue,
                Message::Close(_) => panic!("closed unexpectedly"),
                _ => continue,
            }
        }
    }

    pub async fn recv(&mut self) -> Value {
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

    pub async fn recv_type(&mut self, ty: &str) -> Value {
        loop {
            let v = self.recv().await;
            if v["type"] == ty {
                return v;
            }
        }
    }
}

/// Connect host and guest, create/join/accept, and return both clients plus
/// the raw session tokens and relay keys from their `match_session` messages.
pub struct Pair {
    pub host: Client,
    pub guest: Client,
    pub host_token: [u8; 16],
    pub host_key: [u8; 8],
    pub guest_token: [u8; 16],
    pub guest_key: [u8; 8],
    pub room_id: String,
    pub code: String,
}

pub async fn pair_up(url: &str, host_relay_only: bool, guest_relay_only: bool) -> Pair {
    let mut host = Client::connect_opts(url, "Lino", "0.4.1", 111, host_relay_only).await;
    host.send(json!({"type":"create_room","visibility":"public","allow_spectators":true}))
        .await;
    let code = host.recv_type("room_created").await["code"]
        .as_str()
        .unwrap()
        .to_string();

    let mut guest = Client::connect_opts(url, "Lufi", "0.4.1", 111, guest_relay_only).await;
    guest.send(json!({"type":"join_room","room":code})).await;
    guest.recv_type("join_pending").await;
    let request = host.recv_type("join_request").await;
    host.send(json!({
        "type":"answer_join","request_id":request["request_id"],"accept":true
    }))
    .await;

    let host_session = host.recv_type("match_session").await;
    let guest_session = guest.recv_type("match_session").await;
    let room_id = host_session["room_id"].as_str().unwrap().to_string();

    Pair {
        host,
        guest,
        host_token: decode16(&host_session["session_token"]),
        host_key: decode8(&host_session["relay_key"]),
        guest_token: decode16(&guest_session["session_token"]),
        guest_key: decode8(&guest_session["relay_key"]),
        room_id,
        code,
    }
}

pub fn decode16(v: &Value) -> [u8; 16] {
    let bytes = hex::decode(v.as_str().unwrap()).unwrap();
    bytes.try_into().unwrap()
}

pub fn decode8(v: &Value) -> [u8; 8] {
    let bytes = hex::decode(v.as_str().unwrap()).unwrap();
    bytes.try_into().unwrap()
}
