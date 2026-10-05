//! `client-sim`: a CLI that speaks the game's wire protocol for tests and demos.
//!
//! Commands:
//!   host     --name NAME --room NAME [--spectators] [--password PW] [--ticks N]
//!   join     --name NAME --code CODE [--relay-only]
//!   spectate --name NAME --code CODE
//!   load     --matches N --spectators M --duration SECS
//!
//! `host` and `join` exchange opaque datagrams through the relay, and `host`
//! publishes the spectator feed. `spectate` re-assembles the per-tick input log
//! and prints its SHA-256; it must equal the hash `host` prints.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fighter_protocol::feed::{FeedFrame, MatchResult, MatchStart};
use fighter_protocol::udp::{BindRole, UdpDatagram};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

static SEQ: AtomicU64 = AtomicU64::new(1);

struct Args {
    positional: Vec<String>,
    flags: HashMap<String, String>,
}

impl Args {
    fn parse() -> Self {
        let mut positional = Vec::new();
        let mut flags = HashMap::new();
        let raw: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < raw.len() {
            let arg = &raw[i];
            if let Some(key) = arg.strip_prefix("--") {
                if i + 1 < raw.len() && !raw[i + 1].starts_with("--") {
                    flags.insert(key.to_string(), raw[i + 1].clone());
                    i += 2;
                } else {
                    flags.insert(key.to_string(), "true".to_string());
                    i += 1;
                }
            } else {
                positional.push(arg.clone());
                i += 1;
            }
        }
        Self { positional, flags }
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.flags.get(key).map(String::as_str)
    }

    fn get_or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.get(key).unwrap_or(default)
    }

    fn num(&self, key: &str, default: u64) -> u64 {
        self.get(key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    fn has(&self, key: &str) -> bool {
        self.flags.contains_key(key)
    }
}

#[allow(dead_code)]
struct Client {
    ws: Ws,
    session_id: String,
    resume_token: String,
    client_id: String,
}

impl Client {
    async fn connect(url: &str, name: &str, version: &str, hash: u32, relay_only: bool) -> Self {
        let mut req = url.into_client_request().expect("request");
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static("3dfighter.lobby.v1"),
        );
        let (ws, _) = connect_async(req).await.expect("ws connect");
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let client_id = format!("sim-{}-{}", std::process::id(), seq);
        let mut client = Client {
            ws,
            session_id: String::new(),
            resume_token: String::new(),
            client_id: client_id.clone(),
        };
        client
            .send_json(json!({
                "type": "hello",
                "protocol": 1,
                "game_version": version,
                "content_hash": hash,
                "client_id": client_id,
                "name": name,
                "relay_only": relay_only,
            }))
            .await;
        let welcome = client.recv_json().await;
        client.session_id = welcome["session_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        client.resume_token = welcome["resume_token"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        client
    }

    async fn send_json(&mut self, v: Value) {
        self.ws
            .send(Message::Text(v.to_string().into()))
            .await
            .expect("send json");
    }

    async fn send_binary(&mut self, bytes: Vec<u8>) {
        self.ws
            .send(Message::Binary(bytes.into()))
            .await
            .expect("send binary");
    }

    async fn recv_json(&mut self) -> Value {
        loop {
            match self.next().await {
                Message::Text(t) => return serde_json::from_str(t.as_str()).expect("json"),
                Message::Binary(_) | Message::Ping(_) | Message::Pong(_) => continue,
                Message::Close(_) => panic!("connection closed"),
                _ => continue,
            }
        }
    }

    async fn recv_json_type(&mut self, ty: &str) -> Value {
        loop {
            let v = self.recv_json().await;
            if v["type"] == ty {
                return v;
            }
        }
    }

    async fn recv_binary(&mut self) -> Vec<u8> {
        loop {
            match self.next().await {
                Message::Binary(b) => return b.to_vec(),
                Message::Text(_) | Message::Ping(_) | Message::Pong(_) => continue,
                Message::Close(_) => panic!("connection closed"),
                _ => continue,
            }
        }
    }

    async fn next(&mut self) -> Message {
        self.ws
            .next()
            .await
            .expect("socket closed")
            .expect("ws error")
    }
}

fn input_for(i: usize) -> (u16, u16) {
    let dir1 = ((i % 9) + 1) as u16;
    let buttons1 = ((i % 32) as u16) << 4;
    let dir2 = (((i * 3) % 9) + 1) as u16;
    let buttons2 = (((i * 5) % 32) as u16) << 4;
    (dir1 | buttons1, dir2 | buttons2)
}

fn hash_inputs(inputs: &[(u16, u16)]) -> String {
    let mut hasher = Sha256::new();
    for (p1, p2) in inputs {
        hasher.update(p1.to_le_bytes());
        hasher.update(p2.to_le_bytes());
    }
    hex::encode(hasher.finalize())
}

fn udp_target(welcome: &Value, fallback: &str) -> SocketAddr {
    let host = welcome["udp"]["host"].as_str().unwrap_or(fallback);
    let port = welcome["udp"]["port"].as_u64().unwrap_or(7780);
    format!("{host}:{port}").parse().expect("udp target")
}

/// Bind and keep the binding alive in the background. Returns the socket.
async fn bind_udp(server: SocketAddr, token: [u8; 16], role: BindRole) -> UdpSocket {
    let socket = UdpSocket::bind("0.0.0.0:0").await.expect("bind udp");
    let dg = UdpDatagram::Bind {
        session_token: token,
        role,
        candidates: Vec::new(),
    }
    .encode();
    socket.send_to(&dg, server).await.expect("send bind");
    let mut buf = [0u8; 64];
    let (n, _) = tokio::time::timeout(Duration::from_secs(5), socket.recv_from(&mut buf))
        .await
        .expect("BOUND timeout")
        .expect("recv bound");
    match UdpDatagram::decode(&buf[..n]).expect("decode") {
        UdpDatagram::Bound { observed } => {
            println!("bound: observed {observed}");
        }
        other => panic!("expected BOUND, got {other:?}"),
    }
    socket
}

fn token_from(v: &Value, field: &str) -> Vec<u8> {
    hex::decode(v[field].as_str().unwrap_or_default()).unwrap_or_default()
}

async fn cmd_host(args: &Args) {
    let url = args.get_or("url", "ws://127.0.0.1:8080/v1/ws");
    let name = args.get_or("name", "Lino");
    let room_name = args.get_or("room", "Lino's room");
    let ticks = args.num("ticks", 300) as usize;
    let version = args.get_or("version", "0.4.1");
    let hash = args.num("hash", 111) as u32;
    let password = args.get("password").map(str::to_string);

    let mut client = Client::connect(url, name, version, hash, false).await;
    let mut create = json!({
        "type": "create_room",
        "name": room_name,
        "visibility": "public",
        "allow_spectators": args.has("spectators"),
    });
    if let Some(pw) = password {
        create["password"] = json!(pw);
    }
    client.send_json(create).await;
    let created = client.recv_json_type("room_created").await;
    let code = created["code"].as_str().unwrap_or_default().to_string();
    println!("room code: {code}");
    if let Some(path) = args.get("code-out") {
        std::fs::write(path, &code).expect("write code");
    }

    let request = client.recv_json_type("join_request").await;
    client
        .send_json(json!({
            "type": "answer_join",
            "request_id": request["request_id"],
            "accept": true
        }))
        .await;
    let session = client.recv_json_type("match_session").await;
    let udp = udp_target(&session, "127.0.0.1:7780");
    let token = token_from(&session, "session_token");
    let token: [u8; 16] = token.as_slice().try_into().expect("token");
    let relay_key: [u8; 8] = token_from(&session, "relay_key")
        .as_slice()
        .try_into()
        .unwrap();

    let socket = bind_udp(udp, token, BindRole::Host).await;

    // Wait for peer endpoints, then publish and relay.
    let _peers = client.recv_json_type("peer_endpoints").await;

    let start = MatchStart {
        feed_version: 1,
        match_id: 1,
        stage_index: 0,
        p1_fighter_index: 0,
        p2_fighter_index: 1,
        game_version: version.to_string(),
        stage_id: "ring".into(),
        p1_fighter_id: "kenji".into(),
        p2_fighter_id: "rhea".into(),
        p1_name: name.to_string(),
        p2_name: "Lufi".into(),
    };
    client
        .send_binary(FeedFrame::MatchStart(start).encode().unwrap())
        .await;

    let all_inputs: Vec<(u16, u16)> = (0..ticks).map(input_for).collect();
    let mut buf = [0u8; 2048];
    let mut first = 0usize;
    while first < ticks {
        let count = 6.min(ticks - first);
        let batch = &all_inputs[first..first + count];
        // Publish the confirmed inputs.
        client
            .send_binary(
                FeedFrame::Inputs {
                    match_id: 1,
                    first_tick: first as u32,
                    inputs: batch.to_vec(),
                }
                .encode()
                .unwrap(),
            )
            .await;
        // Send one relayed datagram per tick and verify the echo.
        for (i, (p1, p2)) in batch.iter().enumerate() {
            let payload = [p1.to_le_bytes(), p2.to_le_bytes()].concat();
            let dg = UdpDatagram::Relay {
                relay_key,
                payload: payload.clone(),
            }
            .encode();
            socket.send_to(&dg, udp).await.expect("relay send");
            if let Ok(Ok((n, _))) =
                tokio::time::timeout(Duration::from_millis(500), socket.recv_from(&mut buf)).await
            {
                if let Ok(UdpDatagram::Relayed { payload: echo }) = UdpDatagram::decode(&buf[..n]) {
                    assert_eq!(echo, payload, "relay altered tick {}", first + i);
                }
            }
        }
        if first % 60 == 0 {
            client
                .send_binary(
                    FeedFrame::Checksum {
                        match_id: 1,
                        tick: first as u32,
                        checksum: (first as u32).wrapping_mul(2654435761),
                    }
                    .encode()
                    .unwrap(),
                )
                .await;
        }
        first += count;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    client
        .send_binary(
            FeedFrame::MatchEnd {
                match_id: 1,
                final_tick: ticks as u32,
                result: MatchResult::P1Won,
                p1_wins: 2,
                p2_wins: 0,
            }
            .encode()
            .unwrap(),
        )
        .await;

    println!("host input-log sha256: {}", hash_inputs(&all_inputs));
    tokio::time::sleep(Duration::from_millis(1500)).await;
}

async fn cmd_join(args: &Args) {
    let url = args.get_or("url", "ws://127.0.0.1:8080/v1/ws");
    let name = args.get_or("name", "Lufi");
    let code = args.get_or("code", "");
    let version = args.get_or("version", "0.4.1");
    let hash = args.num("hash", 111) as u32;

    let mut client = Client::connect(url, name, version, hash, args.has("relay-only")).await;
    client
        .send_json(json!({"type":"join_room","room":code}))
        .await;
    let session = client.recv_json_type("match_session").await;
    let udp = udp_target(&session, "127.0.0.1:7780");
    let token: [u8; 16] = token_from(&session, "session_token")
        .as_slice()
        .try_into()
        .unwrap();
    let relay_key: [u8; 8] = token_from(&session, "relay_key")
        .as_slice()
        .try_into()
        .unwrap();
    let socket = bind_udp(udp, token, BindRole::Guest).await;
    let _peers = client.recv_json_type("peer_endpoints").await;

    // Echo relayed datagrams back for a while.
    let mut buf = [0u8; 2048];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.num("duration", 30));
    let mut count = 0u64;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await {
            Ok(Ok((n, from))) => {
                if let Ok(UdpDatagram::Relayed { payload }) = UdpDatagram::decode(&buf[..n]) {
                    let dg = UdpDatagram::Relay { relay_key, payload }.encode();
                    let _ = socket.send_to(&dg, from).await;
                    count += 1;
                }
            }
            _ => break,
        }
    }
    println!("guest relayed {count} datagrams");
}

async fn cmd_spectate(args: &Args) {
    let url = args.get_or("url", "ws://127.0.0.1:8080/v1/ws");
    let name = args.get_or("name", "Spec");
    let code = args.get_or("code", "");
    let version = args.get_or("version", "0.4.1");
    let hash = args.num("hash", 111) as u32;

    let mut client = Client::connect(url, name, version, hash, false).await;
    client
        .send_json(json!({"type":"spectate","room":code}))
        .await;
    client.recv_json_type("spectate_started").await;

    let mut inputs: Vec<(u16, u16)> = Vec::new();
    loop {
        let bytes = client.recv_binary().await;
        match FeedFrame::decode(&bytes).expect("feed frame") {
            FeedFrame::MatchStart(_) => {}
            FeedFrame::Inputs {
                first_tick,
                inputs: batch,
                ..
            } => {
                assert_eq!(first_tick as usize, inputs.len(), "contiguous ticks");
                inputs.extend(batch);
            }
            FeedFrame::Checksum { .. } => {}
            FeedFrame::MatchEnd { final_tick, .. } => {
                println!("spectator ticks: {final_tick}");
                break;
            }
            FeedFrame::FeedReset { .. } => {
                println!("feed reset");
                inputs.clear();
            }
        }
    }
    println!("spectator input-log sha256: {}", hash_inputs(&inputs));
}

async fn cmd_load(args: &Args) {
    let url = args.get_or("url", "ws://127.0.0.1:8080/v1/ws");
    let matches = args.num("matches", 10) as usize;
    let spectators = args.num("spectators", 0) as usize;
    let duration = args.num("duration", 30);
    let version = args.get_or("version", "0.4.1").to_string();
    let hash = args.num("hash", 111) as u32;

    let started = std::time::Instant::now();
    let ok = Arc::new(AtomicU64::new(0));
    let codes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let mut tasks = Vec::new();
    for m in 0..matches {
        let url = url.to_string();
        let version = version.clone();
        let ok = ok.clone();
        let codes = codes.clone();
        tasks.push(tokio::spawn(async move {
            if run_load_match(&url, &version, hash, m, duration, codes).await {
                ok.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for _ in 0..spectators {
        let url = url.to_string();
        let version = version.clone();
        let codes = codes.clone();
        tasks.push(tokio::spawn(async move {
            run_spectator(&url, &version, hash, codes, duration).await;
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    let elapsed = started.elapsed();
    println!(
        "load: {} matches started, {} completed, {} spectators, {:.1}s",
        matches,
        ok.load(Ordering::Relaxed),
        spectators,
        elapsed.as_secs_f64()
    );
}

async fn run_spectator(
    url: &str,
    version: &str,
    hash: u32,
    codes: Arc<Mutex<Vec<String>>>,
    duration: u64,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(duration);
    let mut total_ticks = 0u64;
    while tokio::time::Instant::now() < deadline {
        let code = { codes.lock().await.pop() };
        let Some(code) = code else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let mut client = Client::connect(url, "Spec", version, hash, false).await;
        client
            .send_json(json!({"type":"spectate","room":code}))
            .await;
        if client.recv_json_type("spectate_started").await["type"] != "spectate_started" {
            continue;
        }
        loop {
            if tokio::time::Instant::now() >= deadline {
                return;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, client.recv_binary()).await {
                Ok(bytes) => match FeedFrame::decode(&bytes)
                    .unwrap_or(FeedFrame::FeedReset { match_id: 0 })
                {
                    FeedFrame::Inputs { inputs, .. } => total_ticks += inputs.len() as u64,
                    FeedFrame::MatchEnd { .. } | FeedFrame::FeedReset { .. } => break,
                    _ => {}
                },
                Err(_) => return,
            }
        }
    }
    let _ = total_ticks;
}

async fn run_load_match(
    url: &str,
    version: &str,
    hash: u32,
    index: usize,
    duration: u64,
    codes: Arc<Mutex<Vec<String>>>,
) -> bool {
    let mut host = Client::connect(url, "Host", version, hash, false).await;
    host.send_json(json!({
        "type":"create_room","visibility":"public","allow_spectators":true
    }))
    .await;
    let created = host.recv_json_type("room_created").await;
    let code = created["code"].as_str().unwrap_or_default().to_string();
    codes.lock().await.push(code.clone());

    let mut guest = Client::connect(url, "Guest", version, hash, false).await;
    guest
        .send_json(json!({"type":"join_room","room":code}))
        .await;
    let request = host.recv_json_type("join_request").await;
    host.send_json(json!({
        "type":"answer_join","request_id":request["request_id"],"accept":true
    }))
    .await;
    let _ = host.recv_json_type("match_session").await;
    let _ = guest.recv_json_type("match_session").await;

    // Publish a short feed so spectators have something to fan out.
    let _ = host
        .send_binary(
            FeedFrame::MatchStart(MatchStart {
                feed_version: 1,
                match_id: index as u32 + 1,
                stage_index: 0,
                p1_fighter_index: 0,
                p2_fighter_index: 1,
                game_version: version.to_string(),
                stage_id: "ring".into(),
                p1_fighter_id: "kenji".into(),
                p2_fighter_id: "rhea".into(),
                p1_name: "Host".into(),
                p2_name: "Guest".into(),
            })
            .encode()
            .unwrap(),
        )
        .await;

    let ticks = (duration * 60) as usize;
    let mut first = 0usize;
    while first < ticks {
        // Six ticks every 100 ms: a realistic 60 ticks/s feed.
        let count = 6.min(ticks - first);
        let inputs: Vec<(u16, u16)> = (first..first + count).map(input_for).collect();
        host.send_binary(
            FeedFrame::Inputs {
                match_id: index as u32 + 1,
                first_tick: first as u32,
                inputs,
            }
            .encode()
            .unwrap(),
        )
        .await;
        first += count;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = host
        .send_binary(
            FeedFrame::MatchEnd {
                match_id: index as u32 + 1,
                final_tick: ticks as u32,
                result: MatchResult::P1Won,
                p1_wins: 2,
                p2_wins: 0,
            }
            .encode()
            .unwrap(),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    true
}

fn main() {
    let args = Args::parse();
    let command = args
        .positional
        .first()
        .cloned()
        .or_else(|| args.get("command").map(str::to_string));
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    match command.as_deref() {
        Some("host") => runtime.block_on(cmd_host(&args)),
        Some("join") => runtime.block_on(cmd_join(&args)),
        Some("spectate") => runtime.block_on(cmd_spectate(&args)),
        Some("load") => runtime.block_on(cmd_load(&args)),
        _ => print_usage(),
    }
}

fn print_usage() {
    println!(
        "client-sim (3D Fighter server test client)\n\
         \n\
         Commands:\n\
           host     --name NAME --room NAME [--spectators] [--password PW] [--ticks N] [--code-out FILE]\n\
           join     --name NAME --code CODE [--relay-only] [--duration SECS]\n\
           spectate --name NAME --code CODE\n\
           load     --matches N --spectators M --duration SECS\n\
         \n\
         Common: --url ws://host:port/v1/ws --version 0.4.1 --hash N"
    );
}
