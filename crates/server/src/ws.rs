//! WebSocket lobby transport (`GET /v1/ws`).

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::connect_info::ConnectInfo;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use fighter_protocol::json as proto;
use fighter_protocol::json::{
    parse_client_message, ClientParseError, ErrorCode, LeaveReason, MAX_BINARY_FRAME,
    MAX_TEXT_FRAME,
};
use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;

use crate::app::AppState;
use crate::lobby::{version_lt, welcome_message, ConnAction, OutMsg, Session};
use crate::proxy::{resolve_client_ip, strike_ip};

const SUBPROTOCOL: &str = "3dfighter.lobby.v1";
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(20);

fn has_subprotocol(headers: &HeaderMap) -> bool {
    headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(',').any(|p| p.trim() == SUBPROTOCOL))
        .unwrap_or(false)
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if !has_subprotocol(&headers) {
        return (
            StatusCode::BAD_REQUEST,
            "missing subprotocol 3dfighter.lobby.v1",
        )
            .into_response();
    }
    let peer_ip = peer.ip();
    let client_ip = resolve_client_ip(peer_ip, &headers, &state.trusted_proxies);
    let strike = strike_ip(peer_ip, client_ip, &state.trusted_proxies);
    let (lingering_total, lingering_ip) = {
        let lobby = state.lobby.lock().expect("lobby lock");
        lobby.lingering_counts(client_ip)
    };
    if !state.try_acquire_conn(client_ip, lingering_total, lingering_ip) {
        state
            .metrics
            .connections_rejected
            .with_label_values(&["limit"])
            .inc();
        return (StatusCode::SERVICE_UNAVAILABLE, "server full").into_response();
    }
    state.metrics.connections_total.inc();
    ws.max_message_size(MAX_BINARY_FRAME)
        .max_frame_size(MAX_BINARY_FRAME)
        // Small per-connection buffers cut idle memory (issue #20).
        .read_buffer_size(state.config.limits.ws_read_buffer)
        .write_buffer_size(state.config.limits.ws_write_buffer)
        .max_write_buffer_size(state.config.limits.ws_max_write_buffer)
        .protocols([SUBPROTOCOL])
        .on_upgrade(move |socket| async move {
            handle_socket(socket, state.clone(), client_ip, strike).await;
            state.release_conn(client_ip);
        })
}

/// Read the first `hello`, skipping WebSocket control frames. Returns `None` on
/// a non-hello text frame, a close, an error, or end of stream.
async fn read_hello(stream: &mut SplitStream<WebSocket>) -> Option<proto::Hello> {
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => {
                return match parse_client_message(t.as_str()) {
                    Ok(proto::ClientEnvelope {
                        msg: proto::ClientMessage::Hello(h),
                        ..
                    }) => Some(h),
                    _ => None,
                };
            }
            Some(Ok(Message::Ping(_)))
            | Some(Ok(Message::Pong(_)))
            | Some(Ok(Message::Binary(_))) => continue,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return None,
        }
    }
}

/// Record a malformed/oversized message as a strike. Returns true if the
/// connection must be closed (too many strikes, or now banned).
fn handle_strike(state: &AppState, sid: &str, strike: Option<IpAddr>, message: &str) -> bool {
    state.metrics.malformed.inc();
    let ctx = state.ctx();
    let now = state.clock.now_ms();
    let mut lobby = state.lobby.lock().expect("lobby lock");
    lobby.send_error(sid, ErrorCode::BadMessage, message, None);
    let hash = lobby.sessions.get(sid).map(|s| s.client_id_hash.clone());
    let banned = lobby.bans.record(hash.as_deref(), strike, now);
    if lobby.note_malformed(sid, now) {
        lobby.remove_session_closing(
            sid,
            LeaveReason::Disconnected,
            &ctx,
            now,
            4000,
            "too many malformed messages",
        );
        return true;
    }
    if banned {
        lobby.remove_session_closing(
            sid,
            LeaveReason::Disconnected,
            &ctx,
            now,
            4000,
            "temporarily banned",
        );
        return true;
    }
    false
}

async fn handle_socket(
    socket: WebSocket,
    state: AppState,
    client_ip: IpAddr,
    strike: Option<IpAddr>,
) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<OutMsg>();
    let queued_bytes = Arc::new(AtomicUsize::new(0));

    let writer_queued = queued_bytes.clone();
    let writer = tokio::spawn(async move {
        // Delay the first keepalive ping by one period: an interval's first tick
        // fires immediately, which would race the client's hello with its pong.
        let mut ping =
            tokio::time::interval_at(tokio::time::Instant::now() + PING_INTERVAL, PING_INTERVAL);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(OutMsg::Text(t)) => {
                        if sink.send(Message::Text(t.into())).await.is_err() { break; }
                    }
                    Some(OutMsg::Binary(b)) => {
                        let len = b.len();
                        if sink.send(Message::Binary(b.into())).await.is_err() { break; }
                        writer_queued.fetch_sub(len, Ordering::Relaxed);
                    }
                    Some(OutMsg::Close(code, reason)) => {
                        let _ = sink
                            .send(Message::Close(Some(CloseFrame { code, reason: reason.into() })))
                            .await;
                        break;
                    }
                    None => {
                        let _ = sink.close().await;
                        break;
                    }
                },
                _ = ping.tick() => {
                    if sink.send(Message::Ping(Vec::new().into())).await.is_err() { break; }
                }
            }
        }
    });

    // The first `hello` must arrive within 10 seconds. Control frames (the
    // client's automatic Pong, its Pings) may legitimately arrive first, so they
    // are skipped rather than mistaken for the hello.
    let hello = match tokio::time::timeout(HELLO_TIMEOUT, read_hello(&mut stream)).await {
        Ok(Some(h)) => h,
        _ => {
            let _ = tx.send(OutMsg::Close(4000, "hello required".into()));
            writer.await.ok();
            return;
        }
    };

    let now = state.clock.now_ms();
    if hello.protocol != proto::PROTOCOL_VERSION {
        let _ = tx.send(OutMsg::Text(
            serde_json::to_string(&proto::ServerMessage::Error {
                code: ErrorCode::VersionUnsupported,
                message: "unsupported protocol version".into(),
                rid: None,
            })
            .unwrap_or_default(),
        ));
        let _ = tx.send(OutMsg::Close(4000, "protocol mismatch".into()));
        writer.await.ok();
        return;
    }
    if version_lt(&hello.game_version, &state.config.game.min_game_version) {
        let _ = tx.send(OutMsg::Text(
            serde_json::to_string(&proto::ServerMessage::Error {
                code: ErrorCode::VersionUnsupported,
                message: "game version too old".into(),
                rid: None,
            })
            .unwrap_or_default(),
        ));
        let _ = tx.send(OutMsg::Close(4000, "version unsupported".into()));
        writer.await.ok();
        return;
    }
    if fighter_protocol::text::is_blocked(&hello.name, &state.blocklist) {
        let _ = tx.send(OutMsg::Text(
            serde_json::to_string(&proto::ServerMessage::Error {
                code: ErrorCode::NameInvalid,
                message: "name not allowed".into(),
                rid: None,
            })
            .unwrap_or_default(),
        ));
        let _ = tx.send(OutMsg::Close(4000, "name invalid".into()));
        writer.await.ok();
        return;
    }

    let name = fighter_protocol::text::clean_name(&hello.name);
    let client_hash = crate::lobby::client_id_hash(&hello.client_id);
    let banned = {
        let lobby = state.lobby.lock().expect("lobby lock");
        lobby.bans.is_banned_client(&client_hash, now) || lobby.bans.is_banned_ip(client_ip, now)
    };
    if banned {
        let _ = tx.send(OutMsg::Text(
            serde_json::to_string(&proto::ServerMessage::Error {
                code: ErrorCode::NotAllowed,
                message: "temporarily banned".into(),
                rid: None,
            })
            .unwrap_or_default(),
        ));
        let _ = tx.send(OutMsg::Close(4000, "banned".into()));
        writer.await.ok();
        return;
    }

    // Resume an existing session if the token is valid, otherwise start one.
    let sid = {
        let resumed = if let Some(token) = hello.resume_token.clone() {
            let mut lobby = state.lobby.lock().expect("lobby lock");
            lobby.try_resume(&token, tx.clone(), queued_bytes.clone(), now, &hello)
        } else {
            None
        };
        match resumed {
            Some(sid) => {
                drop(tx);
                tracing::info!(session_id = %sid, "session resumed");
                sid
            }
            None => {
                let new_id = fighter_protocol::ids::generate_session_id();
                let session = Session::new(
                    new_id.clone(),
                    hello.client_id.clone(),
                    name.clone(),
                    hello.game_version.clone(),
                    hello.content_hash,
                    hello.relay_only.unwrap_or(false),
                    hello.region.clone(),
                    client_ip,
                    tx,
                    queued_bytes,
                    now,
                    &state.config,
                );
                let ctx = state.ctx();
                let mut lobby = state.lobby.lock().expect("lobby lock");
                // Bound lingering sessions for this client (issue #12).
                lobby.cap_lingering_for_client(&hello.client_id, &ctx, now);
                lobby.add_session(session)
            }
        }
    };

    let ctx = state.ctx();
    {
        let lobby = state.lobby.lock().expect("lobby lock");
        let welcome = {
            let session = &lobby.sessions[&sid];
            welcome_message(session, &state.config, now, ctx.udp_info(), ctx.limits())
        };
        lobby.send(&sid, &welcome);
        if let Some(room_id) = lobby.sessions[&sid].room.clone() {
            if let Some(obj) = lobby.room_object(&room_id, Some(&sid), false) {
                lobby.send(&sid, &proto::ServerMessage::RoomState { room: obj });
            }
        }
    }
    tracing::info!(
        session_id = %sid,
        name = %name,
        game_version = %hello.game_version,
        "session started"
    );

    loop {
        // Close a connection that stops answering (issue #21): a vanished client
        // behind a proxy never fails our Ping send, so rely on the missing reply.
        let incoming = match tokio::time::timeout(
            Duration::from_millis(state.config.limits.ws_pong_timeout_ms),
            stream.next(),
        )
        .await
        {
            Ok(v) => v,
            Err(_) => {
                tracing::debug!(session_id = %sid, "closing idle connection");
                break;
            }
        };
        let Some(incoming) = incoming else {
            break;
        };
        match incoming {
            Ok(Message::Text(t)) => {
                // Text frames are capped at 8 KiB by the spec; reject before
                // parsing so an oversized frame is never buffered as JSON.
                if t.len() > MAX_TEXT_FRAME {
                    if handle_strike(&state, &sid, strike, "text frame too large") {
                        break;
                    }
                    continue;
                }
                let text = t.as_str().to_string();
                let env = match parse_client_message(&text) {
                    Ok(env) => env,
                    Err(ClientParseError::UnknownType(ty)) => {
                        let lobby = state.lobby.lock().expect("lobby lock");
                        lobby.send_error(
                            &sid,
                            ErrorCode::UnknownType,
                            &format!("unknown type {ty}"),
                            None,
                        );
                        continue;
                    }
                    Err(ClientParseError::BadMessage) => {
                        if handle_strike(&state, &sid, strike, "malformed message") {
                            break;
                        }
                        continue;
                    }
                };
                let action = {
                    let ctx = state.ctx();
                    let mut lobby = state.lobby.lock().expect("lobby lock");
                    lobby.handle(&sid, env, &ctx)
                };
                if let ConnAction::Close(code, reason) = action {
                    let ctx = state.ctx();
                    let mut lobby = state.lobby.lock().expect("lobby lock");
                    lobby.remove_session_closing(
                        &sid,
                        LeaveReason::Disconnected,
                        &ctx,
                        state.clock.now_ms(),
                        code,
                        &reason,
                    );
                    break;
                }
            }
            Ok(Message::Binary(payload)) => {
                // Host spectator-feed publishing (section 8).
                let action = {
                    let ctx = state.ctx();
                    let mut lobby = state.lobby.lock().expect("lobby lock");
                    lobby.handle_binary(&sid, &payload, &ctx, state.clock.now_ms())
                };
                if let ConnAction::Close(code, reason) = action {
                    let ctx = state.ctx();
                    let mut lobby = state.lobby.lock().expect("lobby lock");
                    lobby.remove_session_closing(
                        &sid,
                        LeaveReason::Disconnected,
                        &ctx,
                        state.clock.now_ms(),
                        code,
                        &reason,
                    );
                    break;
                }
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {
                // Keepalive control frames count as activity, so a silent
                // spectator or a feed-only host is not marked idle.
                state
                    .lobby
                    .lock()
                    .expect("lobby lock")
                    .touch(&sid, state.clock.now_ms());
            }
            Ok(Message::Close(_)) => break,
            Err(_) => break,
        }
    }

    {
        // Keep the session for the reconnect grace period instead of tearing it
        // down immediately.
        let mut lobby = state.lobby.lock().expect("lobby lock");
        lobby.mark_disconnected(&sid, state.clock.now_ms());
    }
    writer.await.ok();
    tracing::info!(session_id = %sid, "session socket closed");
}
