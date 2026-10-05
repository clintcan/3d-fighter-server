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
use fighter_protocol::json::{parse_client_message, ClientParseError, ErrorCode, LeaveReason};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;

use crate::app::AppState;
use crate::lobby::{version_lt, welcome_message, ConnAction, OutMsg, Session};

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
    let ip = peer.ip();
    if !state.try_acquire_conn(ip) {
        state
            .metrics
            .connections_rejected
            .with_label_values(&["limit"])
            .inc();
        return (StatusCode::SERVICE_UNAVAILABLE, "server full").into_response();
    }
    state.metrics.connections_total.inc();
    ws.protocols([SUBPROTOCOL])
        .on_upgrade(move |socket| async move {
            handle_socket(socket, state.clone(), ip).await;
            state.release_conn(ip);
        })
}

async fn handle_socket(socket: WebSocket, state: AppState, ip: IpAddr) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<OutMsg>();
    let queued_bytes = Arc::new(AtomicUsize::new(0));

    let writer_queued = queued_bytes.clone();
    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(PING_INTERVAL);
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

    // The first message must be `hello`, within 10 seconds.
    let hello = match tokio::time::timeout(HELLO_TIMEOUT, stream.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => match parse_client_message(t.as_str()) {
            Ok(proto::ClientEnvelope {
                msg: proto::ClientMessage::Hello(h),
                ..
            }) => h,
            _ => {
                let _ = tx.send(OutMsg::Close(4000, "hello required".into()));
                writer.await.ok();
                return;
            }
        },
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
        lobby.bans.is_banned_client(&client_hash, now) || lobby.bans.is_banned_ip(ip, now)
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
                    tx,
                    queued_bytes,
                    now,
                    &state.config,
                );
                let mut lobby = state.lobby.lock().expect("lobby lock");
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
        let incoming = stream.next().await;
        let Some(incoming) = incoming else {
            break;
        };
        match incoming {
            Ok(Message::Text(t)) => {
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
                        state.metrics.malformed.inc();
                        let ctx = state.ctx();
                        let mut lobby = state.lobby.lock().expect("lobby lock");
                        lobby.send_error(&sid, ErrorCode::BadMessage, "malformed message", None);
                        let hash = lobby.sessions.get(&sid).map(|s| s.client_id_hash.clone());
                        let banned =
                            lobby
                                .bans
                                .record(hash.as_deref(), Some(ip), state.clock.now_ms());
                        if lobby.note_malformed(&sid, state.clock.now_ms()) {
                            lobby.remove_session_closing(
                                &sid,
                                LeaveReason::Disconnected,
                                &ctx,
                                state.clock.now_ms(),
                                4000,
                                "too many malformed messages",
                            );
                            break;
                        }
                        if banned {
                            lobby.remove_session_closing(
                                &sid,
                                LeaveReason::Disconnected,
                                &ctx,
                                state.clock.now_ms(),
                                4000,
                                "temporarily banned",
                            );
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
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
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
