//! HTTP endpoints: `/healthz`, `/metrics`, `/v1/rooms`, `/v1/regions`,
//! `/v1/replays` and the authenticated `/admin/*` API (section 9).

use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use fighter_protocol::json::{Severity, Visibility};
use serde_json::{json, Value};
use subtle::ConstantTimeEq;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::timeout::TimeoutLayer;

use crate::app::AppState;
use crate::ws::ws_handler;

pub fn router(state: AppState) -> Router {
    let timeout = Duration::from_millis(state.config.limits.http_timeout_ms);
    let concurrency = state.config.limits.http_max_concurrency;
    Router::new()
        .route("/v1/ws", get(ws_handler))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/v1/rooms", get(rooms))
        .route("/v1/regions", get(regions))
        .route("/v1/replays", get(replays_list))
        .route("/v1/replays/{id}", get(replay_get))
        .route("/admin/notice", post(admin_notice))
        .route("/admin/rooms", get(admin_rooms))
        .route("/admin/rooms/{id}/close", post(admin_close))
        .route("/admin/ban", post(admin_ban))
        // Bound how long a request may take and how many run at once (#10).
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            timeout,
        ))
        .layer(ConcurrencyLimitLayer::new(concurrency))
        .with_state(state)
}

async fn healthz(State(state): State<AppState>) -> Response {
    if state.shutting_down.load(Ordering::SeqCst) {
        (StatusCode::SERVICE_UNAVAILABLE, "shutting down").into_response()
    } else {
        (StatusCode::OK, "ok").into_response()
    }
}

async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    // When an admin token is configured, /metrics requires it too (issue #10).
    if state.admin_token.is_some() && admin_ok(&state, &headers).is_some() {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(),
    )
        .into_response()
}

/// Public room list, no codes, cached for two seconds.
async fn rooms(State(state): State<AppState>) -> Response {
    let now = state.clock.now_ms();
    if let Some((cached_at, body)) = state.rooms_cache.lock().expect("cache lock").as_ref() {
        if now.saturating_sub(*cached_at) < 2_000 {
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                body.clone(),
            )
                .into_response();
        }
    }

    let body = {
        let lobby = state.lobby.lock().expect("lobby lock");
        let mut ids: Vec<String> = lobby
            .rooms
            .values()
            .filter(|r| r.visibility == Visibility::Public)
            .map(|r| r.id.clone())
            .collect();
        ids.sort_by(|a, b| {
            let ra = &lobby.rooms[a];
            let rb = &lobby.rooms[b];
            (ra.created_ms, &ra.id).cmp(&(rb.created_ms, &rb.id))
        });
        let rooms: Vec<_> = ids
            .into_iter()
            .filter_map(|id| lobby.room_object(&id, None, true))
            .collect();
        serde_json::json!({ "rooms": rooms, "online": lobby.online() }).to_string()
    };
    *state.rooms_cache.lock().expect("cache lock") = Some((now, body.clone()));
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

/// Region discovery: the configured region list, or this server as the sole
/// region when none is configured.
async fn regions(State(state): State<AppState>) -> Response {
    let regions: Vec<Value> = if state.config.regions.is_empty() {
        let udp_port = state
            .config
            .server
            .udp_bind
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .unwrap_or(7780);
        vec![json!({
            "region": state.config.server.region,
            "ws_url": "",
            "udp_host": state.config.server.public_udp_host,
            "udp_port": udp_port,
        })]
    } else {
        state
            .config
            .regions
            .iter()
            .map(|r| {
                json!({
                    "region": r.region,
                    "ws_url": r.ws_url,
                    "udp_host": r.udp_host,
                    "udp_port": r.udp_port,
                })
            })
            .collect()
    };
    Json(json!({ "regions": regions })).into_response()
}

async fn replays_list(State(state): State<AppState>) -> Response {
    let store = state.replays.lock().expect("replays");
    if !store.enabled() {
        return (StatusCode::NOT_FOUND, "replays disabled").into_response();
    }
    let (replays, next_cursor) = store.list(50, None);
    Json(json!({ "replays": replays, "next_cursor": next_cursor })).into_response()
}

async fn replay_get(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    // Stream the file rather than reading it all into memory (issue #10).
    let path = {
        let store = state.replays.lock().expect("replays");
        store.path(&id)
    };
    let Some(path) = path else {
        return (StatusCode::NOT_FOUND, "no such replay").into_response();
    };
    match tokio::fs::File::open(&path).await {
        Ok(file) => {
            let stream = tokio_util::io::ReaderStream::new(file);
            let body = axum::body::Body::from_stream(stream);
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/vnd.3dfighter.replay")],
                body,
            )
                .into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "no such replay").into_response(),
    }
}

/// `None` when authorized, otherwise the error response to return.
fn admin_ok(state: &AppState, headers: &HeaderMap) -> Option<Response> {
    let Some(token) = &state.admin_token else {
        return Some((StatusCode::NOT_FOUND, "admin disabled").into_response());
    };
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match provided {
        Some(p) if bool::from(p.as_bytes().ct_eq(token.as_bytes())) => None,
        _ => Some((StatusCode::UNAUTHORIZED, "unauthorized").into_response()),
    }
}

async fn admin_notice(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(e) = admin_ok(&state, &headers) {
        return e;
    }
    let message = body["message"]
        .as_str()
        .unwrap_or("")
        .chars()
        .take(256)
        .collect::<String>();
    let severity = match body["severity"].as_str() {
        Some("warning") => Severity::Warning,
        _ => Severity::Info,
    };
    state
        .lobby
        .lock()
        .expect("lobby lock")
        .broadcast_notice(&message, severity);
    StatusCode::OK.into_response()
}

async fn admin_rooms(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(e) = admin_ok(&state, &headers) {
        return e;
    }
    let rooms = state.lobby.lock().expect("lobby lock").admin_rooms();
    Json(json!({ "rooms": rooms })).into_response()
}

async fn admin_close(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Some(e) = admin_ok(&state, &headers) {
        return e;
    }
    let ctx = state.ctx();
    let now = state.clock.now_ms();
    let closed = state
        .lobby
        .lock()
        .expect("lobby lock")
        .admin_close_room(&id, &ctx, now);
    if closed {
        StatusCode::OK.into_response()
    } else {
        (StatusCode::NOT_FOUND, "no such room").into_response()
    }
}

async fn admin_ban(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(e) = admin_ok(&state, &headers) {
        return e;
    }
    let minutes = body["minutes"].as_u64().unwrap_or(10);
    let hash = body["client_id_hash"].as_str();
    let ip: Option<IpAddr> = body["ip"].as_str().and_then(|s| s.parse().ok());
    let now = state.clock.now_ms();
    state
        .lobby
        .lock()
        .expect("lobby lock")
        .bans
        .ban_for(hash, ip, minutes, now);
    StatusCode::OK.into_response()
}
