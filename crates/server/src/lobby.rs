//! In-memory lobby: sessions, rooms, and the message handlers (section 6).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use fighter_protocol::clock::Clock;
use fighter_protocol::feed::FeedFrame;
use fighter_protocol::ids::{
    generate_bytes, generate_hex_token, generate_room_code, generate_room_id,
};
use fighter_protocol::json as proto;
use fighter_protocol::json::{
    AnswerJoin, CloseReason, ConnectionPath, ConnectionReport, CreateRoom, Emote, ErrorCode,
    JoinRoom, Kick, LeaveReason, ListRooms, ListStatus, PeerInfo, Phase, PlayerSlot, Role,
    Room as RoomObject, RoomStatus, RoomUpdate, ServerMessage, Spectate, UdpInfo, Visibility,
    PROTOCOL_VERSION,
};
use fighter_protocol::ratelimit::TokenBucket;
use fighter_protocol::udp::BindRole;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::mpsc::UnboundedSender;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::moderation::Bans;
use crate::relay::{candidates_for, Bindings};
use crate::replays::{ReplayJob, ReplayMeta, ReplayStore};
use crate::spectate::{MatchLog, SpectatorState, MAX_FEED_TICKS};

/// Server -> client push.
#[allow(dead_code)] // Binary and Close are used from M3 and on.
#[derive(Debug)]
pub enum OutMsg {
    Text(String),
    Binary(Vec<u8>),
    Close(u16, String),
}

/// What the WebSocket task should do after a handler.
#[allow(dead_code)] // Close is produced by M4 hardening paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnAction {
    None,
    Close(u16, String),
}

/// Borrowed handler context.
pub struct Ctx<'a> {
    pub config: &'a Config,
    pub metrics: &'a Metrics,
    pub clock: &'a dyn Clock,
    pub blocklist: &'a [String],
}

impl Ctx<'_> {
    pub fn udp_info(&self) -> UdpInfo {
        let port = self
            .config
            .server
            .udp_bind
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .unwrap_or(7780);
        UdpInfo {
            host: self.config.server.public_udp_host.clone(),
            port,
        }
    }

    pub fn limits(&self) -> proto::Limits {
        proto::Limits {
            max_room_name: self.config.limits.max_room_name,
            max_spectators: self.config.limits.max_spectators,
            reaction_interval_ms: self.config.limits.reaction_interval_ms,
        }
    }
}

pub struct Session {
    pub id: String,
    pub client_id: String,
    pub client_id_hash: String,
    pub name: String,
    pub game_version: String,
    pub content_hash: u32,
    pub relay_only: bool,
    #[allow(dead_code)] // used for region discovery in M5
    pub region: Option<String>,
    /// Resolved client address (peer, or forwarded behind a trusted proxy).
    pub client_ip: std::net::IpAddr,
    pub out: Option<UnboundedSender<OutMsg>>,
    pub room: Option<String>,
    pub role: Option<Role>,
    pub last_seen_ms: u64,
    pub disconnected_at: Option<u64>,
    pub resume_token: String,
    pub session_token: [u8; 16],
    pub relay_key: [u8; 8],
    pub queued_bytes: Arc<AtomicUsize>,
    pub spectator: Option<SpectatorState>,
    feed_bucket: TokenBucket,
    msg_bucket: TokenBucket,
    list_bucket: TokenBucket,
    create_join_bucket: TokenBucket,
    react_bucket: TokenBucket,
    malformed: VecDeque<u64>,
}

impl Session {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        client_id: String,
        name: String,
        game_version: String,
        content_hash: u32,
        relay_only: bool,
        region: Option<String>,
        client_ip: std::net::IpAddr,
        out: UnboundedSender<OutMsg>,
        queued_bytes: Arc<AtomicUsize>,
        now: u64,
        config: &Config,
    ) -> Self {
        let l = &config.limits;
        Self {
            id,
            client_id_hash: client_id_hash(&client_id),
            client_id,
            name,
            game_version,
            content_hash,
            relay_only,
            region,
            client_ip,
            out: Some(out),
            room: None,
            role: None,
            last_seen_ms: now,
            disconnected_at: None,
            resume_token: generate_hex_token(16),
            session_token: generate_bytes::<16>(),
            relay_key: generate_bytes::<8>(),
            queued_bytes,
            spectator: None,
            feed_bucket: TokenBucket::new(
                l.feed_frames_per_second.saturating_mul(2),
                l.feed_frames_per_second as f64,
            ),
            msg_bucket: TokenBucket::new(l.message_burst, l.messages_per_second as f64),
            list_bucket: TokenBucket::new(
                l.list_rooms_per_second.saturating_mul(2).max(1),
                l.list_rooms_per_second as f64,
            ),
            create_join_bucket: TokenBucket::new(
                l.create_join_per_minute.max(1),
                l.create_join_per_minute as f64 / 60.0,
            ),
            react_bucket: TokenBucket::new(1, 1000.0 / l.reaction_interval_ms.max(1) as f64),
            malformed: VecDeque::new(),
        }
    }

    pub fn session_token_hex(&self) -> String {
        hex::encode(self.session_token)
    }

    pub fn relay_key_hex(&self) -> String {
        hex::encode(self.relay_key)
    }
}

pub struct PendingJoin {
    pub request_id: String,
    pub guest: String,
    pub expires_ms: u64,
}

pub struct Room {
    pub id: String,
    pub code: String,
    pub name: String,
    pub visibility: Visibility,
    pub password_hash: Option<[u8; 32]>,
    pub host: String,
    pub guest: Option<String>,
    pub pending: Option<PendingJoin>,
    pub phase: Phase,
    pub fighters: [Option<String>; 2],
    pub stage: Option<String>,
    pub round: Option<u8>,
    pub wins: [u8; 2],
    pub timer: Option<u8>,
    pub allow_spectators: bool,
    pub spectator_delay_ms: u32,
    pub max_spectators: u32,
    pub spectators: Vec<String>,
    pub connection: Option<ConnectionPath>,
    pub created_ms: u64,
    pub last_activity_ms: u64,
    pub host_game_version: String,
    pub host_content_hash: u32,
    /// Per-match shared secret sent in `match_session`; never logged or exposed
    /// anywhere else.
    pub pair_secret: Option<String>,
    pub decline_cooldowns: HashMap<String, u64>,
    pub log: MatchLog,
    /// Guest's copy of the feed, used for M5 verification.
    pub guest_log: MatchLog,
    pub feed_verified: bool,
    pub matches_played: u32,
    pub spectator_peak: u32,
    pub last_replay_ms: u64,
    /// Players this room currently contributes to `online.in_match`.
    pub in_match_counted: usize,
    dirty: bool,
    next_broadcast_ms: u64,
}

impl Room {
    pub fn status(&self) -> RoomStatus {
        if self.phase == Phase::InMatch {
            RoomStatus::InMatch
        } else if self.guest.is_some() {
            RoomStatus::Full
        } else {
            RoomStatus::Open
        }
    }

    fn touch(&mut self, now: u64) {
        self.last_activity_ms = now;
    }
}

pub struct Lobby {
    pub sessions: HashMap<String, Session>,
    pub rooms: HashMap<String, Room>,
    pub codes: HashMap<String, String>,
    pub bindings: Arc<Mutex<Bindings>>,
    pub bans: Bans,
    pub resume_index: HashMap<String, String>,
    pub queue: VecDeque<QueueEntry>,
    pub replays: Arc<Mutex<ReplayStore>>,
    replay_tx: UnboundedSender<ReplayJob>,
    /// Running total of match-log bytes across all rooms, for the global budget.
    pub log_bytes: usize,
    /// Aggregate lobby activity counters, maintained incrementally so `welcome`
    /// and `list_rooms` are O(1) (issue #24).
    pub online_players: usize,
    pub online_in_match: usize,
    pub online_spectating: usize,
    pub online_rooms: usize,
    #[allow(dead_code)] // surfaced by admin/metrics in M5
    pub started_ms: u64,
    pub region: String,
}

/// A client waiting in the quick-match queue (section 12, M5).
pub struct QueueEntry {
    pub session_id: String,
    pub mode: String,
    pub region: String,
    pub joined_ms: u64,
}

impl Lobby {
    pub fn new(
        started_ms: u64,
        region: String,
        bans: Bans,
        replays: Arc<Mutex<ReplayStore>>,
        replay_tx: UnboundedSender<ReplayJob>,
        bindings: Arc<Mutex<Bindings>>,
    ) -> Self {
        Self {
            sessions: HashMap::new(),
            rooms: HashMap::new(),
            codes: HashMap::new(),
            bindings,
            bans,
            resume_index: HashMap::new(),
            queue: VecDeque::new(),
            replays,
            replay_tx,
            log_bytes: 0,
            online_players: 0,
            online_in_match: 0,
            online_spectating: 0,
            online_rooms: 0,
            started_ms,
            region,
        }
    }

    // -- output helpers ---------------------------------------------------

    pub fn send_rid(&self, sid: &str, msg: &ServerMessage, rid: Option<&str>) {
        let Some(sess) = self.sessions.get(sid) else {
            return;
        };
        let mut value = match serde_json::to_value(msg) {
            Ok(v) => v,
            Err(_) => return,
        };
        if let Some(r) = rid {
            if let Some(obj) = value.as_object_mut() {
                obj.insert("rid".to_string(), serde_json::Value::String(r.to_string()));
            }
        }
        if let Some(out) = &sess.out {
            if let Ok(text) = serde_json::to_string(&value) {
                let _ = out.send(OutMsg::Text(text));
            }
        }
    }

    pub fn send(&self, sid: &str, msg: &ServerMessage) {
        self.send_rid(sid, msg, None);
    }

    /// Register a new session and index its resume token.
    pub fn add_session(&mut self, session: Session) -> String {
        let id = session.id.clone();
        self.online_players = self.online_players.saturating_add(1);
        self.resume_index
            .insert(session.resume_token.clone(), id.clone());
        self.sessions.insert(id.clone(), session);
        id
    }

    /// Keep a session alive for the reconnect grace period after its socket
    /// closes. The room, role and tokens are preserved.
    pub fn mark_disconnected(&mut self, sid: &str, now: u64) {
        if let Some(sess) = self.sessions.get_mut(sid) {
            if sess.disconnected_at.is_none() {
                self.online_players = self.online_players.saturating_sub(1);
            }
            sess.out = None;
            sess.disconnected_at = Some(now);
        }
    }

    /// Record any frame received from a client as activity (section 6.1).
    pub fn touch(&mut self, sid: &str, now: u64) {
        if let Some(sess) = self.sessions.get_mut(sid) {
            sess.last_seen_ms = now;
        }
    }

    /// The aggregate lobby activity object (issue #24). O(1): reads counters.
    pub fn online(&self) -> proto::Online {
        proto::Online {
            players: self.online_players as u32,
            in_match: self.online_in_match as u32,
            spectating: self.online_spectating as u32,
            rooms: self.online_rooms as u32,
        }
    }

    /// Recompute a room's contribution to `online.in_match` after its status or
    /// membership changed. Membership-based (independent of socket liveness).
    fn sync_room_in_match(&mut self, room_id: &str) {
        let desired = match self.rooms.get(room_id) {
            Some(room) if room.status() == RoomStatus::InMatch => {
                1 + u32::from(room.guest.is_some())
            }
            _ => 0,
        } as usize;
        let prev = match self.rooms.get(room_id) {
            Some(room) => room.in_match_counted,
            None => return,
        };
        if prev == desired {
            return;
        }
        if let Some(room) = self.rooms.get_mut(room_id) {
            room.in_match_counted = desired;
        }
        if desired >= prev {
            self.online_in_match = self.online_in_match.saturating_add(desired - prev);
        } else {
            self.online_in_match = self.online_in_match.saturating_sub(prev - desired);
        }
    }

    /// Disconnected sessions still kept for reconnection: (total, for this IP).
    pub fn lingering_counts(&self, ip: std::net::IpAddr) -> (usize, usize) {
        let mut total = 0;
        let mut per_ip = 0;
        for s in self.sessions.values() {
            if s.disconnected_at.is_some() {
                total += 1;
                if s.client_ip == ip {
                    per_ip += 1;
                }
            }
        }
        (total, per_ip)
    }

    /// Keep at most `max_lingering_sessions_per_client` disconnected sessions
    /// per client, dropping the oldest (issue #12).
    pub fn cap_lingering_for_client(&mut self, client_id: &str, ctx: &Ctx, now: u64) {
        let max = ctx.config.limits.max_lingering_sessions_per_client;
        loop {
            let mut lingering: Vec<(u64, String)> = self
                .sessions
                .values()
                .filter(|s| s.client_id == client_id && s.disconnected_at.is_some())
                .map(|s| (s.disconnected_at.unwrap_or(0), s.id.clone()))
                .collect();
            if lingering.len() <= max {
                break;
            }
            lingering.sort();
            let oldest = lingering[0].1.clone();
            self.remove_session(&oldest, LeaveReason::Disconnected, ctx, now);
        }
    }

    /// Adopt a disconnected session when a new connection presents its resume
    /// token. Returns the session id.
    pub fn try_resume(
        &mut self,
        token: &str,
        out: UnboundedSender<OutMsg>,
        queued_bytes: Arc<AtomicUsize>,
        now: u64,
        hello: &proto::Hello,
    ) -> Option<String> {
        let Lobby {
            sessions,
            resume_index,
            online_players,
            ..
        } = self;
        let sid = resume_index.get(token).cloned()?;
        let sess = sessions.get_mut(&sid)?;
        resume_index.remove(token);
        sess.resume_token = generate_hex_token(16);
        resume_index.insert(sess.resume_token.clone(), sid.clone());
        sess.out = Some(out);
        sess.queued_bytes = queued_bytes;
        sess.disconnected_at = None;
        sess.last_seen_ms = now;
        sess.name = fighter_protocol::text::clean_name(&hello.name);
        sess.game_version = hello.game_version.clone();
        sess.content_hash = hello.content_hash;
        sess.relay_only = hello.relay_only.unwrap_or(false);
        sess.region = hello.region.clone();
        *online_players = online_players.saturating_add(1);
        Some(sid)
    }

    pub fn active_matches(&self) -> usize {
        self.rooms.values().filter(|r| r.guest.is_some()).count()
    }

    /// Send a server notice to every connected session.
    pub fn broadcast_notice(&self, message: &str, severity: proto::Severity) {
        let msg = ServerMessage::ServerNotice {
            message: message.to_string(),
            severity,
        };
        for sid in self.sessions.keys() {
            self.send(sid, &msg);
        }
    }

    /// Full room list for the admin API, including codes.
    pub fn admin_rooms(&self) -> Vec<RoomObject> {
        let mut ids: Vec<String> = self.rooms.keys().cloned().collect();
        ids.sort();
        ids.into_iter()
            .filter_map(|id| {
                let mut obj = self.room_object(&id, None, true)?;
                obj.code = Some(self.rooms[&id].code.clone());
                Some(obj)
            })
            .collect()
    }

    /// Close a room from the admin API.
    pub fn admin_close_room(&mut self, room_id: &str, ctx: &Ctx, now: u64) -> bool {
        if self.rooms.contains_key(room_id) {
            self.close_room(room_id, CloseReason::Admin, ctx, now);
            true
        } else {
            false
        }
    }

    pub fn send_error(&self, sid: &str, code: ErrorCode, message: &str, rid: Option<&str>) {
        self.send(
            sid,
            &ServerMessage::Error {
                code,
                message: message.to_string(),
                rid: rid.map(str::to_string),
            },
        );
    }

    // -- object building --------------------------------------------------

    pub fn room_object(
        &self,
        room_id: &str,
        viewer: Option<&str>,
        force_no_code: bool,
    ) -> Option<RoomObject> {
        let room = self.rooms.get(room_id)?;
        let is_member = viewer.is_some_and(|v| {
            v == room.host
                || room.guest.as_deref() == Some(v)
                || room.spectators.iter().any(|s| s == v)
        });
        let compatible = match viewer.and_then(|v| self.sessions.get(v)) {
            Some(s) => {
                s.game_version == room.host_game_version && s.content_hash == room.host_content_hash
            }
            None => false,
        };
        let code = if force_no_code || !(is_member || room.visibility == Visibility::Public) {
            None
        } else {
            Some(room.code.clone())
        };

        let host_name = self
            .sessions
            .get(&room.host)
            .map(|s| s.name.clone())
            .unwrap_or_else(|| "Player".to_string());
        let mut players = vec![PlayerSlot {
            role: Role::Host,
            name: host_name.clone(),
            fighter: room.fighters[0].clone(),
        }];
        if let Some(guest) = &room.guest {
            let guest_name = self
                .sessions
                .get(guest)
                .map(|s| s.name.clone())
                .unwrap_or_else(|| "Player".to_string());
            players.push(PlayerSlot {
                role: Role::Guest,
                name: guest_name,
                fighter: room.fighters[1].clone(),
            });
        }

        let wins = if room.phase == Phase::Lobby && room.wins == [0, 0] {
            None
        } else {
            Some(room.wins)
        };

        Some(RoomObject {
            id: room.id.clone(),
            code,
            name: room.name.clone(),
            host_name,
            visibility: room.visibility,
            has_password: room.password_hash.is_some(),
            status: room.status(),
            phase: room.phase,
            game_version: room.host_game_version.clone(),
            content_hash: room.host_content_hash,
            compatible,
            players,
            stage: room.stage.clone(),
            round: room.round,
            wins,
            spectators: room.spectators.len() as u32,
            allow_spectators: room.allow_spectators,
            max_spectators: room.max_spectators,
            spectator_delay_ms: room.spectator_delay_ms,
            connection: room.connection,
            feed_verified: room.feed_verified,
            stats: Some(proto::RoomStats {
                matches: room.matches_played,
                rematches: room.matches_played.saturating_sub(1),
                spectator_peak: room.spectator_peak,
            }),
            region: self.region.clone(),
            created_at: room.created_ms,
        })
    }

    pub fn broadcast_room_state(&self, room_id: &str) {
        let Some(room) = self.rooms.get(room_id) else {
            return;
        };
        let mut ids = vec![room.host.clone()];
        if let Some(g) = &room.guest {
            ids.push(g.clone());
        }
        ids.extend(room.spectators.iter().cloned());
        for id in ids {
            if let Some(obj) = self.room_object(room_id, Some(&id), false) {
                self.send(&id, &ServerMessage::RoomState { room: obj });
            }
        }
    }

    fn update_room_metrics(&self, metrics: &Metrics) {
        let mut open = 0i64;
        let mut full = 0i64;
        let mut in_match = 0i64;
        for room in self.rooms.values() {
            match room.status() {
                RoomStatus::Open => open += 1,
                RoomStatus::Full => full += 1,
                RoomStatus::InMatch => in_match += 1,
            }
        }
        metrics.rooms.with_label_values(&["open"]).set(open);
        metrics.rooms.with_label_values(&["full"]).set(full);
        metrics.rooms.with_label_values(&["in_match"]).set(in_match);
    }

    // -- dispatch ---------------------------------------------------------

    pub fn note_malformed(&mut self, sid: &str, now: u64) -> bool {
        let Some(sess) = self.sessions.get_mut(sid) else {
            return false;
        };
        sess.malformed.push_back(now);
        while let Some(front) = sess.malformed.front() {
            if now.saturating_sub(*front) > 60_000 {
                sess.malformed.pop_front();
            } else {
                break;
            }
        }
        sess.malformed.len() >= 10
    }

    pub fn handle(&mut self, sid: &str, env: proto::ClientEnvelope, ctx: &Ctx) -> ConnAction {
        let proto::ClientEnvelope { rid, msg } = env;
        let now = ctx.clock.now_ms();

        let allowed = match self.sessions.get_mut(sid) {
            Some(s) => {
                s.last_seen_ms = now;
                s.msg_bucket.try_acquire(now)
            }
            None => return ConnAction::None,
        };
        if !allowed {
            ctx.metrics.rate_limited.inc();
            self.send_error(
                sid,
                ErrorCode::RateLimited,
                "too many messages",
                rid.as_deref(),
            );
            return ConnAction::None;
        }
        ctx.metrics
            .messages
            .with_label_values(&[msg.type_name()])
            .inc();

        match msg {
            proto::ClientMessage::Hello(_) => {
                self.send_error(
                    sid,
                    ErrorCode::NotAllowed,
                    "hello already received",
                    rid.as_deref(),
                );
                ConnAction::None
            }
            proto::ClientMessage::Ping(p) => {
                self.send_rid(
                    sid,
                    &ServerMessage::Pong {
                        t: p.t,
                        server_time: now,
                    },
                    rid.as_deref(),
                );
                ConnAction::None
            }
            proto::ClientMessage::ListRooms(m) => {
                self.handle_list_rooms(sid, m, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::CreateRoom(m) => {
                self.handle_create_room(sid, m, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::JoinRoom(m) => {
                self.handle_join_room(sid, m, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::CancelJoin(_) => {
                self.handle_cancel_join(sid, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::AnswerJoin(m) => {
                self.handle_answer_join(sid, m, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::Kick(m) => {
                self.handle_kick(sid, m, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::LeaveRoom(_) => {
                self.handle_leave(sid, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::RoomUpdate(m) => {
                self.handle_room_update(sid, m, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::ConnectionReport(m) => {
                self.handle_connection_report(sid, m, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::Spectate(m) => {
                self.handle_spectate(sid, m, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::StopSpectating(_) => {
                self.handle_stop_spectating(sid, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::React(m) => {
                self.handle_react(sid, m, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::QueueJoin(m) => {
                self.handle_queue_join(sid, m, rid.as_deref(), ctx, now);
                ConnAction::None
            }
            proto::ClientMessage::QueueLeave(_) => {
                self.handle_queue_leave(sid);
                ConnAction::None
            }
        }
    }

    // -- handlers ---------------------------------------------------------

    fn handle_list_rooms(
        &mut self,
        sid: &str,
        m: ListRooms,
        rid: Option<&str>,
        ctx: &Ctx,
        now: u64,
    ) {
        let allowed = self
            .sessions
            .get_mut(sid)
            .map(|s| s.list_bucket.try_acquire(now))
            .unwrap_or(false);
        if !allowed {
            ctx.metrics.rate_limited.inc();
            self.send_error(sid, ErrorCode::RateLimited, "list_rooms too frequent", rid);
            return;
        }
        let (viewer_version, viewer_hash) = match self.sessions.get(sid) {
            Some(s) => (s.game_version.clone(), s.content_hash),
            None => return,
        };
        let status = m.status_or_default();
        let compatible_only = m.compatible_only_or_default();
        let spectatable = m.spectatable.unwrap_or(false);
        let limit = m.limit_or_default() as usize;
        let offset: usize = m
            .cursor
            .as_deref()
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);

        let mut ids: Vec<String> = self
            .rooms
            .values()
            .filter(|r| r.visibility == Visibility::Public)
            .filter(|r| match status {
                ListStatus::Any => true,
                ListStatus::Open => r.status() == RoomStatus::Open,
                ListStatus::InMatch => r.status() == RoomStatus::InMatch,
            })
            .filter(|r| !spectatable || r.allow_spectators)
            .filter(|r| {
                !compatible_only
                    || (r.host_game_version == viewer_version && r.host_content_hash == viewer_hash)
            })
            .map(|r| r.id.clone())
            .collect();
        ids.sort_by(|a, b| {
            let ra = &self.rooms[a];
            let rb = &self.rooms[b];
            (ra.created_ms, &ra.id).cmp(&(rb.created_ms, &rb.id))
        });

        let total = ids.len();
        let page: Vec<RoomObject> = ids
            .into_iter()
            .skip(offset)
            .take(limit)
            .filter_map(|id| self.room_object(&id, Some(sid), false))
            .collect();
        let next_cursor = if offset + limit < total {
            Some((offset + limit).to_string())
        } else {
            None
        };
        self.send_rid(
            sid,
            &ServerMessage::Rooms {
                rooms: page,
                online: Some(self.online()),
                next_cursor,
            },
            rid,
        );
    }

    fn handle_create_room(
        &mut self,
        sid: &str,
        m: CreateRoom,
        rid: Option<&str>,
        ctx: &Ctx,
        now: u64,
    ) {
        if !self.sessions.contains_key(sid) {
            return;
        }
        if self
            .sessions
            .get(sid)
            .and_then(|s| s.room.as_ref())
            .is_some()
        {
            self.send_error(sid, ErrorCode::AlreadyInRoom, "already in a room", rid);
            return;
        }
        let allowed = self
            .sessions
            .get_mut(sid)
            .map(|s| s.create_join_bucket.try_acquire(now))
            .unwrap_or(false);
        if !allowed {
            ctx.metrics.rate_limited.inc();
            self.send_error(sid, ErrorCode::RateLimited, "create_room too frequent", rid);
            return;
        }
        if self.rooms.len() >= ctx.config.limits.max_rooms {
            self.send_error(sid, ErrorCode::ServerFull, "room limit reached", rid);
            return;
        }

        let host_name = self.sessions[sid].name.clone();
        let raw_name = m.name.as_deref().unwrap_or("");
        if !raw_name.is_empty() && fighter_protocol::text::is_blocked(raw_name, ctx.blocklist) {
            self.send_error(sid, ErrorCode::NameInvalid, "room name not allowed", rid);
            return;
        }
        let name = fighter_protocol::text::clean_room_name(raw_name, &host_name);

        let password_hash = match m.password.as_deref() {
            Some(p) => {
                let len = p.chars().count();
                if !(4..=32).contains(&len) {
                    self.send_error(
                        sid,
                        ErrorCode::BadMessage,
                        "password must be 4-32 characters",
                        rid,
                    );
                    return;
                }
                Some(hash_password(p))
            }
            None => None,
        };

        let delay = m
            .spectator_delay_ms
            .unwrap_or(ctx.config.limits.default_spectator_delay_ms)
            .min(10_000);
        let max_spectators = m
            .max_spectators
            .unwrap_or(50)
            .min(ctx.config.limits.max_spectators);

        let (id, code) = self.spawn_room(
            sid,
            name,
            m.visibility,
            password_hash,
            m.allow_spectators,
            delay,
            max_spectators,
            ctx,
            now,
        );

        if let Some(obj) = self.room_object(&id, Some(sid), false) {
            self.send_rid(sid, &ServerMessage::RoomCreated { room: obj, code }, rid);
        }
    }

    /// Create a room and make `host` its host. Shared by `create_room` and
    /// quick match.
    #[allow(clippy::too_many_arguments)]
    fn spawn_room(
        &mut self,
        host: &str,
        name: String,
        visibility: Visibility,
        password_hash: Option<[u8; 32]>,
        allow_spectators: bool,
        delay: u32,
        max_spectators: u32,
        ctx: &Ctx,
        now: u64,
    ) -> (String, String) {
        let id = loop {
            let candidate = generate_room_id();
            if !self.rooms.contains_key(&candidate) {
                break candidate;
            }
        };
        let code = loop {
            let candidate = generate_room_code();
            if !self.codes.contains_key(&candidate) {
                break candidate;
            }
        };
        let (game_version, content_hash) = {
            let s = &self.sessions[host];
            (s.game_version.clone(), s.content_hash)
        };
        let room = Room {
            id: id.clone(),
            code: code.clone(),
            name,
            visibility,
            password_hash,
            host: host.to_string(),
            guest: None,
            pending: None,
            phase: Phase::Lobby,
            fighters: [None, None],
            stage: None,
            round: None,
            wins: [0, 0],
            timer: None,
            allow_spectators,
            spectator_delay_ms: delay,
            max_spectators,
            spectators: Vec::new(),
            connection: None,
            created_ms: now,
            last_activity_ms: now,
            host_game_version: game_version,
            host_content_hash: content_hash,
            pair_secret: None,
            decline_cooldowns: HashMap::new(),
            log: MatchLog::default(),
            guest_log: MatchLog::default(),
            feed_verified: true,
            matches_played: 0,
            spectator_peak: 0,
            last_replay_ms: 0,
            in_match_counted: 0,
            dirty: false,
            next_broadcast_ms: 0,
        };
        self.codes.insert(code.clone(), id.clone());
        self.rooms.insert(id.clone(), room);
        self.online_rooms = self.online_rooms.saturating_add(1);
        if let Some(s) = self.sessions.get_mut(host) {
            s.room = Some(id.clone());
            s.role = Some(Role::Host);
        }
        ctx.metrics.rooms_created_total.inc();
        self.update_room_metrics(ctx.metrics);
        (id, code)
    }

    fn resolve_room(&self, key: &str) -> Option<String> {
        if self.rooms.contains_key(key) {
            return Some(key.to_string());
        }
        self.codes.get(key).cloned()
    }

    fn handle_join_room(&mut self, sid: &str, m: JoinRoom, rid: Option<&str>, ctx: &Ctx, now: u64) {
        let Some(sess) = self.sessions.get(sid) else {
            return;
        };
        if sess.room.is_some() {
            self.send_error(sid, ErrorCode::AlreadyInRoom, "already in a room", rid);
            return;
        }
        let allowed = self
            .sessions
            .get_mut(sid)
            .map(|s| s.create_join_bucket.try_acquire(now))
            .unwrap_or(false);
        if !allowed {
            ctx.metrics.rate_limited.inc();
            self.send_error(sid, ErrorCode::RateLimited, "join_room too frequent", rid);
            return;
        }

        let Some(room_id) = self.resolve_room(&m.room) else {
            self.send_error(sid, ErrorCode::RoomNotFound, "room not found", rid);
            return;
        };
        let (client_id, viewer_version, viewer_hash) = {
            let s = &self.sessions[sid];
            (s.client_id.clone(), s.game_version.clone(), s.content_hash)
        };

        {
            let room = &self.rooms[&room_id];
            if room.host_game_version != viewer_version || room.host_content_hash != viewer_hash {
                self.send_error(
                    sid,
                    ErrorCode::VersionMismatch,
                    "room needs a different game version or data",
                    rid,
                );
                return;
            }
            if room.guest.is_some() {
                self.send_error(sid, ErrorCode::RoomFull, "room is full", rid);
                return;
            }
            if room.pending.is_some() {
                self.send_error(sid, ErrorCode::RoomBusy, "a join request is pending", rid);
                return;
            }
            if let Some(until) = room.decline_cooldowns.get(&client_id) {
                if *until > now {
                    self.send(
                        sid,
                        &ServerMessage::JoinDeclined {
                            room_id: room_id.clone(),
                            reason: "declined".into(),
                        },
                    );
                    return;
                }
            }
            if let Some(hash) = room.password_hash {
                let ok = m
                    .password
                    .as_deref()
                    .map(|p| ct_eq(&hash_password(p), &hash))
                    .unwrap_or(false);
                if !ok {
                    self.send_error(sid, ErrorCode::WrongPassword, "wrong password", rid);
                    return;
                }
            }
        }

        let request_id = generate_hex_token(8);
        let expires = now + ctx.config.limits.join_request_timeout_ms;
        let room = self.rooms.get_mut(&room_id).expect("room exists");
        room.pending = Some(PendingJoin {
            request_id: request_id.clone(),
            guest: sid.to_string(),
            expires_ms: expires,
        });
        room.touch(now);

        let guest_name = self.sessions[sid].name.clone();
        let guest_hash = self.sessions[sid].client_id_hash.clone();
        let host = self.rooms[&room_id].host.clone();
        ctx.metrics.joins.with_label_values(&["requested"]).inc();

        self.send_rid(
            sid,
            &ServerMessage::JoinPending {
                room_id: room_id.clone(),
            },
            rid,
        );
        self.send(
            &host,
            &ServerMessage::JoinRequest {
                request_id,
                name: guest_name,
                client_id_hash: guest_hash,
            },
        );
    }

    fn handle_cancel_join(&mut self, sid: &str, rid: Option<&str>, ctx: &Ctx, now: u64) {
        let room_id = self.find_pending_room_for_guest(sid);
        let Some(room_id) = room_id else {
            self.send_error(sid, ErrorCode::NotAllowed, "no pending join", rid);
            return;
        };
        let host = self.rooms[&room_id].host.clone();
        if let Some(room) = self.rooms.get_mut(&room_id) {
            if let Some(p) = room.pending.take() {
                room.touch(now);
                self.send(
                    &host,
                    &ServerMessage::JoinCancelled {
                        request_id: p.request_id,
                    },
                );
            }
        }
        ctx.metrics.joins.with_label_values(&["cancelled"]).inc();
        self.broadcast_room_state(&room_id);
    }

    fn find_pending_room_for_guest(&self, sid: &str) -> Option<String> {
        self.rooms
            .values()
            .find(|r| r.pending.as_ref().is_some_and(|p| p.guest == sid))
            .map(|r| r.id.clone())
    }

    fn handle_answer_join(
        &mut self,
        sid: &str,
        m: AnswerJoin,
        rid: Option<&str>,
        ctx: &Ctx,
        now: u64,
    ) {
        let Some(room_id) = self
            .rooms
            .values()
            .find(|r| r.host == sid)
            .map(|r| r.id.clone())
        else {
            self.send_error(sid, ErrorCode::NotAllowed, "not a host", rid);
            return;
        };
        let pending = match self.rooms[&room_id].pending.as_ref() {
            Some(p) if p.request_id == m.request_id => p,
            _ => {
                self.send_error(sid, ErrorCode::NotAllowed, "no matching join request", rid);
                return;
            }
        };
        let guest = pending.guest.clone();
        let guest_client_id = self.sessions.get(&guest).map(|s| s.client_id.clone());

        if m.accept {
            let room = self.rooms.get_mut(&room_id).expect("room exists");
            room.pending = None;
            room.guest = Some(guest.clone());
            // A fresh per-match secret for the direct-path guest check (issue #16).
            let pair_secret = generate_hex_token(16);
            room.pair_secret = Some(pair_secret.clone());
            room.touch(now);
            let host = room.host.clone();

            let (
                host_name,
                host_token,
                host_relay,
                host_relay_only,
                host_token_bytes,
                host_key_bytes,
            ) = {
                let h = &self.sessions[&host];
                (
                    h.name.clone(),
                    h.session_token_hex(),
                    h.relay_key_hex(),
                    h.relay_only,
                    h.session_token,
                    h.relay_key,
                )
            };
            let (
                guest_name,
                guest_token,
                guest_relay,
                guest_relay_only,
                guest_token_bytes,
                guest_key_bytes,
            ) = {
                let g = &self.sessions[&guest];
                (
                    g.name.clone(),
                    g.session_token_hex(),
                    g.relay_key_hex(),
                    g.relay_only,
                    g.session_token,
                    g.relay_key,
                )
            };
            if let Some(g) = self.sessions.get_mut(&guest) {
                g.room = Some(room_id.clone());
                g.role = Some(Role::Guest);
            }
            // Register UDP rendezvous slots for both players.
            self.bindings.lock().expect("bindings").register(
                host.clone(),
                room_id.clone(),
                BindRole::Host,
                guest.clone(),
                host_relay_only,
                host_token_bytes,
                host_key_bytes,
                now,
                &ctx.config.limits,
            );
            self.bindings.lock().expect("bindings").register(
                guest.clone(),
                room_id.clone(),
                BindRole::Guest,
                host.clone(),
                guest_relay_only,
                guest_token_bytes,
                guest_key_bytes,
                now,
                &ctx.config.limits,
            );
            ctx.metrics.joins.with_label_values(&["accepted"]).inc();
            self.update_room_metrics(ctx.metrics);
            self.sync_room_in_match(&room_id);

            let udp = ctx.udp_info();
            self.send_rid(
                &host,
                &ServerMessage::MatchSession {
                    room_id: room_id.clone(),
                    role: Role::Host,
                    session_token: host_token,
                    relay_key: host_relay,
                    pair_secret: pair_secret.clone(),
                    peer: PeerInfo { name: guest_name },
                    udp: udp.clone(),
                },
                rid,
            );
            self.send(
                &guest,
                &ServerMessage::MatchSession {
                    room_id: room_id.clone(),
                    role: Role::Guest,
                    session_token: guest_token,
                    relay_key: guest_relay,
                    pair_secret,
                    peer: PeerInfo { name: host_name },
                    udp,
                },
            );
        } else {
            let reason = m
                .reason
                .as_deref()
                .map(|r| r.chars().take(64).collect::<String>())
                .filter(|r| !r.is_empty())
                .unwrap_or_else(|| "declined".to_string());
            {
                let room = self.rooms.get_mut(&room_id).expect("room exists");
                room.pending = None;
                room.touch(now);
                if let Some(cid) = guest_client_id {
                    room.decline_cooldowns
                        .insert(cid, now + ctx.config.limits.decline_cooldown_ms);
                }
            }
            ctx.metrics.joins.with_label_values(&["declined"]).inc();
            self.send(
                &guest,
                &ServerMessage::JoinDeclined {
                    room_id: room_id.clone(),
                    reason,
                },
            );
        }
        self.broadcast_room_state(&room_id);
    }

    fn handle_kick(&mut self, sid: &str, m: Kick, rid: Option<&str>, ctx: &Ctx, now: u64) {
        let Some(room_id) = self
            .rooms
            .values()
            .find(|r| r.host == sid)
            .map(|r| r.id.clone())
        else {
            self.send_error(sid, ErrorCode::NotAllowed, "not a host", rid);
            return;
        };
        let guest = self.rooms[&room_id].guest.clone();
        let Some(guest) = guest else {
            self.send_error(sid, ErrorCode::NotAllowed, "no guest to kick", rid);
            return;
        };
        let reason = m
            .reason
            .as_deref()
            .map(|r| r.chars().take(64).collect::<String>());
        {
            let room = self.rooms.get_mut(&room_id).expect("room exists");
            room.guest = None;
            room.phase = Phase::Lobby;
            room.touch(now);
        }
        // Invalidate both players' UDP bindings; a new guest gets fresh slots on
        // the next accept, and the kicked guest can no longer relay to the host.
        self.bindings
            .lock()
            .expect("bindings")
            .unregister_room(&room_id);
        if let Some(g) = self.sessions.get_mut(&guest) {
            g.room = None;
            g.role = None;
        }
        self.send(
            &guest,
            &ServerMessage::PlayerLeft {
                room_id: room_id.clone(),
                role: Role::Guest,
                reason: LeaveReason::Kicked,
            },
        );
        if let Some(r) = reason {
            tracing::info!(room_id, reason = %r, "guest kicked");
        }
        ctx.metrics.joins.with_label_values(&["kicked"]).inc();
        self.update_room_metrics(ctx.metrics);
        self.sync_room_in_match(&room_id);
        self.broadcast_room_state(&room_id);
    }

    fn handle_leave(&mut self, sid: &str, rid: Option<&str>, ctx: &Ctx, now: u64) {
        let Some(sess) = self.sessions.get(sid) else {
            return;
        };
        let Some(room_id) = sess.room.clone() else {
            self.send_error(sid, ErrorCode::NotAllowed, "not in a room", rid);
            return;
        };
        let role = sess.role.unwrap_or(Role::Spectator);
        match role {
            Role::Host => self.close_room(&room_id, CloseReason::HostLeft, ctx, now),
            Role::Guest => self.remove_guest(&room_id, sid, LeaveReason::Left, ctx, now),
            Role::Spectator => self.remove_spectator(&room_id, sid, ctx),
        }
    }

    fn handle_room_update(
        &mut self,
        sid: &str,
        m: RoomUpdate,
        rid: Option<&str>,
        _ctx: &Ctx,
        now: u64,
    ) {
        let Some(room_id) = self
            .rooms
            .values()
            .find(|r| r.host == sid)
            .map(|r| r.id.clone())
        else {
            self.send_error(sid, ErrorCode::NotAllowed, "not a host", rid);
            return;
        };
        let should_broadcast = {
            let room = self.rooms.get_mut(&room_id).expect("room exists");
            room.phase = m.phase;
            if let Some(f) = m.fighters {
                room.fighters = f;
            }
            if let Some(s) = m.stage {
                room.stage = Some(s);
            }
            if m.round.is_some() {
                room.round = m.round;
            }
            if let Some(w) = m.wins {
                room.wins = w;
            }
            // Omitted optional fields keep their previous value, like the rest.
            if m.timer.is_some() {
                room.timer = m.timer;
            }
            room.touch(now);
            if now >= room.next_broadcast_ms {
                room.next_broadcast_ms = now + 250;
                room.dirty = false;
                true
            } else {
                room.dirty = true;
                false
            }
        };
        self.sync_room_in_match(&room_id);
        if should_broadcast {
            self.broadcast_room_state(&room_id);
        }
    }

    fn handle_connection_report(
        &mut self,
        sid: &str,
        m: ConnectionReport,
        rid: Option<&str>,
        _ctx: &Ctx,
        now: u64,
    ) {
        let Some(room_id) = self.sessions.get(sid).and_then(|s| s.room.clone()) else {
            self.send_error(sid, ErrorCode::NotAllowed, "not in a room", rid);
            return;
        };
        if let Some(room) = self.rooms.get_mut(&room_id) {
            room.connection = Some(m.path);
            room.touch(now);
        }
        if let Some(rtt) = m.rtt_ms {
            tracing::info!(room_id = %room_id, rtt_ms = rtt, path = ?m.path, "connection report");
        }
        self.broadcast_room_state(&room_id);
    }

    fn handle_spectate(&mut self, sid: &str, m: Spectate, rid: Option<&str>, ctx: &Ctx, now: u64) {
        let Some(sess) = self.sessions.get(sid) else {
            return;
        };
        if sess.room.is_some() {
            self.send_error(sid, ErrorCode::AlreadyInRoom, "already in a room", rid);
            return;
        }
        let (viewer_version, viewer_hash) = (sess.game_version.clone(), sess.content_hash);
        let Some(room_id) = self.resolve_room(&m.room) else {
            self.send_error(sid, ErrorCode::RoomNotFound, "room not found", rid);
            return;
        };
        {
            let room = &self.rooms[&room_id];
            // A spectator re-simulates the match, so the build and data must
            // match the room exactly (section 2).
            if room.host_game_version != viewer_version || room.host_content_hash != viewer_hash {
                self.send_error(
                    sid,
                    ErrorCode::VersionMismatch,
                    "room needs a different game version or data",
                    rid,
                );
                return;
            }
            if !room.allow_spectators {
                self.send_error(
                    sid,
                    ErrorCode::SpectatingDisabled,
                    "spectating disabled",
                    rid,
                );
                return;
            }
            if room.spectators.len() >= room.max_spectators as usize {
                self.send_error(sid, ErrorCode::SpectatorsFull, "spectators full", rid);
                return;
            }
            if let Some(hash) = room.password_hash {
                let ok = m
                    .password
                    .as_deref()
                    .map(|p| ct_eq(&hash_password(p), &hash))
                    .unwrap_or(false);
                if !ok {
                    self.send_error(sid, ErrorCode::WrongPassword, "wrong password", rid);
                    return;
                }
            }
        }
        let (delay, live) = {
            let room = self.rooms.get_mut(&room_id).expect("room exists");
            room.spectators.push(sid.to_string());
            room.spectator_peak = room.spectator_peak.max(room.spectators.len() as u32);
            room.touch(now);
            (room.spectator_delay_ms, room.log.is_live())
        };
        if let Some(s) = self.sessions.get_mut(sid) {
            s.room = Some(room_id.clone());
            s.role = Some(Role::Spectator);
            s.spectator = Some(SpectatorState::default());
        }
        ctx.metrics.spectators.inc();
        self.online_spectating = self.online_spectating.saturating_add(1);
        self.send_rid(
            sid,
            &ServerMessage::SpectateStarted {
                room_id: room_id.clone(),
                delay_ms: delay,
                match_live: live,
            },
            rid,
        );
        // Late join: send MATCH_START immediately, then the 20 ms flush task
        // streams the backlog from tick 0 and then live.
        if live {
            if let Some(start) = self.rooms[&room_id].log.start.clone() {
                if let Ok(bytes) = FeedFrame::MatchStart(start).encode() {
                    self.send_binary(sid, bytes);
                }
            }
        }
        self.broadcast_room_state(&room_id);
    }

    fn handle_stop_spectating(&mut self, sid: &str, rid: Option<&str>, ctx: &Ctx, _now: u64) {
        let Some(sess) = self.sessions.get(sid) else {
            return;
        };
        if sess.role != Some(Role::Spectator) {
            self.send_error(sid, ErrorCode::NotAllowed, "not spectating", rid);
            return;
        }
        let room_id = sess.room.clone().expect("spectator has room");
        self.remove_spectator(&room_id, sid, ctx);
    }

    fn handle_react(&mut self, sid: &str, m: proto::React, rid: Option<&str>, ctx: &Ctx, now: u64) {
        let Some(sess) = self.sessions.get(sid) else {
            return;
        };
        let Some(room_id) = sess.room.clone() else {
            self.send_error(sid, ErrorCode::NotAllowed, "not in a room", rid);
            return;
        };
        let role = sess.role.unwrap_or(Role::Spectator);
        let name = sess.name.clone();
        let allowed = self
            .sessions
            .get_mut(sid)
            .map(|s| s.react_bucket.try_acquire(now))
            .unwrap_or(false);
        if !allowed {
            ctx.metrics.rate_limited.inc();
            self.send_error(sid, ErrorCode::RateLimited, "too many reactions", rid);
            return;
        }
        self.broadcast_reaction(&room_id, role, &name, m.emote);
    }

    // -- quick match (section 12, M5) -------------------------------------

    fn handle_queue_join(
        &mut self,
        sid: &str,
        m: proto::QueueJoin,
        rid: Option<&str>,
        ctx: &Ctx,
        now: u64,
    ) {
        let Some(sess) = self.sessions.get(sid) else {
            return;
        };
        if sess.room.is_some() {
            self.send_error(sid, ErrorCode::AlreadyInRoom, "already in a room", rid);
            return;
        }
        let region = m.region.unwrap_or_else(|| self.region.clone());
        let mode = m.mode;
        self.queue.retain(|q| q.session_id != sid);
        self.queue.push_back(QueueEntry {
            session_id: sid.to_string(),
            mode: mode.clone(),
            region,
            joined_ms: now,
        });
        tracing::info!(session_id = %sid, mode = %mode, "queued for quick match");
        self.try_match_queue(ctx, now);
    }

    fn handle_queue_leave(&mut self, sid: &str) {
        let before = self.queue.len();
        self.queue.retain(|q| q.session_id != sid);
        if before != self.queue.len() {
            tracing::info!(session_id = %sid, "left quick-match queue");
        }
    }

    /// Pair the two oldest compatible clients. First in queue hosts.
    fn try_match_queue(&mut self, ctx: &Ctx, now: u64) {
        loop {
            let mut pair: Option<(usize, usize)> = None;
            'outer: for i in 0..self.queue.len() {
                let a = &self.queue[i];
                let Some(sa) = self.sessions.get(&a.session_id) else {
                    continue;
                };
                if sa.room.is_some() {
                    continue;
                }
                for j in (i + 1)..self.queue.len() {
                    let b = &self.queue[j];
                    let Some(sb) = self.sessions.get(&b.session_id) else {
                        continue;
                    };
                    if sb.room.is_some() {
                        continue;
                    }
                    if a.mode == b.mode
                        && a.region == b.region
                        && sa.game_version == sb.game_version
                        && sa.content_hash == sb.content_hash
                    {
                        pair = Some((i, j));
                        break 'outer;
                    }
                }
            }
            let Some((i, j)) = pair else {
                return;
            };
            // Remove both (higher index first).
            let b = self.queue.remove(j).expect("index j");
            let a = self.queue.remove(i).expect("index i");
            let host = a.session_id;
            let guest = b.session_id;
            let host_name = self
                .sessions
                .get(&host)
                .map(|s| s.name.clone())
                .unwrap_or_else(|| "Player".to_string());
            let delay = ctx.config.limits.default_spectator_delay_ms;
            let max_spectators = 50.min(ctx.config.limits.max_spectators);
            let (room_id, _code) = self.spawn_room(
                &host,
                format!("{host_name}'s room"),
                Visibility::Public,
                None,
                true,
                delay,
                max_spectators,
                ctx,
                now,
            );
            // Attach the guest directly.
            if let Some(room) = self.rooms.get_mut(&room_id) {
                room.guest = Some(guest.clone());
                room.touch(now);
            }
            let host_udp = ctx.udp_info();
            let (host_token, host_relay, host_relay_only, host_key_bytes) = {
                let s = &self.sessions[&host];
                (
                    s.session_token_hex(),
                    s.relay_key_hex(),
                    s.relay_only,
                    s.session_token,
                )
            };
            let (guest_name, guest_token, guest_relay, guest_relay_only, guest_key_bytes) = {
                let s = &self.sessions[&guest];
                (
                    s.name.clone(),
                    s.session_token_hex(),
                    s.relay_key_hex(),
                    s.relay_only,
                    s.session_token,
                )
            };
            let host_key = self.sessions[&host].relay_key;
            let guest_key = self.sessions[&guest].relay_key;
            if let Some(s) = self.sessions.get_mut(&guest) {
                s.room = Some(room_id.clone());
                s.role = Some(Role::Guest);
            }
            self.bindings.lock().expect("bindings").register(
                host.clone(),
                room_id.clone(),
                BindRole::Host,
                guest.clone(),
                host_relay_only,
                host_key_bytes,
                host_key,
                now,
                &ctx.config.limits,
            );
            self.bindings.lock().expect("bindings").register(
                guest.clone(),
                room_id.clone(),
                BindRole::Guest,
                host.clone(),
                guest_relay_only,
                guest_key_bytes,
                guest_key,
                now,
                &ctx.config.limits,
            );
            ctx.metrics.joins.with_label_values(&["quick_match"]).inc();
            self.update_room_metrics(ctx.metrics);
            self.sync_room_in_match(&room_id);
            let pair_secret = generate_hex_token(16);
            if let Some(room) = self.rooms.get_mut(&room_id) {
                room.pair_secret = Some(pair_secret.clone());
            }
            self.send(
                &host,
                &ServerMessage::QueueMatched {
                    room_id: room_id.clone(),
                },
            );
            self.send(
                &guest,
                &ServerMessage::QueueMatched {
                    room_id: room_id.clone(),
                },
            );
            self.send(
                &host,
                &ServerMessage::MatchSession {
                    room_id: room_id.clone(),
                    role: Role::Host,
                    session_token: host_token,
                    relay_key: host_relay,
                    pair_secret: pair_secret.clone(),
                    peer: PeerInfo { name: guest_name },
                    udp: host_udp.clone(),
                },
            );
            self.send(
                &guest,
                &ServerMessage::MatchSession {
                    room_id: room_id.clone(),
                    role: Role::Guest,
                    session_token: guest_token,
                    relay_key: guest_relay,
                    pair_secret,
                    peer: PeerInfo {
                        name: self
                            .sessions
                            .get(&host)
                            .map(|s| s.name.clone())
                            .unwrap_or_default(),
                    },
                    udp: host_udp,
                },
            );
            self.broadcast_room_state(&room_id);
        }
    }

    // -- membership mutations --------------------------------------------

    fn remove_guest(
        &mut self,
        room_id: &str,
        guest: &str,
        reason: LeaveReason,
        ctx: &Ctx,
        now: u64,
    ) {
        let Some(room) = self.rooms.get_mut(room_id) else {
            return;
        };
        if room.guest.as_deref() != Some(guest) {
            return;
        }
        room.guest = None;
        room.phase = Phase::Lobby;
        room.pair_secret = None;
        room.touch(now);
        // The match is over; free its logs.
        self.log_bytes = self.log_bytes.saturating_sub(room_log_bytes(room));
        room.log.clear();
        room.guest_log.clear();
        self.bindings
            .lock()
            .expect("bindings")
            .unregister_room(room_id);
        if let Some(s) = self.sessions.get_mut(guest) {
            s.room = None;
            s.role = None;
        }
        let host = room.host.clone();
        let mut ids = vec![host.clone()];
        ids.extend(room.spectators.iter().cloned());
        let msg = ServerMessage::PlayerLeft {
            room_id: room_id.to_string(),
            role: Role::Guest,
            reason,
        };
        for id in ids {
            self.send(&id, &msg);
        }
        ctx.metrics.spectators.set(self.total_spectators() as i64);
        self.update_room_metrics(ctx.metrics);
        self.sync_room_in_match(room_id);
        self.broadcast_room_state(room_id);
    }

    fn remove_spectator(&mut self, room_id: &str, sid: &str, ctx: &Ctx) {
        let removed = {
            let Some(room) = self.rooms.get_mut(room_id) else {
                return;
            };
            let before = room.spectators.len();
            room.spectators.retain(|s| s != sid);
            before != room.spectators.len()
        };
        if let Some(s) = self.sessions.get_mut(sid) {
            s.room = None;
            s.role = None;
            s.spectator = None;
        }
        if removed {
            self.online_spectating = self.online_spectating.saturating_sub(1);
            ctx.metrics.spectators.set(self.total_spectators() as i64);
            self.send(
                sid,
                &ServerMessage::SpectateEnded {
                    room_id: room_id.to_string(),
                    reason: "left".into(),
                },
            );
            self.broadcast_room_state(room_id);
        }
    }

    fn total_spectators(&self) -> usize {
        self.rooms.values().map(|r| r.spectators.len()).sum()
    }

    fn close_room(&mut self, room_id: &str, reason: CloseReason, ctx: &Ctx, now: u64) {
        let _ = now;
        let Some(room) = self.rooms.remove(room_id) else {
            return;
        };
        self.online_rooms = self.online_rooms.saturating_sub(1);
        self.online_in_match = self.online_in_match.saturating_sub(room.in_match_counted);
        self.online_spectating = self.online_spectating.saturating_sub(room.spectators.len());
        self.log_bytes = self.log_bytes.saturating_sub(room_log_bytes(&room));
        self.codes.remove(&room.code);
        self.bindings
            .lock()
            .expect("bindings")
            .unregister_room(room_id);
        let mut targets = Vec::new();
        if let Some(g) = &room.guest {
            targets.push(g.clone());
        }
        targets.extend(room.spectators.iter().cloned());
        if let Some(h) = self.sessions.get_mut(&room.host) {
            h.room = None;
            h.role = None;
        }
        for id in &targets {
            if let Some(s) = self.sessions.get_mut(id) {
                s.room = None;
                s.role = None;
                s.spectator = None;
            }
        }
        let msg = ServerMessage::RoomClosed {
            room_id: room_id.to_string(),
            reason,
        };
        for id in targets {
            self.send(&id, &msg);
        }
        ctx.metrics.spectators.set(self.total_spectators() as i64);
        self.update_room_metrics(ctx.metrics);
    }

    /// Tear down a session and apply its room effects. Used on disconnect and
    /// on timeout. Sends close code 4001.
    pub fn remove_session(&mut self, sid: &str, reason: LeaveReason, ctx: &Ctx, now: u64) {
        self.remove_session_closing(sid, reason, ctx, now, 4001, "disconnected");
    }

    /// Like [`Self::remove_session`] but with an explicit close code.
    pub fn remove_session_closing(
        &mut self,
        sid: &str,
        reason: LeaveReason,
        ctx: &Ctx,
        now: u64,
        close_code: u16,
        close_reason: &str,
    ) {
        let Some(sess) = self.sessions.remove(sid) else {
            return;
        };
        // A connected session leaves the live-player count; a lingering one was
        // already removed from it at disconnect time.
        if sess.disconnected_at.is_none() {
            self.online_players = self.online_players.saturating_sub(1);
        }
        self.resume_index.remove(&sess.resume_token);
        self.queue.retain(|q| q.session_id != sid);
        if let Some(out) = &sess.out {
            let _ = out.send(OutMsg::Close(close_code, close_reason.to_string()));
        }
        if let Some(room_id) = sess.room.clone() {
            match sess.role.unwrap_or(Role::Spectator) {
                Role::Host => {
                    let close_reason = if reason == LeaveReason::Disconnected {
                        CloseReason::HostDisconnected
                    } else {
                        CloseReason::HostLeft
                    };
                    self.close_room(&room_id, close_reason, ctx, now);
                }
                Role::Guest => self.remove_guest(&room_id, sid, reason, ctx, now),
                Role::Spectator => self.remove_spectator(&room_id, sid, ctx),
            }
        }
    }

    fn broadcast_reaction(&self, room_id: &str, from_role: Role, name: &str, emote: Emote) {
        let Some(room) = self.rooms.get(room_id) else {
            return;
        };
        let mut ids = vec![room.host.clone()];
        if let Some(g) = &room.guest {
            ids.push(g.clone());
        }
        ids.extend(room.spectators.iter().cloned());
        let msg = ServerMessage::Reaction {
            room_id: room_id.to_string(),
            from_role,
            name: name.to_string(),
            emote,
        };
        for id in ids {
            self.send(&id, &msg);
        }
    }

    // -- UDP rendezvous and relay (section 7) -----------------------------

    /// Tell both players their peer's endpoints once both have bound.
    pub fn try_notify_peers(&mut self, room_id: &str, now: u64, ctx: &Ctx) {
        let Some(room) = self.rooms.get(room_id) else {
            return;
        };
        let host = room.host.clone();
        let Some(guest) = room.guest.clone() else {
            return;
        };
        let (host_candidates, guest_candidates) = {
            let bindings = self.bindings.lock().expect("bindings");
            let (Some(host_slot), Some(guest_slot)) = (bindings.get(&host), bindings.get(&guest))
            else {
                return;
            };
            if host_slot.endpoint.is_none() || guest_slot.endpoint.is_none() {
                return;
            }
            if host_slot.notified && guest_slot.notified {
                return;
            }
            (
                candidates_for(host_slot, guest_slot),
                candidates_for(guest_slot, host_slot),
            )
        };
        let punch_at = now + ctx.config.limits.punch_delay_ms;
        self.send(
            &host,
            &ServerMessage::PeerEndpoints {
                room_id: room_id.to_string(),
                candidates: host_candidates,
                punch_at,
            },
        );
        self.send(
            &guest,
            &ServerMessage::PeerEndpoints {
                room_id: room_id.to_string(),
                candidates: guest_candidates,
                punch_at,
            },
        );
        let mut bindings = self.bindings.lock().expect("bindings");
        bindings.mark_notified(&host);
        bindings.mark_notified(&guest);
    }

    // -- spectator feed publishing (section 8) ----------------------------

    /// Queue a binary frame to a session and account for its size.
    pub fn send_binary(&self, sid: &str, bytes: Vec<u8>) {
        if let Some(sess) = self.sessions.get(sid) {
            if let Some(out) = &sess.out {
                sess.queued_bytes.fetch_add(bytes.len(), Ordering::Relaxed);
                let _ = out.send(OutMsg::Binary(bytes));
            }
        }
    }

    /// Validate and store a binary feed frame published by the host.
    pub fn handle_binary(&mut self, sid: &str, payload: &[u8], ctx: &Ctx, now: u64) -> ConnAction {
        // Binary frames are activity too: a host publishing the feed for a whole
        // round must not be treated as idle.
        self.touch(sid, now);
        // O(1) lookup via the session, not a scan over every room (issue #19).
        let (Some(room_id), Some(role)) = (
            self.sessions.get(sid).and_then(|s| s.room.clone()),
            self.sessions.get(sid).and_then(|s| s.role),
        ) else {
            self.send_error(sid, ErrorCode::NotAllowed, "not in a match", None);
            return ConnAction::None;
        };
        let is_host = role == Role::Host;
        if !is_host && role != Role::Guest {
            self.send_error(sid, ErrorCode::NotAllowed, "not in a match", None);
            return ConnAction::None;
        }
        if is_host && self.rooms[&room_id].guest.is_none() {
            self.send_error(sid, ErrorCode::NotAllowed, "no guest", None);
            return ConnAction::None;
        }
        let allowed = self
            .sessions
            .get_mut(sid)
            .map(|s| s.feed_bucket.try_acquire(now))
            .unwrap_or(false);
        if !allowed {
            ctx.metrics.rate_limited.inc();
            self.send_error(sid, ErrorCode::RateLimited, "feed too fast", None);
            return ConnAction::None;
        }

        let frame = match FeedFrame::decode(payload) {
            Ok(f) => f,
            Err(_) => {
                self.send_error(sid, ErrorCode::BadMessage, "bad feed frame", None);
                return ConnAction::None;
            }
        };
        ctx.metrics
            .feed_frames
            .with_label_values(&[feed_frame_name(&frame)])
            .inc();

        if !is_host {
            self.verify_guest_frame(&room_id, frame, ctx, now);
            return ConnAction::None;
        }

        match frame {
            FeedFrame::MatchStart(start) => {
                let start_id = start.match_id;
                let previous = self.rooms[&room_id].log.match_id;
                if previous == Some(start_id) {
                    self.send_error(sid, ErrorCode::BadMessage, "match_id must change", None);
                    return ConnAction::None;
                }
                let before = room_log_bytes(&self.rooms[&room_id]);
                self.rooms
                    .get_mut(&room_id)
                    .expect("room exists")
                    .log
                    .begin(start, now);
                {
                    let room = self.rooms.get_mut(&room_id).expect("room exists");
                    // Only reset the guest's copy when it is a different match;
                    // the guest's MATCH_START may have arrived first.
                    if room.guest_log.match_id != Some(start_id) {
                        room.guest_log.clear();
                    }
                    room.feed_verified = true;
                }
                let after = room_log_bytes(&self.rooms[&room_id]);
                self.log_bytes = adjust_log_bytes(self.log_bytes, before, after);
                let specs = self.rooms[&room_id].spectators.clone();
                for spec in specs {
                    if let Some(s) = self.sessions.get_mut(&spec) {
                        s.spectator = Some(SpectatorState::default());
                    }
                    self.send_binary(&spec, payload.to_vec());
                }
            }
            FeedFrame::Inputs {
                match_id,
                first_tick,
                inputs,
            } => {
                let log = &self.rooms[&room_id].log;
                if log.match_id != Some(match_id) {
                    self.send_error(sid, ErrorCode::BadMessage, "unknown match", None);
                    return ConnAction::None;
                }
                if first_tick as usize != log.tick_count() {
                    self.feed_reset(&room_id, match_id, ctx);
                    self.send_error(sid, ErrorCode::BadMessage, "input gap", None);
                    return ConnAction::None;
                }
                if log.tick_count() + inputs.len() > MAX_FEED_TICKS {
                    self.send_error(sid, ErrorCode::BadMessage, "feed too long", None);
                    return ConnAction::None;
                }
                // A feed may not run more than the configured backlog ahead of
                // real time (60 ticks per second since MATCH_START).
                let elapsed_ms = now.saturating_sub(log.started_ms);
                let allowed = (elapsed_ms.saturating_mul(60) / 1000) as u32
                    + ctx.config.limits.feed_max_backlog_ticks;
                if first_tick.saturating_add(inputs.len() as u32) > allowed {
                    self.send_error(
                        sid,
                        ErrorCode::BadMessage,
                        "feed faster than real time",
                        None,
                    );
                    return ConnAction::None;
                }
                // Global match-log budget across all rooms.
                if self.log_bytes.saturating_add(inputs.len() * 4)
                    > ctx.config.limits.max_match_log_bytes
                {
                    self.send_error(sid, ErrorCode::ServerFull, "match log budget reached", None);
                    return ConnAction::None;
                }
                let start = log.tick_count();
                self.rooms
                    .get_mut(&room_id)
                    .expect("room exists")
                    .log
                    .push_inputs(&inputs, now);
                self.log_bytes = self.log_bytes.saturating_add(inputs.len() * 4);
                let mismatch = {
                    let room = &self.rooms[&room_id];
                    room.feed_verified && inputs_mismatch(room, start, &inputs)
                };
                if mismatch {
                    self.mark_feed_mismatch(&room_id, ctx);
                }
            }
            FeedFrame::Checksum {
                match_id,
                tick,
                checksum,
            } => {
                let log = &self.rooms[&room_id].log;
                if log.match_id != Some(match_id)
                    || tick % 60 != 0
                    || tick > log.tick_count() as u32
                {
                    self.send_error(sid, ErrorCode::BadMessage, "bad checksum", None);
                    return ConnAction::None;
                }
                if self.log_bytes.saturating_add(12) > ctx.config.limits.max_match_log_bytes {
                    self.send_error(sid, ErrorCode::ServerFull, "match log budget reached", None);
                    return ConnAction::None;
                }
                self.rooms
                    .get_mut(&room_id)
                    .expect("room exists")
                    .log
                    .push_checksum(tick, checksum, now);
                self.log_bytes = self.log_bytes.saturating_add(12);
            }
            FeedFrame::MatchEnd {
                match_id,
                final_tick,
                result,
                p1_wins,
                p2_wins,
            } => {
                if self.rooms[&room_id].log.match_id != Some(match_id) {
                    self.send_error(sid, ErrorCode::BadMessage, "unknown match", None);
                    return ConnAction::None;
                }
                let end = fighter_protocol::feed::MatchEnd {
                    match_id,
                    final_tick,
                    result,
                    p1_wins,
                    p2_wins,
                };
                {
                    let room = self.rooms.get_mut(&room_id).expect("room exists");
                    room.log.end = Some((end, now));
                    room.matches_played += 1;
                }
                ctx.metrics.matches.with_label_values(&["finished"]).inc();
                self.store_replay(&room_id, ctx, now);
            }
            FeedFrame::FeedReset { .. } => {
                self.send_error(sid, ErrorCode::BadMessage, "reserved frame", None);
            }
        }
        ConnAction::None
    }

    /// Store the guest's copy of the feed and compare it with the host's.
    fn verify_guest_frame(&mut self, room_id: &str, frame: FeedFrame, ctx: &Ctx, now: u64) {
        let mut mismatch = false;
        let budget = ctx.config.limits.max_match_log_bytes;
        let current = self.log_bytes;
        let before = match self.rooms.get(room_id) {
            Some(room) => room_log_bytes(room),
            None => return,
        };
        {
            let Some(room) = self.rooms.get_mut(room_id) else {
                return;
            };
            match frame {
                FeedFrame::MatchStart(start) => room.guest_log.begin(start, now),
                FeedFrame::Inputs {
                    match_id,
                    first_tick,
                    inputs,
                } => {
                    if room.guest_log.match_id != Some(match_id)
                        || first_tick as usize != room.guest_log.tick_count()
                    {
                        return;
                    }
                    if current.saturating_add(inputs.len() * 4) > budget {
                        return;
                    }
                    let start = room.guest_log.tick_count();
                    room.guest_log.push_inputs(&inputs, 0);
                    if room.feed_verified && inputs_mismatch(room, start, &inputs) {
                        mismatch = true;
                    }
                }
                FeedFrame::Checksum {
                    match_id,
                    tick,
                    checksum,
                } => {
                    if room.guest_log.match_id != Some(match_id) {
                        return;
                    }
                    if current.saturating_add(12) > budget {
                        return;
                    }
                    room.guest_log.push_checksum(tick, checksum, 0);
                    if room.feed_verified {
                        if let Some((_, host_checksum, _)) =
                            room.log.checksums.iter().find(|(t, _, _)| *t == tick)
                        {
                            if *host_checksum != checksum {
                                mismatch = true;
                            }
                        }
                    }
                }
                FeedFrame::MatchEnd { .. } | FeedFrame::FeedReset { .. } => {}
            }
        }
        let after = match self.rooms.get(room_id) {
            Some(room) => room_log_bytes(room),
            None => return,
        };
        self.log_bytes = adjust_log_bytes(self.log_bytes, before, after);
        if mismatch {
            self.mark_feed_mismatch(room_id, ctx);
        }
    }

    /// Flag a room whose host and guest feeds disagree (section 8.4).
    fn mark_feed_mismatch(&mut self, room_id: &str, ctx: &Ctx) {
        {
            let Some(room) = self.rooms.get_mut(room_id) else {
                return;
            };
            if !room.feed_verified {
                return;
            }
            room.feed_verified = false;
        }
        ctx.metrics.feed_mismatch.inc();
        tracing::warn!(room_id = %room_id, "host and guest feeds disagree");
        self.broadcast_room_state(room_id);
    }

    /// Write a finished match to the replay store.
    fn store_replay(&mut self, room_id: &str, ctx: &Ctx, now: u64) {
        if !self.replays.lock().expect("replays").enabled() {
            return;
        }
        let (start, end) = match self.rooms.get(room_id) {
            Some(room) => match (room.log.start.clone(), room.log.end.clone()) {
                (Some(s), Some((e, _))) => (s, e),
                _ => return,
            },
            None => return,
        };
        // Only plausible, non-trivial matches, and at most one replay a second
        // per room (issue #9).
        if end.final_tick < 60 || now.saturating_sub(self.rooms[room_id].last_replay_ms) < 1_000 {
            return;
        }
        let mut body = Vec::new();
        {
            let room = &self.rooms[room_id];
            let mut push = |frame: FeedFrame| {
                if let Ok(bytes) = frame.encode() {
                    body.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    body.extend_from_slice(&bytes);
                }
            };
            push(FeedFrame::MatchStart(start.clone()));
            for (i, chunk) in room.log.inputs.chunks(600).enumerate() {
                push(FeedFrame::Inputs {
                    match_id: start.match_id,
                    first_tick: (i * 600) as u32,
                    inputs: chunk.to_vec(),
                });
            }
            for (tick, checksum, _) in &room.log.checksums {
                push(FeedFrame::Checksum {
                    match_id: start.match_id,
                    tick: *tick,
                    checksum: *checksum,
                });
            }
            push(FeedFrame::MatchEnd {
                match_id: end.match_id,
                final_tick: end.final_tick,
                result: end.result,
                p1_wins: end.p1_wins,
                p2_wins: end.p2_wins,
            });
        }
        let id = format!("{now:x}-{}", generate_hex_token(4));
        let meta = ReplayMeta {
            id,
            stage: start.stage_id.clone(),
            p1_fighter: start.p1_fighter_id.clone(),
            p2_fighter: start.p2_fighter_id.clone(),
            p1_name: start.p1_name.clone(),
            p2_name: start.p2_name.clone(),
            result: match end.result {
                fighter_protocol::feed::MatchResult::P1Won => 0,
                fighter_protocol::feed::MatchResult::P2Won => 1,
                fighter_protocol::feed::MatchResult::Draw => 2,
                fighter_protocol::feed::MatchResult::Aborted => 3,
            },
            duration_ticks: end.final_tick,
            finished_at: now,
            game_version: start.game_version.clone(),
            content_hash: self.rooms[room_id].host_content_hash,
        };
        if let Some(room) = self.rooms.get_mut(room_id) {
            room.last_replay_ms = now;
        }
        // Hand the write to the background task: no file I/O under the lock.
        let _ = self.replay_tx.send((meta, body));
        ctx.metrics.matches.with_label_values(&["stored"]).inc();
    }

    /// End the current feed, tell spectators, and start fresh.
    fn feed_reset(&mut self, room_id: &str, match_id: u32, ctx: &Ctx) {
        if let Some(room) = self.rooms.get_mut(room_id) {
            self.log_bytes = self.log_bytes.saturating_sub(room_log_bytes(room));
            room.log.clear();
            room.guest_log.clear();
        }
        let specs = self.rooms[room_id].spectators.clone();
        if let Ok(bytes) = (FeedFrame::FeedReset { match_id }).encode() {
            for spec in specs {
                if let Some(s) = self.sessions.get_mut(&spec) {
                    s.spectator = Some(SpectatorState::default());
                }
                self.send_binary(&spec, bytes.clone());
            }
        }
        ctx.metrics.feed_frames.with_label_values(&["reset"]).inc();
    }

    /// Push any newly-visible frames to every spectator. Called every 20 ms.
    pub fn flush_spectators(&mut self, ctx: &Ctx) {
        let now = ctx.clock.now_ms();
        let max_queued = ctx.config.limits.spectator_max_queued_bytes;
        let mut too_slow: Vec<(String, String)> = Vec::new();
        {
            let Lobby {
                rooms, sessions, ..
            } = self;
            for room in rooms.values() {
                if room.spectators.is_empty() {
                    continue;
                }
                let Some(match_id) = room.log.match_id else {
                    continue;
                };
                if room.log.start.is_none() {
                    continue;
                }
                let delay = room.spectator_delay_ms as u64;
                let visible = room.log.visible_ticks(now, delay);
                for spec_id in &room.spectators {
                    let Some(sess) = sessions.get_mut(spec_id) else {
                        continue;
                    };
                    let Some(mut state) = sess.spectator.clone() else {
                        continue;
                    };
                    let mut projected = sess.queued_bytes.load(Ordering::Relaxed);
                    if projected >= max_queued {
                        // Buffer already full: count the stall and try later.
                        state.stall_flushes += 1;
                        if state.stall_flushes > 25 {
                            too_slow.push((room.id.clone(), spec_id.clone()));
                            continue;
                        }
                        sess.spectator = Some(state);
                        continue;
                    }
                    let mut frames: Vec<Vec<u8>> = Vec::new();
                    let mut blocked = false;
                    // Inputs, re-batched into frames of at most 600 ticks.
                    let mut tick = state.delivered_ticks;
                    while tick < visible {
                        let count = (visible - tick).min(600);
                        let slice = &room.log.inputs[tick as usize..(tick + count) as usize];
                        let frame = FeedFrame::Inputs {
                            match_id,
                            first_tick: tick,
                            inputs: slice.to_vec(),
                        };
                        if let Ok(bytes) = frame.encode() {
                            if projected + bytes.len() > max_queued {
                                blocked = true;
                                break;
                            }
                            projected += bytes.len();
                            frames.push(bytes);
                        }
                        tick += count;
                    }
                    state.delivered_ticks = tick;
                    // Checksums, delayed.
                    while !blocked && state.checksum_cursor < room.log.checksums.len() {
                        let (t, c, recv) = room.log.checksums[state.checksum_cursor];
                        if recv.saturating_add(delay) > now {
                            break;
                        }
                        let frame = FeedFrame::Checksum {
                            match_id,
                            tick: t,
                            checksum: c,
                        };
                        if let Ok(bytes) = frame.encode() {
                            if projected + bytes.len() > max_queued {
                                blocked = true;
                                break;
                            }
                            projected += bytes.len();
                            frames.push(bytes);
                        }
                        state.checksum_cursor += 1;
                    }
                    // End of match, delayed.
                    if !blocked {
                        if let Some((end, recv)) = &room.log.end {
                            if !state.end_sent && recv.saturating_add(delay) <= now {
                                let frame = FeedFrame::MatchEnd {
                                    match_id: end.match_id,
                                    final_tick: end.final_tick,
                                    result: end.result,
                                    p1_wins: end.p1_wins,
                                    p2_wins: end.p2_wins,
                                };
                                if let Ok(bytes) = frame.encode() {
                                    if projected + bytes.len() > max_queued {
                                        blocked = true;
                                    } else {
                                        frames.push(bytes);
                                        state.end_sent = true;
                                    }
                                }
                            }
                        }
                    }
                    state.stall_flushes = if blocked { state.stall_flushes + 1 } else { 0 };
                    sess.spectator = Some(state);
                    for bytes in frames {
                        queue_binary(sess, bytes);
                    }
                }
            }
        }
        for (room_id, sid) in too_slow {
            self.send(
                &sid,
                &ServerMessage::SpectateEnded {
                    room_id: room_id.clone(),
                    reason: "too_slow".into(),
                },
            );
            self.remove_spectator(&room_id, &sid, ctx);
        }
    }

    // -- periodic sweep ---------------------------------------------------

    pub fn sweep(&mut self, ctx: &Ctx) {
        let now = ctx.clock.now_ms();

        // Expire UDP bindings and prune their rate-limit buckets.
        self.bindings.lock().expect("bindings").sweep(
            now,
            ctx.config.limits.binding_expiry_ms,
            ctx.config.limits.binding_max_age_ms,
        );

        // Drop queue entries whose session is gone or already matched.
        let queued: Vec<String> = self
            .queue
            .iter()
            .filter(|q| {
                self.sessions
                    .get(&q.session_id)
                    .map(|s| s.room.is_some())
                    .unwrap_or(true)
            })
            .map(|q| q.session_id.clone())
            .collect();
        self.queue.retain(|q| !queued.contains(&q.session_id));

        // Reap old replays.
        self.replays
            .lock()
            .expect("replays")
            .cleanup(now, ctx.config.storage.replay_retention_days);

        // Expire pending joins.
        let expired: Vec<(String, String, String, String)> = self
            .rooms
            .values()
            .filter_map(|r| {
                r.pending.as_ref().and_then(|p| {
                    if p.expires_ms <= now {
                        Some((
                            r.id.clone(),
                            r.host.clone(),
                            p.guest.clone(),
                            p.request_id.clone(),
                        ))
                    } else {
                        None
                    }
                })
            })
            .collect();
        for (room_id, host, guest, request_id) in expired {
            if let Some(room) = self.rooms.get_mut(&room_id) {
                room.pending = None;
                room.touch(now);
            }
            ctx.metrics.joins.with_label_values(&["timeout"]).inc();
            self.send(
                &guest,
                &ServerMessage::JoinDeclined {
                    room_id: room_id.clone(),
                    reason: "timeout".into(),
                },
            );
            self.send(&host, &ServerMessage::JoinCancelled { request_id });
            self.broadcast_room_state(&room_id);
        }

        // Idle rooms (host only, no activity).
        let idle: Vec<String> = self
            .rooms
            .values()
            .filter(|r| {
                r.guest.is_none()
                    && r.spectators.is_empty()
                    && now.saturating_sub(r.last_activity_ms) > ctx.config.limits.idle_room_ms
            })
            .map(|r| r.id.clone())
            .collect();
        for room_id in idle {
            self.close_room(&room_id, CloseReason::Expired, ctx, now);
        }

        // Coalesced room_state broadcasts.
        let dirty: Vec<String> = self
            .rooms
            .values()
            .filter(|r| r.dirty && now >= r.next_broadcast_ms)
            .map(|r| r.id.clone())
            .collect();
        for room_id in dirty {
            if let Some(room) = self.rooms.get_mut(&room_id) {
                room.dirty = false;
                room.next_broadcast_ms = now + 250;
            }
            self.broadcast_room_state(&room_id);
        }

        // Sessions silent past the timeout are treated as disconnected and get
        // the reconnect grace period before their membership is torn down.
        let silent: Vec<String> = self
            .sessions
            .values()
            .filter(|s| {
                s.disconnected_at.is_none()
                    && now.saturating_sub(s.last_seen_ms) > ctx.config.limits.session_silent_ms
            })
            .map(|s| s.id.clone())
            .collect();
        for sid in silent {
            self.mark_disconnected(&sid, now);
        }

        // Disconnected sessions past the grace period are removed for real.
        let gone: Vec<String> = self
            .sessions
            .values()
            .filter(|s| {
                s.disconnected_at
                    .is_some_and(|d| now.saturating_sub(d) > ctx.config.limits.resume_grace_ms)
            })
            .map(|s| s.id.clone())
            .collect();
        for sid in gone {
            self.remove_session(&sid, LeaveReason::Disconnected, ctx, now);
        }

        self.bans.sweep(now);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn client_id_hash(client_id: &str) -> String {
    let digest = Sha256::digest(client_id.as_bytes());
    hex::encode(digest)[..8].to_string()
}

fn hash_password(password: &str) -> [u8; 32] {
    Sha256::digest(password.as_bytes()).into()
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

/// Queue a binary frame directly on a session and account for its size.
fn queue_binary(sess: &Session, bytes: Vec<u8>) {
    if let Some(out) = &sess.out {
        sess.queued_bytes.fetch_add(bytes.len(), Ordering::Relaxed);
        let _ = out.send(OutMsg::Binary(bytes));
    }
}

fn feed_frame_name(frame: &FeedFrame) -> &'static str {
    match frame {
        FeedFrame::MatchStart(_) => "match_start",
        FeedFrame::Inputs { .. } => "inputs",
        FeedFrame::Checksum { .. } => "checksum",
        FeedFrame::MatchEnd { .. } => "match_end",
        FeedFrame::FeedReset { .. } => "feed_reset",
    }
}

/// True when both logs have a tick in `start..start+inputs.len()` and disagree.
fn inputs_mismatch(room: &Room, start: usize, inputs: &[(u16, u16)]) -> bool {
    for i in 0..inputs.len() {
        let tick = start + i;
        if let (Some(host), Some(guest)) =
            (room.log.inputs.get(tick), room.guest_log.inputs.get(tick))
        {
            if host != guest {
                return true;
            }
        }
    }
    false
}

/// Total match-log bytes for a room (host plus guest copies).
fn room_log_bytes(room: &Room) -> usize {
    room.log.byte_size() + room.guest_log.byte_size()
}

/// Apply a before/after delta to a running byte total, saturating at zero.
fn adjust_log_bytes(total: usize, before: usize, after: usize) -> usize {
    (total as i64 - before as i64 + after as i64).max(0) as usize
}

/// Compare dotted versions like `"0.4.1"`. Missing parts count as zero.
pub fn version_lt(a: &str, b: &str) -> bool {
    fn parse(v: &str) -> Vec<u64> {
        v.split('.').map(|p| p.parse().unwrap_or(0)).collect()
    }
    parse(a) < parse(b)
}
pub fn welcome_message(
    session: &Session,
    config: &Config,
    now: u64,
    udp: UdpInfo,
    limits: proto::Limits,
    online: proto::Online,
) -> ServerMessage {
    let older_than_latest = version_lt(&session.game_version, &config.game.latest_game_version);
    ServerMessage::Welcome {
        protocol: PROTOCOL_VERSION,
        session_id: session.id.clone(),
        resume_token: session.resume_token.clone(),
        server_time: now,
        region: config.server.region.clone(),
        udp,
        limits,
        online: Some(online),
        motd: config.game.motd.clone(),
        latest_game_version: if older_than_latest {
            Some(config.game.latest_game_version.clone())
        } else {
            None
        },
        update_url: if older_than_latest {
            Some(config.game.update_url.clone())
        } else {
            None
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replays::ReplayStore;
    use fighter_protocol::clock::TestClock;
    use std::path::PathBuf;

    fn test_lobby() -> Lobby {
        let (replay_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        Lobby::new(
            0,
            "test".into(),
            Bans::new(600_000, 86_400_000),
            Arc::new(Mutex::new(ReplayStore::new(
                PathBuf::from("target/test-replays"),
                false,
                1_000,
                512 * 1024 * 1024,
            ))),
            replay_tx,
            Arc::new(Mutex::new(Bindings::default())),
        )
    }

    fn add_host(lobby: &mut Lobby, config: &Config, now: u64) {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let session = Session::new(
            "s1".into(),
            "cid".into(),
            "Host".into(),
            "0.4.1".into(),
            111,
            false,
            None,
            "127.0.0.1".parse().unwrap(),
            tx,
            Arc::new(AtomicUsize::new(0)),
            now,
            config,
        );
        lobby.add_session(session);
    }

    // #6: room_update keeps the timer when the field is omitted.
    #[test]
    fn room_update_keeps_timer_when_omitted() {
        let config = Config::default();
        let metrics = Metrics::new();
        let clock = TestClock::new(1_000);
        let blocklist: Vec<String> = Vec::new();
        let ctx = Ctx {
            config: &config,
            metrics: &metrics,
            clock: &clock,
            blocklist: &blocklist,
        };
        let mut lobby = test_lobby();
        add_host(&mut lobby, &config, 1_000);
        let (room_id, _code) = lobby.spawn_room(
            "s1",
            "Room".into(),
            Visibility::Public,
            None,
            true,
            3_000,
            50,
            &ctx,
            1_000,
        );
        lobby.rooms.get_mut(&room_id).unwrap().timer = Some(5);

        // An update that omits `timer` must not clear it.
        lobby.handle_room_update(
            "s1",
            RoomUpdate {
                phase: Phase::InMatch,
                fighters: None,
                stage: None,
                round: Some(2),
                wins: None,
                timer: None,
            },
            None,
            &ctx,
            1_000,
        );
        assert_eq!(lobby.rooms[&room_id].timer, Some(5));

        // Supplying `timer` still updates it.
        lobby.handle_room_update(
            "s1",
            RoomUpdate {
                phase: Phase::InMatch,
                fighters: None,
                stage: None,
                round: None,
                wins: None,
                timer: Some(9),
            },
            None,
            &ctx,
            1_000,
        );
        assert_eq!(lobby.rooms[&room_id].timer, Some(9));
    }
}
