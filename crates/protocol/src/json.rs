//! Lobby JSON messages (section 6) and the `Room` object (section 6.4).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Wire protocol version, carried in `hello` and `welcome`.
pub const PROTOCOL_VERSION: u8 = 1;
/// Maximum text frame size.
pub const MAX_TEXT_FRAME: usize = 8 * 1024;
/// Maximum binary frame size.
pub const MAX_BINARY_FRAME: usize = 64 * 1024;
/// Maximum `rid` length.
pub const MAX_RID_LEN: usize = 32;

// ---------------------------------------------------------------------------
// Shared enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    Public,
    Unlisted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoomStatus {
    Open,
    Full,
    InMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Lobby,
    CharacterSelect,
    StageSelect,
    InMatch,
    Results,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Host,
    Guest,
    Spectator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionPath {
    Direct,
    Relay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Emote {
    Clap,
    Fire,
    Wow,
    Laugh,
    Gg,
    Ouch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateKind {
    Public,
    Local,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaveReason {
    Left,
    Disconnected,
    Kicked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseReason {
    HostLeft,
    HostDisconnected,
    Expired,
    Admin,
}

/// Error codes from section 6.5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadMessage,
    UnknownType,
    NotAllowed,
    VersionUnsupported,
    VersionMismatch,
    RoomNotFound,
    RoomFull,
    RoomBusy,
    WrongPassword,
    SpectatingDisabled,
    SpectatorsFull,
    RateLimited,
    NameInvalid,
    AlreadyInRoom,
    ServerFull,
    Internal,
}

// ---------------------------------------------------------------------------
// Room object
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerSlot {
    pub role: Role,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fighter: Option<String>,
}

/// Per-room match statistics (section 12, M5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomStats {
    pub matches: u32,
    pub rematches: u32,
    pub spectator_peak: u32,
}

fn default_true() -> bool {
    true
}

/// The room summary sent in `rooms`, `room_created` and `room_state`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Room {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub name: String,
    pub host_name: String,
    pub visibility: Visibility,
    pub has_password: bool,
    pub status: RoomStatus,
    pub phase: Phase,
    pub game_version: String,
    pub content_hash: u32,
    pub compatible: bool,
    pub players: Vec<PlayerSlot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub round: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wins: Option<[u8; 2]>,
    pub spectators: u32,
    pub allow_spectators: bool,
    pub max_spectators: u32,
    pub spectator_delay_ms: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<ConnectionPath>,
    #[serde(default = "default_true")]
    pub feed_verified: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<RoomStats>,
    pub region: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UdpInfo {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    pub max_room_name: u32,
    pub max_spectators: u32,
    pub reaction_interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    pub ip: String,
    pub port: u16,
    pub kind: CandidateKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub name: String,
}

// ---------------------------------------------------------------------------
// Client -> server messages (section 6.2)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListStatus {
    Open,
    InMatch,
    Any,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hello {
    pub protocol: u8,
    pub game_version: String,
    pub content_hash: u32,
    pub client_id: String,
    pub name: String,
    #[serde(default)]
    pub resume_token: Option<String>,
    #[serde(default)]
    pub relay_only: Option<bool>,
    #[serde(default)]
    pub region: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ping {
    pub t: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListRooms {
    #[serde(default)]
    pub status: Option<ListStatus>,
    #[serde(default)]
    pub spectatable: Option<bool>,
    #[serde(default)]
    pub compatible_only: Option<bool>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

impl ListRooms {
    pub fn status_or_default(&self) -> ListStatus {
        self.status.unwrap_or(ListStatus::Any)
    }
    pub fn compatible_only_or_default(&self) -> bool {
        self.compatible_only.unwrap_or(true)
    }
    pub fn limit_or_default(&self) -> u32 {
        self.limit.unwrap_or(50).clamp(1, 100)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateRoom {
    #[serde(default)]
    pub name: Option<String>,
    pub visibility: Visibility,
    #[serde(default)]
    pub password: Option<String>,
    pub allow_spectators: bool,
    #[serde(default)]
    pub spectator_delay_ms: Option<u32>,
    #[serde(default)]
    pub max_spectators: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinRoom {
    pub room: String,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelJoin {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnswerJoin {
    pub request_id: String,
    pub accept: bool,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Kick {
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaveRoom {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoomUpdate {
    pub phase: Phase,
    #[serde(default)]
    pub fighters: Option<[Option<String>; 2]>,
    #[serde(default)]
    pub stage: Option<String>,
    #[serde(default)]
    pub round: Option<u8>,
    #[serde(default)]
    pub wins: Option<[u8; 2]>,
    #[serde(default)]
    pub timer: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionReport {
    pub path: ConnectionPath,
    #[serde(default)]
    pub rtt_ms: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Spectate {
    pub room: String,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StopSpectating {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct React {
    pub emote: Emote,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueJoin {
    pub mode: String,
    #[serde(default)]
    pub region: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueLeave {}

#[derive(Debug, Clone, PartialEq)]
pub enum ClientMessage {
    Hello(Hello),
    Ping(Ping),
    ListRooms(ListRooms),
    CreateRoom(CreateRoom),
    JoinRoom(JoinRoom),
    CancelJoin(CancelJoin),
    AnswerJoin(AnswerJoin),
    Kick(Kick),
    LeaveRoom(LeaveRoom),
    RoomUpdate(RoomUpdate),
    ConnectionReport(ConnectionReport),
    Spectate(Spectate),
    StopSpectating(StopSpectating),
    React(React),
    QueueJoin(QueueJoin),
    QueueLeave(QueueLeave),
}

impl ClientMessage {
    /// The `"type"` string, for logging.
    pub fn type_name(&self) -> &'static str {
        match self {
            ClientMessage::Hello(_) => "hello",
            ClientMessage::Ping(_) => "ping",
            ClientMessage::ListRooms(_) => "list_rooms",
            ClientMessage::CreateRoom(_) => "create_room",
            ClientMessage::JoinRoom(_) => "join_room",
            ClientMessage::CancelJoin(_) => "cancel_join",
            ClientMessage::AnswerJoin(_) => "answer_join",
            ClientMessage::Kick(_) => "kick",
            ClientMessage::LeaveRoom(_) => "leave_room",
            ClientMessage::RoomUpdate(_) => "room_update",
            ClientMessage::ConnectionReport(_) => "connection_report",
            ClientMessage::Spectate(_) => "spectate",
            ClientMessage::StopSpectating(_) => "stop_spectating",
            ClientMessage::React(_) => "react",
            ClientMessage::QueueJoin(_) => "queue_join",
            ClientMessage::QueueLeave(_) => "queue_leave",
        }
    }
}

/// A client message plus its optional request id.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientEnvelope {
    pub rid: Option<String>,
    pub msg: ClientMessage,
}

/// Why a client text frame could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientParseError {
    /// Malformed JSON, not an object, missing `type`, or missing/invalid fields.
    BadMessage,
    /// A well-formed message with a `type` we do not know.
    UnknownType(String),
}

impl ClientParseError {
    pub fn error_code(&self) -> ErrorCode {
        match self {
            ClientParseError::BadMessage => ErrorCode::BadMessage,
            ClientParseError::UnknownType(_) => ErrorCode::UnknownType,
        }
    }
}

fn from_value<T: for<'de> Deserialize<'de>>(v: &Value) -> Result<T, ClientParseError> {
    serde_json::from_value(v.clone()).map_err(|_| ClientParseError::BadMessage)
}

/// Parse one text frame into an envelope. Never panics.
pub fn parse_client_message(text: &str) -> Result<ClientEnvelope, ClientParseError> {
    let value: Value = serde_json::from_str(text).map_err(|_| ClientParseError::BadMessage)?;
    let obj = value.as_object().ok_or(ClientParseError::BadMessage)?;
    let ty = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or(ClientParseError::BadMessage)?;

    let rid = match obj.get("rid").and_then(Value::as_str) {
        Some(r) => {
            if r.chars().count() > MAX_RID_LEN {
                return Err(ClientParseError::BadMessage);
            }
            Some(r.to_string())
        }
        None => None,
    };

    let msg = match ty {
        "hello" => ClientMessage::Hello(from_value(&value)?),
        "ping" => ClientMessage::Ping(from_value(&value)?),
        "list_rooms" => ClientMessage::ListRooms(from_value(&value)?),
        "create_room" => ClientMessage::CreateRoom(from_value(&value)?),
        "join_room" => ClientMessage::JoinRoom(from_value(&value)?),
        "cancel_join" => ClientMessage::CancelJoin(from_value(&value)?),
        "answer_join" => ClientMessage::AnswerJoin(from_value(&value)?),
        "kick" => ClientMessage::Kick(from_value(&value)?),
        "leave_room" => ClientMessage::LeaveRoom(from_value(&value)?),
        "room_update" => ClientMessage::RoomUpdate(from_value(&value)?),
        "connection_report" => ClientMessage::ConnectionReport(from_value(&value)?),
        "spectate" => ClientMessage::Spectate(from_value(&value)?),
        "stop_spectating" => ClientMessage::StopSpectating(from_value(&value)?),
        "react" => ClientMessage::React(from_value(&value)?),
        "queue_join" => ClientMessage::QueueJoin(from_value(&value)?),
        "queue_leave" => ClientMessage::QueueLeave(from_value(&value)?),
        other => return Err(ClientParseError::UnknownType(other.to_string())),
    };
    Ok(ClientEnvelope { rid, msg })
}

// ---------------------------------------------------------------------------
// Server -> client messages (section 6.3)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Welcome {
        protocol: u8,
        session_id: String,
        resume_token: String,
        server_time: u64,
        region: String,
        udp: UdpInfo,
        limits: Limits,
        #[serde(skip_serializing_if = "Option::is_none")]
        motd: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        latest_game_version: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        update_url: Option<String>,
    },
    Pong {
        t: u64,
        server_time: u64,
    },
    Rooms {
        rooms: Vec<Room>,
        #[serde(skip_serializing_if = "Option::is_none")]
        next_cursor: Option<String>,
    },
    RoomCreated {
        room: Room,
        code: String,
    },
    JoinPending {
        room_id: String,
    },
    JoinRequest {
        request_id: String,
        name: String,
        client_id_hash: String,
    },
    JoinCancelled {
        request_id: String,
    },
    JoinDeclined {
        room_id: String,
        reason: String,
    },
    MatchSession {
        room_id: String,
        role: Role,
        session_token: String,
        relay_key: String,
        peer: PeerInfo,
        udp: UdpInfo,
    },
    PeerEndpoints {
        room_id: String,
        candidates: Vec<Candidate>,
        punch_at: u64,
    },
    RoomState {
        room: Room,
    },
    PlayerLeft {
        room_id: String,
        role: Role,
        reason: LeaveReason,
    },
    RoomClosed {
        room_id: String,
        reason: CloseReason,
    },
    SpectateStarted {
        room_id: String,
        delay_ms: u32,
        match_live: bool,
    },
    SpectateEnded {
        room_id: String,
        reason: String,
    },
    Reaction {
        room_id: String,
        #[serde(rename = "from")]
        from_role: Role,
        name: String,
        emote: Emote,
    },
    QueueMatched {
        room_id: String,
    },
    ServerNotice {
        message: String,
        severity: Severity,
    },
    Error {
        code: ErrorCode,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        rid: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hello_and_extracts_rid() {
        let text = r#"{"type":"hello","protocol":1,"game_version":"0.4.1","content_hash":7,
            "client_id":"abc","name":"Lino","rid":"r1","extra_ignored":true}"#;
        let env = parse_client_message(text).unwrap();
        assert_eq!(env.rid.as_deref(), Some("r1"));
        match env.msg {
            ClientMessage::Hello(h) => {
                assert_eq!(h.game_version, "0.4.1");
                assert_eq!(h.name, "Lino");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn unknown_type_is_reported_not_bad_message() {
        let err = parse_client_message(r#"{"type":"teleport"}"#).unwrap_err();
        assert_eq!(err, ClientParseError::UnknownType("teleport".into()));
        assert_eq!(err.error_code(), ErrorCode::UnknownType);
    }

    #[test]
    fn malformed_and_missing_fields_are_bad_message() {
        assert_eq!(
            parse_client_message("not json").unwrap_err(),
            ClientParseError::BadMessage
        );
        assert_eq!(
            parse_client_message(r#"{"type":"ping"}"#).unwrap_err(),
            ClientParseError::BadMessage
        );
        assert_eq!(
            parse_client_message(r#"{"type":"ping","t":1,"rid":"` + &"x".repeat(33) + r#""}"#)
                .unwrap_err(),
            ClientParseError::BadMessage
        );
    }

    #[test]
    fn server_message_serializes_with_type_tag() {
        let msg = ServerMessage::Pong {
            t: 5,
            server_time: 1000,
        };
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["type"], "pong");
        assert_eq!(v["t"], 5);
    }

    #[test]
    fn room_omits_absent_optional_fields() {
        let room = Room {
            id: "r_1".into(),
            code: None,
            name: "Room".into(),
            host_name: "Lino".into(),
            visibility: Visibility::Public,
            has_password: false,
            status: RoomStatus::Open,
            phase: Phase::Lobby,
            game_version: "0.4.1".into(),
            content_hash: 1,
            compatible: true,
            players: vec![PlayerSlot {
                role: Role::Host,
                name: "Lino".into(),
                fighter: None,
            }],
            stage: None,
            round: None,
            wins: None,
            spectators: 0,
            allow_spectators: true,
            max_spectators: 50,
            spectator_delay_ms: 3000,
            connection: None,
            feed_verified: true,
            stats: None,
            region: "asia".into(),
            created_at: 0,
        };
        let v = serde_json::to_value(&room).unwrap();
        assert!(v.get("code").is_none());
        assert!(v.get("stage").is_none());
        assert!(v.get("connection").is_none());
        assert_eq!(v["players"][0]["role"], "host");
    }
}
