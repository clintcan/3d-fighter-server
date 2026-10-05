//! Configuration: TOML file plus `FIGHTER__SECTION__KEY` environment overrides.

use std::net::SocketAddr;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub tls: Option<TlsConfig>,
    pub game: GameConfig,
    pub limits: LimitsConfig,
    pub admin: AdminConfig,
    pub storage: StorageConfig,
    pub moderation: ModerationConfig,
    pub regions: Vec<RegionConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub region: String,
    pub http_bind: String,
    pub udp_bind: String,
    pub public_udp_host: String,
    pub log_format: String,
    pub log_ips: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            region: "asia".into(),
            http_bind: "0.0.0.0:8080".into(),
            udp_bind: "0.0.0.0:7780".into(),
            public_udp_host: "localhost".into(),
            log_format: "pretty".into(),
            log_ips: false,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[allow(dead_code)] // in-process TLS arrives in M4
pub struct TlsConfig {
    pub cert: String,
    pub key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct GameConfig {
    pub min_game_version: String,
    pub latest_game_version: String,
    pub update_url: String,
    pub motd: Option<String>,
}

impl Default for GameConfig {
    fn default() -> Self {
        Self {
            min_game_version: "0.4.1".into(),
            latest_game_version: "0.4.1".into(),
            update_url: String::new(),
            motd: None,
        }
    }
}

/// Defaults from AGENTS.md section 10.2.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LimitsConfig {
    pub max_connections: usize,
    pub max_connections_per_ip: usize,
    pub max_rooms: usize,
    pub default_spectator_delay_ms: u32,
    pub messages_per_second: u32,
    pub message_burst: u32,
    pub list_rooms_per_second: u32,
    pub create_join_per_minute: u32,
    pub reaction_interval_ms: u64,
    pub join_request_timeout_ms: u64,
    pub decline_cooldown_ms: u64,
    pub idle_room_ms: u64,
    pub session_silent_ms: u64,
    pub max_room_name: u32,
    pub max_spectators: u32,
    pub max_players_per_client: usize,
    pub relay_datagrams_per_second: u32,
    pub relay_bytes_per_second: u32,
    pub binding_expiry_ms: u64,
    pub binding_max_age_ms: u64,
    pub punch_delay_ms: u64,
    pub udp_ping_per_second: u32,
    pub udp_unauth_per_second: u32,
    pub feed_frames_per_second: u32,
    pub spectator_max_queued_bytes: usize,
    pub resume_grace_ms: u64,
    pub ban_base_ms: u64,
    pub ban_max_ms: u64,
    pub shutdown_grace_ms: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_connections: 10_000,
            max_connections_per_ip: 32,
            max_rooms: 5_000,
            default_spectator_delay_ms: 3_000,
            messages_per_second: 20,
            message_burst: 40,
            list_rooms_per_second: 2,
            create_join_per_minute: 6,
            reaction_interval_ms: 2_000,
            join_request_timeout_ms: 30_000,
            decline_cooldown_ms: 30_000,
            idle_room_ms: 30 * 60 * 1_000,
            session_silent_ms: 45_000,
            max_room_name: 32,
            max_spectators: 200,
            max_players_per_client: 1,
            relay_datagrams_per_second: 200,
            relay_bytes_per_second: 64 * 1024,
            binding_expiry_ms: 60_000,
            binding_max_age_ms: 6 * 60 * 60 * 1_000,
            punch_delay_ms: 300,
            udp_ping_per_second: 10,
            udp_unauth_per_second: 20,
            feed_frames_per_second: 30,
            spectator_max_queued_bytes: 1024 * 1024,
            resume_grace_ms: 30_000,
            ban_base_ms: 10 * 60 * 1_000,
            ban_max_ms: 24 * 60 * 60 * 1_000,
            shutdown_grace_ms: 60_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AdminConfig {
    pub token_env: String,
    /// Optional inline token; overrides the environment variable when set.
    pub token: Option<String>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            token_env: "FIGHTER_ADMIN_TOKEN".into(),
            token: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub replays: bool,
    pub database: String,
    pub replay_dir: String,
    pub replay_retention_days: u32,
}

/// Optional word blocklist for names and room names (section 10.4).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ModerationConfig {
    pub blocklist: Vec<String>,
}

/// A region advertised by `GET /v1/regions` (section 12, M5).
#[derive(Debug, Clone, Deserialize)]
pub struct RegionConfig {
    pub region: String,
    pub ws_url: String,
    pub udp_host: String,
    pub udp_port: u16,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            replays: false,
            database: "fighter.db".into(),
            replay_dir: "replays".into(),
            replay_retention_days: 7,
        }
    }
}

impl Config {
    /// Load from a TOML file (if given) and apply `FIGHTER__...` overrides from
    /// the process environment.
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut value = match path {
            Some(p) => {
                let text = std::fs::read_to_string(p)
                    .with_context(|| format!("reading config {}", p.display()))?;
                toml::from_str::<toml::Value>(&text)
                    .with_context(|| format!("parsing config {}", p.display()))?
            }
            None => toml::Value::Table(Default::default()),
        };
        apply_env_overrides(&mut value, std::env::vars())?;
        value.try_into().context("deserializing configuration")
    }

    pub fn http_addr(&self) -> Result<SocketAddr> {
        self.server
            .http_bind
            .parse()
            .with_context(|| format!("invalid http_bind {}", self.server.http_bind))
    }
}

/// Apply `FIGHTER__SECTION__KEY=value` variables into the TOML tree.
fn apply_env_overrides(
    root: &mut toml::Value,
    vars: impl Iterator<Item = (String, String)>,
) -> Result<()> {
    for (key, val) in vars {
        let Some(rest) = key.strip_prefix("FIGHTER__") else {
            continue;
        };
        let parts: Vec<String> = rest
            .split("__")
            .filter(|s| !s.is_empty())
            .map(|s| s.to_ascii_lowercase())
            .collect();
        if parts.is_empty() {
            continue;
        }
        set_path(root, &parts, coerce(&val));
    }
    Ok(())
}

fn set_path(root: &mut toml::Value, parts: &[String], value: toml::Value) {
    let mut cur = root;
    for part in &parts[..parts.len() - 1] {
        let table = cur.as_table_mut().expect("config root is a table");
        cur = table
            .entry(part.clone())
            .or_insert_with(|| toml::Value::Table(Default::default()));
    }
    if let Some(table) = cur.as_table_mut() {
        table.insert(parts[parts.len() - 1].clone(), value);
    }
}

/// Best-effort scalar coercion: bool, then integer, then float, else string.
fn coerce(raw: &str) -> toml::Value {
    if let Ok(b) = raw.parse::<bool>() {
        return toml::Value::Boolean(b);
    }
    if let Ok(i) = raw.parse::<i64>() {
        return toml::Value::Integer(i);
    }
    if let Ok(f) = raw.parse::<f64>() {
        return toml::Value::Float(f);
    }
    toml::Value::String(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let c = Config::default();
        assert_eq!(c.server.region, "asia");
        assert_eq!(c.limits.max_connections, 10_000);
        assert_eq!(c.limits.max_connections_per_ip, 32);
        assert_eq!(c.game.min_game_version, "0.4.1");
    }

    #[test]
    fn env_overrides_apply_with_coercion() {
        let mut value = toml::Value::Table(Default::default());
        let vars = vec![
            ("FIGHTER__SERVER__REGION".to_string(), "eu".to_string()),
            (
                "FIGHTER__LIMITS__MAX_CONNECTIONS".to_string(),
                "123".to_string(),
            ),
            ("FIGHTER__SERVER__LOG_IPS".to_string(), "true".to_string()),
        ];
        apply_env_overrides(&mut value, vars.into_iter()).unwrap();
        let cfg: Config = value.try_into().unwrap();
        assert_eq!(cfg.server.region, "eu");
        assert_eq!(cfg.limits.max_connections, 123);
        assert!(cfg.server.log_ips);
    }

    #[test]
    fn parses_example_file() {
        let text = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/server.example.toml"),
        )
        .unwrap();
        let cfg: Config = toml::from_str(&text).unwrap();
        assert_eq!(cfg.server.region, "asia");
        assert_eq!(cfg.game.latest_game_version, "0.4.1");
    }
}
