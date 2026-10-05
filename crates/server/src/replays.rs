//! Replay storage (section 9, M5).
//!
//! Finished match logs are written as a binary file: the feed frames
//! (MATCH_START, INPUTS, CHECKSUM, MATCH_END) concatenated, each prefixed with a
//! little-endian `u32` length, matching `GET /v1/replays/{id}`. A small JSON index
//! gives the list endpoint its metadata and survives restarts.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayMeta {
    pub id: String,
    pub stage: String,
    pub p1_fighter: String,
    pub p2_fighter: String,
    pub p1_name: String,
    pub p2_name: String,
    pub result: u8,
    pub duration_ticks: u32,
    pub finished_at: u64,
    pub game_version: String,
    pub content_hash: u32,
}

#[derive(Debug, Default)]
pub struct ReplayStore {
    dir: PathBuf,
    enabled: bool,
    metas: Vec<ReplayMeta>,
}

impl ReplayStore {
    pub fn new(dir: PathBuf, enabled: bool) -> Self {
        let mut store = Self {
            dir,
            enabled,
            metas: Vec::new(),
        };
        store.load_index();
        store
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    fn index_path(&self) -> PathBuf {
        self.dir.join("index.json")
    }

    fn load_index(&mut self) {
        if !self.enabled {
            return;
        }
        if let Ok(text) = std::fs::read_to_string(self.index_path()) {
            if let Ok(metas) = serde_json::from_str::<Vec<ReplayMeta>>(&text) {
                self.metas = metas;
            }
        }
    }

    fn save_index(&self) {
        if !self.enabled {
            return;
        }
        if let Ok(text) = serde_json::to_string(&self.metas) {
            let _ = std::fs::create_dir_all(&self.dir);
            let _ = std::fs::write(self.index_path(), text);
        }
    }

    /// Store a replay. `body` is the concatenated, length-prefixed frames.
    pub fn save(&mut self, meta: ReplayMeta, body: &[u8]) -> Option<String> {
        if !self.enabled {
            return None;
        }
        if std::fs::create_dir_all(&self.dir).is_err() {
            return None;
        }
        let path = self.dir.join(format!("{}.replay", meta.id));
        if std::fs::write(&path, body).is_err() {
            return None;
        }
        let id = meta.id.clone();
        self.metas.push(meta);
        self.save_index();
        Some(id)
    }

    /// Newest first, offset by `cursor` (a decimal string).
    pub fn list(&self, limit: usize, cursor: Option<&str>) -> (Vec<ReplayMeta>, Option<String>) {
        let offset: usize = cursor.and_then(|c| c.parse().ok()).unwrap_or(0);
        let mut all: Vec<&ReplayMeta> = self.metas.iter().collect();
        all.sort_by_key(|m| std::cmp::Reverse(m.finished_at));
        let page: Vec<ReplayMeta> = all.into_iter().skip(offset).take(limit).cloned().collect();
        let next = if offset + limit < self.metas.len() {
            Some((offset + limit).to_string())
        } else {
            None
        };
        (page, next)
    }

    pub fn load(&self, id: &str) -> Option<Vec<u8>> {
        if !self.enabled || !id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
            return None;
        }
        std::fs::read(self.dir.join(format!("{id}.replay"))).ok()
    }

    /// Remove replays older than the retention window.
    pub fn cleanup(&mut self, now_ms: u64, retention_days: u32) {
        if !self.enabled {
            return;
        }
        let cutoff = now_ms.saturating_sub(retention_days as u64 * 24 * 60 * 60 * 1_000);
        let expired: Vec<String> = self
            .metas
            .iter()
            .filter(|m| m.finished_at < cutoff)
            .map(|m| m.id.clone())
            .collect();
        for id in &expired {
            let _ = std::fs::remove_file(self.dir.join(format!("{id}.replay")));
        }
        self.metas.retain(|m| m.finished_at >= cutoff);
        if !expired.is_empty() {
            self.save_index();
        }
    }
}
