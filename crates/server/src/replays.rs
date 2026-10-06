//! Replay storage (section 9, M5).
//!
//! Finished match logs are written as a binary file: the feed frames
//! (MATCH_START, INPUTS, CHECKSUM, MATCH_END) concatenated, each prefixed with a
//! little-endian `u32` length, matching `GET /v1/replays/{id}`. A small JSON index
//! gives the list endpoint its metadata and survives restarts.
//!
//! Writes happen on a background task (see `serve::replay_writer`), so the lobby
//! lock is never held for file I/O. The store enforces a count and byte quota by
//! evicting the oldest replays first (issue #9).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// A replay write handed to the background writer.
pub type ReplayJob = (ReplayMeta, Vec<u8>);

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

#[derive(Debug)]
pub struct ReplayStore {
    dir: PathBuf,
    enabled: bool,
    /// Kept sorted by `finished_at` ascending.
    metas: Vec<ReplayMeta>,
    total_bytes: u64,
    max_count: usize,
    max_bytes: u64,
}

impl ReplayStore {
    pub fn new(dir: PathBuf, enabled: bool, max_count: usize, max_bytes: u64) -> Self {
        let mut store = Self {
            dir,
            enabled,
            metas: Vec::new(),
            total_bytes: 0,
            max_count,
            max_bytes,
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
                self.metas.sort_by_key(|m| m.finished_at);
                self.total_bytes = self
                    .metas
                    .iter()
                    .filter_map(|m| {
                        std::fs::metadata(self.dir.join(format!("{}.replay", m.id)))
                            .ok()
                            .map(|md| md.len())
                    })
                    .sum();
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
    /// Called from the background writer, never while the lobby lock is held.
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
        self.total_bytes = self.total_bytes.saturating_add(body.len() as u64);
        // Insert keeping the list sorted by finish time.
        let pos = self
            .metas
            .partition_point(|m| m.finished_at <= meta.finished_at);
        self.metas.insert(pos, meta);
        self.evict();
        self.save_index();
        Some(id)
    }

    /// Drop the oldest replays until the count and byte quotas are met.
    fn evict(&mut self) {
        while self.metas.len() > self.max_count || self.total_bytes > self.max_bytes {
            if self.metas.is_empty() {
                break;
            }
            let old = self.metas.remove(0);
            let path = self.dir.join(format!("{}.replay", old.id));
            if let Ok(md) = std::fs::metadata(&path) {
                self.total_bytes = self.total_bytes.saturating_sub(md.len());
            }
            let _ = std::fs::remove_file(&path);
        }
    }

    /// Newest first, offset by `cursor` (a decimal string).
    pub fn list(&self, limit: usize, cursor: Option<&str>) -> (Vec<ReplayMeta>, Option<String>) {
        let offset: usize = cursor.and_then(|c| c.parse().ok()).unwrap_or(0);
        let page: Vec<ReplayMeta> = self
            .metas
            .iter()
            .rev()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();
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

    pub fn path(&self, id: &str) -> Option<PathBuf> {
        if !self.enabled || !id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
            return None;
        }
        Some(self.dir.join(format!("{id}.replay")))
    }

    /// Remove replays older than the retention window.
    pub fn cleanup(&mut self, now_ms: u64, retention_days: u32) {
        if !self.enabled {
            return;
        }
        let cutoff = now_ms.saturating_sub(retention_days as u64 * 24 * 60 * 60 * 1_000);
        let mut removed = false;
        self.metas.retain(|m| {
            if m.finished_at < cutoff {
                let path = self.dir.join(format!("{}.replay", m.id));
                if let Ok(md) = std::fs::metadata(&path) {
                    self.total_bytes = self.total_bytes.saturating_sub(md.len());
                }
                let _ = std::fs::remove_file(&path);
                removed = true;
                false
            } else {
                true
            }
        });
        if removed {
            self.save_index();
        }
    }
}
