//! Moderation manager for per-node peer blocking and stream flagging
//!
//! Thread-safe manager using `tokio::sync::RwLock` for concurrent access.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;
use tracing::{info, warn};

use super::config::ModerationConfig;
use super::types::{BlockEntry, FlagEntry, FlagReason, ModerationList};

/// Thread-safe moderation manager
pub struct ModerationManager {
    config: ModerationConfig,
    blocked_peers: Arc<RwLock<HashMap<String, BlockEntry>>>,
    whitelist: Arc<RwLock<HashSet<String>>>,
    flags: Arc<RwLock<Vec<FlagEntry>>>,
}

impl ModerationManager {
    /// Create a new moderation manager with the given config
    pub fn new(config: ModerationConfig) -> Self {
        Self {
            config,
            blocked_peers: Arc::new(RwLock::new(HashMap::new())),
            whitelist: Arc::new(RwLock::new(HashSet::new())),
            flags: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Block a peer with an optional reason
    pub async fn block_peer(&self, peer_id: &str, reason: Option<String>) {
        let entry = BlockEntry {
            peer_id: peer_id.to_string(),
            reason,
            blocked_at: now_unix(),
        };
        self.blocked_peers
            .write()
            .await
            .insert(peer_id.to_string(), entry);
        info!("Blocked peer: {}", peer_id);
        self.maybe_persist().await;
    }

    /// Unblock a peer
    pub async fn unblock_peer(&self, peer_id: &str) -> bool {
        let removed = self.blocked_peers.write().await.remove(peer_id).is_some();
        if removed {
            info!("Unblocked peer: {}", peer_id);
            self.maybe_persist().await;
        }
        removed
    }

    /// Check if a peer is allowed to connect.
    ///
    /// A peer is allowed when:
    /// - It is not on the block list, AND
    /// - Either whitelist_only is false, or the peer is on the whitelist.
    pub async fn is_peer_allowed(&self, peer_id: &str) -> bool {
        let blocked = self.blocked_peers.read().await;
        if blocked.contains_key(peer_id) {
            return false;
        }
        if self.config.whitelist_only {
            let wl = self.whitelist.read().await;
            return wl.contains(peer_id);
        }
        true
    }

    /// Get list of blocked peers
    pub async fn blocked_peers(&self) -> Vec<BlockEntry> {
        self.blocked_peers.read().await.values().cloned().collect()
    }

    /// Add a peer to the whitelist
    pub async fn whitelist_peer(&self, peer_id: &str) {
        self.whitelist.write().await.insert(peer_id.to_string());
        info!("Whitelisted peer: {}", peer_id);
        self.maybe_persist().await;
    }

    /// Remove a peer from the whitelist
    pub async fn remove_whitelist(&self, peer_id: &str) -> bool {
        let removed = self.whitelist.write().await.remove(peer_id);
        if removed {
            info!("Removed peer from whitelist: {}", peer_id);
            self.maybe_persist().await;
        }
        removed
    }

    /// Flag a stream
    pub async fn flag_stream(
        &self,
        stream_id: &str,
        reason: FlagReason,
        details: Option<String>,
        reporter: Option<String>,
    ) {
        let entry = FlagEntry {
            stream_id: stream_id.to_string(),
            reason,
            details,
            reporter,
            flagged_at: now_unix(),
        };
        self.flags.write().await.push(entry);
        info!("Flagged stream: {}", stream_id);
        self.maybe_persist().await;
    }

    /// Remove all flags for a stream
    pub async fn unflag_stream(&self, stream_id: &str) -> usize {
        let mut flags = self.flags.write().await;
        let before = flags.len();
        flags.retain(|f| f.stream_id != stream_id);
        let removed = before - flags.len();
        if removed > 0 {
            info!(
                "Unflagged stream: {} ({} flags removed)",
                stream_id, removed
            );
            drop(flags);
            self.maybe_persist().await;
        }
        removed
    }

    /// Get all flagged streams
    pub async fn flagged_streams(&self) -> Vec<FlagEntry> {
        self.flags.read().await.clone()
    }

    /// Check if a stream is flagged
    pub async fn is_stream_flagged(&self, stream_id: &str) -> bool {
        self.flags
            .read()
            .await
            .iter()
            .any(|f| f.stream_id == stream_id)
    }

    /// Load moderation state from a JSON file
    pub async fn load_from_file(&self, path: &std::path::Path) -> Result<(), String> {
        let data = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
        let list: ModerationList = serde_json::from_str(&data)
            .map_err(|e| format!("Failed to parse moderation list: {}", e))?;
        self.import_list(list).await;
        info!("Loaded moderation state from {}", path.display());
        Ok(())
    }

    /// Save moderation state to a JSON file
    pub async fn save_to_file(&self, path: &std::path::Path) -> Result<(), String> {
        let list = self.export_list().await;
        let data = serde_json::to_string_pretty(&list)
            .map_err(|e| format!("Failed to serialize: {}", e))?;
        std::fs::write(path, data)
            .map_err(|e| format!("Failed to write {}: {}", path.display(), e))?;
        info!("Saved moderation state to {}", path.display());
        Ok(())
    }

    /// Import a shared moderation list (merges with current state)
    pub async fn import_list(&self, list: ModerationList) {
        {
            let mut blocked = self.blocked_peers.write().await;
            for entry in list.blocked_peers {
                blocked.insert(entry.peer_id.clone(), entry);
            }
        }
        {
            let mut wl = self.whitelist.write().await;
            for peer_id in list.whitelisted_peers {
                wl.insert(peer_id);
            }
        }
        {
            let mut flags = self.flags.write().await;
            flags.extend(list.flagged_streams);
        }
    }

    /// Export the current moderation state as a shareable list
    pub async fn export_list(&self) -> ModerationList {
        let blocked = self.blocked_peers.read().await;
        let wl = self.whitelist.read().await;
        let flags = self.flags.read().await;

        ModerationList {
            version: 1,
            node_id: None,
            created_at: now_unix(),
            blocked_peers: blocked.values().cloned().collect(),
            whitelisted_peers: wl.iter().cloned().collect(),
            flagged_streams: flags.clone(),
        }
    }

    /// Auto-persist if configured
    async fn maybe_persist(&self) {
        if self.config.auto_persist {
            if let Some(ref path) = self.config.persistence_path {
                if let Err(e) = self.save_to_file(path).await {
                    warn!("Auto-persist failed: {}", e);
                }
            }
        }
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_block_and_unblock_peer() {
        let mgr = ModerationManager::new(ModerationConfig::default());

        assert!(mgr.is_peer_allowed("peer1").await);

        mgr.block_peer("peer1", Some("spam".into())).await;
        assert!(!mgr.is_peer_allowed("peer1").await);
        assert_eq!(mgr.blocked_peers().await.len(), 1);

        assert!(mgr.unblock_peer("peer1").await);
        assert!(mgr.is_peer_allowed("peer1").await);
        assert_eq!(mgr.blocked_peers().await.len(), 0);
    }

    #[tokio::test]
    async fn test_whitelist_only_mode() {
        let config = ModerationConfig {
            whitelist_only: true,
            ..Default::default()
        };
        let mgr = ModerationManager::new(config);

        // Not whitelisted → denied
        assert!(!mgr.is_peer_allowed("peer1").await);

        mgr.whitelist_peer("peer1").await;
        assert!(mgr.is_peer_allowed("peer1").await);

        // Blocked overrides whitelist
        mgr.block_peer("peer1", None).await;
        assert!(!mgr.is_peer_allowed("peer1").await);
    }

    #[tokio::test]
    async fn test_flag_and_unflag_stream() {
        let mgr = ModerationManager::new(ModerationConfig::default());

        assert!(!mgr.is_stream_flagged("stream1").await);

        mgr.flag_stream("stream1", FlagReason::Spam, None, None)
            .await;
        assert!(mgr.is_stream_flagged("stream1").await);
        assert_eq!(mgr.flagged_streams().await.len(), 1);

        let removed = mgr.unflag_stream("stream1").await;
        assert_eq!(removed, 1);
        assert!(!mgr.is_stream_flagged("stream1").await);
    }

    #[tokio::test]
    async fn test_export_import() {
        let mgr = ModerationManager::new(ModerationConfig::default());
        mgr.block_peer("peer1", Some("test".into())).await;
        mgr.whitelist_peer("peer2").await;
        mgr.flag_stream("s1", FlagReason::Copyright, None, None)
            .await;

        let list = mgr.export_list().await;
        assert_eq!(list.version, 1);
        assert_eq!(list.blocked_peers.len(), 1);
        assert_eq!(list.whitelisted_peers.len(), 1);
        assert_eq!(list.flagged_streams.len(), 1);

        // Import into a fresh manager
        let mgr2 = ModerationManager::new(ModerationConfig::default());
        mgr2.import_list(list).await;
        assert!(!mgr2.is_peer_allowed("peer1").await);
        assert!(mgr2.is_stream_flagged("s1").await);
    }

    #[tokio::test]
    async fn test_persistence_roundtrip() {
        let dir = std::env::temp_dir().join("obn_mod_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("moderation.json");

        let mgr = ModerationManager::new(ModerationConfig::default());
        mgr.block_peer("peer1", Some("test".into())).await;
        mgr.flag_stream("s1", FlagReason::Abuse, None, None).await;
        mgr.save_to_file(&path).await.unwrap();

        let mgr2 = ModerationManager::new(ModerationConfig::default());
        mgr2.load_from_file(&path).await.unwrap();
        assert!(!mgr2.is_peer_allowed("peer1").await);
        assert!(mgr2.is_stream_flagged("s1").await);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
