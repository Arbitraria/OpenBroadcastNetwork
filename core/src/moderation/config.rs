//! Configuration for the moderation system

use std::path::PathBuf;

/// Configuration for the moderation manager
#[derive(Debug, Clone, Default)]
pub struct ModerationConfig {
    /// Path to persist moderation state (JSON file)
    pub persistence_path: Option<PathBuf>,
    /// Automatically persist changes to disk
    pub auto_persist: bool,
    /// Only allow whitelisted peers to connect
    pub whitelist_only: bool,
}
