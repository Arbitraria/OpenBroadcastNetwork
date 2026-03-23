//! Per-node moderation system for peer blocking and stream flagging
//!
//! Provides blacklists/whitelists, stream flagging, and import/export
//! of moderation lists for sharing between nodes.

pub mod config;
pub mod manager;
pub mod types;

pub use config::ModerationConfig;
pub use manager::ModerationManager;
pub use types::{BlockEntry, FlagEntry, FlagReason, ModerationList};
