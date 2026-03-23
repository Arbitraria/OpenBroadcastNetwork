//! Moderation types for peer blocking and stream flagging
//!
//! Serializable types for moderation state management and sharing.

use serde::{Deserialize, Serialize};

/// Entry representing a blocked peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockEntry {
    /// The peer ID that is blocked
    pub peer_id: String,
    /// Optional reason for the block
    pub reason: Option<String>,
    /// Unix timestamp when the peer was blocked
    pub blocked_at: u64,
}

/// Reason for flagging a stream
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "value")]
pub enum FlagReason {
    /// Unsolicited or repetitive content
    Spam,
    /// Content that violates community standards
    Inappropriate,
    /// Content that infringes copyright
    Copyright,
    /// Harassment or abusive content
    Abuse,
    /// Other reason with free-text description
    Other(String),
}

impl std::fmt::Display for FlagReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlagReason::Spam => write!(f, "Spam"),
            FlagReason::Inappropriate => write!(f, "Inappropriate"),
            FlagReason::Copyright => write!(f, "Copyright"),
            FlagReason::Abuse => write!(f, "Abuse"),
            FlagReason::Other(s) => write!(f, "Other: {}", s),
        }
    }
}

/// Entry representing a flagged stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlagEntry {
    /// The stream ID that was flagged
    pub stream_id: String,
    /// Reason for flagging
    pub reason: FlagReason,
    /// Optional details about the flag
    pub details: Option<String>,
    /// Optional reporter identity
    pub reporter: Option<String>,
    /// Unix timestamp when the stream was flagged
    pub flagged_at: u64,
}

/// Shareable moderation list format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModerationList {
    /// Schema version
    pub version: u32,
    /// Node ID that created this list
    pub node_id: Option<String>,
    /// Unix timestamp when the list was created
    pub created_at: u64,
    /// Blocked peers
    pub blocked_peers: Vec<BlockEntry>,
    /// Whitelisted peer IDs
    pub whitelisted_peers: Vec<String>,
    /// Flagged streams
    pub flagged_streams: Vec<FlagEntry>,
}

impl Default for ModerationList {
    fn default() -> Self {
        Self {
            version: 1,
            node_id: None,
            created_at: 0,
            blocked_peers: Vec::new(),
            whitelisted_peers: Vec::new(),
            flagged_streams: Vec::new(),
        }
    }
}
