//! Type definitions for the hybrid overlay
//!
//! This module defines the types used by the hybrid overlay implementation.

use crate::overlay::interface::StreamId;
use crate::overlay::peer::LocalPeerId;
use std::collections::HashMap;

/// Stream metadata
#[derive(Debug, Clone)]
pub struct StreamMetadata {
    /// Unique stream ID
    pub stream_id: StreamId,
    /// Publisher peer ID
    pub publisher: LocalPeerId,
    /// Stream metadata (codec, bitrate, etc.)
    pub metadata: HashMap<String, String>,
    /// Timestamp when the stream was published
    pub timestamp: u64,
    /// List of relay peers
    pub relay_peers: Vec<LocalPeerId>,
    /// Stream quality metrics
    pub quality: StreamQuality,
    /// Whether the stream is active
    pub is_active: bool,
    /// Whether this node is subscribed
    pub is_subscribed: bool,
}

/// Stream quality metrics
#[derive(Debug, Clone, Default)]
pub struct StreamQuality {
    /// Bandwidth in bytes per second
    pub bandwidth_bps: u64,
    /// Latency in milliseconds
    pub latency_ms: u64,
    /// Packet loss percentage
    pub packet_loss: f32,
    /// Frame rate
    pub framerate: f32,
}
