//! Protocol definitions and shared types for OpenBroadcastNetwork.
//!
//! This crate defines the wire protocol messages used for communication
//! between peers in the decentralized streaming network.

#![allow(non_snake_case, missing_docs)]

use serde::{Deserialize, Serialize};

/// Protocol version for compatibility checking.
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum allowed chunk size in bytes (2 MB).
pub const MAX_CHUNK_SIZE: usize = 2 * 1024 * 1024;

/// Messages exchanged between peers over the overlay network.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerMessage {
    /// Announce a new stream to the network.
    StreamAnnounce {
        stream_id: String,
        publisher_id: String,
        codec: String,
        /// Optional human-readable title.
        title: Option<String>,
    },

    /// Request to subscribe to a stream.
    SubscribeRequest {
        stream_id: String,
        subscriber_id: String,
    },

    /// Acknowledge a subscription.
    SubscribeAck { stream_id: String, accepted: bool },

    /// A chunk of media data.
    MediaChunk {
        stream_id: String,
        sequence: u64,
        /// Unix timestamp in milliseconds.
        timestamp_ms: u64,
        /// Whether this is a keyframe / initialization segment.
        is_init: bool,
        data: Vec<u8>,
    },

    /// Heartbeat / keepalive.
    Ping { nonce: u64 },

    /// Heartbeat response.
    Pong { nonce: u64 },

    /// Peer is leaving the network gracefully.
    Goodbye { reason: String },
}

/// Stream metadata shared during discovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    pub stream_id: String,
    pub publisher_id: String,
    pub title: Option<String>,
    pub codec: String,
    /// Number of known subscribers.
    pub subscriber_count: u32,
    /// When the stream started (Unix timestamp ms).
    pub started_at: u64,
}

/// Result of a network health check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub peer_id: String,
    pub version: u32,
    pub uptime_secs: u64,
    pub connected_peers: u32,
    pub active_streams: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_message_serializes() {
        let msg = PeerMessage::Ping { nonce: 42 };
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: PeerMessage = serde_json::from_str(&json).unwrap();
        match decoded {
            PeerMessage::Ping { nonce } => assert_eq!(nonce, 42),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn stream_info_serializes() {
        let info = StreamInfo {
            stream_id: "test-stream".into(),
            publisher_id: "peer-abc".into(),
            title: Some("My Stream".into()),
            codec: "video/mp4".into(),
            subscriber_count: 5,
            started_at: 1700000000000,
        };
        let json = serde_json::to_string(&info).unwrap();
        let decoded: StreamInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.stream_id, "test-stream");
        assert_eq!(decoded.subscriber_count, 5);
    }

    #[test]
    fn health_status_serializes() {
        let status = HealthStatus {
            peer_id: "12D3KooW...".into(),
            version: PROTOCOL_VERSION,
            uptime_secs: 3600,
            connected_peers: 10,
            active_streams: 2,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("12D3KooW"));
    }
}
