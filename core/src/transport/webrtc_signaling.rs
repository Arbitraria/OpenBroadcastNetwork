//! WebRTC signaling types for browser-to-browser P2P connections
//!
//! This module provides signaling message types for establishing WebRTC
//! data channel connections between browsers. The actual WebRTC implementation
//! runs in the browser using the native WebRTC API.
//!
//! # Architecture
//!
//! ```text
//! Browser A                    Server                    Browser B
//!     |                          |                          |
//!     |-- peer_join ------------>|                          |
//!     |<-- peer_list ------------|                          |
//!     |                          |<-- peer_join ------------|
//!     |<-- peer_join (notify) ---|                          |
//!     |                          |--- peer_list ----------->|
//!     |                          |--- peer_join (notify) -->|
//!     |-- offer ---------------->|--- offer --------------->|
//!     |<-- answer ---------------|<-- answer ---------------|
//!     |-- ice_candidate -------->|--- ice_candidate ------->|
//!     |<-- ice_candidate --------|<-- ice_candidate --------|
//!     |<========= WebRTC DataChannel Established ==========>|
//! ```

use serde::{Deserialize, Serialize};

/// WebRTC signaling messages for peer connection establishment
///
/// These messages are exchanged via a signaling server (WebSocket) to establish
/// direct peer-to-peer WebRTC connections between browsers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SignalingMessage {
    /// SDP offer from initiating peer
    #[serde(rename = "offer")]
    Offer {
        /// Unique identifier of the sending peer
        peer_id: String,
        /// Target peer ID (if directed)
        target_peer_id: Option<String>,
        /// SDP offer description
        sdp: String,
    },

    /// SDP answer in response to an offer
    #[serde(rename = "answer")]
    Answer {
        /// Unique identifier of the sending peer
        peer_id: String,
        /// Target peer ID
        target_peer_id: String,
        /// SDP answer description
        sdp: String,
    },

    /// ICE candidate for NAT traversal
    #[serde(rename = "ice_candidate")]
    IceCandidate {
        /// Unique identifier of the sending peer
        peer_id: String,
        /// Target peer ID
        target_peer_id: String,
        /// ICE candidate string
        candidate: String,
        /// SDP media identifier
        sdp_mid: Option<String>,
        /// SDP media line index
        sdp_mline_index: Option<u32>,
    },

    /// Peer joining a stream
    #[serde(rename = "peer_join")]
    PeerJoin {
        /// Unique identifier of the peer
        peer_id: String,
        /// Stream ID the peer wants to join
        stream_id: String,
        /// Whether this peer is a publisher
        is_publisher: bool,
    },

    /// Peer leaving a stream
    #[serde(rename = "peer_leave")]
    PeerLeave {
        /// Unique identifier of the peer
        peer_id: String,
    },

    /// List of peers in a stream (sent to newly joined peers)
    #[serde(rename = "peer_list")]
    PeerList {
        /// List of peer IDs currently in the stream
        peers: Vec<PeerInfo>,
    },

    /// Peer update (e.g., subscriber becomes relayer)
    #[serde(rename = "peer_update")]
    PeerUpdate {
        /// Unique identifier of the peer
        peer_id: String,
        /// Stream ID the peer is in
        #[serde(default)]
        stream_id: Option<String>,
        /// Whether this peer is a publisher
        is_publisher: bool,
        /// Whether this peer can relay data to other browsers
        #[serde(default)]
        can_relay: bool,
    },

    /// Error message from signaling server
    #[serde(rename = "error")]
    Error {
        /// Error message
        message: String,
    },
}

/// Information about a peer in the signaling system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Unique identifier of the peer
    pub peer_id: String,
    /// Whether this peer is a publisher
    pub is_publisher: bool,
    /// Whether this peer can relay data to other browsers
    #[serde(default)]
    pub can_relay: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signaling_message_serialization() {
        let offer = SignalingMessage::Offer {
            peer_id: "peer-123".to_string(),
            target_peer_id: Some("peer-456".to_string()),
            sdp: "v=0\r\no=- ...".to_string(),
        };

        let json = serde_json::to_string(&offer).unwrap();
        assert!(json.contains("\"type\":\"offer\""));
        assert!(json.contains("\"peer_id\":\"peer-123\""));

        let parsed: SignalingMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            SignalingMessage::Offer { peer_id, .. } => {
                assert_eq!(peer_id, "peer-123");
            }
            _ => panic!("Expected Offer message"),
        }
    }

    #[test]
    fn test_peer_join_serialization() {
        let join = SignalingMessage::PeerJoin {
            peer_id: "peer-123".to_string(),
            stream_id: "stream-abc".to_string(),
            is_publisher: true,
        };

        let json = serde_json::to_string(&join).unwrap();
        assert!(json.contains("\"type\":\"peer_join\""));
        assert!(json.contains("\"is_publisher\":true"));
    }

    #[test]
    fn test_peer_update_serialization() {
        let update = SignalingMessage::PeerUpdate {
            peer_id: "peer-123".to_string(),
            stream_id: Some("stream-abc".to_string()),
            is_publisher: false,
            can_relay: true,
        };

        let json = serde_json::to_string(&update).unwrap();
        assert!(json.contains("\"type\":\"peer_update\""));
        assert!(json.contains("\"can_relay\":true"));

        let parsed: SignalingMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            SignalingMessage::PeerUpdate { can_relay, .. } => {
                assert!(can_relay);
            }
            _ => panic!("Expected PeerUpdate message"),
        }
    }

    #[test]
    fn test_peer_info_can_relay_default() {
        let json = r#"{"peer_id":"peer-1","is_publisher":false}"#;
        let info: PeerInfo = serde_json::from_str(json).unwrap();
        assert!(!info.can_relay);
    }

    #[test]
    fn test_ice_candidate_serialization() {
        let ice = SignalingMessage::IceCandidate {
            peer_id: "peer-123".to_string(),
            target_peer_id: "peer-456".to_string(),
            candidate: "candidate:123 1 udp 2130706431 192.168.1.1 50000 typ host".to_string(),
            sdp_mid: Some("0".to_string()),
            sdp_mline_index: Some(0),
        };

        let json = serde_json::to_string(&ice).unwrap();
        assert!(json.contains("\"type\":\"ice_candidate\""));
        assert!(json.contains("\"candidate\":"));
    }
}
