//! Control message system for the pub/sub framework
//!
//! This module implements the control plane messages for stream coordination.

use crate::pubsub::message::{Message, MessageId};
use crate::pubsub::topic::Topic;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// Control message type for the streaming system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    /// Command from a controller to a node
    Command(ControlCommand),
    /// Response from a node to a controller
    Response(ControlResponse),
    /// Stream announcement
    StreamAnnouncement(StreamAnnouncement),
    /// Stream status update
    StreamStatus(StreamStatusUpdate),
    /// Node status update
    NodeStatus(NodeStatusUpdate),
    /// Network topology update
    TopologyUpdate(TopologyUpdate),
    /// Capability announcement
    CapabilityAnnouncement(CapabilityAnnouncement),
}

impl ControlMessage {
    /// Convert to a pubsub message
    pub fn to_message(&self) -> Message {
        let payload = serde_json::to_vec(self).unwrap_or_default();
        Message::new_control(payload)
            .with_content_type("application/json")
    }
    
    /// Parse from a pubsub message
    pub fn from_message(message: &Message) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(&message.payload)
    }
}

/// Control command types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlCommand {
    /// Start a stream
    StartStream {
        /// Stream ID
        stream_id: String,
        /// Stream configuration
        config: StreamConfig,
    },
    
    /// Stop a stream
    StopStream {
        /// Stream ID
        stream_id: String,
    },
    
    /// Update a stream's configuration
    UpdateStream {
        /// Stream ID
        stream_id: String,
        /// Updated configuration
        config: StreamConfig,
    },
    
    /// Request the status of a stream
    GetStreamStatus {
        /// Stream ID
        stream_id: String,
    },
    
    /// Request the status of a node
    GetNodeStatus {
        /// Node ID (None means the receiving node)
        node_id: Option<String>,
    },
    
    /// Relay a stream through a specific node
    RelayStream {
        /// Stream ID
        stream_id: String,
        /// Node ID to relay through
        relay_node_id: String,
    },
    
    /// Disconnect a peer
    DisconnectPeer {
        /// Peer ID to disconnect
        peer_id: String,
        /// Reason for disconnection
        reason: String,
    },
}

/// Control response types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    /// Success response
    Success {
        /// Command ID this is responding to
        command_id: String,
        /// Optional data
        data: Option<serde_json::Value>,
    },
    
    /// Error response
    Error {
        /// Command ID this is responding to
        command_id: String,
        /// Error code
        code: u32,
        /// Error message
        message: String,
    },
    
    /// Stream status response
    StreamStatus {
        /// Command ID this is responding to
        command_id: String,
        /// Stream ID
        stream_id: String,
        /// Stream status
        status: StreamStatusUpdate,
    },
    
    /// Node status response
    NodeStatus {
        /// Command ID this is responding to
        command_id: String,
        /// Node ID
        node_id: String,
        /// Node status
        status: NodeStatusUpdate,
    },
}

/// Stream configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Stream name
    pub name: String,
    /// Stream description
    pub description: String,
    /// Stream metadata
    pub metadata: HashMap<String, String>,
    /// Maximum bitrate
    pub max_bitrate: u32,
    /// Video configuration
    pub video: Option<VideoConfig>,
    /// Audio configuration
    pub audio: Option<AudioConfig>,
    /// Topics associated with this stream
    pub topics: Vec<String>,
    /// Whether the stream is public
    pub is_public: bool,
    /// Access control settings
    pub access_control: AccessControl,
}

/// Video configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoConfig {
    /// Video codec
    pub codec: String,
    /// Width in pixels
    pub width: u32,
    /// Height in pixels
    pub height: u32,
    /// Frame rate
    pub framerate: f32,
    /// Keyframe interval
    pub keyframe_interval: u32,
}

/// Audio configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioConfig {
    /// Audio codec
    pub codec: String,
    /// Sample rate
    pub sample_rate: u32,
    /// Number of channels
    pub channels: u8,
    /// Bitrate
    pub bitrate: u32,
}

/// Access control configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessControl {
    /// Whether authentication is required
    pub require_auth: bool,
    /// Allowed user IDs
    pub allowed_users: Vec<String>,
    /// Allowed group IDs
    pub allowed_groups: Vec<String>,
    /// Whether moderation is enabled
    pub enable_moderation: bool,
}

/// Stream announcement message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamAnnouncement {
    /// Stream ID
    pub stream_id: String,
    /// Publisher node ID
    pub publisher_id: String,
    /// Stream configuration
    pub config: StreamConfig,
    /// Stream URL
    pub url: String,
    /// Announcement timestamp
    pub timestamp: u64,
}

/// Stream status update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStatusUpdate {
    /// Stream ID
    pub stream_id: String,
    /// Stream state
    pub state: StreamState,
    /// Connected viewers count
    pub viewers: u32,
    /// Current bitrate
    pub current_bitrate: u32,
    /// Buffer health (0.0 - 1.0)
    pub buffer_health: f32,
    /// Frame statistics
    pub frame_stats: FrameStats,
    /// Error message (if any)
    pub error: Option<String>,
    /// Update timestamp
    pub timestamp: u64,
}

/// Stream state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamState {
    /// Stream is initializing
    Initializing,
    /// Stream is live
    Live,
    /// Stream is paused
    Paused,
    /// Stream is stopping
    Stopping,
    /// Stream is stopped
    Stopped,
    /// Stream is in an error state
    Error,
}

/// Frame statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FrameStats {
    /// Total frames processed
    pub frames_processed: u64,
    /// Frames dropped
    pub frames_dropped: u64,
    /// Current frame rate
    pub current_fps: f32,
    /// Average keyframe size
    pub avg_keyframe_size: u32,
    /// Average frame size
    pub avg_frame_size: u32,
}

/// Node status update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatusUpdate {
    /// Node ID
    pub node_id: String,
    /// Node type
    pub node_type: String,
    /// Node state
    pub state: NodeState,
    /// Node uptime in seconds
    pub uptime: u64,
    /// CPU usage percentage
    pub cpu_usage: f32,
    /// Memory usage percentage
    pub memory_usage: f32,
    /// Network bandwidth usage
    pub bandwidth_usage: BandwidthUsage,
    /// Number of active streams
    pub active_streams: u32,
    /// Number of connected peers
    pub connected_peers: u32,
    /// Node version
    pub version: String,
    /// Update timestamp
    pub timestamp: u64,
}

/// Node state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is starting
    Starting,
    /// Node is running
    Running,
    /// Node is stopping
    Stopping,
    /// Node is in an error state
    Error,
}

/// Bandwidth usage statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BandwidthUsage {
    /// Incoming bandwidth in bits per second
    pub incoming_bps: u64,
    /// Outgoing bandwidth in bits per second
    pub outgoing_bps: u64,
    /// Total bandwidth in bits per second
    pub total_bps: u64,
}

/// Network topology update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyUpdate {
    /// Node ID sending the update
    pub node_id: String,
    /// Connected peers
    pub peers: Vec<PeerInfo>,
    /// Stream routing information
    pub stream_routes: Vec<StreamRoute>,
    /// Update timestamp
    pub timestamp: u64,
}

/// Peer information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Peer ID
    pub peer_id: String,
    /// Connection type
    pub connection_type: String,
    /// Whether this is an outbound connection
    pub is_outbound: bool,
    /// Connection latency in milliseconds
    pub latency_ms: u32,
    /// Connection establishment time
    pub connected_since: u64,
}

/// Stream routing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRoute {
    /// Stream ID
    pub stream_id: String,
    /// Source node ID
    pub source_id: String,
    /// List of relay nodes in the path
    pub relay_path: Vec<String>,
    /// Number of hops from the source
    pub hops: u32,
}

/// Capability announcement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityAnnouncement {
    /// Node ID
    pub node_id: String,
    /// Supported features
    pub features: Vec<String>,
    /// Supported codecs
    pub codecs: Vec<String>,
    /// Maximum inbound bandwidth
    pub max_inbound_bandwidth: u64,
    /// Maximum outbound bandwidth
    pub max_outbound_bandwidth: u64,
    /// Maximum concurrent streams
    pub max_streams: u32,
    /// Whether this node can relay streams
    pub can_relay: bool,
    /// Whether this node can transcode streams
    pub can_transcode: bool,
    /// Announcement timestamp
    pub timestamp: u64,
} 