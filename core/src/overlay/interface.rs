//! Core interfaces for the overlay network
//!
//! This module defines the fundamental interfaces for the overlay network
//! used in the decentralized streaming system. These interfaces form the contract
//! that all overlay implementations must follow.
//!
//! # Core Abstractions
//!
//! The overlay network is defined by several key abstractions:
//!
//! 1. **Overlay Trait** - The primary interface that defines the overlay network capabilities
//!    including peer discovery, connection management, and stream operations.
//!
//! 2. **StreamId** - A unique identifier for data streams in the overlay network.
//!    Used for creating, tracking, and managing stream data flow.
//!
//! 3. **OverlayConfig** - Configuration parameters for initializing an overlay network.
//!    Controls behavior, networking parameters, and protocol settings.
//!
//! 4. **OverlayEvent** - Events emitted by the overlay network that indicate state
//!    changes, peer connections, and stream data availability.
//!
//! 5. **OverlayError** - Error types that can occur during overlay network operations.
//!    Provides detailed error information for diagnostics and recovery.
//!
//! # Implementation Contract
//!
//! Any implementation of the `Overlay` trait must provide thread-safe methods that handle:
//! - Starting and stopping the overlay network
//! - Connecting to peers by address
//! - Creating and removing streams
//! - Adding peers to streams
//! - Publishing stream data chunks
//! - Processing incoming events
//!
//! # Integration Points
//!
//! This module is a cornerstone that is used by:
//! - `overlay::libp2p_impl` - The primary libp2p-based implementation
//! - Client applications that interact with the overlay network
//! - Testing frameworks for verifying overlay network behavior
//!
//! # Design Philosophy
//!
//! The interfaces are designed with these principles:
//! - **Async-first** - All network operations are async to support high concurrency
//! - **Error transparency** - Detailed error types for better diagnostics
//! - **Abstraction** - Implementation details are hidden behind trait interfaces
//! - **Configurability** - Extensive configuration options for different deployment scenarios

use crate::overlay::peer::{LocalPeerId, PeerInfo};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

/// Overlay network errors
#[derive(Debug, thiserror::Error)]
pub enum OverlayError {
    /// Peer connection error
    #[error("Peer connection error: {0}")]
    ConnectionError(String),

    /// Peer discovery error
    #[error("Peer discovery error: {0}")]
    DiscoveryError(String),

    /// Stream relay error
    #[error("Stream relay error: {0}")]
    RelayError(String),

    /// Topology error
    #[error("Topology error: {0}")]
    TopologyError(String),

    /// Invalid peer ID
    #[error("Invalid peer ID: {0}")]
    InvalidPeerId(String),

    /// Operation not possible when not running
    #[error("Operation not possible: not running")]
    NotRunning,

    /// Stream not found error
    #[error("Stream not found: {0:?}")]
    StreamNotFound(StreamId),

    /// Stream already exists error
    #[error("Stream already exists: {0:?}")]
    StreamAlreadyExists(StreamId),

    /// No chunk handler available
    #[error("No chunk handler available")]
    NoChunkHandler,

    /// Peer not found error
    #[error("Peer not found: {0}")]
    PeerNotFound(libp2p::PeerId),

    /// Already running error
    #[error("Operation not possible: already running")]
    AlreadyRunning,

    /// Operation timeout
    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),

    /// Protocol error
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    /// System is stopping
    #[error("System is stopping")]
    Stopping,

    /// General error
    #[error("General error: {0}")]
    General(String),

    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Other error
    #[error("Error: {0}")]
    Other(String),
}

/// A unique stream identifier
///
/// Uses `bytes::Bytes` for zero-copy cloning, which is important for performance
/// when stream IDs are frequently cloned (e.g., in hash map operations).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamId(pub bytes::Bytes);

impl StreamId {
    /// Create a new random stream ID
    pub fn new_random() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut bytes = vec![0u8; 16];
        rng.fill(&mut bytes[..]);
        Self(bytes::Bytes::from(bytes))
    }

    /// Create a stream ID from a byte vector
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes::Bytes::from(bytes))
    }

    /// Create a stream ID from a Bytes object (zero-copy)
    pub fn from_bytes_ref(bytes: bytes::Bytes) -> Self {
        Self(bytes)
    }

    /// Create a stream ID from a string
    pub fn from_string(s: &str) -> Self {
        Self(bytes::Bytes::from(s.to_owned()))
    }

    /// Get the inner bytes as a slice
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Get the inner Bytes object (zero-copy clone)
    pub fn bytes(&self) -> bytes::Bytes {
        self.0.clone()
    }

    /// Try to interpret as a UTF-8 string
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Try UTF-8 first, fall back to hex
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "{}", s),
            Err(_) => {
                for byte in self.0.iter() {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
        }
    }
}

// Serialization support - serialize as bytes for binary formats
impl Serialize for StreamId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for StreamId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
        Ok(StreamId::from_bytes(bytes))
    }
}

/// Overlay network events
#[derive(Debug, Clone)]
pub enum OverlayEvent {
    /// A new peer has been discovered
    PeerDiscovered {
        /// Peer ID
        peer_id: LocalPeerId,
        /// Peer information
        info: PeerInfo,
    },

    /// A peer has connected
    PeerConnected {
        /// Peer ID
        peer_id: LocalPeerId,
        /// Peer information
        info: PeerInfo,
    },

    /// A peer has disconnected
    PeerDisconnected {
        /// Peer ID
        peer_id: LocalPeerId,
        /// Reason for disconnection
        reason: String,
    },

    /// A stream has been published
    StreamPublished {
        /// Stream ID
        stream_id: StreamId,
        /// Publisher peer ID
        publisher: LocalPeerId,
    },

    /// A stream has been relayed
    StreamRelayed {
        /// Stream ID
        stream_id: StreamId,
        /// Source peer ID
        source: LocalPeerId,
        /// Target peer ID
        target: LocalPeerId,
    },

    /// A stream has been stopped
    StreamStopped {
        /// Stream ID
        stream_id: StreamId,
        /// Reason for stopping
        reason: String,
    },

    /// Stream data received
    StreamData {
        /// Stream ID
        stream_id: StreamId,
        /// Source peer ID
        source: LocalPeerId,
        /// Data payload
        data: Vec<u8>,
    },

    /// The topology has changed
    TopologyChanged {
        /// Number of peers
        peer_count: usize,
        /// Number of relay nodes
        relay_count: usize,
    },
}

/// Configuration for the overlay network
#[derive(Debug, Clone)]
pub struct OverlayConfig {
    /// Local peer ID
    pub local_peer_id: LocalPeerId,
    /// Bootstrap peers to connect to
    pub bootstrap_peers: Vec<String>,
    /// Whether to use Kademlia DHT for peer discovery
    pub enable_kademlia: bool,
    /// Whether to enable bootstrap discovery
    pub enable_bootstrap_discovery: bool,
    /// Whether to enable DHT discovery
    pub enable_dht_discovery: bool,
    /// Maximum number of connections to maintain
    pub max_connections: usize,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Peer ttl (time before refreshing peer information)
    pub peer_ttl: Duration,
    /// Heartbeat interval for peer liveness checks
    pub heartbeat_interval: Duration,
    /// Topology rebalance interval
    pub rebalance_interval: Duration,
    /// Whether to enable geo-aware topology rebalancing
    pub enable_geo_aware: bool,
    /// Whether to enable relay client for NAT traversal
    pub enable_relay_client: bool,
    /// Relay server address to use for NAT traversal (optional)
    /// Format: /ip4/<ip>/tcp/<port>/p2p/<peer-id>
    pub relay_server: Option<String>,
}

impl Default for OverlayConfig {
    fn default() -> Self {
        Self {
            local_peer_id: LocalPeerId::new_random(),
            bootstrap_peers: vec![],
            enable_kademlia: true,
            enable_bootstrap_discovery: true,
            enable_dht_discovery: true,
            max_connections: 50,
            connection_timeout: Duration::from_secs(30),
            peer_ttl: Duration::from_secs(300), // 5 minutes
            heartbeat_interval: Duration::from_secs(15),
            rebalance_interval: Duration::from_secs(60),
            enable_geo_aware: true,
            enable_relay_client: true,
            relay_server: None,
        }
    }
}

/// Overlay network statistics
#[derive(Debug, Clone, Default)]
pub struct OverlayStats {
    /// Number of connected peers
    pub connected_peers: usize,
    /// Number of discovered peers
    pub discovered_peers: usize,
    /// Number of active streams
    pub active_streams: usize,
    /// Number of relay nodes
    pub relay_nodes: usize,
    /// Incoming bandwidth (bytes/sec)
    pub incoming_bandwidth: u64,
    /// Outgoing bandwidth (bytes/sec)
    pub outgoing_bandwidth: u64,
    /// Average latency to peers (ms)
    pub average_latency_ms: u64,
}

/// The core interface for the overlay network
#[async_trait::async_trait]
pub trait Overlay {
    /// Start the overlay network
    async fn start(&self) -> Result<(), OverlayError>;
    /// Stop the overlay network
    async fn stop(&self) -> Result<(), OverlayError>;
    /// Check if the overlay is running
    fn is_running(&self) -> bool;
    /// Get the local peer ID
    fn local_peer_id(&self) -> LocalPeerId;
    /// Connect to a peer
    async fn connect_peer(&self, addr: &str) -> Result<PeerInfo, OverlayError>;
    /// Disconnect from a peer
    async fn disconnect_peer(&self, peer_id: &LocalPeerId) -> Result<(), OverlayError>;
    /// Publish a stream
    async fn publish_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError>;
    /// Relay a stream to a peer
    async fn relay_stream(
        &self,
        stream_id: &StreamId,
        target: &LocalPeerId,
    ) -> Result<(), OverlayError>;
    /// Stop a stream
    async fn stop_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError>;
    /// Get the next overlay event
    async fn next_event(&self) -> Option<OverlayEvent>;
    /// Get the list of connected peers
    async fn connected_peers(&self) -> Result<Vec<PeerInfo>, OverlayError>;
    /// Get the list of active streams
    async fn active_streams(&self) -> Result<Vec<StreamId>, OverlayError>;
    /// Get the overlay statistics
    async fn stats(&self) -> Result<OverlayStats, OverlayError>;
    /// Force a topology rebalance
    async fn rebalance_topology(&self) -> Result<(), OverlayError>;
    /// Subscribe to a stream (receive data from publishers)
    async fn subscribe_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError>;
    /// Publish stream data to all subscribers
    async fn publish_stream_data(
        &self,
        stream_id: &StreamId,
        data: Vec<u8>,
    ) -> Result<(), OverlayError>;
}
