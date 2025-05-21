//! Core interfaces for the overlay network
//!
//! This module defines the fundamental interfaces for the overlay network
//! used in the decentralized streaming system.

use crate::overlay::peer::{Peer, PeerId, PeerInfo};
use std::fmt;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
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
    
    /// Operation timeout
    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),
    
    /// Protocol error
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    
    /// System is stopping
    #[error("System is stopping")]
    Stopping,
    
    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    
    /// Other error
    #[error("Error: {0}")]
    Other(String),
}

/// A unique stream identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamId(pub Vec<u8>);

impl StreamId {
    /// Create a new random stream ID
    pub fn new_random() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut bytes = vec![0u8; 16];
        rng.fill(&mut bytes[..]);
        Self(bytes)
    }
    
    /// Create a stream ID from a byte vector
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
    
    /// Create a stream ID from a string
    pub fn from_string(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
    
    /// Get the inner bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Overlay network events
#[derive(Debug, Clone)]
pub enum OverlayEvent {
    /// A new peer has been discovered
    PeerDiscovered {
        /// Peer ID
        peer_id: PeerId,
        /// Peer information
        info: PeerInfo,
    },
    
    /// A peer has connected
    PeerConnected {
        /// Peer ID
        peer_id: PeerId,
        /// Peer information
        info: PeerInfo,
    },
    
    /// A peer has disconnected
    PeerDisconnected {
        /// Peer ID
        peer_id: PeerId,
        /// Reason for disconnection
        reason: String,
    },
    
    /// A stream has been published
    StreamPublished {
        /// Stream ID
        stream_id: StreamId,
        /// Publisher peer ID
        publisher: PeerId,
    },
    
    /// A stream has been relayed
    StreamRelayed {
        /// Stream ID
        stream_id: StreamId,
        /// Source peer ID
        source: PeerId,
        /// Target peer ID
        target: PeerId,
    },
    
    /// A stream has been stopped
    StreamStopped {
        /// Stream ID
        stream_id: StreamId,
        /// Reason for stopping
        reason: String,
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
    pub local_peer_id: PeerId,
    /// Bootstrap peers to connect to
    pub bootstrap_peers: Vec<String>,
    /// Whether to use mDNS for local peer discovery
    pub enable_mdns: bool,
    /// Whether to use Kademlia DHT for peer discovery
    pub enable_kademlia: bool,
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
}

impl Default for OverlayConfig {
    fn default() -> Self {
        Self {
            local_peer_id: PeerId::new_random(),
            bootstrap_peers: Vec::new(),
            enable_mdns: true,
            enable_kademlia: true,
            max_connections: 50,
            connection_timeout: Duration::from_secs(30),
            peer_ttl: Duration::from_secs(300), // 5 minutes
            heartbeat_interval: Duration::from_secs(15),
            rebalance_interval: Duration::from_secs(60),
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
pub trait Overlay {
    /// Start the overlay network
    fn start(&self) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>>;
    
    /// Stop the overlay network
    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>>;
    
    /// Check if the overlay is running
    fn is_running(&self) -> bool;
    
    /// Get the local peer ID
    fn local_peer_id(&self) -> PeerId;
    
    /// Connect to a peer
    fn connect_peer(&self, addr: &str) 
        -> Pin<Box<dyn Future<Output = Result<PeerInfo, OverlayError>> + Send>>;
    
    /// Disconnect from a peer
    fn disconnect_peer(&self, peer_id: &PeerId) 
        -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>>;
    
    /// Publish a stream
    fn publish_stream(&self, stream_id: &StreamId) 
        -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>>;
    
    /// Relay a stream to a peer
    fn relay_stream(&self, stream_id: &StreamId, target: &PeerId) 
        -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>>;
    
    /// Stop a stream
    fn stop_stream(&self, stream_id: &StreamId) 
        -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>>;
    
    /// Get the next overlay event
    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<OverlayEvent>> + Send>>;
    
    /// Get the list of connected peers
    fn connected_peers(&self) 
        -> Pin<Box<dyn Future<Output = Result<Vec<PeerInfo>, OverlayError>> + Send>>;
    
    /// Get the list of active streams
    fn active_streams(&self) 
        -> Pin<Box<dyn Future<Output = Result<Vec<StreamId>, OverlayError>> + Send>>;
    
    /// Get the overlay statistics
    fn stats(&self) -> Pin<Box<dyn Future<Output = Result<OverlayStats, OverlayError>> + Send>>;
    
    /// Force a topology rebalance
    fn rebalance_topology(&self) 
        -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>>;
} 