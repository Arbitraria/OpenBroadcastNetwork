//! Discovery interface definitions
//!
//! This module defines common interfaces for peer discovery mechanisms.

use std::net::SocketAddr;
use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Serialize, Deserialize};

/// Re-export for convenience
pub use libp2p::core::multiaddr::Multiaddr;
pub use libp2p::PeerId as Libp2pPeerId;

/// Information about a discovered peer
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Unique identifier for the peer
    pub id: Vec<u8>,
    
    /// Network addresses for the peer
    pub addresses: Vec<SocketAddr>,
    
    /// Protocol versions supported by the peer
    pub protocols: Vec<String>,
    
    /// Custom metadata associated with the peer
    pub metadata: HashMap<String, Vec<u8>>,
    
    /// When this peer was first discovered
    #[serde(with = "humantime_serde::option")]
    pub first_seen: Option<std::time::SystemTime>,
    
    /// When this peer was last seen
    #[serde(with = "humantime_serde::option")]
    pub last_seen: Option<std::time::SystemTime>,
    
    /// Connection status of the peer
    pub connection_status: ConnectionStatus,
}

/// Connection status of a peer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionStatus {
    /// Not currently connected
    Disconnected,
    
    /// Currently attempting to connect
    Connecting,
    
    /// Successfully connected
    Connected,
    
    /// Connection failed
    Failed,
}

/// Events emitted by discovery mechanisms
#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    /// A new peer was discovered
    PeerDiscovered(PeerInfo),
    
    /// A previously discovered peer was updated
    PeerUpdated(PeerInfo),
    
    /// A previously discovered peer is no longer available
    PeerExpired(Vec<u8>),
    
    /// Peer connection status changed
    PeerConnectionStatusChanged {
        /// Peer ID
        peer_id: Vec<u8>,
        /// New connection status
        status: ConnectionStatus,
        /// Optional error if status is Failed
        error: Option<String>,
    },
    
    /// Discovery service started
    ServiceStarted,
    
    /// Discovery service stopped
    ServiceStopped,
    
    /// An error occurred during discovery
    Error(DiscoveryError),
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        Self::Disconnected
    }
}

/// Errors that can occur during discovery
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    /// Bootstrap error (e.g. connection to bootstrap nodes failed)
    #[error("Bootstrap error: {0}")]
    BootstrapError(#[from] Box<dyn std::error::Error + Send + Sync>),
    
    /// DHT error (e.g. unable to lookup peer in DHT)
    #[error("DHT error: {0}")]
    DhtError(#[from] libp2p::kad::GetClosestPeersError),
    
    /// mDNS error (e.g. failed to initialize mDNS service)
    #[error("mDNS error: {0}")]
    MdnsError(#[from] Box<dyn std::error::Error + Send + Sync>),
    
    /// I/O error occurred
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    
    /// Network address parsing error
    #[error("Invalid address: {0}")]
    AddrParseError(#[from] std::net::AddrParseError),
    
    /// Peer ID parsing error
    #[error("Invalid peer ID: {0}")]
    PeerIdError(#[from] libp2p::core::multiaddr::Error),
    
    /// Configuration error
    #[error("Invalid configuration: {0}")]
    ConfigError(String),
    
    /// Timeout while waiting for operation
    #[error("Operation timed out after {0:?}")]
    TimeoutError(std::time::Duration),
    
    /// Discovery service is not running
    #[error("Discovery service is not running")]
    NotRunning,
    
    /// Discovery service is already running
    #[error("Discovery service is already running")]
    AlreadyRunning,
    
    /// Invalid peer information provided
    #[error("Invalid peer information: {0}")]
    InvalidPeerInfo(String),
}

/// Core discovery trait that all discovery implementations must implement
#[async_trait::async_trait]
pub trait Discovery: Send + Sync + 'static {
    /// Start the discovery service
    /// 
    /// # Errors
    /// 
    /// Returns an error if:
    /// - The service is already running
    /// - Failed to initialize the discovery mechanism
    /// - Failed to start background tasks
    async fn start(&mut self) -> Result<(), DiscoveryError>;
    
    /// Stop the discovery service
    /// 
    /// # Errors
    /// 
    /// Returns an error if:
    /// - The service is not running
    /// - Failed to stop background tasks
    async fn stop(&mut self) -> Result<(), DiscoveryError>;
    
    /// Announce a peer to the network
    /// 
    /// # Errors
    /// 
    /// Returns an error if:
    /// - The service is not running
    /// - Invalid peer information provided
    /// - Failed to send announcement
    async fn announce(&self, info: PeerInfo) -> Result<(), DiscoveryError>;
    
    /// Lookup a peer by ID
    /// 
    /// # Errors
    /// 
    /// Returns an error if:
    /// - The service is not running
    /// - Invalid peer ID format
    /// - Lookup operation failed
    async fn lookup_peer(&self, peer_id: &[u8]) -> Result<Option<PeerInfo>, DiscoveryError>;
    
    /// Find peers matching the given criteria
    /// 
    /// # Errors
    /// 
    /// Returns an error if:
    /// - The service is not running
    /// - Invalid criteria format
    /// - Search operation failed
    async fn find_peers(&self, criteria: &str) -> Result<Vec<PeerInfo>, DiscoveryError>;
    
    /// Get the next discovery event with an optional timeout
    /// 
    /// # Errors
    /// 
    /// Returns an error if:
    /// - The service is not running
    /// - Failed to receive event
    async fn next_event(&mut self, timeout: Option<Duration>) -> Result<Option<DiscoveryEvent>, DiscoveryError>;
    
    /// Check if the discovery service is running
    fn is_running(&self) -> bool;
    
    /// Get the local peer ID
    fn local_peer_id(&self) -> Option<Vec<u8>>;
    
    /// Get the list of discovered peers
    async fn discovered_peers(&self) -> Result<Vec<PeerInfo>, DiscoveryError>;
}