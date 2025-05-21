//! Peer management for the overlay network
//!
//! This module handles peer identification, information, and connections.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// A unique peer identifier
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerId(pub Vec<u8>);

impl PeerId {
    /// Create a new random peer ID
    pub fn new_random() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut bytes = vec![0u8; 32];
        rng.fill(&mut bytes[..]);
        Self(bytes)
    }
    
    /// Create a peer ID from a byte vector
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
    
    /// Create a peer ID from a string
    pub fn from_string(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
    
    /// Get the inner bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    
    /// Convert to a base58 string (compatible with libp2p)
    pub fn to_base58(&self) -> String {
        bs58::encode(&self.0).into_string()
    }
    
    /// Create from a base58 string
    pub fn from_base58(s: &str) -> Result<Self, bs58::decode::Error> {
        let bytes = bs58::decode(s).into_vec()?;
        Ok(Self(bytes))
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeerId({})", self.to_base58())
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_base58())
    }
}

/// Role of a peer in the network
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerRole {
    /// Source/publisher of content
    Publisher,
    /// Relay node
    Relay,
    /// Edge consumer
    Consumer,
    /// Both consumer and relay
    HybridRelay,
    /// Gateway node that bridges networks
    Gateway,
    /// Unknown role
    Unknown,
}

impl Default for PeerRole {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Peer connection status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionStatus {
    /// Connected and exchanging data
    Connected,
    /// Connection in progress
    Connecting,
    /// Disconnected
    Disconnected,
    /// Error state
    Error,
}

/// Detailed information about a peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Peer ID
    pub id: PeerId,
    /// Available addresses to connect to this peer
    pub addresses: Vec<String>,
    /// Peer role in the network
    pub role: PeerRole,
    /// Connection status
    pub status: ConnectionStatus,
    /// Protocol versions supported
    pub protocols: Vec<String>,
    /// Peer metadata/capabilities
    pub metadata: HashMap<String, String>,
    /// Connection latency in milliseconds (if measured)
    pub latency_ms: Option<u64>,
    /// Last seen timestamp (seconds since UNIX epoch)
    pub last_seen: u64,
    /// Geographic region (if known)
    pub region: Option<String>,
    /// Bandwidth capacity (if advertised)
    pub bandwidth_capacity: Option<u64>,
}

impl Default for PeerInfo {
    fn default() -> Self {
        Self {
            id: PeerId::new_random(),
            addresses: Vec::new(),
            role: PeerRole::Unknown,
            status: ConnectionStatus::Disconnected,
            protocols: Vec::new(),
            metadata: HashMap::new(),
            latency_ms: None,
            last_seen: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            region: None,
            bandwidth_capacity: None,
        }
    }
}

/// A connection to a peer
#[derive(Debug)]
pub struct PeerConnection {
    /// Remote peer ID
    pub peer_id: PeerId,
    /// Connection status
    pub status: ConnectionStatus,
    /// When the connection was established
    pub connected_at: Option<Instant>,
    /// Last activity timestamp
    pub last_activity: Instant,
    /// Connection latency in milliseconds
    pub latency_ms: u64,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Bytes received
    pub bytes_received: u64,
    /// Whether this is an outbound connection
    pub is_outbound: bool,
}

impl PeerConnection {
    /// Create a new peer connection
    pub fn new(peer_id: PeerId, is_outbound: bool) -> Self {
        Self {
            peer_id,
            status: ConnectionStatus::Connecting,
            connected_at: None,
            last_activity: Instant::now(),
            latency_ms: 0,
            bytes_sent: 0,
            bytes_received: 0,
            is_outbound,
        }
    }
    
    /// Mark the connection as established
    pub fn mark_connected(&mut self) {
        self.status = ConnectionStatus::Connected;
        self.connected_at = Some(Instant::now());
    }
    
    /// Update the last activity timestamp
    pub fn update_activity(&mut self) {
        self.last_activity = Instant::now();
    }
    
    /// Record bytes sent
    pub fn record_sent(&mut self, bytes: u64) {
        self.bytes_sent += bytes;
        self.update_activity();
    }
    
    /// Record bytes received
    pub fn record_received(&mut self, bytes: u64) {
        self.bytes_received += bytes;
        self.update_activity();
    }
    
    /// Check if the connection is idle (no activity)
    pub fn is_idle(&self, timeout: Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }
    
    /// Update the measured latency
    pub fn update_latency(&mut self, latency_ms: u64) {
        self.latency_ms = latency_ms;
    }
}

/// Full peer object including connection information
#[derive(Debug)]
pub struct Peer {
    /// Peer ID
    pub id: PeerId,
    /// Peer information
    pub info: PeerInfo,
    /// Active connection (if any)
    pub connection: Option<PeerConnection>,
    /// Last successful connection timestamp
    pub last_connected: Option<Instant>,
    /// Connection attempts count
    pub connection_attempts: u32,
    /// Whether this peer is in the blocklist
    pub is_blocked: bool,
    /// Additional peer-specific settings
    pub settings: HashMap<String, String>,
}

impl Peer {
    /// Create a new peer
    pub fn new(id: PeerId, info: PeerInfo) -> Self {
        Self {
            id,
            info,
            connection: None,
            last_connected: None,
            connection_attempts: 0,
            is_blocked: false,
            settings: HashMap::new(),
        }
    }
    
    /// Check if the peer is connected
    pub fn is_connected(&self) -> bool {
        self.connection.as_ref().map_or(false, |conn| 
            conn.status == ConnectionStatus::Connected)
    }
    
    /// Set the peer as connected
    pub fn set_connected(&mut self, connection: PeerConnection) {
        self.info.status = ConnectionStatus::Connected;
        self.last_connected = Some(Instant::now());
        self.connection = Some(connection);
    }
    
    /// Set the peer as disconnected
    pub fn set_disconnected(&mut self) {
        self.info.status = ConnectionStatus::Disconnected;
        self.connection = None;
    }
    
    /// Increment connection attempt counter
    pub fn increment_attempts(&mut self) {
        self.connection_attempts += 1;
    }
    
    /// Check if the peer should be backoff (too many attempts)
    pub fn should_backoff(&self, max_attempts: u32) -> bool {
        self.connection_attempts > max_attempts
    }
    
    /// Reset connection attempts
    pub fn reset_attempts(&mut self) {
        self.connection_attempts = 0;
    }
    
    /// Block the peer
    pub fn block(&mut self) {
        self.is_blocked = true;
    }
    
    /// Unblock the peer
    pub fn unblock(&mut self) {
        self.is_blocked = false;
    }
} 