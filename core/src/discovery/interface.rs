//! Discovery interface definitions
//!
//! This module defines common interfaces for peer discovery mechanisms.

use std::future::Future;
use std::pin::Pin;
use std::net::SocketAddr;
use std::fmt;
use std::error::Error;
use std::collections::HashMap;

/// Information about a discovered peer
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    /// Unique identifier for the peer
    pub id: Vec<u8>,
    /// Network addresses for the peer
    pub addresses: Vec<SocketAddr>,
    /// Protocol versions supported by the peer
    pub protocols: Vec<String>,
    /// Custom metadata associated with the peer
    pub metadata: HashMap<String, Vec<u8>>,
}

/// Events emitted by discovery mechanisms
#[derive(Debug)]
pub enum DiscoveryEvent {
    /// A new peer was discovered
    PeerDiscovered(PeerInfo),
    /// A previously discovered peer was updated
    PeerUpdated(PeerInfo),
    /// A previously discovered peer is no longer available
    PeerExpired(Vec<u8>),
    /// An error occurred during discovery
    Error(DiscoveryError),
}

/// Errors that can occur during discovery
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    /// Bootstrap error (e.g. connection to bootstrap nodes failed)
    #[error("Bootstrap error: {0}")]
    BootstrapError(String),
    
    /// DHT error (e.g. unable to lookup peer in DHT)
    #[error("DHT error: {0}")]
    DhtError(String),
    
    /// mDNS error (e.g. failed to initialize mDNS service)
    #[error("mDNS error: {0}")]
    MdnsError(String),
    
    /// Generic IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Core discovery trait that all discovery implementations must implement
pub trait Discovery {
    /// Start the discovery mechanism
    fn start(&mut self) -> Result<(), DiscoveryError>;
    
    /// Stop the discovery mechanism
    fn stop(&mut self) -> Result<(), DiscoveryError>;
    
    /// Announce self to the network
    fn announce(&mut self, info: PeerInfo) -> Pin<Box<dyn Future<Output = Result<(), DiscoveryError>> + Send>>;
    
    /// Lookup a specific peer by ID
    fn lookup_peer(&mut self, id: &[u8]) -> Pin<Box<dyn Future<Output = Result<Option<PeerInfo>, DiscoveryError>> + Send>>;
    
    /// Find peers by predicate (e.g. supporting a specific protocol)
    fn find_peers(&mut self, predicate: Option<String>) -> Pin<Box<dyn Future<Output = Result<Vec<PeerInfo>, DiscoveryError>> + Send>>;
    
    /// Get the next discovery event
    fn next_event(&mut self) -> Pin<Box<dyn Future<Output = Option<DiscoveryEvent>> + Send>>;
    
    /// Check if discovery is running
    fn is_running(&self) -> bool;
} 