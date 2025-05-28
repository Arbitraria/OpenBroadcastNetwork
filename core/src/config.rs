//! Configuration for the decentralized streaming system
//!
//! This module provides configuration structures for various components of the system,
//! allowing customization of behavior without code changes. The configuration system
//! is designed to be flexible, serializable, and hierarchical to support different
//! deployment environments and use cases.
//!
//! Key components that can be configured include:
//! - Network settings (connections, timeouts, bootstrap nodes)
//! - Media processing (buffer sizes, encoding parameters)
//! - Peer discovery mechanisms (mDNS, DHT, bootstrap)
//! - Overlay network parameters (topology, relay policies)
//!
//! Configurations can be loaded from files, environment variables, or set programmatically.

use std::time::Duration;
use serde::{Serialize, Deserialize};

/// Core configuration of the decentralized streaming system
/// 
/// This is the top-level configuration structure that contains all settings for
/// the decentralized streaming system. It is typically loaded at application startup
/// and passed to the various components during initialization.
/// 
/// The configuration is organized into logical subsections for different aspects
/// of the system, making it easier to manage and understand the available options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreConfig {
    /// Network configuration
    pub network: NetworkConfig,
    
    /// Media configuration
    pub media: MediaConfig,
    
    /// Discovery configuration
    pub discovery: DiscoveryConfig,
}

/// Network configuration for peer connections
/// 
/// Controls how the node interacts with the network, including listening ports,
/// connection limits, timeouts, and bootstrap nodes. These settings affect the
/// node's ability to connect to other peers and maintain those connections.
/// 
/// Network settings have a significant impact on connectivity, especially in
/// challenging network environments (NATs, firewalls, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Listening port for libp2p
    pub listen_port: u16,
    
    /// Bootstrap nodes to connect to on startup
    pub bootstrap_nodes: Vec<String>,
    
    /// Maximum number of connections
    pub max_connections: usize,
    
    /// Connection idle timeout in seconds
    pub connection_timeout_secs: u64,
}

/// Media processing configuration
/// 
/// Controls how media content is processed, buffered, and transmitted through
/// the network. These settings affect media quality, latency, and resource usage.
/// 
/// Media configuration can be adjusted based on the expected content type,
/// available bandwidth, and quality requirements of the application.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaConfig {
    /// Maximum buffer size for media chunks in KB
    pub max_buffer_size_kb: usize,
    
    /// Segment duration in seconds
    pub segment_duration_secs: f64,
    
    /// Whether to enable transcoding
    pub enable_transcoding: bool,
}

/// Peer discovery configuration
/// 
/// Controls how the node discovers other peers in the network using various
/// discovery mechanisms. These settings determine which discovery protocols
/// are enabled and how aggressively the node searches for new peers.
/// 
/// The discovery configuration should be tailored to the deployment environment,
/// with different settings appropriate for local networks, private deployments,
/// or public internet scenarios.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Enable mDNS discovery
    pub enable_mdns: bool,
    
    /// Enable DHT discovery
    pub enable_dht: bool,
    
    /// Peer refresh interval in seconds
    pub peer_refresh_interval_secs: u64,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            network: NetworkConfig::default(),
            media: MediaConfig::default(),
            discovery: DiscoveryConfig::default(),
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            listen_port: 9000,
            bootstrap_nodes: vec![],
            max_connections: 50,
            connection_timeout_secs: 60,
        }
    }
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            max_buffer_size_kb: 1024,
            segment_duration_secs: 2.0,
            enable_transcoding: false,
        }
    }
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            enable_mdns: true,
            enable_dht: true,
            peer_refresh_interval_secs: 30,
        }
    }
}
