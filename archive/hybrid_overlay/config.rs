//! Configuration for the hybrid overlay
//!
//! This module defines the configuration options for the hybrid overlay.

use crate::overlay::network::NetworkConfig;
use crate::overlay::peer::PeerRole;
use crate::overlay::topology::TopologyConfig;

/// Configuration for the hybrid overlay
#[derive(Debug, Clone)]
pub struct HybridOverlayConfig {
    /// Network configuration
    pub network_config: NetworkConfig,
    /// Topology configuration
    pub topology_config: TopologyConfig,
    /// How often to rebalance the tree (in seconds)
    pub rebalance_interval: u64,
    /// Maximum fan-out for tree nodes
    pub max_fan_out: usize,
    /// Heartbeat interval (in seconds)
    pub heartbeat_interval: u64,
    /// Estimated bandwidth of this node
    pub bandwidth: u64,
    /// Default role for this node
    pub default_role: PeerRole,
    /// Whether to enable geolocation-aware balancing
    pub geo_aware: bool,
    /// Channel buffer sizes
    pub channel_buffer_size: usize,
}

impl Default for HybridOverlayConfig {
    fn default() -> Self {
        Self {
            network_config: NetworkConfig::default(),
            topology_config: TopologyConfig::default(),
            rebalance_interval: 30,
            max_fan_out: 3,
            heartbeat_interval: 5,
            bandwidth: 1000000, // 1 Mbps
            default_role: PeerRole::Relay,
            geo_aware: true,
            channel_buffer_size: 100,
        }
    }
}
