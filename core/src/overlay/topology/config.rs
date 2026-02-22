//! Configuration for topology management
//!
//! This module provides configuration structures for the overlay network topology.

use super::geo::GeoIP;
use std::time::Duration;

/// Weights for relay selection scoring
#[derive(Debug, Clone)]
pub struct RelayScoreWeights {
    /// Weight for bandwidth
    pub bandwidth_weight: f32,
    /// Weight for latency
    pub latency_weight: f32,
    /// Weight for geographic proximity
    pub geo_weight: f32,
    /// Weight for connection quality
    pub quality_weight: f32,
}

impl Default for RelayScoreWeights {
    fn default() -> Self {
        Self {
            bandwidth_weight: 0.3,
            latency_weight: 0.3,
            geo_weight: 0.2,
            quality_weight: 0.2,
        }
    }
}

/// Configuration for topology management
#[derive(Debug, Clone)]
pub struct TopologyConfig {
    /// Minimum number of peers to maintain per region
    pub min_peers_per_region: usize,
    /// Maximum depth for the tree
    pub max_tree_depth: usize,
    /// Geographic bias factor (0.0-1.0)
    pub geo_bias: f32,
    /// Connection quality threshold for relay selection
    pub min_relay_quality: f32,
    /// Rebalance interval
    pub rebalance_interval: Duration,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Grace period for temporary disconnections
    pub disconnection_grace_period: Duration,
    /// Enable geo-aware topology
    pub enable_geo_aware: bool,
    /// Cache lifetime for peer information
    pub peer_cache_lifetime: Duration,
    /// Maximum consecutive failures before marking a connection as degraded
    pub max_consecutive_failures: usize,
    /// Minimum success rate for a healthy connection (0.0-1.0)
    pub min_success_rate: f32,
    /// Health record expiry duration
    pub health_record_expiry: Duration,
    /// Number of peers that triggers rebalancing
    pub rebalance_threshold: usize,
    /// GeoIP provider (if available)
    pub geo_provider: Option<GeoIP>,
    /// Maximum allowed latency (ms)
    pub max_latency: u64,
    /// Default bandwidth capacity
    pub default_bandwidth: u64,
    /// Score weights for relay selection
    pub score_weights: RelayScoreWeights,
    /// Connection health threshold (0.0-1.0)
    pub health_threshold: f32,
    /// Proactive rebalancing threshold
    pub proactive_rebalance_threshold: f32,
    /// Prefer same-region connections
    pub prefer_same_region: bool,
    /// Maximum distance for "close" peers (km)
    pub max_close_distance_km: f64,
    /// Whether to use geographic coordinates for optimization
    pub use_coordinates: bool,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            min_peers_per_region: 3,
            max_tree_depth: 5,
            geo_bias: 0.7,
            min_relay_quality: 0.6,
            rebalance_interval: Duration::from_secs(60),
            health_check_interval: Duration::from_secs(30),
            disconnection_grace_period: Duration::from_secs(300),
            enable_geo_aware: true,
            peer_cache_lifetime: Duration::from_secs(3600),
            max_consecutive_failures: 3,
            min_success_rate: 0.7,
            health_record_expiry: Duration::from_secs(3600 * 24), // 24 hours
            rebalance_threshold: 10,
            geo_provider: None,
            max_latency: 500,
            default_bandwidth: 5_000_000, // 5 Mbps
            score_weights: RelayScoreWeights::default(),
            health_threshold: 0.7,
            proactive_rebalance_threshold: 0.3,
            prefer_same_region: true,
            max_close_distance_km: 1000.0,
            use_coordinates: true,
        }
    }
}
