//! Topology manager for overlay networks
//!
//! This module implements the main manager for the overlay network topology,
//! handling peer connections, stream trees, and topology optimization.
//!
//! # Responsibilities
//!
//! The `TopologyManager` has several key responsibilities:
//!
//! 1. **Peer Management** - Tracking peers, their roles, and connection status
//! 2. **Stream Tree Management** - Organizing peers into efficient stream distribution trees
//! 3. **Topology Optimization** - Rebalancing connections for optimal performance
//! 4. **Geographic Awareness** - Considering peer proximity for lower latency connections
//! 5. **Health Tracking** - Monitoring connection health and reliability
//!
//! # Architecture
//!
//! The `TopologyManager` maintains several internal data structures:
//!
//! - Peer mappings - Track all known peers and their metadata
//! - Stream trees - Map stream IDs to their distribution trees
//! - Health records - Track connection reliability metrics
//! - Geographic data - Used for proximity-based optimization
//!
//! # Dependencies
//!
//! This component depends on:
//! - `overlay::peer` - For peer types and role definitions
//! - `overlay::interface` - For stream IDs and error types
//! - `overlay::metrics` - For telemetry and metrics collection
//! - `overlay::topology::config` - For topology configuration parameters
//! - `overlay::topology::health` - For connection health tracking
//! - `overlay::topology::geo` - For geographic optimization
//!
//! # Integration Points
//!
//! The `TopologyManager` is primarily used by:
//! - `Libp2pOverlay` in `overlay/libp2p_impl.rs` - For overall network management
//! - `RelayManager` in `overlay/relay/manager.rs` - For stream relay coordination
//!
//! # Thread Safety
//!
//! This component is designed to be thread-safe using:
//! - `Arc<TopologyManager>` for shared ownership
//! - Internal `RwLock` guards for concurrent data structure access
//! - Async-aware locking for all public methods

use crate::overlay::interface::{StreamId, OverlayError};
use crate::overlay::peer::{Peer, PeerRole};
use crate::overlay::metrics::OverlayMetrics;
use libp2p::PeerId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use super::config::TopologyConfig;
use super::health::ConnectionHealth;

/// Manager for multiple relay trees
#[derive(Debug)]
pub struct TopologyManager {
    /// Local peer ID
    pub local_peer_id: PeerId,
    /// Configuration
    pub config: TopologyConfig,
    /// Connected peers
    pub peers: Arc<RwLock<HashMap<PeerId, Peer>>>,
    /// Active stream trees
    pub trees: Arc<RwLock<HashMap<StreamId, crate::overlay::tree::StreamTree>>>,
    /// Background tasks
    pub tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
    /// Connection health tracking
    pub connection_health: Arc<RwLock<HashMap<PeerId, ConnectionHealth>>>,
    /// Metrics
    pub metrics: Option<Arc<OverlayMetrics>>,
}

impl TopologyManager {
    /// Create a new topology manager
    pub fn new(
        local_peer_id: PeerId,
        config: TopologyConfig,
        metrics: Option<Arc<OverlayMetrics>>,
    ) -> Self {
        let peers = Arc::new(RwLock::new(HashMap::new()));
        let trees = Arc::new(RwLock::new(HashMap::new()));
        let tasks = Arc::new(RwLock::new(Vec::new()));
        let connection_health = Arc::new(RwLock::new(HashMap::new()));
        
        Self {
            local_peer_id,
            config,
            peers,
            trees,
            tasks,
            connection_health,
            metrics,
        }
    }
    
    /// Start the topology manager
    pub async fn start(&self) -> Result<(), OverlayError> {
        let mut tasks = self.tasks.write().await;
        
        // Start the periodic rebalance task
        let trees_clone = self.trees.clone();
        let peers_clone = self.peers.clone();
        let config_clone = self.config.clone();
        let rebalance_task = tokio::spawn(async move {
            Self::periodic_rebalance(trees_clone, peers_clone, config_clone).await;
        });
        tasks.push(rebalance_task);
        
        // Start the connection health monitoring task
        let connection_health_clone = self.connection_health.clone();
        let peers_clone = self.peers.clone();
        let config_clone = self.config.clone();
        let health_task = tokio::spawn(async move {
            Self::monitor_connection_health(connection_health_clone, peers_clone, config_clone).await;
        });
        tasks.push(health_task);
        
        Ok(())
    }
    
    /// Stop the topology manager
    pub async fn stop(&self) -> Result<(), OverlayError> {
        let mut tasks = self.tasks.write().await;
        
        for task in tasks.drain(..) {
            task.abort();
        }
        
        Ok(())
    }
    
    /// Periodically rebalance all streams
    async fn periodic_rebalance(
        trees: Arc<RwLock<HashMap<StreamId, crate::overlay::tree::StreamTree>>>,
        peers: Arc<RwLock<HashMap<PeerId, Peer>>>,
        config: TopologyConfig
    ) {
        let mut interval = tokio::time::interval(config.rebalance_interval);
        
        loop {
            interval.tick().await;
            
            // Get a list of stream IDs to rebalance
            let stream_ids = {
                let trees = trees.read().await;
                trees.keys().cloned().collect::<Vec<_>>()
            };
            
            // Rebalance each stream
            for stream_id in stream_ids {
                let mut trees = trees.write().await;
                
                if let Some(tree) = trees.get_mut(&stream_id) {
                    // Just call rebalance as it doesn't need our parameters
                    tree.rebalance();
                }
            }
        }
    }
    
    /// Monitors connection health for all connected peers
    async fn monitor_connection_health(
        connection_health: Arc<RwLock<HashMap<PeerId, ConnectionHealth>>>,
        peers: Arc<RwLock<HashMap<PeerId, Peer>>>,
        config: TopologyConfig
    ) {
        let mut interval = tokio::time::interval(config.health_check_interval);
        
        loop {
            interval.tick().await;
            
            // Get a list of peers to check
            let peer_ids = {
                let peers = peers.read().await;
                peers.keys().cloned().collect::<Vec<_>>()
            };
            
            // Update health for each peer
            for peer_id in peer_ids {
                let mut health_lock = connection_health.write().await;
                
                // Get or create health record
                let health = health_lock.entry(peer_id).or_insert_with(|| {
                    ConnectionHealth::default()
                });
                
                // Update last checked time and degraded status
                health.update_degraded_status(
                    config.min_relay_quality,
                    config.max_consecutive_failures,
                    config.min_success_rate
                );
            }
            
            // Clean up old health records
            Self::cleanup_health_records(
                connection_health.clone(),
                config.health_record_expiry
            ).await;
        }
    }
    
    /// Clean up old health records
    async fn cleanup_health_records(
        connection_health: Arc<RwLock<HashMap<PeerId, ConnectionHealth>>>,
        max_age: Duration
    ) {
        let now = Instant::now();
        let mut health = connection_health.write().await;
        
        let expired: Vec<PeerId> = health.iter()
            .filter(|(_, h)| now.duration_since(h.last_checked) > max_age)
            .map(|(id, _)| *id)
            .collect();
            
        for id in expired {
            health.remove(&id);
        }
    }
    
    /// Add a peer to a stream
    pub async fn add_peer_to_stream(
        &self,
        stream_id: &StreamId,
        peer_id: PeerId,
        role: PeerRole
    ) -> Result<(), OverlayError> {
        let mut trees = self.trees.write().await;
        
        // Get or create the tree for this stream
        let tree = trees.entry(stream_id.clone()).or_insert_with(|| {
            crate::overlay::tree::StreamTree::new(stream_id.clone())
        });
        
        // If this is the publisher and no source exists yet, set it
        if role == PeerRole::Publisher && tree.source.is_none() {
            tree.source = Some(peer_id.clone());
        }
        
        // Add the peer to the tree
        // We need to provide a default bandwidth capacity if not specified
        let bandwidth = self.config.default_bandwidth;
        tree.add_peer(peer_id, role, bandwidth);
        
        // If we have accumulated enough peers, trigger a rebalance
        if tree.nodes.len() > self.config.rebalance_threshold {
            tree.rebalance();
        }
        
        Ok(())
    }
    
    /// Remove a peer from a stream
    pub async fn remove_peer_from_stream(
        &self,
        stream_id: &StreamId,
        peer_id: &PeerId
    ) -> Result<(), OverlayError> {
        let mut trees = self.trees.write().await;
        
        if let Some(tree) = trees.get_mut(stream_id) {
            tree.remove_peer(peer_id);
            
            // If the tree is now empty, remove it
            if tree.nodes.is_empty() {
                trees.remove(stream_id);
            } else {
                // Otherwise rebalance
                tree.rebalance();
            }
            
            Ok(())
        } else {
            Err(OverlayError::TopologyError(
                format!("Stream not found: {:?}", stream_id)
            ))
        }
    }
    
    /// Record a connection success for a peer
    pub async fn record_connection_success(&self, peer_id: &PeerId) {
        let mut health_lock = self.connection_health.write().await;
        
        // Get or create health record
        let health = health_lock.entry(*peer_id).or_insert_with(ConnectionHealth::default);
        
        // Update metrics
        health.record_success();
        
        // Clear degraded flag if quality is good enough
        if health.quality >= self.config.min_relay_quality {
            health.is_degraded = false;
        }
    }
    
    /// Record a connection failure for a peer
    pub async fn record_connection_failure(&self, peer_id: &PeerId) {
        let mut health_lock = self.connection_health.write().await;
        
        // Get or create health record
        let health = health_lock.entry(*peer_id).or_insert_with(ConnectionHealth::default);
        
        // Update metrics
        health.record_failure();
        
        // Mark as degraded if failures exceed threshold
        if health.consecutive_failures >= self.config.max_consecutive_failures {
            health.is_degraded = true;
        }
    }
    
    /// Update peer information
    pub async fn update_peer(&self, peer: Peer) -> Result<(), OverlayError> {
        let mut peers = self.peers.write().await;
        // Convert LocalPeerId to PeerId for map key
        match (&peer.id).try_into() {
            Ok(peer_id) => {
                peers.insert(peer_id, peer);
                Ok(())
            }
            Err(e) => Err(OverlayError::InvalidPeerId(format!("Failed to convert peer ID: {}", e)))
        }
    }
    
    /// Remove a peer
    pub async fn remove_peer(&self, peer_id: &PeerId) -> Result<(), OverlayError> {
        let mut peers = self.peers.write().await;
        peers.remove(peer_id);
        
        // Remove from all trees
        let mut trees = self.trees.write().await;
        for (_, tree) in trees.iter_mut() {
            if tree.nodes.contains_key(peer_id) {
                let _ = tree.remove_peer(peer_id);
            }
        }
        
        Ok(())
    }
    
    /// Get the relay path for a stream from source to target
    pub async fn get_relay_path(
        &self, 
        stream_id: &StreamId, 
        target_id: &PeerId
    ) -> Result<Vec<PeerId>, OverlayError> {
        let trees = self.trees.read().await;
        
        if let Some(tree) = trees.get(stream_id) {
            if let Some(path) = tree.get_path_to_peer(target_id) {
                Ok(path)
            } else {
                Err(OverlayError::TopologyError(
                    format!("Peer not in stream: {}", target_id)
                ))
            }
        } else {
            Err(OverlayError::TopologyError(
                format!("Stream not found: {:?}", stream_id)
            ))
        }
    }
    
    /// Get all peers in a stream
    pub async fn get_stream_peers(&self, stream_id: &StreamId) -> Result<Vec<PeerId>, OverlayError> {
        let trees = self.trees.read().await;
        
        if let Some(tree) = trees.get(stream_id) {
            Ok(tree.nodes.keys().cloned().collect())
        } else {
            Err(OverlayError::TopologyError(
                format!("Stream not found: {:?}", stream_id)
            ))
        }
    }
    
    /// Get the publisher for a stream
    pub async fn get_stream_publisher(&self, stream_id: &StreamId) -> Result<PeerId, OverlayError> {
        let trees = self.trees.read().await;
        
        if let Some(tree) = trees.get(stream_id) {
            if let Some(source) = &tree.source {
                Ok(source.clone())
            } else {
                Err(OverlayError::TopologyError(
                    format!("No source found for stream: {:?}", stream_id)
                ))
            }
        } else {
            Err(OverlayError::TopologyError(
                format!("Stream not found: {:?}", stream_id)
            ))
        }
    }
    
    /// Rebalance a stream's topology with geo-awareness
    pub async fn rebalance_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        let mut trees = self.trees.write().await;
        let peers = self.peers.read().await;
        
        if let Some(tree) = trees.get_mut(stream_id) {
            tree.rebalance();
            Ok(())
        } else {
            Err(OverlayError::TopologyError(format!("Stream not found: {:?}", stream_id)))
        }
    }
    
    /// Update the TopologyConfig
    pub fn update_config(&mut self, config: TopologyConfig) {
        self.config = config;
    }
}
