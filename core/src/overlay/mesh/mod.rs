//! Mesh network implementation
//!
//! This module provides functionality for managing mesh network connections
//! between peers in the overlay network.

mod mesh;

pub use mesh::{StreamMesh, MeshStats, MeshNode};

use crate::overlay::peer::{LocalPeerId, PeerRole};
use crate::overlay::interface::StreamId;
use libp2p::PeerId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the mesh network
#[derive(Debug, Clone)]
pub struct MeshConfig {
    /// Minimum number of connections per peer
    pub min_connections: usize,
    /// Target number of connections per peer 
    pub target_connections: usize,
    /// Maximum number of connections per peer
    pub max_connections: usize,
    /// How often to prune/rebalance connections
    pub prune_interval: Duration,
    /// How long to wait before retrying a failed connection
    pub backoff_time: Duration,
    /// Maximum number of connection attempts before giving up
    pub max_connection_attempts: usize,
    /// Target number of peers in the mesh 
    pub target_peers: usize,
    /// Minimum number of peers in the mesh
    pub min_peers: usize,
    /// Maximum number of peers in the mesh
    pub max_peers: usize,
    /// Minimum number of outbound connections
    pub outbound_min: usize,
    /// Maximum number of outbound connections
    pub outbound_max: usize,
    /// Heartbeat interval for mesh maintenance
    pub heartbeat_interval: Duration,
    /// Length of message history to keep
    pub history_length: usize,
    /// Number of peers to gossip history to
    pub history_gossip: usize,
    /// Target mesh size for gossipsub
    pub mesh_n: usize,
    /// Lower bound for mesh size
    pub mesh_n_low: usize,
    /// Upper bound for mesh size
    pub mesh_n_high: usize,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            min_connections: 3,
            target_connections: 5,
            max_connections: 10,
            prune_interval: Duration::from_secs(30),
            backoff_time: Duration::from_secs(60),
            max_connection_attempts: 3,
            target_peers: 8,
            min_peers: 2,
            max_peers: 12,
            outbound_min: 2,
            outbound_max: 4,
            heartbeat_interval: Duration::from_secs(1),
            history_length: 10,
            history_gossip: 3,
            mesh_n: 6,
            mesh_n_low: 4,
            mesh_n_high: 12,
        }
    }
}

/// Manages the mesh network for all streams
#[derive(Debug)]
pub struct MeshNetwork {
    /// Local peer ID
    pub local_peer_id: LocalPeerId,
    /// Configuration
    pub config: MeshConfig,
    /// Meshes for each stream
    pub stream_meshes: HashMap<StreamId, StreamMesh>,
}

impl MeshNetwork {
    /// Create a new mesh network manager
    pub fn new(local_peer_id: LocalPeerId, config: MeshConfig) -> Self {
        Self {
            local_peer_id,
            config,
            stream_meshes: HashMap::new(),
        }
    }
    
    /// Add a stream to the mesh network
    pub fn add_stream(&mut self, stream_id: StreamId) -> bool {
        if self.stream_meshes.contains_key(&stream_id) {
            return false;
        }
        
        let mesh = StreamMesh::new(stream_id.clone());
        self.stream_meshes.insert(stream_id, mesh);
        true
    }
    
    /// Remove a stream from the mesh network
    pub fn remove_stream(&mut self, stream_id: &StreamId) -> bool {
        self.stream_meshes.remove(stream_id).is_some()
    }
    
    /// Add a peer to a stream's mesh
    pub fn add_peer_to_stream(&mut self, stream_id: &StreamId, peer_id: PeerId, role: PeerRole) -> bool {
        if let Some(mesh) = self.stream_meshes.get_mut(stream_id) {
            // Default bandwidth is configurable but we'll use a reasonable value
            let default_bandwidth = 1_000_000; // 1 Mbps
            mesh.add_peer(peer_id, role, default_bandwidth)
        } else {
            false
        }
    }
    
    /// Remove a peer from a stream's mesh
    pub fn remove_peer_from_stream(&mut self, stream_id: &StreamId, peer_id: &PeerId) -> bool {
        if let Some(mesh) = self.stream_meshes.get_mut(stream_id) {
            mesh.remove_peer(peer_id)
        } else {
            false
        }
    }
    
    /// Get statistics for a stream's mesh
    pub fn get_stream_stats(&self, stream_id: &StreamId) -> Option<MeshStats> {
        self.stream_meshes.get(stream_id).map(|mesh| mesh.get_stats())
    }
    
    /// Rebalance all stream meshes
    pub fn rebalance_all(&mut self) -> usize {
        let mut changes = 0;
        
        for mesh in self.stream_meshes.values_mut() {
            if mesh.rebalance() {
                changes += 1;
            }
        }
        
        changes
    }
}
