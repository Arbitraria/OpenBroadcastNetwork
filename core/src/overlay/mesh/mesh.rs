//! Mesh-based overlay implementation
//!
//! This module provides a mesh-based overlay for resilient distribution
//! when the tree structure fails or is not optimal.

use crate::overlay::interface::StreamId;
use crate::overlay::peer::PeerRole;
use ::libp2p::PeerId;
use std::collections::{HashMap, HashSet};

/// Maximum number of peers a node can connect to in the mesh
const MAX_MESH_PEERS: usize = 5;

/// A node in the mesh network
#[derive(Debug, Clone)]
pub struct MeshNode {
    /// Peer ID of this node
    pub peer_id: PeerId,
    /// Role of this peer in the mesh
    pub role: PeerRole,
    /// Connected peers
    pub peers: HashSet<PeerId>,
    /// Estimated bandwidth capacity
    pub bandwidth: u64,
    /// Whether this node is the stream source
    pub is_source: bool,
}

impl MeshNode {
    /// Create a new mesh node
    pub fn new(peer_id: PeerId, role: PeerRole, is_source: bool) -> Self {
        Self {
            peer_id,
            role,
            peers: HashSet::new(),
            bandwidth: 0,
            is_source,
        }
    }

    /// Check if this node can accept more connections
    pub fn can_accept_connections(&self) -> bool {
        self.peers.len() < MAX_MESH_PEERS
    }

    /// Add a peer connection
    pub fn add_peer(&mut self, peer_id: PeerId) -> bool {
        if self.can_accept_connections() {
            self.peers.insert(peer_id);
            true
        } else {
            false
        }
    }

    /// Remove a peer connection
    pub fn remove_peer(&mut self, peer_id: &PeerId) -> bool {
        self.peers.remove(peer_id)
    }
}

/// A mesh overlay for a single stream
#[derive(Debug)]
pub struct StreamMesh {
    /// ID of the stream
    pub stream_id: StreamId,
    /// Nodes in the mesh
    pub nodes: HashMap<PeerId, MeshNode>,
    /// Source peer ID
    pub source: Option<PeerId>,
}

impl StreamMesh {
    /// Create a new stream mesh
    pub fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            nodes: HashMap::new(),
            source: None,
        }
    }

    /// Set the source peer for this mesh
    pub fn set_source(&mut self, peer_id: PeerId) -> bool {
        if self.source.is_some() {
            return false;
        }

        let source_node = MeshNode::new(peer_id, PeerRole::Publisher, true);
        self.nodes.insert(peer_id, source_node);
        self.source = Some(peer_id);
        true
    }

    /// Add a peer to the mesh
    pub fn add_peer(&mut self, peer_id: PeerId, role: PeerRole, bandwidth: u64) -> bool {
        if self.nodes.contains_key(&peer_id) {
            return false;
        }

        let is_source = if let Some(source) = self.source {
            source == peer_id
        } else {
            false
        };

        let mut node = MeshNode::new(peer_id, role, is_source);
        node.bandwidth = bandwidth;

        self.nodes.insert(peer_id, node);
        self.connect_peer_to_mesh(peer_id)
    }

    /// Connect a peer to the mesh network
    fn connect_peer_to_mesh(&mut self, peer_id: PeerId) -> bool {
        if !self.nodes.contains_key(&peer_id) {
            return false;
        }

        let mut connected = false;

        // Sort other nodes by bandwidth (descending) to prioritize high-capacity nodes
        let mut candidates: Vec<(PeerId, u64)> = self
            .nodes
            .iter()
            .filter(|(id, node)| **id != peer_id && node.can_accept_connections())
            .map(|(id, node)| (*id, node.bandwidth))
            .collect();

        candidates.sort_by(|a, b| b.1.cmp(&a.1));

        // Connect to the source first if possible
        if let Some(source_id) = self.source {
            if source_id != peer_id {
                if let Some(source_node) = self.nodes.get_mut(&source_id) {
                    if source_node.can_accept_connections() {
                        source_node.add_peer(peer_id);

                        if let Some(node) = self.nodes.get_mut(&peer_id) {
                            node.add_peer(source_id);
                            connected = true;
                        }
                    }
                }
            }
        }

        // Connect to up to MAX_MESH_PEERS high-bandwidth peers
        for (candidate_id, _) in candidates {
            if let Some(node) = self.nodes.get_mut(&peer_id) {
                if !node.can_accept_connections() {
                    break;
                }

                if !node.peers.contains(&candidate_id) {
                    node.add_peer(candidate_id);

                    if let Some(candidate_node) = self.nodes.get_mut(&candidate_id) {
                        candidate_node.add_peer(peer_id);
                        connected = true;
                    }
                }
            }
        }

        connected
    }

    /// Remove a peer from the mesh
    pub fn remove_peer(&mut self, peer_id: &PeerId) -> bool {
        if !self.nodes.contains_key(peer_id) {
            return false;
        }

        // If this is the source, we can't remove it
        if let Some(source_id) = self.source {
            if *peer_id == source_id {
                return false;
            }
        }

        // Get connected peers before removing
        let connected_peers = {
            if let Some(node) = self.nodes.get(peer_id) {
                node.peers.clone()
            } else {
                HashSet::new()
            }
        };

        // Remove connections to this peer
        for connected_id in &connected_peers {
            if let Some(connected_node) = self.nodes.get_mut(connected_id) {
                connected_node.remove_peer(peer_id);
            }
        }

        // Remove the peer
        self.nodes.remove(peer_id);

        // Reconsider connections to maintain mesh connectivity
        self.rebalance();

        true
    }

    /// Get mesh statistics
    pub fn get_stats(&self) -> MeshStats {
        let mut stats = MeshStats {
            peer_count: self.nodes.len(),
            source_connected_count: 0,
            avg_connections: 0.0,
            max_connections: 0,
        };

        if self.nodes.is_empty() {
            return stats;
        }

        let source_id = self.source;

        // Calculate statistics
        let mut total_connections = 0;

        for (id, node) in &self.nodes {
            let conn_count = node.peers.len();
            total_connections += conn_count;
            stats.max_connections = stats.max_connections.max(conn_count);

            // Count peers connected to source
            if let Some(source_id) = source_id {
                if node.peers.contains(&source_id) && *id != source_id {
                    stats.source_connected_count += 1;
                }
            }
        }

        stats.avg_connections = total_connections as f64 / self.nodes.len() as f64;

        stats
    }

    /// Get connected peers for a peer
    pub fn get_connected_peers(&self, peer_id: &PeerId) -> Option<&HashSet<PeerId>> {
        self.nodes.get(peer_id).map(|node| &node.peers)
    }

    /// Check if a peer is in the mesh
    pub fn contains_peer(&self, peer_id: &PeerId) -> bool {
        self.nodes.contains_key(peer_id)
    }

    /// Rebalance the mesh to optimize connections
    pub fn rebalance(&mut self) -> bool {
        // Get all peers that need more connections
        let peers_needing_connections: Vec<PeerId> = self
            .nodes
            .iter()
            .filter(|(_, node)| node.can_accept_connections())
            .map(|(id, _)| *id)
            .collect();

        let mut changes = false;

        // Try to connect under-connected peers
        for peer_id in peers_needing_connections {
            changes |= self.connect_peer_to_mesh(peer_id);
        }

        changes
    }
}

/// Statistics about a mesh
#[derive(Debug, Clone)]
pub struct MeshStats {
    /// Number of peers in the mesh
    pub peer_count: usize,
    /// Number of peers directly connected to the source
    pub source_connected_count: usize,
    /// Average connections per peer
    pub avg_connections: f64,
    /// Maximum connections on any peer
    pub max_connections: usize,
}
