//! Tree-based overlay implementation
//!
//! This module provides a tree-based overlay for efficient distribution of
//! streaming data from source to viewers.

use crate::overlay::interface::{PeerRole, StreamId};
use libp2p::PeerId;
use std::collections::{HashMap, HashSet};

/// Maximum number of children for a node in the tree
const MAX_CHILDREN: usize = 3;

/// A node in the distribution tree
#[derive(Debug, Clone)]
pub struct TreeNode {
    /// Peer ID of this node
    pub peer_id: PeerId,
    /// Role of this peer in the tree
    pub role: PeerRole,
    /// Parent peer ID (if any)
    pub parent: Option<PeerId>,
    /// Children of this node
    pub children: HashSet<PeerId>,
    /// Depth in the tree (0 = source)
    pub depth: usize,
    /// Estimated bandwidth capacity
    pub bandwidth: u64,
    /// Estimated latency to parent (ms)
    pub latency: u64,
}

impl TreeNode {
    /// Create a new tree node
    pub fn new(peer_id: PeerId, role: PeerRole) -> Self {
        Self {
            peer_id,
            role,
            parent: None,
            children: HashSet::new(),
            depth: 0,
            bandwidth: 0,
            latency: 0,
        }
    }

    /// Create a source node
    pub fn source(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            role: PeerRole::Source,
            parent: None,
            children: HashSet::new(),
            depth: 0,
            bandwidth: 0,
            latency: 0,
        }
    }

    /// Check if this node can accept more children
    pub fn can_accept_children(&self) -> bool {
        self.role != PeerRole::Leaf && self.children.len() < MAX_CHILDREN
    }

    /// Add a child to this node
    pub fn add_child(&mut self, peer_id: PeerId) -> bool {
        if self.can_accept_children() {
            self.children.insert(peer_id);
            true
        } else {
            false
        }
    }

    /// Remove a child from this node
    pub fn remove_child(&mut self, peer_id: &PeerId) -> bool {
        self.children.remove(peer_id)
    }
}

/// A tree overlay for a single stream
#[derive(Debug)]
pub struct StreamTree {
    /// ID of the stream
    pub stream_id: StreamId,
    /// Nodes in the tree
    pub nodes: HashMap<PeerId, TreeNode>,
    /// Source peer ID
    pub source: Option<PeerId>,
}

impl StreamTree {
    /// Create a new stream tree
    pub fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            nodes: HashMap::new(),
            source: None,
        }
    }

    /// Set the source peer for this tree
    pub fn set_source(&mut self, peer_id: PeerId) -> bool {
        if self.source.is_some() {
            return false;
        }

        let source_node = TreeNode::source(peer_id);
        self.nodes.insert(peer_id, source_node);
        self.source = Some(peer_id);
        true
    }

    /// Add a peer to the tree
    pub fn add_peer(&mut self, peer_id: PeerId, role: PeerRole, bandwidth: u64) -> bool {
        if self.nodes.contains_key(&peer_id) {
            return false;
        }

        let mut node = TreeNode::new(peer_id, role);
        node.bandwidth = bandwidth;

        // If this is a leaf node, it can't have children
        if role == PeerRole::Leaf {
            self.nodes.insert(peer_id, node);
            return self.find_parent_for_peer(peer_id);
        }

        // Otherwise add as a relay node
        self.nodes.insert(peer_id, node);
        self.find_parent_for_peer(peer_id)
    }

    /// Find a suitable parent for a peer
    fn find_parent_for_peer(&mut self, peer_id: PeerId) -> bool {
        if self.source.is_none() {
            return false;
        }

        if let Some(source_id) = self.source {
            if peer_id == source_id {
                return true; // Source doesn't need a parent
            }
        }

        // Find a node that can accept children, prioritizing:
        // 1. Lower depth in the tree
        // 2. Higher bandwidth capacity
        // We avoid creating very deep trees by preferring nodes closer to the source

        let mut best_parent: Option<PeerId> = None;
        let mut best_depth = usize::MAX;
        let mut best_bandwidth = 0;

        for (candidate_id, node) in &self.nodes {
            if node.can_accept_children() && *candidate_id != peer_id {
                if node.depth < best_depth || 
                   (node.depth == best_depth && node.bandwidth > best_bandwidth) {
                    best_parent = Some(*candidate_id);
                    best_depth = node.depth;
                    best_bandwidth = node.bandwidth;
                }
            }
        }

        if let Some(parent_id) = best_parent {
            // Add this peer as a child of the parent
            if let Some(parent) = self.nodes.get_mut(&parent_id) {
                parent.add_child(peer_id);
            }

            // Set parent for this peer and update depth
            if let Some(node) = self.nodes.get_mut(&peer_id) {
                node.parent = Some(parent_id);
                node.depth = best_depth + 1;
            }

            true
        } else {
            // No suitable parent found
            false
        }
    }

    /// Remove a peer from the tree
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

        // Get the parent and children before removing
        let (parent, children) = {
            let node = self.nodes.get(peer_id).unwrap();
            (node.parent, node.children.clone())
        };

        // Remove the peer from its parent's children
        if let Some(parent_id) = parent {
            if let Some(parent_node) = self.nodes.get_mut(&parent_id) {
                parent_node.remove_child(peer_id);
            }
        }

        // Remove the peer
        self.nodes.remove(peer_id);

        // Reconnect any children to new parents
        for child_id in children {
            if let Some(child) = self.nodes.get_mut(&child_id) {
                child.parent = None;
            }
            self.find_parent_for_peer(child_id);
        }

        true
    }

    /// Get tree statistics
    pub fn get_stats(&self) -> TreeStats {
        let mut stats = TreeStats {
            peer_count: self.nodes.len(),
            depth: 0,
            relay_count: 0,
            leaf_count: 0,
        };

        for node in self.nodes.values() {
            match node.role {
                PeerRole::Relay => stats.relay_count += 1,
                PeerRole::Leaf => stats.leaf_count += 1,
                _ => {}
            }
            stats.depth = stats.depth.max(node.depth);
        }

        stats
    }

    /// Get peers at a specific depth in the tree
    pub fn get_peers_at_depth(&self, depth: usize) -> Vec<PeerId> {
        self.nodes
            .values()
            .filter(|node| node.depth == depth)
            .map(|node| node.peer_id)
            .collect()
    }

    /// Get children of a peer
    pub fn get_children(&self, peer_id: &PeerId) -> Option<&HashSet<PeerId>> {
        self.nodes.get(peer_id).map(|node| &node.children)
    }

    /// Get parent of a peer
    pub fn get_parent(&self, peer_id: &PeerId) -> Option<PeerId> {
        self.nodes.get(peer_id).and_then(|node| node.parent)
    }

    /// Check if a peer is in the tree
    pub fn contains_peer(&self, peer_id: &PeerId) -> bool {
        self.nodes.contains_key(peer_id)
    }

    /// Rebalance the tree to optimize distribution
    pub fn rebalance(&mut self) -> bool {
        // Implementation will be added in a future update
        // For now we just return true to indicate "success"
        true
    }
}

/// Statistics about a tree
#[derive(Debug, Clone)]
pub struct TreeStats {
    /// Number of peers in the tree
    pub peer_count: usize,
    /// Depth of the tree (longest path from source)
    pub depth: usize,
    /// Number of relay nodes
    pub relay_count: usize,
    /// Number of leaf nodes
    pub leaf_count: usize,
} 