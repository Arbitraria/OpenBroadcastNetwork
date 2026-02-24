//! Tree-based overlay implementation
//!
//! This module provides a tree-based overlay for efficient distribution of
//! streaming data from source to viewers. The tree structure creates an efficient
//! hierarchical distribution pattern that minimizes latency and optimizes bandwidth
//! utilization across the network.
//!
//! Key concepts:
//! - **Source node**: Root of the tree, publishing original content
//! - **Relay nodes**: Internal nodes that receive and forward content
//! - **Consumer nodes**: Leaf nodes that only receive content
//!
//! The tree automatically optimizes itself based on network conditions, geographic
//! proximity, and peer capabilities to ensure reliable and efficient content delivery.

use crate::overlay::interface::StreamId;
use crate::overlay::peer::PeerRole;
use crate::overlay::topology::{GeoLocation, RelayScoreWeights, TopologyConfig};
use libp2p::PeerId;
use std::collections::{HashMap, HashSet};

/// Default maximum number of children for a node in the tree
const DEFAULT_MAX_CHILDREN: usize = 3;

/// Absolute maximum number of children for any node in the tree
const ABSOLUTE_MAX_CHILDREN: usize = 7;

/// Minimum acceptable connection quality score (0.0-1.0)
const MIN_CONNECTION_QUALITY: f32 = 0.5;

/// Maximum distance on Earth in kilometers (half circumference)
const MAX_EARTH_DISTANCE_KM: f64 = 20_000.0;

/// A node in the content distribution tree
///
/// Represents a single peer in the tree-based overlay network. Each node maintains
/// references to its parent and children, forming a hierarchical structure for
/// efficient content distribution. The tree is constructed to optimize latency
/// and bandwidth utilization based on peer capabilities and network conditions.
///
/// Nodes are positioned in the tree according to their role, bandwidth capacity,
/// and network proximity to other nodes. This positioning ensures efficient
/// content distribution from the source (root) to all consumers (leaves).
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
    /// Estimated bandwidth capacity (bytes/sec)
    pub bandwidth: u64,
    /// Estimated latency to parent (ms)
    pub latency: u64,
    /// Maximum number of children this node can support (dynamic)
    pub max_children: usize,
    /// Connection quality score (0.0-1.0)
    pub connection_quality: f32,
    /// Last time node metrics were updated
    pub last_updated: std::time::Instant,
    /// Successive failures count
    pub failure_count: usize,
    /// Geographic location of this node
    pub location: Option<GeoLocation>,
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
            max_children: DEFAULT_MAX_CHILDREN,
            connection_quality: 1.0, // Start with perfect quality
            last_updated: std::time::Instant::now(),
            failure_count: 0,
            location: None,
        }
    }

    /// Create a source node
    pub fn source(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            role: PeerRole::Publisher,
            parent: None,
            children: HashSet::new(),
            depth: 0,
            bandwidth: 0,
            latency: 0,
            // Source nodes can potentially handle more children
            max_children: DEFAULT_MAX_CHILDREN + 1,
            connection_quality: 1.0, // Start with perfect quality
            last_updated: std::time::Instant::now(),
            failure_count: 0,
            location: None,
        }
    }

    /// Check if this node can accept more children
    pub fn can_accept_children(&self) -> bool {
        self.role != PeerRole::Consumer
            && self.children.len() < self.max_children
            && self.connection_quality >= MIN_CONNECTION_QUALITY
    }

    /// Update node metrics and adjust fan-out capacity
    pub fn update_metrics(&mut self, bandwidth: u64, latency: u64, success_rate: f32) {
        self.bandwidth = bandwidth;
        self.latency = latency;
        self.last_updated = std::time::Instant::now();

        // Calculate a connection quality score (0.0-1.0) based on latency and success rate
        // Lower latency and higher success rate = better quality
        let latency_factor = if latency < 50 {
            1.0
        } else if latency < 100 {
            0.9
        } else if latency < 200 {
            0.7
        } else if latency < 500 {
            0.5
        } else {
            0.3
        };

        self.connection_quality = latency_factor * success_rate;

        // Update max_children based on bandwidth, latency, and connection quality
        self.adjust_fan_out();
    }

    /// Adjust the maximum number of children based on node capabilities
    pub fn adjust_fan_out(&mut self) {
        // Base capacity on bandwidth (bytes/sec)
        // Assuming ~500KB/s per stream is needed for good quality
        let bandwidth_capacity = (self.bandwidth / 500_000) as usize;

        // Adjust based on connection quality
        let quality_adjusted = (bandwidth_capacity as f32 * self.connection_quality) as usize;

        // Clamp to reasonable values
        let new_max = quality_adjusted.clamp(1, ABSOLUTE_MAX_CHILDREN);

        // Publisher nodes always get a minimum capacity
        if self.role == PeerRole::Publisher {
            self.max_children = new_max.max(DEFAULT_MAX_CHILDREN);
        } else {
            self.max_children = new_max;
        }
    }

    /// Record a connection failure
    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        // Reduce connection quality on failures
        self.connection_quality *= 0.8;
        self.adjust_fan_out();
    }

    /// Record a connection success
    pub fn record_success(&mut self) {
        if self.failure_count > 0 {
            self.failure_count -= 1;
        }
        // Gradually recover connection quality on success
        self.connection_quality = (self.connection_quality + 0.1).min(1.0);
        self.adjust_fan_out();
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
///
/// Represents a complete distribution tree for a specific stream. The tree consists of
/// multiple TreeNode instances arranged in a hierarchical structure with a single source
/// node at the root and multiple relay and consumer nodes forming the branches and leaves.
///
/// The StreamTree manages the entire tree structure, handling node additions, removals,
/// and optimizations to ensure efficient content distribution. It automatically adjusts
/// the tree topology based on network conditions, peer capabilities, and geographic location
/// to minimize latency and maximize throughput.
#[derive(Debug)]
pub struct StreamTree {
    /// ID of the stream
    pub stream_id: StreamId,
    /// Nodes in the tree
    pub nodes: HashMap<PeerId, TreeNode>,
    /// Source peer ID
    pub source: Option<PeerId>,
    /// Disconnected nodes cache (for temporary disconnections)
    pub disconnected_nodes: HashMap<PeerId, (TreeNode, std::time::Instant)>,
    /// Last time tree was rebalanced
    pub last_rebalanced: std::time::Instant,
    /// Rebalance counter
    pub rebalance_count: usize,
    /// Topology configuration for scoring
    pub config: TopologyConfig,
}

impl StreamTree {
    /// Create a new empty stream tree for the specified stream
    ///
    /// # Arguments
    /// * `stream_id` - The unique identifier for the stream this tree will distribute
    /// * `config` - Topology configuration for scoring and rebalancing
    ///
    /// # Returns
    /// A new StreamTree instance with no nodes
    pub fn new(stream_id: StreamId, config: TopologyConfig) -> Self {
        Self {
            stream_id,
            nodes: HashMap::new(),
            source: None,
            disconnected_nodes: HashMap::new(),
            last_rebalanced: std::time::Instant::now(),
            rebalance_count: 0,
            config,
        }
    }

    /// Set the source peer for this tree
    ///
    /// Establishes a peer as the root of the distribution tree (the content publisher).
    /// This node will be the original source of all content flowing through the tree.
    ///
    /// # Arguments
    /// * `peer_id` - The ID of the peer to set as the source
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
        if role == PeerRole::Consumer {
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
                if node.depth < best_depth
                    || (node.depth == best_depth && node.bandwidth > best_bandwidth)
                {
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
        let _node = self.nodes.remove(peer_id).unwrap();

        // Reconnect any children to new parents
        for child_id in children {
            if let Some(child) = self.nodes.get_mut(&child_id) {
                child.parent = None;
            }
            self.find_parent_for_peer(child_id);
        }

        true
    }

    /// Handle a temporary disconnect by preserving node state
    pub fn handle_temporary_disconnect(
        &mut self,
        peer_id: &PeerId,
        grace_period: std::time::Duration,
    ) -> bool {
        if !self.nodes.contains_key(peer_id) {
            return false;
        }

        // Can't temporarily disconnect the source
        if let Some(source_id) = self.source {
            if *peer_id == source_id {
                return false;
            }
        }

        // Clone the node before removal
        let node = self.nodes.get(peer_id).cloned();

        if let Some(node) = node {
            // Get the data we need before moving the node
            let parent = node.parent;
            let children = node.children.clone();

            // Save node info with timestamp for potential reconnection
            self.disconnected_nodes
                .insert(*peer_id, (node, std::time::Instant::now()));

            // Remove from parent
            if let Some(parent_id) = parent {
                if let Some(parent_node) = self.nodes.get_mut(&parent_id) {
                    parent_node.remove_child(peer_id);
                }
            }

            // Remove node from active nodes
            self.nodes.remove(peer_id);

            // Handle children - either find new parents or keep them waiting
            for child_id in children {
                if let Some(child) = self.nodes.get_mut(&child_id) {
                    child.parent = None;
                }
                // Try to find a new parent, but it's not critical in temporary disconnection
                let _ = self.find_parent_for_peer(child_id);
            }

            // Clean up expired disconnections
            self.cleanup_disconnected_nodes(grace_period);

            true
        } else {
            false
        }
    }

    /// Handle a reconnection attempt
    pub fn handle_reconnection(&mut self, peer_id: &PeerId) -> bool {
        // Check if this peer is in the disconnected nodes cache
        if let Some((mut node, _disconnect_time)) = self.disconnected_nodes.remove(peer_id) {
            // Add the node back to the tree
            // Note: We'll need to find a new parent and potentially new children
            node.parent = None; // Clear parent as we'll need to find a new one
            node.children.clear(); // Clear children as they may have found new parents

            // Record the reconnection as a success
            node.record_success();

            // Add back to the active nodes
            self.nodes.insert(*peer_id, node);

            // Find a new parent for this node
            if !self.find_parent_for_peer(*peer_id) {
                // If we couldn't find a parent, this is still a success but it's orphaned
                // It will be picked up in the next rebalance
            }

            // Some of the node's original children might want to reconnect to it
            // This is handled during rebalance

            true
        } else {
            // Node not in disconnected cache, treat as a new connection
            false
        }
    }

    /// Clean up disconnected nodes that have exceeded their grace period
    fn cleanup_disconnected_nodes(&mut self, grace_period: std::time::Duration) {
        let now = std::time::Instant::now();
        let expired: Vec<PeerId> = self
            .disconnected_nodes
            .iter()
            .filter(|(_, (_, time))| now.duration_since(*time) > grace_period)
            .map(|(id, _)| *id)
            .collect();

        for id in expired {
            self.disconnected_nodes.remove(&id);
        }
    }

    /// Get a path from source to a peer in the tree
    pub fn get_path_to_peer(&self, peer_id: &PeerId) -> Option<Vec<PeerId>> {
        if !self.nodes.contains_key(peer_id) {
            return None;
        }

        let mut path = Vec::new();
        let mut current = *peer_id;

        // Build path from peer to source
        while let Some(node) = self.nodes.get(&current) {
            path.push(current);

            if let Some(parent) = node.parent {
                current = parent;
            } else {
                break;
            }
        }

        // Reverse to get path from source to peer
        path.reverse();

        if path.is_empty() {
            None
        } else {
            Some(path)
        }
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
                PeerRole::Consumer => stats.leaf_count += 1,
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

    /// Calculate geo score between two nodes
    ///
    /// Returns a score from 0.0 to 1.0 where higher is better (closer/same region).
    fn calculate_geo_score(&self, node1: &TreeNode, node2: &TreeNode) -> f32 {
        if !self.config.enable_geo_aware {
            return 0.5; // Neutral score if disabled
        }

        match (&node1.location, &node2.location) {
            (Some(loc1), Some(loc2)) => {
                // Calculate distance-based score
                let distance = loc1.distance_to(loc2);
                let distance_score = (1.0 - (distance / MAX_EARTH_DISTANCE_KM)).max(0.0) as f32;

                // Apply same-region bonus if configured
                if self.config.prefer_same_region && loc1.is_same_region(loc2) {
                    // Boost score by 20% for same region, capped at 1.0
                    (distance_score * 1.2).min(1.0)
                } else {
                    distance_score
                }
            }
            _ => {
                // No location info available, return neutral score
                0.5
            }
        }
    }

    /// Calculate combined score for a candidate parent
    ///
    /// Uses configurable weights for depth, bandwidth, latency, and geo proximity.
    fn calculate_candidate_score(
        &self,
        candidate: &TreeNode,
        child: &TreeNode,
        weights: &RelayScoreWeights,
        max_depth: usize,
    ) -> f32 {
        // Depth score: prefer lower depth (closer to source)
        // Normalize: depth 0 = 1.0, max_depth = 0.0
        let depth_score = if max_depth > 0 {
            1.0 - (candidate.depth as f32 / max_depth as f32)
        } else {
            1.0
        };

        // Bandwidth score: higher is better
        // Normalize using log scale (1MB/s = 0.5, 10MB/s = 0.7, 100MB/s = 0.9)
        let bw_score = if candidate.bandwidth > 0 {
            ((candidate.bandwidth as f64).log10() / 10.0).min(1.0) as f32
        } else {
            0.0
        };

        // Latency score: lower is better
        // Normalize: 0ms = 1.0, 500ms+ = 0.0
        let lat_score = if candidate.latency < 500 {
            1.0 - (candidate.latency as f32 / 500.0)
        } else {
            0.0
        };

        // Geo score: closer/same region is better
        let geo_score = self.calculate_geo_score(candidate, child);

        // Combine scores with weights
        // Note: We use quality_weight for depth since that's what makes sense here
        (depth_score * weights.quality_weight)
            + (bw_score * weights.bandwidth_weight)
            + (lat_score * weights.latency_weight)
            + (geo_score * weights.geo_weight)
    }

    /// Rebalance the tree to optimize distribution
    pub fn rebalance(&mut self) -> bool {
        self.last_rebalanced = std::time::Instant::now();
        self.rebalance_count += 1;

        // Get max depth for normalization
        let max_depth = self.config.max_tree_depth;
        let weights = self.config.score_weights.clone();

        // 1. Find overloaded nodes (high latency or approaching capacity)
        let overloaded_nodes: Vec<PeerId> = self
            .nodes
            .iter()
            .filter(|(_, node)| {
                node.latency > 200 && node.children.len() > 1 || // High latency nodes
                node.children.len() >= node.max_children.saturating_sub(1) || // Near capacity
                node.connection_quality < 0.7 // Poor connection quality
            })
            .map(|(id, _)| *id)
            .collect();

        // 2. Find nodes with capacity for more children
        let candidates: Vec<(PeerId, usize, u64)> = self
            .nodes
            .iter()
            .filter(|(_, node)| {
                node.can_accept_children()
                    && node.children.len() < node.max_children.saturating_sub(1)
                    && node.connection_quality > 0.8
            })
            .map(|(id, node)| (*id, node.depth, node.bandwidth))
            .collect();

        if candidates.is_empty() || overloaded_nodes.is_empty() {
            return false; // No rebalancing possible
        }

        let mut changes_made = false;

        // 3. For each overloaded node, move some children to better parents
        for overloaded_id in overloaded_nodes {
            if let Some(overloaded) = self.nodes.get(&overloaded_id) {
                // Skip if no children to move
                if overloaded.children.is_empty() {
                    continue;
                }

                // Get children to consider for moving
                let children: Vec<PeerId> = overloaded.children.iter().cloned().collect();

                // Try to move up to half the children if heavily overloaded
                let to_move = if overloaded.connection_quality < 0.5 {
                    children.len().max(1) / 2
                } else {
                    children.len().max(1) / 3
                };

                for i in 0..to_move.min(children.len()) {
                    let child_id = children[i];

                    // Get child node info for scoring
                    let child_node = match self.nodes.get(&child_id) {
                        Some(n) => n.clone(),
                        None => continue,
                    };

                    // Find best new parent for this child using geo-aware scoring
                    let mut best_parent = None;
                    let mut best_score = f32::NEG_INFINITY;

                    for (candidate_id, _depth, _bandwidth) in &candidates {
                        // Skip if candidate is the child itself or already full
                        if *candidate_id == child_id {
                            continue;
                        }

                        if let Some(candidate) = self.nodes.get(candidate_id) {
                            if !candidate.can_accept_children() {
                                continue;
                            }

                            // Calculate combined score using weights
                            let score = self.calculate_candidate_score(
                                candidate,
                                &child_node,
                                &weights,
                                max_depth,
                            );

                            if score > best_score {
                                best_parent = Some(*candidate_id);
                                best_score = score;
                            }
                        }
                    }

                    // Move the child to the new parent if found
                    if let Some(new_parent_id) = best_parent {
                        // First update the data structures
                        if let Some(overloaded_node) = self.nodes.get_mut(&overloaded_id) {
                            overloaded_node.remove_child(&child_id);
                        }

                        if let Some(new_parent) = self.nodes.get_mut(&new_parent_id) {
                            new_parent.add_child(child_id);
                        }

                        // First get the parent's depth before any mutable borrows
                        let parent_depth = if let Some(parent) = self.nodes.get(&new_parent_id) {
                            parent.depth
                        } else {
                            0
                        };

                        // Now update the child with all the data we need
                        if let Some(child) = self.nodes.get_mut(&child_id) {
                            child.parent = Some(new_parent_id);
                            child.depth = parent_depth + 1;
                        }

                        changes_made = true;
                    }
                }
            }
        }

        // 4. Check for orphaned nodes (no parent) and find them a parent
        let orphans: Vec<PeerId> = self
            .nodes
            .iter()
            .filter(|(id, node)| {
                node.parent.is_none() && self.source.map_or(false, |src| **id != src)
                // Not the source
            })
            .map(|(id, _)| *id)
            .collect();

        for orphan_id in orphans {
            if self.find_parent_for_peer(orphan_id) {
                changes_made = true;
            }
        }

        changes_made
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay::topology::Region;

    fn create_stream_id(id: &str) -> StreamId {
        StreamId::from_string(id)
    }

    #[test]
    fn test_geo_score_same_region() {
        let stream_id = create_stream_id("test_geo");
        let config = TopologyConfig::default();
        let tree = StreamTree::new(stream_id, config);

        // Create two nodes in the same region (US)
        let mut node1 = TreeNode::new(PeerId::random(), PeerRole::Relay);
        node1.location = Some(GeoLocation::new(40.7128, -74.0060, "US".to_string())); // NYC

        let mut node2 = TreeNode::new(PeerId::random(), PeerRole::Consumer);
        node2.location = Some(GeoLocation::new(34.0522, -118.2437, "US".to_string())); // LA

        let score = tree.calculate_geo_score(&node1, &node2);

        // Same region should give a high score (with bonus)
        assert!(
            score > 0.7,
            "Same region score should be high, got {}",
            score
        );
    }

    #[test]
    fn test_geo_score_different_regions() {
        let stream_id = create_stream_id("test_geo_diff");
        let config = TopologyConfig::default();
        let tree = StreamTree::new(stream_id, config);

        // Create two nodes in different regions
        let mut node1 = TreeNode::new(PeerId::random(), PeerRole::Relay);
        node1.location = Some(GeoLocation::new(40.7128, -74.0060, "US".to_string())); // NYC

        let mut node2 = TreeNode::new(PeerId::random(), PeerRole::Consumer);
        node2.location = Some(GeoLocation::new(35.6762, 139.6503, "JP".to_string())); // Tokyo

        let score = tree.calculate_geo_score(&node1, &node2);

        // Different regions, far apart should give lower score
        assert!(
            score < 0.7,
            "Different region score should be lower, got {}",
            score
        );
    }

    #[test]
    fn test_geo_score_no_location() {
        let stream_id = create_stream_id("test_geo_none");
        let config = TopologyConfig::default();
        let tree = StreamTree::new(stream_id, config);

        // Create nodes without location
        let node1 = TreeNode::new(PeerId::random(), PeerRole::Relay);
        let node2 = TreeNode::new(PeerId::random(), PeerRole::Consumer);

        let score = tree.calculate_geo_score(&node1, &node2);

        // No location should give neutral score
        assert!(
            (score - 0.5).abs() < 0.01,
            "No location score should be 0.5, got {}",
            score
        );
    }

    #[test]
    fn test_candidate_score_prefers_lower_depth() {
        let stream_id = create_stream_id("test_depth");
        let config = TopologyConfig::default();
        let tree = StreamTree::new(stream_id, config.clone());

        let child = TreeNode::new(PeerId::random(), PeerRole::Consumer);

        let mut shallow = TreeNode::new(PeerId::random(), PeerRole::Relay);
        shallow.depth = 1;
        shallow.bandwidth = 5_000_000;
        shallow.latency = 50;

        let mut deep = TreeNode::new(PeerId::random(), PeerRole::Relay);
        deep.depth = 4;
        deep.bandwidth = 5_000_000;
        deep.latency = 50;

        let shallow_score =
            tree.calculate_candidate_score(&shallow, &child, &config.score_weights, 5);
        let deep_score = tree.calculate_candidate_score(&deep, &child, &config.score_weights, 5);

        assert!(
            shallow_score > deep_score,
            "Shallow node should score higher: {} vs {}",
            shallow_score,
            deep_score
        );
    }
}
