//! Topology management for the overlay network
//!
//! This module handles the organization of peers into an efficient
//! tree-mesh hybrid structure for stream relay.

use crate::overlay::peer::{Peer, PeerId, PeerInfo, PeerRole};
use crate::overlay::interface::{StreamId, OverlayError};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use std::cmp::Ordering;
use std::str::FromStr;
use std::net::IpAddr;
use rand::Rng;
use geo_ip::{GeoIP, GeoIPError, Coordinates, ASN};

/// Geographic region codes (simplified)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Region {
    NorthAmerica,
    SouthAmerica,
    Europe,
    Asia,
    Africa,
    Oceania,
    Unknown,
}

impl FromStr for Region {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "na" | "north_america" => Ok(Region::NorthAmerica),
            "sa" | "south_america" => Ok(Region::SouthAmerica),
            "eu" | "europe" => Ok(Region::Europe),
            "as" | "asia" => Ok(Region::Asia),
            "af" | "africa" => Ok(Region::Africa),
            "oc" | "oceania" => Ok(Region::Oceania),
            _ => Ok(Region::Unknown),
        }
    }
}

impl ToString for Region {
    fn to_string(&self) -> String {
        match self {
            Region::NorthAmerica => "north_america".to_string(),
            Region::SouthAmerica => "south_america".to_string(),
            Region::Europe => "europe".to_string(),
            Region::Asia => "asia".to_string(),
            Region::Africa => "africa".to_string(),
            Region::Oceania => "oceania".to_string(),
            Region::Unknown => "unknown".to_string(),
        }
    }
}

/// Geographic location information
#[derive(Debug, Clone)]
pub struct GeoLocation {
    /// Latitude
    pub latitude: f64,
    /// Longitude
    pub longitude: f64,
    /// Country code
    pub country: String,
    /// Region
    pub region: Region,
    /// City
    pub city: Option<String>,
    /// ISP/ASN information
    pub asn: Option<String>,
}

impl Default for GeoLocation {
    fn default() -> Self {
        Self {
            latitude: 0.0,
            longitude: 0.0,
            country: String::from("XX"),
            region: Region::Unknown,
            city: None,
            asn: None,
        }
    }
}

impl GeoLocation {
    /// Create a new location from coordinates and country
    pub fn new(latitude: f64, longitude: f64, country: String) -> Self {
        let region = Self::country_to_region(&country);
        
        Self {
            latitude,
            longitude,
            country,
            region,
            city: None,
            asn: None,
        }
    }
    
    /// Calculate distance to another location (using Haversine formula)
    pub fn distance_to(&self, other: &GeoLocation) -> f64 {
        // Earth radius in kilometers
        const EARTH_RADIUS: f64 = 6371.0;
        
        let lat1 = self.latitude.to_radians();
        let lon1 = self.longitude.to_radians();
        let lat2 = other.latitude.to_radians();
        let lon2 = other.longitude.to_radians();
        
        let dlat = lat2 - lat1;
        let dlon = lon2 - lon1;
        
        let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        
        EARTH_RADIUS * c
    }
    
    /// Check if this location is in the same region as another
    pub fn is_same_region(&self, other: &GeoLocation) -> bool {
        self.region == other.region
    }
    
    /// Get region from country code
    fn country_to_region(country: &str) -> Region {
        // This is a simplified mapping and not comprehensive
        match country {
            "US" | "CA" | "MX" => Region::NorthAmerica,
            "BR" | "AR" | "CL" | "CO" | "PE" | "VE" => Region::SouthAmerica,
            "GB" | "DE" | "FR" | "IT" | "ES" | "NL" | "SE" | "NO" | "FI" | "PL" => Region::Europe,
            "CN" | "JP" | "KR" | "IN" | "SG" | "TH" | "MY" | "ID" => Region::Asia,
            "ZA" | "NG" | "EG" | "KE" | "MA" => Region::Africa,
            "AU" | "NZ" => Region::Oceania,
            _ => Region::Unknown,
        }
    }
    
    /// Create a location for a peer based on its IP address
    pub fn from_peer_info(peer: &crate::overlay::peer::PeerInfo) -> Self {
        // If the peer already has region info, use that
        if let Some(region_str) = peer.metadata.get("region") {
            if let Ok(region) = Region::from_str(region_str) {
                return Self {
                    latitude: peer.metadata.get("latitude")
                        .and_then(|v| v.parse::<f64>().ok())
                        .unwrap_or(0.0),
                    longitude: peer.metadata.get("longitude")
                        .and_then(|v| v.parse::<f64>().ok())
                        .unwrap_or(0.0),
                    country: peer.metadata.get("country")
                        .cloned()
                        .unwrap_or_else(|| "XX".to_string()),
                    region,
                    city: peer.metadata.get("city").cloned(),
                    asn: peer.metadata.get("asn").cloned(),
                };
            }
        }
        
        // Try to determine location from IP addresses
        for addr in &peer.addresses {
            if let Some(ip) = Self::extract_ip_from_addr(addr) {
                if let Some(location) = Self::lookup_ip_location(&ip) {
                    return location;
                }
            }
        }
        
        // Use default if no info available
        Self::default()
    }
    
    /// Extract IP address from a peer address string
    fn extract_ip_from_addr(addr: &str) -> Option<IpAddr> {
        addr.split(':')
            .next()
            .and_then(|ip_str| ip_str.parse::<IpAddr>().ok())
    }
    
    /// Look up location information for an IP address
    fn lookup_ip_location(ip: &IpAddr) -> Option<Self> {
        // In a real implementation, this would use a GeoIP database
        // For now, we'll return random locations for testing
        
        // Simulate GeoIP lookup with randomized values
        let mut rng = rand::thread_rng();
        
        // Generate random coordinates and country
        let regions = [
            (Region::NorthAmerica, "US"),
            (Region::Europe, "DE"),
            (Region::Asia, "JP"),
            (Region::SouthAmerica, "BR"),
            (Region::Africa, "ZA"),
            (Region::Oceania, "AU"),
        ];
        
        let (region, country) = regions[rng.gen_range(0..regions.len())];
        
        let latitude = rng.gen_range(-90.0..90.0);
        let longitude = rng.gen_range(-180.0..180.0);
        
        Some(Self {
            latitude,
            longitude,
            country: country.to_string(),
            region,
            city: Some("City".to_string()),
            asn: Some("AS12345".to_string()),
        })
    }
}

/// Configuration for topology management
#[derive(Debug, Clone)]
pub struct TopologyConfig {
    /// Maximum number of children per relay node
    pub max_fanout: usize,
    /// Maximum tree depth
    pub max_depth: usize,
    /// Rebalance interval
    pub rebalance_interval: Duration,
    /// Whether to use region-aware topology
    pub region_aware: bool,
    /// Maximum latency for direct connections (ms)
    pub max_latency_ms: u64,
    /// Relay score weights
    pub relay_score_weights: RelayScoreWeights,
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
            max_fanout: 3,
            max_depth: 5,
            rebalance_interval: Duration::from_secs(60),
            region_aware: true,
            max_latency_ms: 300,
            relay_score_weights: RelayScoreWeights::default(),
            prefer_same_region: true,
            max_close_distance_km: 1000.0,
            use_coordinates: true,
        }
    }
}

/// Weights used for scoring potential relay nodes
#[derive(Debug, Clone, Copy)]
pub struct RelayScoreWeights {
    /// Weight for bandwidth capacity
    pub bandwidth_weight: f32,
    /// Weight for latency
    pub latency_weight: f32,
    /// Weight for geographic proximity
    pub proximity_weight: f32,
    /// Weight for connection stability
    pub stability_weight: f32,
    /// Weight for current load
    pub load_weight: f32,
    /// Weight for regional preference
    pub region_weight: f32,
}

impl Default for RelayScoreWeights {
    fn default() -> Self {
        Self {
            bandwidth_weight: 0.25,
            latency_weight: 0.25,
            proximity_weight: 0.15,
            stability_weight: 0.15,
            load_weight: 0.1,
            region_weight: 0.1,
        }
    }
}

/// A node in the relay tree
#[derive(Debug, Clone)]
pub struct RelayNode {
    /// The peer ID of this node
    pub peer_id: PeerId,
    /// The parent node (if any)
    pub parent: Option<PeerId>,
    /// The children of this node
    pub children: HashSet<PeerId>,
    /// The depth in the tree (0 for root)
    pub depth: usize,
    /// Last update timestamp
    pub last_updated: Instant,
    /// The relay score (higher is better)
    pub score: f32,
    /// Whether this node is a publisher
    pub is_publisher: bool,
    /// Geographic location
    pub location: Option<GeoLocation>,
    /// Regional connection counts
    pub region_connections: HashMap<Region, usize>,
}

impl RelayNode {
    /// Create a new relay node
    pub fn new(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            parent: None,
            children: HashSet::new(),
            depth: 0,
            last_updated: Instant::now(),
            score: 0.0,
            is_publisher: false,
            location: None,
            region_connections: HashMap::new(),
        }
    }
    
    /// Create a new publisher node
    pub fn new_publisher(peer_id: PeerId) -> Self {
        let mut node = Self::new(peer_id);
        node.is_publisher = true;
        node
    }
    
    /// Add a child to this node and update region stats
    pub fn add_child(&mut self, child_id: PeerId, child_region: Option<&Region>) {
        self.children.insert(child_id);
        
        // Update region connection count
        if let Some(region) = child_region {
            *self.region_connections.entry(region.clone()).or_insert(0) += 1;
        }
        
        self.last_updated = Instant::now();
    }
    
    /// Remove a child from this node and update region stats
    pub fn remove_child(&mut self, child_id: &PeerId, child_region: Option<&Region>) {
        self.children.remove(child_id);
        
        // Update region connection count
        if let Some(region) = child_region {
            if let Some(count) = self.region_connections.get_mut(region) {
                if *count > 0 {
                    *count -= 1;
                }
            }
        }
        
        self.last_updated = Instant::now();
    }
    
    /// Set node location
    pub fn set_location(&mut self, location: GeoLocation) {
        self.location = Some(location);
    }
    
    /// Get connection count for a specific region
    pub fn connections_in_region(&self, region: &Region) -> usize {
        self.region_connections.get(region).copied().unwrap_or(0)
    }
    
    /// Calculate geographic distance to another node
    pub fn distance_to(&self, other: &RelayNode) -> Option<f64> {
        match (&self.location, &other.location) {
            (Some(loc1), Some(loc2)) => Some(loc1.distance_to(loc2)),
            _ => None,
        }
    }
    
    /// Check if node is in the same region as another
    pub fn is_same_region(&self, other: &RelayNode) -> bool {
        match (&self.location, &other.location) {
            (Some(loc1), Some(loc2)) => loc1.is_same_region(loc2),
            _ => false,
        }
    }
    
    /// Check if this node can accept more children
    pub fn can_accept_children(&self, max_fanout: usize) -> bool {
        self.children.len() < max_fanout
    }
    
    /// Set the parent of this node
    pub fn set_parent(&mut self, parent_id: PeerId) {
        self.parent = Some(parent_id);
        self.last_updated = Instant::now();
    }
    
    /// Clear the parent of this node
    pub fn clear_parent(&mut self) {
        self.parent = None;
        self.last_updated = Instant::now();
    }
    
    /// Update the depth of this node
    pub fn update_depth(&mut self, depth: usize) {
        self.depth = depth;
        self.last_updated = Instant::now();
    }
    
    /// Update the score of this node
    pub fn update_score(&mut self, score: f32) {
        self.score = score;
        self.last_updated = Instant::now();
    }
}

/// A relay tree for a stream
#[derive(Debug)]
pub struct RelayTree {
    /// The stream ID this tree is for
    pub stream_id: StreamId,
    /// The root node (publisher)
    pub root: PeerId,
    /// All nodes in the tree
    pub nodes: HashMap<PeerId, RelayNode>,
    /// Creation timestamp
    pub created_at: Instant,
    /// Last rebalance timestamp
    pub last_rebalanced: Instant,
}

impl RelayTree {
    /// Create a new relay tree for a stream
    pub fn new(stream_id: StreamId, publisher_id: PeerId) -> Self {
        let mut nodes = HashMap::new();
        
        // Create the root node
        let root_node = RelayNode::new_publisher(publisher_id.clone());
        nodes.insert(publisher_id.clone(), root_node);
        
        Self {
            stream_id,
            root: publisher_id,
            nodes,
            created_at: Instant::now(),
            last_rebalanced: Instant::now(),
        }
    }
    
    /// Add a peer to the tree
    pub fn add_peer(&mut self, peer_id: PeerId, config: &TopologyConfig) -> Result<(), OverlayError> {
        if self.nodes.contains_key(&peer_id) {
            return Err(OverlayError::TopologyError(
                format!("Peer already in tree: {}", peer_id)
            ));
        }
        
        // Create the new node
        let mut node = RelayNode::new(peer_id.clone());
        
        // Find the best parent for this node
        let best_parent = self.find_best_parent(&peer_id, config)?;
        node.set_parent(best_parent.clone());
        
        // Calculate depth
        if let Some(parent_node) = self.nodes.get(&best_parent) {
            node.update_depth(parent_node.depth + 1);
        }
        
        // Add this node as a child to its parent
        if let Some(parent_node) = self.nodes.get_mut(&best_parent) {
            parent_node.add_child(peer_id.clone(), None);
        }
        
        // Add the node to the tree
        self.nodes.insert(peer_id, node);
        
        Ok(())
    }
    
    /// Remove a peer from the tree
    pub fn remove_peer(&mut self, peer_id: &PeerId) -> Result<(), OverlayError> {
        if !self.nodes.contains_key(peer_id) {
            return Err(OverlayError::TopologyError(
                format!("Peer not in tree: {}", peer_id)
            ));
        }
        
        // Cannot remove the root node
        if peer_id == &self.root {
            return Err(OverlayError::TopologyError(
                "Cannot remove the root node".to_string()
            ));
        }
        
        // Get the node to remove
        let node = self.nodes.remove(peer_id).unwrap();
        
        // If this node has children, reassign them
        if !node.children.is_empty() {
            let parent_id = node.parent.clone().unwrap_or_else(|| self.root.clone());
            
            for child_id in node.children {
                if let Some(child_node) = self.nodes.get_mut(&child_id) {
                    child_node.set_parent(parent_id.clone());
                    child_node.update_depth(self.nodes[&parent_id].depth + 1);
                }
                
                if let Some(parent_node) = self.nodes.get_mut(&parent_id) {
                    parent_node.add_child(child_id, None);
                }
            }
        }
        
        // Remove this node from its parent's children
        if let Some(parent_id) = node.parent {
            if let Some(parent_node) = self.nodes.get_mut(&parent_id) {
                parent_node.remove_child(peer_id, None);
            }
        }
        
        Ok(())
    }
    
    /// Find the best parent for a new node
    fn find_best_parent(&self, peer_id: &PeerId, config: &TopologyConfig) -> Result<PeerId, OverlayError> {
        // Simple strategy: BFS from root to find first node that can accept children
        let mut queue = VecDeque::new();
        queue.push_back(self.root.clone());
        
        while let Some(node_id) = queue.pop_front() {
            let node = self.nodes.get(&node_id).unwrap();
            
            // Skip if this node is at max depth
            if node.depth >= config.max_depth {
                continue;
            }
            
            // Check if this node can accept more children
            if node.can_accept_children(config.max_fanout) {
                return Ok(node_id);
            }
            
            // Add children to queue
            for child_id in &node.children {
                queue.push_back(child_id.clone());
            }
        }
        
        // If all nodes are full, return the root as fallback
        Ok(self.root.clone())
    }
    
    /// Rebalance the tree with geo-awareness
    pub fn rebalance(&mut self, peers: &HashMap<PeerId, Peer>, config: &TopologyConfig) {
        // Skip if we don't have many peers
        if self.nodes.len() < 3 {
            return;
        }
        
        info!("Rebalancing relay tree for stream {:?} with {} nodes", self.stream_id, self.nodes.len());
        
        if config.region_aware {
            self.rebalance_geo_aware(peers, config);
        } else {
            self.rebalance_standard(peers, config);
        }
    }
    
    /// Rebalance using standard (non-geo) approach
    fn rebalance_standard(&mut self, peers: &HashMap<PeerId, Peer>, config: &TopologyConfig) {
        // Standard rebalancing logic
        let mut orphaned_peers = Vec::new();
        
        // Find peers that are poorly connected (high latency or overloaded parents)
        for (peer_id, node) in &self.nodes {
            // Skip the publisher (root)
            if *peer_id == self.root {
                continue;
            }
            
            let should_reconnect = if let Some(parent_id) = &node.parent {
                if let Some(parent) = self.nodes.get(parent_id) {
                    // Check if parent is overloaded
                    parent.children.len() > config.max_fanout || 
                    // Or if we need to limit tree depth
                    node.depth > config.max_depth
                } else {
                    // Parent not found in tree
                    true
                }
            } else {
                // No parent
                true
            };
            
            if should_reconnect {
                orphaned_peers.push(*peer_id);
            }
        }
        
        // Reconnect orphaned peers
        for peer_id in orphaned_peers {
            // Temporarily remove from tree
            if let Some(node) = self.nodes.get(&peer_id) {
                if let Some(parent_id) = &node.parent {
                    if let Some(parent) = self.nodes.get_mut(parent_id) {
                        parent.remove_child(&peer_id, None);
                    }
                }
            }
            
            // Find a new parent
            if let Ok(new_parent_id) = self.find_best_parent_by_score(&peer_id, &self.nodes, config) {
                if let Some(peer) = self.nodes.get_mut(&peer_id) {
                    peer.set_parent(new_parent_id);
                }
                if let Some(parent) = self.nodes.get_mut(&new_parent_id) {
                    parent.add_child(peer_id, None);
                }
            }
        }
    }
    
    /// Rebalance with geo-awareness
    fn rebalance_geo_aware(&mut self, peers: &HashMap<PeerId, Peer>, config: &TopologyConfig) {
        // Update location information for all nodes
        self.update_node_locations(peers);
        
        // Calculate optimal region distribution
        let region_distribution = self.calculate_optimal_region_distribution();
        
        // Find peers that should be reconnected
        let mut reconnect_candidates = Vec::new();
        
        for (peer_id, node) in &self.nodes {
            // Skip the publisher (root)
            if *peer_id == self.root {
                continue;
            }
            
            // Check if current parent is good
            let should_reconnect = if let Some(parent_id) = &node.parent {
                if let Some(parent) = self.nodes.get(parent_id) {
                    let different_regions = match (&node.location, &parent.location) {
                        (Some(node_loc), Some(parent_loc)) => !node_loc.is_same_region(parent_loc),
                        _ => false,
                    };
                    
                    // Reconnect if:
                    // 1. Parent is overloaded
                    // 2. We're trying to limit tree depth
                    // 3. We're in different regions and prefer same-region (if possible)
                    parent.children.len() > config.max_fanout || 
                    node.depth > config.max_depth ||
                    (config.prefer_same_region && different_regions)
                } else {
                    // Parent not found
                    true
                }
            } else {
                // No parent
                true
            };
            
            if should_reconnect {
                reconnect_candidates.push(*peer_id);
            }
        }
        
        // Sort candidates by depth (deeper nodes first)
        reconnect_candidates.sort_by(|a, b| {
            let depth_a = self.nodes.get(a).map(|n| n.depth).unwrap_or(0);
            let depth_b = self.nodes.get(b).map(|n| n.depth).unwrap_or(0);
            depth_b.cmp(&depth_a)
        });
        
        // Reconnect candidates
        for peer_id in reconnect_candidates {
            let peer_region = self.nodes.get(&peer_id)
                .and_then(|n| n.location.as_ref())
                .map(|l| l.region.clone());
            
            // Remove from current parent
            if let Some(node) = self.nodes.get(&peer_id) {
                if let Some(parent_id) = &node.parent {
                    if let Some(parent) = self.nodes.get_mut(parent_id) {
                        parent.remove_child(&peer_id, peer_region.as_ref());
                    }
                }
            }
            
            // Find a new parent - prefer same region if possible
            if let Ok(new_parent_id) = self.find_best_parent_geo_aware(&peer_id, &self.nodes, config) {
                if let Some(peer) = self.nodes.get_mut(&peer_id) {
                    peer.set_parent(new_parent_id);
                    
                    // Update depth based on new parent
                    if let Some(parent) = self.nodes.get(&new_parent_id) {
                        peer.update_depth(parent.depth + 1);
                    }
                }
                
                if let Some(parent) = self.nodes.get_mut(&new_parent_id) {
                    parent.add_child(peer_id, peer_region.as_ref());
                }
            }
        }
    }
    
    /// Find the best parent considering geographic location
    fn find_best_parent_geo_aware(
        &self, 
        peer_id: &PeerId, 
        tree: &HashMap<PeerId, RelayNode>,
        config: &TopologyConfig
    ) -> Result<PeerId, OverlayError> {
        let peer_node = tree.get(peer_id).ok_or_else(|| 
            OverlayError::TopologyError(format!("Peer not found: {}", peer_id))
        )?;
        
        let peer_region = peer_node.location.as_ref().map(|l| &l.region);
        
        // Score potential parents
        let mut candidates: Vec<(PeerId, f32)> = Vec::new();
        
        for (candidate_id, candidate) in tree {
            // Skip self
            if candidate_id == peer_id {
                continue;
            }
            
            // Skip if already full
            if !candidate.can_accept_children(config.max_fanout) {
                continue;
            }
            
            // Skip if this would create a loop
            if self.would_create_loop(peer_id, candidate_id) {
                continue;
            }
            
            // Calculate a score for this candidate
            let mut score = candidate.score;
            
            // Adjust score based on depth - prefer shallower nodes
            score -= (candidate.depth as f32) * 0.1;
            
            // Adjust score based on region match
            if let (Some(peer_region), Some(candidate_location)) = (peer_region, &candidate.location) {
                if &candidate_location.region == peer_region {
                    // Boost score for same region
                    score += config.relay_score_weights.region_weight;
                }
                
                // Adjust score based on geographic distance if both have coordinates
                if config.use_coordinates {
                    if let Some(distance) = peer_node.distance_to(candidate) {
                        // Lower score as distance increases
                        let distance_factor = (config.max_close_distance_km / (distance + 1.0)).min(1.0);
                        score += config.relay_score_weights.proximity_weight * distance_factor as f32;
                    }
                }
            }
            
            candidates.push((*candidate_id, score));
        }
        
        // Sort by score (highest first)
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        
        // Pick the best candidate
        if let Some((best_id, _)) = candidates.first() {
            Ok(*best_id)
        } else {
            // Fallback to the publisher if no suitable candidate found
            Ok(self.root)
        }
    }
    
    /// Check if adding a parent would create a loop in the tree
    fn would_create_loop(&self, peer_id: &PeerId, potential_parent_id: &PeerId) -> bool {
        // If we're trying to make the peer its own parent
        if peer_id == potential_parent_id {
            return true;
        }
        
        // Check if peer is already an ancestor of the potential parent
        let mut current = Some(*potential_parent_id);
        while let Some(id) = current {
            if &id == peer_id {
                return true;
            }
            
            current = self.nodes.get(&id).and_then(|n| n.parent);
        }
        
        false
    }
    
    /// Update location information for all nodes
    fn update_node_locations(&mut self, peers: &HashMap<PeerId, Peer>) {
        for (peer_id, node) in &mut self.nodes {
            if node.location.is_none() {
                if let Some(peer) = peers.get(peer_id) {
                    let location = GeoLocation::from_peer_info(&peer.info);
                    node.set_location(location);
                }
            }
        }
    }
    
    /// Calculate optimal distribution of nodes by region
    fn calculate_optimal_region_distribution(&self) -> HashMap<Region, usize> {
        let mut region_counts = HashMap::new();
        let mut total_with_region = 0;
        
        // Count nodes by region
        for node in self.nodes.values() {
            if let Some(location) = &node.location {
                *region_counts.entry(location.region.clone()).or_insert(0) += 1;
                total_with_region += 1;
            }
        }
        
        region_counts
    }
    
    /// Get the relay path from the root to a peer
    pub fn get_path_to_peer(&self, peer_id: &PeerId) -> Option<Vec<PeerId>> {
        if !self.nodes.contains_key(peer_id) {
            return None;
        }
        
        let mut path = Vec::new();
        let mut current = peer_id.clone();
        
        // Follow parent links up to the root
        while current != self.root {
            path.push(current.clone());
            
            if let Some(node) = self.nodes.get(&current) {
                if let Some(parent) = &node.parent {
                    current = parent.clone();
                } else {
                    break;
                }
            } else {
                return None;
            }
        }
        
        // Add the root
        path.push(self.root.clone());
        
        // Reverse to get path from root to peer
        path.reverse();
        
        Some(path)
    }
    
    /// Get the tree depth
    pub fn get_depth(&self) -> usize {
        self.nodes.values().map(|n| n.depth).max().unwrap_or(0)
    }
    
    /// Get the number of leaf nodes
    pub fn get_leaf_count(&self) -> usize {
        self.nodes.values().filter(|n| n.children.is_empty()).count()
    }
    
    /// Get the average fan-out
    pub fn get_avg_fanout(&self) -> f64 {
        let internal_nodes: Vec<_> = self.nodes.values().filter(|n| !n.children.is_empty()).collect();
        if internal_nodes.is_empty() {
            return 0.0;
        }
        
        let total_children: usize = internal_nodes.iter().map(|n| n.children.len()).sum();
        total_children as f64 / internal_nodes.len() as f64
    }
}

/// Calculate a relay score for a peer
fn calculate_relay_score(peer: &Peer, weights: &RelayScoreWeights) -> f32 {
    let mut score = 0.0;
    
    // Bandwidth score - higher bandwidth is better
    if let Some(bandwidth) = peer.info.bandwidth_capacity {
        // Normalize bandwidth to 0-1 scale (assuming 1 Gbps as max)
        let normalized_bandwidth = (bandwidth as f32 / 1_000_000_000.0).min(1.0);
        score += weights.bandwidth_weight * normalized_bandwidth;
    }
    
    // Latency score - lower latency is better
    if let Some(latency) = peer.info.latency_ms {
        // Normalize latency to 0-1 scale (0ms = 1, 500ms+ = 0)
        let normalized_latency = (1.0 - (latency as f32 / 500.0)).max(0.0);
        score += weights.latency_weight * normalized_latency;
    }
    
    // Stability score - based on connection history
    if let Some(last_connected) = peer.last_connected {
        // If recently connected, score higher
        let seconds_since = last_connected.elapsed().as_secs();
        if seconds_since < 3600 { // Less than an hour
            let staleness = (1.0 - (seconds_since as f32 / 3600.0)).max(0.0);
            score += weights.stability_weight * staleness;
        }
    }
    
    // Reduce score for peers that have had many connection attempts
    if peer.connection_attempts > 0 {
        let attempt_penalty = (peer.connection_attempts as f32 / 10.0).min(1.0);
        score -= weights.stability_weight * attempt_penalty * 0.5;
    }
    
    // Role-based score
    match peer.info.role {
        PeerRole::Relay => score += 0.2,
        PeerRole::Gateway => score += 0.15,
        PeerRole::HybridRelay => score += 0.1,
        _ => {}
    }
    
    score
}

/// Manager for multiple relay trees
pub struct TopologyManager {
    /// Configuration
    config: TopologyConfig,
    /// Streams and their relay trees
    trees: RwLock<HashMap<StreamId, RelayTree>>,
    /// Known peers
    peers: RwLock<HashMap<PeerId, Peer>>,
    /// Rebalance task handle
    rebalance_task: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

impl TopologyManager {
    /// Create a new topology manager
    pub fn new(config: TopologyConfig) -> Self {
        Self {
            config,
            trees: RwLock::new(HashMap::new()),
            peers: RwLock::new(HashMap::new()),
            rebalance_task: RwLock::new(None),
        }
    }
    
    /// Start the topology manager
    pub async fn start(&self) -> Result<(), OverlayError> {
        let config = self.config.clone();
        let trees_ref = self.trees.clone();
        let peers_ref = self.peers.clone();
        
        // Start rebalance task
        let rebalance_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.rebalance_interval);
            
            loop {
                interval.tick().await;
                
                // Rebalance all trees
                let mut trees = trees_ref.write().await;
                let peers = peers_ref.read().await;
                
                for (_, tree) in trees.iter_mut() {
                    tree.rebalance(&peers, &config);
                }
                
                debug!("Rebalanced {} relay trees", trees.len());
            }
        });
        
        // Store task handle
        let mut handle = self.rebalance_task.write().await;
        *handle = Some(rebalance_task);
        
        Ok(())
    }
    
    /// Stop the topology manager
    pub async fn stop(&self) -> Result<(), OverlayError> {
        // Abort rebalance task
        let mut handle = self.rebalance_task.write().await;
        if let Some(task) = handle.take() {
            task.abort();
        }
        
        Ok(())
    }
    
    /// Create a new stream with the given publisher
    pub async fn create_stream(&self, stream_id: StreamId, publisher_id: PeerId) -> Result<(), OverlayError> {
        let mut trees = self.trees.write().await;
        
        if trees.contains_key(&stream_id) {
            return Err(OverlayError::TopologyError(
                format!("Stream already exists: {:?}", stream_id)
            ));
        }
        
        // Create a new relay tree
        let tree = RelayTree::new(stream_id.clone(), publisher_id);
        trees.insert(stream_id, tree);
        
        Ok(())
    }
    
    /// Remove a stream
    pub async fn remove_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        let mut trees = self.trees.write().await;
        
        if trees.remove(stream_id).is_none() {
            return Err(OverlayError::TopologyError(
                format!("Stream not found: {:?}", stream_id)
            ));
        }
        
        Ok(())
    }
    
    /// Add a peer to a stream
    pub async fn add_peer_to_stream(
        &self, 
        stream_id: &StreamId, 
        peer_id: PeerId
    ) -> Result<(), OverlayError> {
        let mut trees = self.trees.write().await;
        
        if let Some(tree) = trees.get_mut(stream_id) {
            tree.add_peer(peer_id, &self.config)
        } else {
            Err(OverlayError::TopologyError(
                format!("Stream not found: {:?}", stream_id)
            ))
        }
    }
    
    /// Remove a peer from a stream
    pub async fn remove_peer_from_stream(
        &self, 
        stream_id: &StreamId, 
        peer_id: &PeerId
    ) -> Result<(), OverlayError> {
        let mut trees = self.trees.write().await;
        
        if let Some(tree) = trees.get_mut(stream_id) {
            tree.remove_peer(peer_id)
        } else {
            Err(OverlayError::TopologyError(
                format!("Stream not found: {:?}", stream_id)
            ))
        }
    }
    
    /// Update peer information
    pub async fn update_peer(&self, peer: Peer) -> Result<(), OverlayError> {
        let mut peers = self.peers.write().await;
        peers.insert(peer.id.clone(), peer);
        Ok(())
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
            Ok(tree.root.clone())
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
            tree.rebalance(&peers, &self.config);
            Ok(())
        } else {
            Err(OverlayError::TopologyError(format!("Stream not found: {:?}", stream_id)))
        }
    }
    
    /// Periodically rebalance all streams
    async fn periodic_rebalance(
        trees: Arc<RwLock<HashMap<StreamId, RelayTree>>>,
        peers: Arc<RwLock<HashMap<PeerId, Peer>>>,
        config: TopologyConfig
    ) {
        let mut interval = tokio::time::interval(config.rebalance_interval);
        
        loop {
            interval.tick().await;
            
            let stream_ids = {
                let trees = trees.read().await;
                trees.keys().cloned().collect::<Vec<_>>()
            };
            
            for stream_id in stream_ids {
                let mut trees = trees.write().await;
                let peers = peers.read().await;
                
                if let Some(tree) = trees.get_mut(&stream_id) {
                    tree.rebalance(&peers, &config);
                }
            }
        }
    }
} 