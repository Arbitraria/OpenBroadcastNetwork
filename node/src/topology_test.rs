use std::collections::{HashMap, HashSet};
use std::time::Instant;

// Stream identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamId(String);

impl StreamId {
    fn new(id: &str) -> Self {
        Self(id.to_string())
    }
}

// Peer identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PeerId(u32);

impl PeerId {
    fn new(id: u32) -> Self {
        Self(id)
    }
}

// Geographic region codes
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Region {
    NorthAmerica,
    SouthAmerica,
    Europe,
    Asia,
    Africa,
    Oceania,
    Unknown,
}

// Geographic location information
#[derive(Debug, Clone)]
struct GeoLocation {
    latitude: f64,
    longitude: f64,
    country: String,
    region: Region,
}

impl GeoLocation {
    fn new(latitude: f64, longitude: f64, country: String) -> Self {
        let region = match country.as_str() {
            "US" | "CA" | "MX" => Region::NorthAmerica,
            "BR" | "AR" | "CL" => Region::SouthAmerica,
            "GB" | "DE" | "FR" | "IT" => Region::Europe,
            "CN" | "JP" | "KR" | "IN" => Region::Asia,
            "ZA" | "NG" | "EG" => Region::Africa,
            "AU" | "NZ" => Region::Oceania,
            _ => Region::Unknown,
        };
        
        Self { latitude, longitude, country, region }
    }
    
    fn distance_to(&self, other: &GeoLocation) -> f64 {
        const EARTH_RADIUS: f64 = 6371.0; // km
        
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
    
    fn is_same_region(&self, other: &GeoLocation) -> bool {
        self.region == other.region
    }
}

// A node in the relay tree
#[derive(Debug, Clone)]
struct RelayNode {
    peer_id: PeerId,
    parent: Option<PeerId>,
    children: HashSet<PeerId>,
    depth: usize,
    last_updated: Instant,
    score: f32,
    is_publisher: bool,
    location: Option<GeoLocation>,
    region_connections: HashMap<Region, usize>,
}

impl RelayNode {
    fn new(peer_id: PeerId) -> Self {
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
    
    fn new_publisher(peer_id: PeerId) -> Self {
        let mut node = Self::new(peer_id);
        node.is_publisher = true;
        node
    }
    
    fn set_location(&mut self, location: GeoLocation) {
        self.location = Some(location);
    }
    
    fn add_child(&mut self, child_id: PeerId) {
        self.children.insert(child_id);
        self.last_updated = Instant::now();
    }
    
    fn remove_child(&mut self, child_id: &PeerId) {
        self.children.remove(child_id);
        self.last_updated = Instant::now();
    }
    
    fn set_parent(&mut self, parent_id: PeerId) {
        self.parent = Some(parent_id);
        self.last_updated = Instant::now();
    }
    
    fn distance_to(&self, other: &RelayNode) -> Option<f64> {
        match (&self.location, &other.location) {
            (Some(loc1), Some(loc2)) => Some(loc1.distance_to(loc2)),
            _ => None,
        }
    }
}

// A relay tree for a stream
struct RelayTree {
    stream_id: StreamId,
    root: PeerId,
    nodes: HashMap<PeerId, RelayNode>,
}

impl RelayTree {
    fn new(stream_id: StreamId, publisher_id: PeerId) -> Self {
        let mut nodes = HashMap::new();
        
        // Create the root node
        let root_node = RelayNode::new_publisher(publisher_id);
        nodes.insert(publisher_id, root_node);
        
        Self {
            stream_id,
            root: publisher_id,
            nodes,
        }
    }
    
    fn add_peer(&mut self, peer_id: PeerId, max_fanout: usize) -> Result<(), String> {
        if self.nodes.contains_key(&peer_id) {
            return Err(format!("Peer already in tree: {:?}", peer_id));
        }
        
        // Find a parent that can accept more children
        let parent_id = self.find_parent_with_space(max_fanout)
            .ok_or_else(|| "No available parent".to_string())?;
        
        // Create new node
        let mut new_node = RelayNode::new(peer_id);
        new_node.set_parent(parent_id);
        
        // Update parent's children
        self.nodes.get_mut(&parent_id).unwrap().add_child(peer_id);
        
        // Calculate depth
        let parent_depth = self.nodes[&parent_id].depth;
        new_node.depth = parent_depth + 1;
        
        // Add node to tree
        self.nodes.insert(peer_id, new_node);
        
        Ok(())
    }
    
    fn find_parent_with_space(&self, max_fanout: usize) -> Option<PeerId> {
        for (id, node) in &self.nodes {
            if node.children.len() < max_fanout {
                return Some(*id);
            }
        }
        None
    }
    
    // Rebalance the tree based on geographic proximity
    fn rebalance_geo_aware(&mut self, max_fanout: usize) {
        println!("Rebalancing tree with geo-awareness");
        
        // Skip small trees
        if self.nodes.len() < 3 {
            return;
        }
        
        // Find non-optimal connections (e.g., peers in same region with different parents)
        let mut region_map: HashMap<Region, Vec<PeerId>> = HashMap::new();
        
        // Group nodes by region
        for (id, node) in &self.nodes {
            if let Some(location) = &node.location {
                region_map.entry(location.region.clone())
                    .or_insert_with(Vec::new)
                    .push(*id);
            }
        }
        
        // For each region with multiple nodes, try to organize them together
        for (region, peers) in &region_map {
            println!("Organizing region {:?} with {} peers", region, peers.len());
            
            if peers.len() < 2 {
                continue;
            }
            
            // Find a potential parent in this region
            let parent_candidates: Vec<_> = peers.iter()
                .filter(|p| {
                    let node = &self.nodes[p];
                    !node.is_publisher && node.children.len() < max_fanout
                })
                .collect();
                
            if parent_candidates.is_empty() {
                continue;
            }
            
            let region_parent = *parent_candidates[0];
            
            // Try to move other peers in this region under this parent
            for peer_id in peers {
                let node = &self.nodes[peer_id];
                
                // Skip if this is the selected parent, the publisher, or already has this parent
                if *peer_id == region_parent || 
                   node.is_publisher || 
                   node.parent == Some(region_parent) {
                    continue;
                }
                
                // Move this peer under the region parent, if it doesn't create a loop
                if !self.would_create_loop(*peer_id, region_parent) {
                    println!("  Moving peer {:?} under region parent {:?}", peer_id, region_parent);
                    
                    // Remove from current parent
                    if let Some(old_parent) = self.nodes[peer_id].parent {
                        self.nodes.get_mut(&old_parent).unwrap().remove_child(peer_id);
                    }
                    
                    // Add to new parent
                    self.nodes.get_mut(&region_parent).unwrap().add_child(*peer_id);
                    
                    // Update node's parent
                    self.nodes.get_mut(peer_id).unwrap().set_parent(region_parent);
                }
            }
        }
    }
    
    fn would_create_loop(&self, peer_id: PeerId, potential_parent_id: PeerId) -> bool {
        // Check if peer is already an ancestor of the potential parent
        let mut current = Some(potential_parent_id);
        while let Some(id) = current {
            if id == peer_id {
                return true;
            }
            
            current = self.nodes[&id].parent;
        }
        
        false
    }
    
    fn print_tree(&self) {
        println!("Tree for stream {:?}:", self.stream_id);
        self.print_node(self.root, 0);
    }
    
    fn print_node(&self, node_id: PeerId, indent: usize) {
        let node = &self.nodes[&node_id];
        
        let role = if node.is_publisher { "Publisher" } else { "Relay" };
        let region = node.location.as_ref()
            .map(|l| format!("{:?}", l.region))
            .unwrap_or_else(|| "Unknown".to_string());
            
        println!("{:indent$}{:?} ({}, {})", "", node_id, role, region, indent=indent*2);
        
        for child_id in &node.children {
            self.print_node(*child_id, indent + 1);
        }
    }
}

fn main() {
    println!("=== Topology Rebalancing Test ===\n");
    
    // Create a stream
    let stream_id = StreamId::new("test-stream");
    let publisher_id = PeerId::new(1);
    
    let mut tree = RelayTree::new(stream_id, publisher_id);
    
    // Configure publisher location
    let mut publisher = tree.nodes.get_mut(&publisher_id).unwrap();
    publisher.set_location(GeoLocation::new(37.7749, -122.4194, "US".to_string()));
    
    // Add peers with locations
    let locations = [
        (2, GeoLocation::new(34.0522, -118.2437, "US".to_string())),  // Los Angeles (North America)
        (3, GeoLocation::new(40.7128, -74.0060, "US".to_string())),   // New York (North America)
        (4, GeoLocation::new(51.5074, -0.1278, "GB".to_string())),    // London (Europe)
        (5, GeoLocation::new(48.8566, 2.3522, "FR".to_string())),     // Paris (Europe)
        (6, GeoLocation::new(35.6762, 139.6503, "JP".to_string())),   // Tokyo (Asia)
        (7, GeoLocation::new(22.3193, 114.1694, "CN".to_string())),   // Hong Kong (Asia)
        (8, GeoLocation::new(19.4326, -99.1332, "MX".to_string())),   // Mexico City (North America)
    ];
    
    // Add peers in random order
    let max_fanout = 3;
    for (id, location) in locations.iter() {
        if let Err(e) = tree.add_peer(PeerId::new(*id), max_fanout) {
            println!("Error adding peer {}: {}", id, e);
            continue;
        }
        
        // Set location
        tree.nodes.get_mut(&PeerId::new(*id)).unwrap().set_location(location.clone());
    }
    
    // Print initial tree
    println!("Initial tree structure:");
    tree.print_tree();
    
    // Rebalance the tree
    println!("\nRebalancing tree...");
    tree.rebalance_geo_aware(max_fanout);
    
    // Print final tree
    println!("\nTree structure after rebalancing:");
    tree.print_tree();
    
    println!("\n=== Topology Test Complete ===");
} 