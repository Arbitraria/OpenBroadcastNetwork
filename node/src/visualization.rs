#[cfg(feature = "visualization")]
use colored::*;
use decentralized_stream_core::prelude::*;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::time::{Duration, Instant};
#[cfg(feature = "visualization")]
use term_table::row::Row;
#[cfg(feature = "visualization")]
use term_table::table_cell::{Alignment, TableCell};
#[cfg(feature = "visualization")]
use term_table::{Table, TableStyle};

/// Format bytes to human-readable string (e.g., KB, MB, GB)
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes < KB {
        format!("{} B", bytes)
    } else if bytes < MB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else if bytes < GB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes < TB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    }
}

/// Format duration to human-readable string
pub fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        let minutes = seconds / 60;
        let secs = seconds % 60;
        format!("{}m {}s", minutes, secs)
    } else {
        let hours = seconds / 3600;
        let minutes = (seconds % 3600) / 60;
        format!("{}h {}m", hours, minutes)
    }
}

/// Create a styled table for streams
pub fn create_stream_table(streams: &[StreamInfo]) -> String {
    let mut table = Table::new();
    table.style = TableStyle::extended();

    // Add header
    table.add_row(Row::new(vec![
        TableCell::new_with_alignment("ID", 1, Alignment::Left),
        TableCell::new_with_alignment("Name", 1, Alignment::Left),
        TableCell::new_with_alignment("Publisher", 1, Alignment::Left),
        TableCell::new_with_alignment("Subscribers", 1, Alignment::Right),
        TableCell::new_with_alignment("Bitrate", 1, Alignment::Right),
        TableCell::new_with_alignment("Age", 1, Alignment::Right),
        TableCell::new_with_alignment("Relays", 1, Alignment::Right),
        TableCell::new_with_alignment("Depth", 1, Alignment::Right),
    ]));

    // Add stream rows
    for stream in streams {
        let short_id = if stream.id.len() > 10 {
            format!("{}...", &stream.id[0..10])
        } else {
            stream.id.clone()
        };
        
        let short_publisher = if stream.publisher.len() > 10 {
            format!("{}...", &stream.publisher[0..10])
        } else {
            stream.publisher.clone()
        };
        
        let bitrate_str = format_bytes(stream.bitrate);
        let age_str = format_duration(stream.age);
        
        table.add_row(Row::new(vec![
            TableCell::new_with_alignment(short_id, 1, Alignment::Left),
            TableCell::new_with_alignment(stream.name.clone(), 1, Alignment::Left),
            TableCell::new_with_alignment(short_publisher, 1, Alignment::Left),
            TableCell::new_with_alignment(stream.subscribers.to_string(), 1, Alignment::Right),
            TableCell::new_with_alignment(bitrate_str + "/s", 1, Alignment::Right),
            TableCell::new_with_alignment(age_str, 1, Alignment::Right),
            TableCell::new_with_alignment(stream.relays.to_string(), 1, Alignment::Right),
            TableCell::new_with_alignment(stream.max_depth.to_string(), 1, Alignment::Right),
        ]));
    }

    table.render()
}

/// Create a styled table for peers
pub fn create_peer_table(peers: &[PeerDisplayInfo], show_location: bool) -> String {
    let mut table = Table::new();
    table.style = TableStyle::extended();

    // Add header
    let mut header = vec![
        TableCell::new_with_alignment("ID", 1, Alignment::Left),
        TableCell::new_with_alignment("Role", 1, Alignment::Left),
        TableCell::new_with_alignment("Status", 1, Alignment::Left),
        TableCell::new_with_alignment("Latency", 1, Alignment::Right),
        TableCell::new_with_alignment("Streams", 1, Alignment::Right),
    ];
    
    if show_location {
        header.push(TableCell::new_with_alignment("Location", 1, Alignment::Left));
    }
    
    header.push(TableCell::new_with_alignment("Bandwidth", 1, Alignment::Right));
    header.push(TableCell::new_with_alignment("Score", 1, Alignment::Right));
    
    table.add_row(Row::new(header));

    // Add peer rows
    for peer in peers {
        let short_id = if peer.id.len() > 10 {
            format!("{}...", &peer.id[0..10])
        } else {
            peer.id.clone()
        };
        
        let role_str = match peer.role {
            PeerRole::Publisher => "Publisher".green(),
            PeerRole::Consumer => "Consumer".blue(),
            PeerRole::Relay => "Relay".yellow(),
            PeerRole::HybridRelay => "Hybrid".magenta(),
            PeerRole::Gateway => "Gateway".cyan(),
            _ => "Unknown".normal(),
        };
        
        let status_str = match peer.status.as_str() {
            "Connected" => "Connected".green(),
            "Disconnected" => "Disconnected".red(),
            "Connecting" => "Connecting".yellow(),
            _ => peer.status.normal(),
        };
        
        let latency_str = peer.latency_ms.map_or("N/A".to_string(), |ms| {
            format!("{} ms", ms)
        });
        
        let streams_str = peer.streams.len().to_string();
        
        let mut row = vec![
            TableCell::new_with_alignment(short_id, 1, Alignment::Left),
            TableCell::new_with_alignment(role_str, 1, Alignment::Left),
            TableCell::new_with_alignment(status_str, 1, Alignment::Left),
            TableCell::new_with_alignment(latency_str, 1, Alignment::Right),
            TableCell::new_with_alignment(streams_str, 1, Alignment::Right),
        ];
        
        if show_location {
            let location_str = peer.location.as_ref().map_or("Unknown".to_string(), |loc| {
                format!("{}, {}", loc.country_code, loc.region)
            });
            row.push(TableCell::new_with_alignment(location_str, 1, Alignment::Left));
        }
        
        let bandwidth_str = format_bytes(peer.bandwidth_bps);
        let score_str = format!("{:.2}", peer.relay_score);
        
        row.push(TableCell::new_with_alignment(bandwidth_str + "/s", 1, Alignment::Right));
        row.push(TableCell::new_with_alignment(score_str, 1, Alignment::Right));
        
        table.add_row(Row::new(row));
    }

    table.render()
}

/// Stream information for display
#[derive(Debug, Clone)]
pub struct StreamInfo {
    /// Stream ID
    pub id: String,
    /// Stream name
    pub name: String,
    /// Publisher ID
    pub publisher: String,
    /// Number of subscribers
    pub subscribers: usize,
    /// Current bitrate
    pub bitrate: u64,
    /// Stream age
    pub age: Duration,
    /// Number of relays
    pub relays: usize,
    /// Maximum depth
    pub max_depth: u32,
}

/// Peer information for display
#[derive(Debug, Clone)]
pub struct PeerDisplayInfo {
    /// Peer ID
    pub id: String,
    /// Addresses
    pub addresses: Vec<String>,
    /// Peer role
    pub role: PeerRole,
    /// Connection status
    pub status: String,
    /// Latency in ms
    pub latency_ms: Option<u32>,
    /// Last seen
    pub last_seen: Option<Instant>,
    /// Geographic location
    pub location: Option<GeoLocation>,
    /// Streams this peer is involved with
    pub streams: HashSet<String>,
    /// Relay score
    pub relay_score: f32,
    /// Relay hop count
    pub relay_hops: u32,
    /// Bandwidth metrics (bytes/sec)
    pub bandwidth_bps: u64,
}

impl From<PeerInfo> for PeerDisplayInfo {
    fn from(info: PeerInfo) -> Self {
        Self {
            id: info.id.short_id(),
            role: info.role,
            addresses: info.addresses,
            region: info.region,
            latency_ms: info.latency_ms.map(|ms| ms as u32),
            bandwidth: info.bandwidth_capacity,
            connections: 0, // Would be filled in by the caller
            status: "Connected".to_string(),
            last_seen: None,
            location: info.geo_location,
            streams: HashSet::new(),
            relay_score: 0.0,
            relay_hops: 0,
            bandwidth_bps: 0,
        }
    }
}

/// Generate an ASCII visualization of the network tree
pub fn visualize_tree(
    peers: &HashMap<PeerId, PeerInfo>,
    root_id: &PeerId,
    connections: &HashMap<PeerId, Vec<PeerId>>,
) -> String {
    let mut result = String::new();
    let root = peers.get(root_id).unwrap();
    
    result.push_str(&format!("{} ({})\n", root_id, format!("{:?}", root.role).green()));
    
    if let Some(children) = connections.get(root_id) {
        for (i, child_id) in children.iter().enumerate() {
            let is_last = i == children.len() - 1;
            let prefix = if is_last { "└── " } else { "├── " };
            
            result.push_str(&format!("{}{}", prefix, visualize_tree_node(
                peers, 
                child_id, 
                connections, 
                if is_last { "    " } else { "│   " },
            )));
        }
    }
    
    result
}

fn visualize_tree_node(
    peers: &HashMap<PeerId, PeerInfo>,
    node_id: &PeerId,
    connections: &HashMap<PeerId, Vec<PeerId>>,
    prefix: &str,
) -> String {
    let mut result = String::new();
    let node = peers.get(node_id).unwrap();
    
    // Display node
    let role_str = match node.role {
        PeerRole::Publisher => format!("{:?}", node.role).green(),
        PeerRole::Relay => format!("{:?}", node.role).blue(),
        PeerRole::HybridRelay => format!("{:?}", node.role).yellow(),
        PeerRole::Consumer => format!("{:?}", node.role).cyan(),
        _ => format!("{:?}", node.role).white(),
    };
    
    result.push_str(&format!("{} ({})\n", node_id, role_str));
    
    // Display children
    if let Some(children) = connections.get(node_id) {
        for (i, child_id) in children.iter().enumerate() {
            let is_last = i == children.len() - 1;
            let child_prefix = if is_last { 
                format!("{}└── ", prefix) 
            } else { 
                format!("{}├── ", prefix) 
            };
            
            let new_prefix = if is_last {
                format!("{}    ", prefix)
            } else {
                format!("{}│   ", prefix)
            };
            
            result.push_str(&format!("{}{}", child_prefix, visualize_tree_node(
                peers, 
                child_id, 
                connections, 
                &new_prefix,
            )));
        }
    }
    
    result
}

/// Convert DOT format to ASCII if graphviz is not available
pub fn dot_to_ascii(dot: &str) -> String {
    // This is a very simple conversion that tries to extract nodes and edges
    let mut result = String::new();
    let mut nodes = HashMap::new();
    let mut edges = Vec::new();
    
    // Extract nodes and edges
    for line in dot.lines() {
        let line = line.trim();
        if line.contains("->") {
            let parts: Vec<&str> = line.split("->").collect();
            if parts.len() == 2 {
                let from = parts[0].trim().trim_matches('"');
                let to = parts[1].split('[').next().unwrap_or("").trim().trim_matches('"');
                edges.push((from.to_string(), to.to_string()));
            }
        } else if line.contains("label=") {
            let id = line.split('[').next().unwrap_or("").trim().trim_matches('"');
            let label = if let Some(start) = line.find("label=\"") {
                let start = start + 7;
                if let Some(end) = line[start..].find('"') {
                    line[start..start+end].to_string()
                } else {
                    id.to_string()
                }
            } else {
                id.to_string()
            };
            nodes.insert(id.to_string(), label);
        }
    }
    
    // Build a simple adjacency list
    let mut adjacency = HashMap::new();
    for (from, to) in edges {
        adjacency.entry(from.clone())
            .or_insert_with(Vec::new)
            .push(to.clone());
    }
    
    // Find root nodes (nodes with no incoming edges)
    let mut has_incoming = HashSet::new();
    for (_, targets) in &adjacency {
        for target in targets {
            has_incoming.insert(target.clone());
        }
    }
    
    let roots: Vec<_> = nodes.keys()
        .filter(|n| !has_incoming.contains(*n))
        .cloned()
        .collect();
    
    // Generate ASCII tree for each root
    for root in roots {
        result.push_str(&format!("{}\n", nodes.get(&root).unwrap_or(&root)));
        if let Some(children) = adjacency.get(&root) {
            for (i, child) in children.iter().enumerate() {
                let is_last = i == children.len() - 1;
                let prefix = if is_last { "└── " } else { "├── " };
                result.push_str(&format!("{}{}\n", prefix, nodes.get(child).unwrap_or(child)));
            }
        }
        result.push('\n');
    }
    
    result
}

/// Tree node for visualization
#[derive(Debug, Clone)]
pub struct TreeNode {
    /// Node ID
    pub id: String,
    /// Node role
    pub role: PeerRole,
    /// Geographic location
    pub location: Option<GeoLocation>,
    /// Latency in ms
    pub latency_ms: Option<u32>,
    /// Children nodes
    pub children: Vec<TreeNode>,
    /// Depth in the tree
    pub depth: u32,
}

/// Visualize a tree in text format
pub fn visualize_tree(root: &TreeNode, writer: &mut dyn Write) -> std::io::Result<()> {
    visualize_tree_node(root, writer, 0, &mut vec![false])
}

/// Visualize a tree node recursively
fn visualize_tree_node(
    node: &TreeNode,
    writer: &mut dyn Write,
    depth: usize,
    is_last: &mut Vec<bool>,
) -> std::io::Result<()> {
    // Print indentation
    for i in 0..depth {
        if i == depth - 1 {
            if is_last[i] {
                write!(writer, "└── ")?;
            } else {
                write!(writer, "├── ")?;
            }
        } else {
            if is_last[i] {
                write!(writer, "    ")?;
            } else {
                write!(writer, "│   ")?;
            }
        }
    }
    
    // Print node
    let role_str = match node.role {
        PeerRole::Publisher => "Publisher".green(),
        PeerRole::Consumer => "Consumer".blue(),
        PeerRole::Relay => "Relay".yellow(),
        PeerRole::HybridRelay => "Hybrid".magenta(),
        PeerRole::Gateway => "Gateway".cyan(),
        _ => "Unknown".normal(),
    };
    
    let location_str = node.location.as_ref().map_or("".to_string(), |loc| {
        format!(" ({}, {})", loc.country_code, loc.region)
    });
    
    let latency_str = node.latency_ms.map_or("".to_string(), |ms| {
        format!(" [{}ms]", ms)
    });
    
    writeln!(
        writer,
        "{} {} - {}{}{}",
        role_str,
        node.depth,
        node.id,
        location_str,
        latency_str
    )?;
    
    // Process children
    for (i, child) in node.children.iter().enumerate() {
        let is_child_last = i == node.children.len() - 1;
        
        if depth + 1 >= is_last.len() {
            is_last.push(is_child_last);
        } else {
            is_last[depth + 1] = is_child_last;
        }
        
        visualize_tree_node(child, writer, depth + 1, is_last)?;
    }
    
    Ok(())
}

/// Generate GraphViz DOT format for network visualization
pub fn generate_dot_graph(
    peers: &HashMap<PeerId, PeerInfo>,
    stream_id: Option<&StreamId>,
) -> String {
    let mut dot = String::new();
    dot.push_str("digraph network {\n");
    dot.push_str("  node [shape=box, style=filled];\n");
    
    // Define node colors by role
    dot.push_str("  node [colorscheme=set19];\n");
    
    // Add nodes
    for (id, info) in peers {
        let label = format!("{}\\n{}", id.short_id(), info.role.to_string());
        
        let color = match info.role {
            PeerRole::Publisher => "1", // red
            PeerRole::Consumer => "2",  // blue
            PeerRole::Relay => "3",     // green
            PeerRole::HybridRelay => "4", // purple
            PeerRole::Gateway => "5",    // orange
            _ => "9", // brown
        };
        
        let location = info.geo_location.as_ref().map_or("Unknown".to_string(), |loc| {
            format!("{}, {}", loc.country_code, loc.region)
        });
        
        dot.push_str(&format!(
            "  \"{}\" [label=\"{}\n{}\", fillcolor={}];\n",
            id.short_id(),
            label,
            location,
            color
        ));
    }
    
    // Add edges
    for (id, info) in peers {
        // Add connections
        for conn in &info.connections {
            // Skip if this is a stream-specific view and the connection isn't related
            if let Some(stream) = stream_id {
                if !conn.streams.contains(stream) {
                    continue;
                }
            }
            
            // Add edge with latency
            let latency = conn.latency_ms.unwrap_or(0);
            dot.push_str(&format!(
                "  \"{}\" -> \"{}\" [label=\"{}ms\", penwidth={}];\n",
                id.short_id(),
                conn.peer_id.short_id(),
                latency,
                2.0
            ));
        }
    }
    
    dot.push_str("}\n");
    dot
}

/// Builds a tree structure for a stream from peer information
pub fn build_stream_tree(
    stream_id: &StreamId,
    peers: &HashMap<PeerId, PeerInfo>,
    publisher_id: &PeerId,
) -> Option<TreeNode> {
    // Find the publisher
    let publisher = match peers.get(publisher_id) {
        Some(peer) => peer,
        None => return None,
    };
    
    // Create set of visited peers to avoid cycles
    let mut visited = HashSet::new();
    visited.insert(publisher_id.clone());
    
    // Create root node
    let mut root = TreeNode {
        id: publisher_id.short_id(),
        role: publisher.role,
        location: publisher.geo_location.clone(),
        latency_ms: None,
        children: Vec::new(),
        depth: 0,
    };
    
    // Build the tree recursively
    build_tree_node(
        publisher_id,
        &mut root,
        peers,
        stream_id,
        &mut visited,
        0,
    );
    
    Some(root)
}

/// Recursively builds a tree node and its children
fn build_tree_node(
    peer_id: &PeerId,
    node: &mut TreeNode,
    peers: &HashMap<PeerId, PeerInfo>,
    stream_id: &StreamId,
    visited: &mut HashSet<PeerId>,
    depth: u32,
) {
    // Find direct consumers/relays of this peer for this stream
    let peer = match peers.get(peer_id) {
        Some(peer) => peer,
        None => return,
    };
    
    for conn in &peer.connections {
        // Check if this connection is for the given stream
        if !conn.streams.contains(stream_id) {
            continue;
        }
        
        // Check if we've already visited this peer
        if visited.contains(&conn.peer_id) {
            continue;
        }
        
        // Mark as visited
        visited.insert(conn.peer_id.clone());
        
        // Create child node
        let child_peer = match peers.get(&conn.peer_id) {
            Some(peer) => peer,
            None => continue,
        };
        
        let mut child = TreeNode {
            id: conn.peer_id.short_id(),
            role: child_peer.role,
            location: child_peer.geo_location.clone(),
            latency_ms: conn.latency_ms,
            children: Vec::new(),
            depth: depth + 1,
        };
        
        // Recursively build children
        build_tree_node(
            &conn.peer_id,
            &mut child,
            peers,
            stream_id,
            visited,
            depth + 1,
        );
        
        node.children.push(child);
    }
    
    // Sort children by role (publishers first, then relays, then consumers)
    node.children.sort_by(|a, b| {
        let role_a = match a.role {
            PeerRole::Publisher => 0,
            PeerRole::HybridRelay => 1,
            PeerRole::Relay => 2,
            PeerRole::Gateway => 3,
            PeerRole::Consumer => 4,
            _ => 5,
        };
        
        let role_b = match b.role {
            PeerRole::Publisher => 0,
            PeerRole::HybridRelay => 1,
            PeerRole::Relay => 2,
            PeerRole::Gateway => 3,
            PeerRole::Consumer => 4,
            _ => 5,
        };
        
        role_a.cmp(&role_b)
    });
} 