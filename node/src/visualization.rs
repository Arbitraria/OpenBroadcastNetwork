#[cfg(feature = "visualization")]
use colored::*;
use decentralized_stream_core::prelude::*;
use std::collections::{HashMap, HashSet};
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
pub fn format_duration(seconds: u64) -> String {
    let days = seconds / (24 * 3600);
    let hours = (seconds % (24 * 3600)) / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;

    if days > 0 {
        format!("{}d {}h {}m {}s", days, hours, minutes, seconds)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}

/// Create a styled table for streams
pub fn create_stream_table(streams: &[StreamInfo]) -> String {
    let mut table = Table::new();
    table.style = TableStyle::extended();

    // Add header
    table.add_row(Row::new(vec![
        TableCell::new_with_alignment("ID", 1, Alignment::Left),
        TableCell::new_with_alignment("Publisher", 1, Alignment::Left),
        TableCell::new_with_alignment("Subscribers", 1, Alignment::Center),
        TableCell::new_with_alignment("Bandwidth", 1, Alignment::Right),
        TableCell::new_with_alignment("Age", 1, Alignment::Right),
    ]));

    // Add stream rows
    for stream in streams {
        let bandwidth = format_bytes(stream.bandwidth_bps);
        let age = format_duration(stream.age_seconds);
        table.add_row(Row::new(vec![
            TableCell::new_with_alignment(stream.id.to_string(), 1, Alignment::Left),
            TableCell::new_with_alignment(stream.publisher.to_string(), 1, Alignment::Left),
            TableCell::new_with_alignment(stream.subscribers.to_string(), 1, Alignment::Center),
            TableCell::new_with_alignment(bandwidth, 1, Alignment::Right),
            TableCell::new_with_alignment(age, 1, Alignment::Right),
        ]));
    }

    table.render()
}

/// Create a styled table for peers
pub fn create_peer_table(peers: &[PeerDisplayInfo]) -> String {
    let mut table = Table::new();
    table.style = TableStyle::extended();

    // Add header
    table.add_row(Row::new(vec![
        TableCell::new_with_alignment("ID", 1, Alignment::Left),
        TableCell::new_with_alignment("Role", 1, Alignment::Left),
        TableCell::new_with_alignment("Address", 1, Alignment::Left),
        TableCell::new_with_alignment("Region", 1, Alignment::Center),
        TableCell::new_with_alignment("Latency", 1, Alignment::Right),
        TableCell::new_with_alignment("Bandwidth", 1, Alignment::Right),
        TableCell::new_with_alignment("Connections", 1, Alignment::Right),
    ]));

    // Add peer rows
    for peer in peers {
        let role_str = format!("{:?}", peer.role);
        let role_colored = match peer.role {
            PeerRole::Publisher => role_str.green(),
            PeerRole::Relay => role_str.blue(),
            PeerRole::HybridRelay => role_str.yellow(),
            PeerRole::Consumer => role_str.cyan(),
            _ => role_str.white(),
        };

        let address = peer.addresses.first().cloned().unwrap_or_default();
        let latency = peer
            .latency_ms
            .map(|ms| format!("{}ms", ms))
            .unwrap_or_else(|| "N/A".to_string());
        let bandwidth = peer
            .bandwidth
            .map(format_bytes)
            .unwrap_or_else(|| "N/A".to_string());

        table.add_row(Row::new(vec![
            TableCell::new_with_alignment(
                peer.id.to_string().chars().take(10).collect::<String>() + "...",
                1,
                Alignment::Left,
            ),
            TableCell::new_with_alignment(role_colored.to_string(), 1, Alignment::Left),
            TableCell::new_with_alignment(address, 1, Alignment::Left),
            TableCell::new_with_alignment(peer.region.clone().unwrap_or_default(), 1, Alignment::Center),
            TableCell::new_with_alignment(latency, 1, Alignment::Right),
            TableCell::new_with_alignment(bandwidth, 1, Alignment::Right),
            TableCell::new_with_alignment(peer.connections.to_string(), 1, Alignment::Right),
        ]));
    }

    table.render()
}

/// Stream information for display
#[derive(Debug, Clone)]
pub struct StreamInfo {
    /// Stream ID
    pub id: StreamId,
    /// Publisher ID
    pub publisher: PeerId,
    /// Number of subscribers
    pub subscribers: usize,
    /// Bandwidth usage
    pub bandwidth_bps: u64,
    /// Stream age in seconds
    pub age_seconds: u64,
}

/// Peer information for display
#[derive(Debug, Clone)]
pub struct PeerDisplayInfo {
    /// Peer ID
    pub id: PeerId,
    /// Role in the network
    pub role: PeerRole,
    /// Addresses (for display, usually just the first one is shown)
    pub addresses: Vec<String>,
    /// Region
    pub region: Option<String>,
    /// Latency in milliseconds
    pub latency_ms: Option<u64>,
    /// Bandwidth capability
    pub bandwidth: Option<u64>,
    /// Number of connections
    pub connections: usize,
}

impl From<PeerInfo> for PeerDisplayInfo {
    fn from(info: PeerInfo) -> Self {
        Self {
            id: info.id,
            role: info.role,
            addresses: info.addresses,
            region: info.region,
            latency_ms: info.latency_ms,
            bandwidth: info.bandwidth_capacity,
            connections: 0, // Would be filled in by the caller
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