use decentralized_stream_core::overlay::peer::{PeerId, PeerInfo, PeerRole};
use decentralized_stream_core::overlay::topology::Region;
use decentralized_stream_node::visualization::{
    PeerDisplayInfo, StreamInfo, create_peer_table, create_stream_table, visualize_tree
};
use std::collections::HashMap;

fn main() {
    println!("Testing CLI visualization functionality");
    
    // Create a set of peers for display
    let mut peers = Vec::new();
    
    for i in 1..6 {
        let peer_id = PeerId::from(format!("peer-{}", i));
        let role = match i {
            1 => PeerRole::Publisher,
            2 | 3 => PeerRole::Relay,
            4 => PeerRole::HybridRelay,
            _ => PeerRole::Consumer,
        };
        
        let region = match i % 3 {
            0 => Some("north_america".to_string()),
            1 => Some("europe".to_string()),
            _ => Some("asia".to_string()),
        };
        
        let peer = PeerDisplayInfo {
            id: peer_id,
            role,
            addresses: vec![format!("192.168.1.{}:8000", i)],
            region,
            latency_ms: Some(i * 10),
            bandwidth: Some(10_000_000 / i),
            connections: i,
        };
        
        peers.push(peer);
    }
    
    // Create a set of streams for display
    let mut streams = Vec::new();
    
    for i in 1..4 {
        let stream = StreamInfo {
            id: format!("stream-{}", i).into(),
            publisher: format!("peer-1").into(),
            subscribers: i * 5,
            bandwidth_bps: i * 1_000_000,
            age_seconds: i * 60,
        };
        
        streams.push(stream);
    }
    
    // Display peer table
    println!("Peer Table:");
    println!("{}", create_peer_table(&peers));
    
    // Display stream table
    println!("\nStream Table:");
    println!("{}", create_stream_table(&streams));
    
    // Create mock data for topology visualization
    let mut peer_map = HashMap::new();
    let mut connection_map = HashMap::new();
    
    for peer in &peers {
        let info = PeerInfo {
            id: peer.id.clone(),
            role: peer.role,
            addresses: peer.addresses.clone(),
            protocols: vec!["stream/1.0.0".to_string()],
            metadata: HashMap::new(),
            latency_ms: peer.latency_ms,
            last_seen: 0,
            region: peer.region.clone(),
            bandwidth_capacity: peer.bandwidth,
        };
        
        peer_map.insert(peer.id.clone(), info);
    }
    
    // Set up connections for tree visualization
    let root_id = peers[0].id.clone();
    let mut connections = Vec::new();
    
    for i in 1..3 {
        connections.push(peers[i].id.clone());
    }
    connection_map.insert(root_id.clone(), connections.clone());
    
    // Second level connections
    for i in 1..3 {
        let parent_id = peers[i].id.clone();
        let mut children = Vec::new();
        if i+2 < peers.len() {
            children.push(peers[i+2].id.clone());
        }
        connection_map.insert(parent_id, children);
    }
    
    // Display tree visualization
    println!("\nTree Visualization:");
    println!("{}", visualize_tree(&peer_map, &root_id, &connection_map));
} 