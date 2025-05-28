//! Tests for overlay implementations
//!
//! This module contains tests for the tree, mesh, and hybrid overlay implementations.

#[cfg(test)]
mod tests {
    use crate::prelude::*;
    // Fix import path for tree and mesh modules
    use crate::overlay::tree::{StreamTree, TreeNode, TreeStats};
    use crate::overlay::mesh::{StreamMesh, MeshNode, MeshStats};
    use std::collections::HashSet;
    use std::str::FromStr;
    use libp2p::PeerId;
    
    // Re-export for backward compatibility
    pub use crate::overlay::interface::StreamId;

    // Helper to create a random PeerId
    fn random_peer_id() -> PeerId {
        PeerId::random()
    }

    // Helper to create a StreamId
    fn create_stream_id(id: &str) -> StreamId {
        StreamId(id.as_bytes().to_vec())
    }

    #[test]
    fn test_stream_id_display() {
        let stream_id = create_stream_id("test_stream");
        let display = format!("{}", stream_id);
        assert!(!display.is_empty());
    }

    #[test]
    fn test_tree_node() {
        let peer_id = random_peer_id();
        let mut node = TreeNode::new(peer_id, PeerRole::Relay);
        
        // Test initial state
        assert_eq!(node.peer_id, peer_id);
        assert_eq!(node.role, PeerRole::Relay);
        assert!(node.parent.is_none());
        assert!(node.children.is_empty());
        assert_eq!(node.depth, 0);
        
        // Test adding children
        let child1 = random_peer_id();
        let child2 = random_peer_id();
        let child3 = random_peer_id();
        
        assert!(node.add_child(child1));
        assert!(node.add_child(child2));
        assert!(node.add_child(child3));
        
        // Should have 3 children now
        assert_eq!(node.children.len(), 3);
        assert!(node.children.contains(&child1));
        assert!(node.children.contains(&child2));
        assert!(node.children.contains(&child3));
        
        // Test removing a child
        assert!(node.remove_child(&child2));
        assert_eq!(node.children.len(), 2);
        assert!(!node.children.contains(&child2));
        
        // Test adding children
        let leaf_peer_id = random_peer_id();
        let leaf_node = TreeNode::new(leaf_peer_id.clone(), PeerRole::Consumer);
        assert!(node.add_child(leaf_peer_id));
        // Verify the leaf node cannot accept children
        assert!(!leaf_node.can_accept_children());
    }

    #[test]
    fn test_stream_tree() {
        let stream_id = create_stream_id("test_stream");
        let mut tree = StreamTree::new(stream_id.clone());
        
        // Test initial state
        assert_eq!(tree.stream_id, stream_id);
        assert!(tree.nodes.is_empty());
        assert!(tree.source.is_none());
        
        // Set source
        let source_id = random_peer_id();
        assert!(tree.set_source(source_id));
        assert_eq!(tree.source, Some(source_id));
        assert_eq!(tree.nodes.len(), 1);
        
        // Can't set source twice
        assert!(!tree.set_source(random_peer_id()));
        
        // Add some relay nodes
        let relay1 = random_peer_id();
        let relay2 = random_peer_id();
        assert!(tree.add_peer(relay1, PeerRole::Relay, 10000));
        assert!(tree.add_peer(relay2, PeerRole::Relay, 20000));
        
        // Add some leaf nodes
        let leaf1 = random_peer_id();
        let leaf2 = random_peer_id();
        let leaf3 = random_peer_id();
        assert!(tree.add_peer(leaf1, PeerRole::Consumer, 5000));
        assert!(tree.add_peer(leaf2, PeerRole::Consumer, 8000));
        assert!(tree.add_peer(leaf3, PeerRole::Consumer, 3000));
        
        // Should have 6 nodes total (source + 2 relays + 3 leaves)
        assert_eq!(tree.nodes.len(), 6);
        
        // Check that parent-child relationships were established
        // Every node except source should have a parent
        for (id, node) in &tree.nodes {
            if *id != source_id {
                assert!(node.parent.is_some());
            }
        }
        
        // Stats should be reasonable
        let stats = tree.get_stats();
        assert_eq!(stats.peer_count, 6);
        assert!(stats.depth > 0);
        assert_eq!(stats.relay_count, 2);
        assert_eq!(stats.leaf_count, 3);
        
        // Test removing a node
        assert!(tree.remove_peer(&leaf1));
        assert_eq!(tree.nodes.len(), 5);
        assert!(!tree.contains_peer(&leaf1));
        
        // Can't remove the source
        assert!(!tree.remove_peer(&source_id));
        
        // Test removing a relay - children should be reassigned
        let relay_children: HashSet<PeerId> = tree.nodes.get(&relay1)
            .map(|node| node.children.clone())
            .unwrap_or_default();
            
        assert!(tree.remove_peer(&relay1));
        
        // Ensure orphaned children found new parents
        for child_id in relay_children {
            if tree.contains_peer(&child_id) {
                assert!(tree.nodes.get(&child_id).unwrap().parent.is_some());
            }
        }
    }

    #[test]
    fn test_mesh_node() {
        let peer_id = random_peer_id();
        let mut node = MeshNode::new(peer_id, PeerRole::Relay, false);
        
        // Test initial state
        assert_eq!(node.peer_id, peer_id);
        assert_eq!(node.role, PeerRole::Relay);
        assert!(node.peers.is_empty());
        assert!(!node.is_source);
        
        // Test adding peers
        let peer1 = random_peer_id();
        let peer2 = random_peer_id();
        
        assert!(node.add_peer(peer1));
        assert!(node.add_peer(peer2));
        
        // Should have 2 peers now
        assert_eq!(node.peers.len(), 2);
        assert!(node.peers.contains(&peer1));
        assert!(node.peers.contains(&peer2));
        
        // Test removing a peer
        assert!(node.remove_peer(&peer1));
        assert_eq!(node.peers.len(), 1);
        assert!(!node.peers.contains(&peer1));
        
        // Test peer capacity
        for _ in 0..10 {
            let peer = random_peer_id();
            node.add_peer(peer);
        }
        
        // After adding many peers, should reach capacity
        assert!(!node.can_accept_connections());
    }

    #[test]
    fn test_stream_mesh() {
        let stream_id = create_stream_id("test_stream");
        let mut mesh = StreamMesh::new(stream_id.clone());
        
        // Test initial state
        assert_eq!(mesh.stream_id, stream_id);
        assert!(mesh.nodes.is_empty());
        assert!(mesh.source.is_none());
        
        // Set source
        let source_id = random_peer_id();
        assert!(mesh.set_source(source_id));
        assert_eq!(mesh.source, Some(source_id));
        assert_eq!(mesh.nodes.len(), 1);
        
        // Can't set source twice
        assert!(!mesh.set_source(random_peer_id()));
        
        // Add some peers
        let peer1 = random_peer_id();
        let peer2 = random_peer_id();
        let peer3 = random_peer_id();
        assert!(mesh.add_peer(peer1, PeerRole::Consumer, 10000));
        assert!(mesh.add_peer(peer2, PeerRole::Relay, 15000));
        assert!(mesh.add_peer(peer3, PeerRole::Consumer, 5000));
        
        // Should have 4 nodes total (source + 3 peers)
        assert_eq!(mesh.nodes.len(), 4);
        
        // Each peer should have at least one connection
        for (id, node) in &mesh.nodes {
            if *id != source_id {
                assert!(!node.peers.is_empty());
            }
        }
        
        // Stats should be reasonable
        let stats = mesh.get_stats();
        assert_eq!(stats.peer_count, 4);
        assert!(stats.source_connected_count > 0);
        assert!(stats.avg_connections > 0.0);
        
        // Test removing a node
        assert!(mesh.remove_peer(&peer1));
        assert_eq!(mesh.nodes.len(), 3);
        assert!(!mesh.contains_peer(&peer1));
        
        // Can't remove the source
        assert!(!mesh.remove_peer(&source_id));
        
        // Test rebalance
        assert!(mesh.rebalance());
    }
} 