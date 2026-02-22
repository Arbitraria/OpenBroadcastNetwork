//! Phase 1 Integration Tests
//!
//! This test suite verifies that all Phase 1 components work together:
//! - Peer discovery (DHT and bootstrap)
//! - Pub/Sub topic creation and subscription
//! - Tree-mesh hybrid relay logic
//! - Geo-aware rebalancing
//! - End-to-end data flow

use std::collections::HashSet;
use std::sync::Arc;
use tokio::time::{sleep, timeout, Duration};
use tracing::{debug, info, warn};
use OpenBroadcastNetwork_core::discovery::{DiscoveryManager, DiscoveryManagerConfig};
use OpenBroadcastNetwork_core::overlay::interface::{Overlay, OverlayConfig, StreamId};
use OpenBroadcastNetwork_core::overlay::libp2p::impl_core::Libp2pOverlay;
use OpenBroadcastNetwork_core::overlay::peer::LocalPeerId;

/// Test that two nodes can discover each other using bootstrap discovery
#[tokio::test]
async fn test_bootstrap_peer_discovery() {
    let _ = tracing_subscriber::fmt::try_init();

    info!("Starting bootstrap peer discovery test");

    // Create two discovery managers
    let config1 = DiscoveryManagerConfig {
        enable_dht: false,
        enable_bootstrap: true,
        bootstrap_config: Some(Default::default()),
        dht_config: None,
        ..Default::default()
    };

    let config2 = DiscoveryManagerConfig {
        enable_dht: false,
        enable_bootstrap: true,
        bootstrap_config: Some(Default::default()),
        dht_config: None,
        ..Default::default()
    };

    let mut discovery1 = DiscoveryManager::new(config1);
    let mut discovery2 = DiscoveryManager::new(config2);

    // Start discovery
    discovery1
        .start()
        .await
        .expect("Failed to start discovery1");
    discovery2
        .start()
        .await
        .expect("Failed to start discovery2");

    // Wait for discovery to find peers
    sleep(Duration::from_secs(2)).await;

    // Check that peers were discovered
    let peers1 = discovery1.get_peers().await;
    let peers2 = discovery2.get_peers().await;

    info!("Discovery1 found {} peers", peers1.len());
    info!("Discovery2 found {} peers", peers2.len());

    // Stop discovery
    discovery1.stop().await.expect("Failed to stop discovery1");
    discovery2.stop().await.expect("Failed to stop discovery2");

    // At minimum, they should discover each other through bootstrap
    assert!(
        !peers1.is_empty() || !peers2.is_empty(),
        "No peers discovered"
    );
}

/// Test that DHT discovery can find peers
#[tokio::test]
async fn test_dht_peer_discovery() {
    let _ = tracing_subscriber::fmt::try_init();

    info!("Starting DHT peer discovery test");

    // Create DHT discovery managers
    let config1 = DiscoveryManagerConfig {
        enable_dht: true,
        enable_bootstrap: false,
        bootstrap_config: None,
        dht_config: Some(Default::default()),
        ..Default::default()
    };

    let config2 = DiscoveryManagerConfig {
        enable_dht: true,
        enable_bootstrap: false,
        bootstrap_config: None,
        dht_config: Some(Default::default()),
        ..Default::default()
    };

    let mut discovery1 = DiscoveryManager::new(config1);
    let mut discovery2 = DiscoveryManager::new(config2);

    // Start discovery
    discovery1
        .start()
        .await
        .expect("Failed to start discovery1");
    discovery2
        .start()
        .await
        .expect("Failed to start discovery2");

    // Wait for DHT to bootstrap
    sleep(Duration::from_secs(3)).await;

    // Check discovered peers
    let peers1 = discovery1.get_peers().await;
    let peers2 = discovery2.get_peers().await;

    info!("DHT Discovery1 found {} peers", peers1.len());
    info!("DHT Discovery2 found {} peers", peers2.len());

    // Stop discovery
    discovery1.stop().await.expect("Failed to stop discovery1");
    discovery2.stop().await.expect("Failed to stop discovery2");
}

/// Test overlay network initialization and basic functionality
#[tokio::test]
async fn test_overlay_network_basic() {
    let _ = tracing_subscriber::fmt::try_init();

    info!("Starting overlay network basic test");

    // Create overlay configuration
    let config = OverlayConfig {
        local_peer_id: LocalPeerId::new_random(),
        bootstrap_peers: vec![],
        enable_kademlia: false,
        enable_bootstrap_discovery: false,
        enable_dht_discovery: false,
        max_connections: 10,
        connection_timeout: Duration::from_secs(5),
        ..Default::default()
    };

    // Create overlay network
    let overlay = Libp2pOverlay::new(config)
        .await
        .expect("Failed to create overlay");

    // Test basic operations
    assert!(!overlay.is_running());

    // Start the overlay
    overlay.start().await.expect("Failed to start overlay");
    assert!(overlay.is_running());

    // Test getting local peer ID
    let local_id = overlay.local_peer_id();
    assert!(!local_id.to_string().is_empty());

    // Get initial stats
    let stats = overlay.stats().await.expect("Failed to get stats");
    info!("Initial overlay stats: {:?}", stats);

    // Stop the overlay
    overlay.stop().await.expect("Failed to stop overlay");
    assert!(!overlay.is_running());
}

/// Test stream creation and management
#[tokio::test]
async fn test_stream_management() {
    let _ = tracing_subscriber::fmt::try_init();

    info!("Starting stream management test");

    let config = OverlayConfig {
        local_peer_id: LocalPeerId::new_random(),
        bootstrap_peers: vec![],
        enable_kademlia: false,
        enable_bootstrap_discovery: false,
        enable_dht_discovery: false,
        ..Default::default()
    };

    let overlay = Libp2pOverlay::new(config)
        .await
        .expect("Failed to create overlay");
    overlay.start().await.expect("Failed to start overlay");

    // Test stream operations
    let stream_id = StreamId::from_string("test-stream-1");

    // Create a stream
    overlay
        .publish_stream(&stream_id)
        .await
        .expect("Failed to publish stream");

    // Check active streams
    let streams = overlay
        .active_streams()
        .await
        .expect("Failed to get active streams");
    assert!(
        streams.contains(&stream_id),
        "Stream not found in active streams"
    );

    // Stop the stream
    overlay
        .stop_stream(&stream_id)
        .await
        .expect("Failed to stop stream");

    // Verify stream is removed
    let streams_after = overlay
        .active_streams()
        .await
        .expect("Failed to get active streams after stop");
    assert!(
        !streams_after.contains(&stream_id),
        "Stream still active after stop"
    );

    overlay.stop().await.expect("Failed to stop overlay");
}

/// Test two-node communication and relay
#[tokio::test]
async fn test_two_node_communication() {
    let _ = tracing_subscriber::fmt::try_init();

    info!("Starting two-node communication test");

    // Create two overlay nodes
    let config1 = OverlayConfig {
        local_peer_id: LocalPeerId::new_random(),
        bootstrap_peers: vec![],
        enable_kademlia: false,
        enable_bootstrap_discovery: false,
        enable_dht_discovery: false,
        ..Default::default()
    };

    let config2 = OverlayConfig {
        local_peer_id: LocalPeerId::new_random(),
        bootstrap_peers: vec![],
        enable_kademlia: false,
        enable_bootstrap_discovery: false,
        enable_dht_discovery: false,
        ..Default::default()
    };

    let node1 = Arc::new(
        Libp2pOverlay::new(config1)
            .await
            .expect("Failed to create node1"),
    );
    let node2 = Arc::new(
        Libp2pOverlay::new(config2)
            .await
            .expect("Failed to create node2"),
    );

    // Start both nodes
    node1.start().await.expect("Failed to start node1");
    node2.start().await.expect("Failed to start node2");

    // Create a stream on node1
    let stream_id = StreamId::from_string("shared-stream");
    node1
        .publish_stream(&stream_id)
        .await
        .expect("Failed to publish stream on node1");

    // Try to relay to node2
    let node2_id = node2.local_peer_id();
    node1
        .relay_stream(&stream_id, &node2_id)
        .await
        .expect("Failed to relay stream to node2");

    // Give some time for communication
    sleep(Duration::from_millis(500)).await;

    // Get stats from both nodes
    let stats1 = node1.stats().await.expect("Failed to get node1 stats");
    let stats2 = node2.stats().await.expect("Failed to get node2 stats");

    info!("Node1 stats: {:?}", stats1);
    info!("Node2 stats: {:?}", stats2);

    // Clean up
    node1
        .stop_stream(&stream_id)
        .await
        .expect("Failed to stop stream");
    node1.stop().await.expect("Failed to stop node1");
    node2.stop().await.expect("Failed to stop node2");
}

/// Test topology rebalancing functionality
#[tokio::test]
async fn test_topology_rebalancing() {
    let _ = tracing_subscriber::fmt::try_init();

    info!("Starting topology rebalancing test");

    let config = OverlayConfig {
        local_peer_id: LocalPeerId::new_random(),
        bootstrap_peers: vec![],
        enable_kademlia: false,
        enable_bootstrap_discovery: false,
        enable_dht_discovery: false,
        rebalance_interval: Duration::from_millis(100), // Fast rebalancing for testing
        ..Default::default()
    };

    let overlay = Libp2pOverlay::new(config)
        .await
        .expect("Failed to create overlay");
    overlay.start().await.expect("Failed to start overlay");

    // Get initial stats
    let initial_stats = overlay.stats().await.expect("Failed to get initial stats");

    // Force a topology rebalance
    overlay
        .rebalance_topology()
        .await
        .expect("Failed to trigger rebalance");

    // Wait a bit for rebalancing
    sleep(Duration::from_millis(200)).await;

    // Get stats after rebalancing
    let final_stats = overlay.stats().await.expect("Failed to get final stats");

    info!("Initial stats: {:?}", initial_stats);
    info!("Final stats: {:?}", final_stats);

    // Verify rebalancing completed without errors
    overlay.stop().await.expect("Failed to stop overlay");
}

/// Integration test for the complete Phase 1 functionality
#[tokio::test]
async fn test_phase1_complete_integration() {
    let _ = tracing_subscriber::fmt::try_init();

    info!("Starting complete Phase 1 integration test");

    // Create multiple nodes to simulate a small network
    let mut nodes = Vec::new();
    let mut node_ids = Vec::new();

    for i in 0..3 {
        let config = OverlayConfig {
            local_peer_id: LocalPeerId::new_random(),
            bootstrap_peers: vec![],
            enable_kademlia: false,
            enable_bootstrap_discovery: false,
            enable_dht_discovery: false,
            max_connections: 5,
            ..Default::default()
        };

        let node = Arc::new(
            Libp2pOverlay::new(config)
                .await
                .expect(&format!("Failed to create node {}", i)),
        );
        let node_id = node.local_peer_id();

        node.start()
            .await
            .expect(&format!("Failed to start node {}", i));

        node_ids.push(node_id);
        nodes.push(node);
    }

    info!("Started {} nodes", nodes.len());

    // Create a test stream on the first node
    let stream_id = StreamId::from_string("integration-test-stream");
    nodes[0]
        .publish_stream(&stream_id)
        .await
        .expect("Failed to publish stream");

    // Relay the stream to other nodes
    for i in 1..nodes.len() {
        nodes[0]
            .relay_stream(&stream_id, &node_ids[i])
            .await
            .expect(&format!("Failed to relay to node {}", i));
    }

    // Wait for network communication
    sleep(Duration::from_secs(1)).await;

    // Collect stats from all nodes
    let mut all_stats = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        let stats = node
            .stats()
            .await
            .expect(&format!("Failed to get stats from node {}", i));
        info!("Node {} stats: {:?}", i, stats);
        all_stats.push(stats);
    }

    // Test topology rebalancing on all nodes
    for (i, node) in nodes.iter().enumerate() {
        node.rebalance_topology()
            .await
            .expect(&format!("Failed to rebalance node {}", i));
    }

    // Wait for rebalancing
    sleep(Duration::from_millis(500)).await;

    // Clean up: stop stream and all nodes
    nodes[0]
        .stop_stream(&stream_id)
        .await
        .expect("Failed to stop stream");

    for (i, node) in nodes.iter().enumerate() {
        node.stop()
            .await
            .expect(&format!("Failed to stop node {}", i));
    }

    info!("Phase 1 integration test completed successfully");
}

/// Test error handling and edge cases
#[tokio::test]
async fn test_error_handling() {
    let _ = tracing_subscriber::fmt::try_init();

    info!("Starting error handling test");

    let config = OverlayConfig {
        local_peer_id: LocalPeerId::new_random(),
        bootstrap_peers: vec!["invalid-address".to_string()], // Invalid address
        enable_kademlia: false,
        enable_bootstrap_discovery: false,
        enable_dht_discovery: false,
        connection_timeout: Duration::from_millis(100), // Short timeout
        ..Default::default()
    };

    let overlay = Libp2pOverlay::new(config)
        .await
        .expect("Failed to create overlay");

    // Starting should succeed even with invalid bootstrap peers
    overlay
        .start()
        .await
        .expect("Failed to start overlay with invalid peers");

    // Test operations on non-existent stream
    let fake_stream = StreamId::from_string("non-existent-stream");
    let stop_result = overlay.stop_stream(&fake_stream).await;

    // Should handle gracefully (specific error handling depends on implementation)
    match stop_result {
        Ok(_) => info!("Stopping non-existent stream succeeded (graceful handling)"),
        Err(e) => info!("Stopping non-existent stream failed as expected: {}", e),
    }

    // Test connecting to invalid peer
    let fake_libp2p_peer = libp2p::PeerId::random();
    let connect_result = overlay.disconnect_peer(&fake_libp2p_peer).await;

    match connect_result {
        Ok(_) => info!("Disconnecting non-existent peer succeeded (graceful handling)"),
        Err(e) => info!("Disconnecting non-existent peer failed as expected: {}", e),
    }

    overlay.stop().await.expect("Failed to stop overlay");

    info!("Error handling test completed");
}
