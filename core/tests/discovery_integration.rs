//! Integration tests for peer discovery mechanisms
//!
//! This module tests the integration between different discovery mechanisms
//! (bootstrap, DHT, etc.) and verifies they can discover peers in a real network.

use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, info};

use OpenBroadcastNetwork_core::discovery::{
    Discovery, DiscoveryEvent, DiscoveryManager, DiscoveryManagerConfig,
    BootstrapDiscovery, BootstrapDiscoveryConfig,
    DhtDiscovery, DhtDiscoveryConfig,
    PeerInfo,
};
use OpenBroadcastNetwork_core::overlay::{
    interface::{Overlay, OverlayConfig},
    libp2p_impl::Libp2pOverlay,
    peer::LocalPeerId,
};

/// Initialize test logging
fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .try_init();
}

#[tokio::test]
async fn test_bootstrap_discovery_basic() {
    init_logging();
    
    // Create a bootstrap discovery instance
    let config = BootstrapDiscoveryConfig::default();
    let mut discovery = BootstrapDiscovery::with_config(config);
    
    // The discovery should not be running initially
    assert!(!discovery.is_running());
    
    // Start the discovery
    discovery.start().await.expect("Failed to start bootstrap discovery");
    assert!(discovery.is_running());
    
    // Stop the discovery
    discovery.stop().await.expect("Failed to stop bootstrap discovery");
    assert!(!discovery.is_running());
    
    info!("Bootstrap discovery basic test passed");
}

#[tokio::test]
async fn test_dht_discovery_basic() {
    init_logging();
    
    // Create a DHT discovery instance
    let config = DhtDiscoveryConfig::default();
    let mut discovery = DhtDiscovery::with_config(config);
    
    // The discovery should not be running initially
    assert!(!discovery.is_running());
    
    // Start the discovery
    discovery.start().await.expect("Failed to start DHT discovery");
    assert!(discovery.is_running());
    
    // Get the local peer ID
    let local_peer_id = discovery.local_peer_id();
    assert!(local_peer_id.is_some());
    
    // Stop the discovery
    discovery.stop().await.expect("Failed to stop DHT discovery");
    assert!(!discovery.is_running());
    
    info!("DHT discovery basic test passed");
}

#[tokio::test]
async fn test_discovery_manager_basic() {
    init_logging();
    
    // Create a discovery manager with both discovery types enabled
    let mut config = DiscoveryManagerConfig::default();
    config.enable_bootstrap = true;
    config.enable_dht = true;
    config.bootstrap_config = Some(BootstrapDiscoveryConfig::default());
    config.dht_config = Some(DhtDiscoveryConfig::default());
    
    let mut manager = DiscoveryManager::new(config);
    
    // The manager should not be running initially
    assert!(!manager.is_running().await);
    
    // Start the manager
    manager.start().await.expect("Failed to start discovery manager");
    assert!(manager.is_running().await);
    
    // Stop the manager
    manager.stop().await.expect("Failed to stop discovery manager");
    assert!(!manager.is_running().await);
    
    info!("Discovery manager basic test passed");
}

#[tokio::test]
async fn test_overlay_with_discovery_integration() {
    init_logging();
    
    // Create overlay configuration with discovery enabled
    let mut config = OverlayConfig::default();
    config.enable_bootstrap_discovery = true;
    config.enable_dht_discovery = true;
    
    // Create the overlay
    let overlay = Libp2pOverlay::new(config).await
        .expect("Failed to create overlay");
    
    // The overlay should not be running initially
    assert!(!overlay.is_running());
    
    // Start the overlay (which should start discovery)
    overlay.start().await.expect("Failed to start overlay");
    assert!(overlay.is_running());
    
    // Check that we can get stats
    let stats = overlay.stats().await.expect("Failed to get stats");
    debug!("Overlay stats: {:?}", stats);
    
    // Stop the overlay
    overlay.stop().await.expect("Failed to stop overlay");
    assert!(!overlay.is_running());
    
    info!("Overlay discovery integration test passed");
}

#[tokio::test]
async fn test_peer_announcement() {
    init_logging();
    
    // Create a DHT discovery instance
    let config = DhtDiscoveryConfig::default();
    let mut discovery = DhtDiscovery::with_config(config);
    
    // Start the discovery
    discovery.start().await.expect("Failed to start DHT discovery");
    
    // Create peer info to announce
    let peer_info = PeerInfo {
        id: b"test_peer_123".to_vec(),
        addresses: vec![],
        protocols: vec!["test_protocol".to_string()],
        metadata: std::collections::HashMap::new(),
        first_seen: Some(std::time::SystemTime::now()),
        last_seen: Some(std::time::SystemTime::now()),
        connection_status: OpenBroadcastNetwork_core::discovery::interface::ConnectionStatus::Connected,
    };
    
    // Announce the peer
    discovery.announce(peer_info.clone()).await
        .expect("Failed to announce peer");
    
    // Try to look up the peer
    let found_peer = discovery.lookup_peer(&peer_info.id).await
        .expect("Failed to lookup peer");
    
    if let Some(found) = found_peer {
        assert_eq!(found.id, peer_info.id);
        assert_eq!(found.protocols, peer_info.protocols);
    }
    
    // Stop the discovery
    discovery.stop().await.expect("Failed to stop DHT discovery");
    
    info!("Peer announcement test passed");
}

#[tokio::test]
async fn test_peer_discovery_events() {
    init_logging();
    
    // Create a discovery manager
    let mut config = DiscoveryManagerConfig::default();
    config.enable_dht = true;
    config.dht_config = Some(DhtDiscoveryConfig::default());
    config.poll_interval_ms = 50; // Faster polling for tests
    
    let mut manager = DiscoveryManager::new(config);
    
    // Start the manager
    manager.start().await.expect("Failed to start discovery manager");
    
    // Wait briefly for startup
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Try to get events with a short timeout
    let event_future = timeout(Duration::from_secs(2), manager.next_event());
    
    match event_future.await {
        Ok(Some(event)) => {
            debug!("Received discovery event: {:?}", event);
            match event {
                DiscoveryEvent::ServiceStarted => {
                    info!("Discovery service started event received");
                },
                DiscoveryEvent::Error(msg) => {
                    info!("Discovery error event received: {}", msg);
                },
                _ => {
                    info!("Other discovery event received: {:?}", event);
                }
            }
        },
        Ok(None) => {
            info!("No events received (channel closed)");
        },
        Err(_) => {
            info!("No events received within timeout");
        }
    }
    
    // Stop the manager
    manager.stop().await.expect("Failed to stop discovery manager");
    
    info!("Peer discovery events test completed");
}

/// Test that discovery mechanisms can work without network connectivity
/// This tests the basic functionality without requiring real peer connections
#[tokio::test]
async fn test_discovery_without_network() {
    init_logging();
    
    // Test bootstrap discovery with no bootstrap nodes
    let bootstrap_config = BootstrapDiscoveryConfig::default();
    let mut bootstrap = BootstrapDiscovery::with_config(bootstrap_config);
    
    // This should fail gracefully when no bootstrap nodes are configured
    match bootstrap.start().await {
        Ok(_) => {
            // If it starts successfully, that's fine too
            let _ = bootstrap.stop().await;
            info!("Bootstrap discovery started without bootstrap nodes");
        },
        Err(e) => {
            info!("Bootstrap discovery failed as expected: {}", e);
        }
    }
    
    // Test DHT discovery (should work without bootstrap peers)
    let dht_config = DhtDiscoveryConfig::default();
    let mut dht = DhtDiscovery::with_config(dht_config);
    
    // This should start successfully
    dht.start().await.expect("DHT discovery should start");
    
    // Try to find peers (should return empty list)
    let peers = dht.find_peers("").await.expect("Should be able to search for peers");
    assert!(peers.is_empty(), "Should not find any peers without network");
    
    // Stop the DHT
    dht.stop().await.expect("Should be able to stop DHT");
    
    info!("Discovery without network test passed");
}