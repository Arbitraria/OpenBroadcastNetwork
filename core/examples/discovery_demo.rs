//! Demonstration of the integrated peer discovery system
//!
//! This example shows how to use the completed peer discovery implementation
//! with both DHT and bootstrap discovery mechanisms integrated into the overlay network.

use std::time::Duration;
use tracing::info;

use OpenBroadcastNetwork_core::{
    discovery::{
        DhtDiscoveryConfig, DiscoveryManager, DiscoveryManagerConfig,
    },
    overlay::{
        interface::{Overlay, OverlayConfig},
        libp2p::impl_core::Libp2pOverlay,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().init();

    info!("Starting peer discovery demo");

    // Test 1: Standalone Discovery Manager
    info!("=== Test 1: Standalone Discovery Manager ===");
    test_standalone_discovery_manager().await?;

    // Test 2: Overlay with Integrated Discovery
    info!("=== Test 2: Overlay with Integrated Discovery ===");
    test_overlay_with_discovery().await?;

    info!("Demo completed successfully!");
    Ok(())
}

/// Test the standalone discovery manager
async fn test_standalone_discovery_manager() -> Result<(), Box<dyn std::error::Error>> {
    // Create discovery manager configuration with both DHT and bootstrap enabled
    let mut config = DiscoveryManagerConfig::default();
    config.enable_dht = true;
    config.enable_bootstrap = false; // Disable bootstrap to avoid needing real servers
    config.dht_config = Some(DhtDiscoveryConfig::default());
    config.poll_interval_ms = 100; // Fast polling for demo

    // Create and start discovery manager
    let mut manager = DiscoveryManager::new(config);

    info!("Starting discovery manager...");
    manager.start().await?;

    assert!(
        manager.is_running().await,
        "Discovery manager should be running"
    );
    info!("Discovery manager started successfully");

    // Check for peers (should be empty initially)
    let peers = manager.get_peers().await;
    info!("Initial discovered peers: {}", peers.len());

    // Wait briefly to see if any events come through
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check for events
    if let Some(event) = manager.next_event().await {
        info!("Received discovery event: {:?}", event);
    } else {
        info!("No discovery events received");
    }

    // Stop the manager
    info!("Stopping discovery manager...");
    manager.stop().await?;
    assert!(
        !manager.is_running().await,
        "Discovery manager should be stopped"
    );
    info!("Discovery manager stopped successfully");

    Ok(())
}

/// Test the overlay network with integrated discovery
async fn test_overlay_with_discovery() -> Result<(), Box<dyn std::error::Error>> {
    // Create overlay configuration with discovery enabled
    let mut config = OverlayConfig::default();
    config.enable_dht_discovery = true;
    config.enable_bootstrap_discovery = false; // Disable bootstrap to avoid needing real servers
    config.enable_kademlia = true;

    info!("Creating overlay with integrated discovery...");
    let overlay = Libp2pOverlay::new(config).await?;

    // Check initial state
    assert!(
        !overlay.is_running(),
        "Overlay should not be running initially"
    );
    info!(
        "Overlay created with local peer ID: {}",
        overlay.local_peer_id()
    );

    // Start the overlay (this will start the integrated discovery)
    info!("Starting overlay with discovery...");
    overlay.start().await?;
    assert!(overlay.is_running(), "Overlay should be running");
    info!("Overlay started successfully with integrated discovery");

    // Check initial stats
    let stats = overlay.stats().await?;
    info!(
        "Initial overlay stats: connected_peers={}, discovered_peers={}, active_streams={}",
        stats.connected_peers, stats.discovered_peers, stats.active_streams
    );

    // Wait briefly to allow discovery to work
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Check stats again
    let stats = overlay.stats().await?;
    info!(
        "Updated overlay stats: connected_peers={}, discovered_peers={}, active_streams={}",
        stats.connected_peers, stats.discovered_peers, stats.active_streams
    );

    // Get connected peers
    let peers = overlay.connected_peers().await?;
    info!("Connected peers: {}", peers.len());

    // Check for overlay events
    if let Some(event) = overlay.next_event().await {
        info!("Received overlay event: {:?}", event);
    } else {
        info!("No overlay events received");
    }

    // Stop the overlay
    info!("Stopping overlay...");
    overlay.stop().await?;
    assert!(!overlay.is_running(), "Overlay should be stopped");
    info!("Overlay stopped successfully");

    Ok(())
}
