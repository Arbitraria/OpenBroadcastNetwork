use libp2p::identity::Keypair;
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use OpenBroadcastNetwork_core::overlay::interface::{Overlay, OverlayConfig, OverlayEvent};
use OpenBroadcastNetwork_core::overlay::libp2p::impl_core::Libp2pOverlay;
use OpenBroadcastNetwork_core::overlay::peer::LocalPeerId;

#[tokio::test]
async fn test_overlay_basic_functionality() {
    // Create a realistic overlay configuration
    let keypair_a = Keypair::generate_ed25519();
    let peer_id_a = LocalPeerId::from(keypair_a.public().to_peer_id());

    let config_a = OverlayConfig {
        local_peer_id: peer_id_a.clone(),
        bootstrap_peers: Vec::new(),
        enable_kademlia: false,
        enable_bootstrap_discovery: false,
        enable_dht_discovery: false,
        max_connections: 10,
        connection_timeout: Duration::from_secs(10),
        peer_ttl: Duration::from_secs(60),
        heartbeat_interval: Duration::from_secs(5),
        rebalance_interval: Duration::from_secs(30),
        ..Default::default()
    };

    // Test overlay creation
    let overlay_result = Libp2pOverlay::new(config_a).await;
    assert!(
        overlay_result.is_ok(),
        "Failed to create overlay: {:?}",
        overlay_result.err()
    );

    let overlay = overlay_result.unwrap();

    // Test overlay start/stop
    assert!(!overlay.is_running());

    let start_result = overlay.start().await;
    assert!(
        start_result.is_ok(),
        "Failed to start overlay: {:?}",
        start_result.err()
    );
    assert!(overlay.is_running());

    // Test stats retrieval
    let stats_result = overlay.stats().await;
    assert!(
        stats_result.is_ok(),
        "Failed to get stats: {:?}",
        stats_result.err()
    );

    // Test stop
    let stop_result = overlay.stop().await;
    assert!(
        stop_result.is_ok(),
        "Failed to stop overlay: {:?}",
        stop_result.err()
    );
    assert!(!overlay.is_running());
}
