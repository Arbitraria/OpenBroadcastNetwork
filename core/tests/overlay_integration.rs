use OpenBroadcastNetwork_core::overlay::libp2p_impl::Libp2pOverlay;
use OpenBroadcastNetwork_core::overlay::interface::OverlayConfig;
use OpenBroadcastNetwork_core::overlay::interface::{OverlayEvent, Overlay};
use OpenBroadcastNetwork_core::overlay::peer::LocalPeerId;
use tokio::time::{sleep, Duration};

use OpenBroadcastNetwork_core::test_report::{TestReport, TestResult};
use std::time::{SystemTime, Instant};

#[tokio::test]
async fn test_overlay_pubsub_and_discovery() {
    let test_start = Instant::now();
    let mut report = TestReport::new();
    let mut test_status = "pass".to_string();
    let mut error_msg = None;
    
    // Main test logic
    let test_result = async {
        // 1. Start Node A
        let config_a = OverlayConfig {
            // TODO: Fill in required fields for your project
            // e.g., listen_addr: "/ip4/127.0.0.1/tcp/4001".parse().unwrap(),
            ..Default::default()
        };
        let node_a = Libp2pOverlay::new(config_a);
        node_a.start().await.unwrap();

        // 2. Start Node B, bootstrapping to Node A
        let config_b = OverlayConfig {
            bootstrap_peers: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            // Use a different port for B, e.g., 4002
            ..Default::default()
        };
        let node_b = Libp2pOverlay::new(config_b);
        node_b.start().await.unwrap();

        // 3. Subscribe both nodes to the same topic
        let topic = "test-topic";
        node_a.subscribe(topic).await.unwrap();
        node_b.subscribe(topic).await.unwrap();

        // 4. Publish a message from A
        let msg = b"hello world";
        node_a.publish(topic, msg.to_vec()).await.unwrap();

        // 5. Wait and check for message reception on B
        let mut received = false;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            if let Some(OverlayEvent::PubsubMessage { topic: t, data, .. }) = node_b.next_event().await {
                if t == topic && data == msg {
                    received = true;
                    break;
                }
            }
        }
        assert!(received, "Node B did not receive the pubsub message");

        // 6. Check peer discovery
        let peers_a = node_a.connected_peers().await.unwrap();
        let peers_b = node_b.connected_peers().await.unwrap();
        assert!(!peers_a.is_empty() && !peers_b.is_empty(), "Peer discovery failed");

        // (Optional) Stop nodes
        node_a.stop().await.unwrap();
        node_b.stop().await.unwrap();
        Ok::<(), String>(())
    }.await;

    if let Err(e) = test_result {
        test_status = "fail".to_string();
        error_msg = Some(format!("{e:?}"));
    }
    let duration_ms = test_start.elapsed().as_millis();
    let result = TestResult {
        name: "test_overlay_pubsub_and_discovery".to_string(),
        module: "overlay".to_string(),
        status: test_status,
        error: error_msg,
        duration_ms,
        timestamp: SystemTime::now(),
    };
    report.add_result(result);
    let _ = report.save_to_file("test_report.json");
}
