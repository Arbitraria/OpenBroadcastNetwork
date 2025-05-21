//! Tests for the discovery implementations

#[cfg(test)]
mod tests {
    use super::super::interface::{Discovery, DiscoveryEvent, DiscoveryError, PeerInfo};
    use super::super::bootstrap::{BootstrapDiscovery, BootstrapDiscoveryConfig};
    use super::super::dht::{DhtDiscovery, DhtDiscoveryConfig};
    use super::super::mdns::{MdnsDiscovery, MdnsDiscoveryConfig};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;
    use futures::StreamExt;
    use tokio::time::timeout;

    /// Create a test peer info instance
    fn create_test_peer_info(id: Vec<u8>, addr: SocketAddr) -> PeerInfo {
        PeerInfo {
            id,
            addresses: vec![addr],
            protocols: vec!["test/1.0".to_string()],
            metadata: HashMap::new(),
        }
    }

    /// A mock discovery implementation for testing
    struct MockDiscovery {
        running: bool,
        peers: HashMap<Vec<u8>, PeerInfo>,
    }

    impl MockDiscovery {
        fn new() -> Self {
            Self {
                running: false,
                peers: HashMap::new(),
            }
        }

        fn add_peer(&mut self, info: PeerInfo) {
            self.peers.insert(info.id.clone(), info);
        }
    }

    impl Discovery for MockDiscovery {
        fn start(&mut self) -> Result<(), DiscoveryError> {
            self.running = true;
            Ok(())
        }
        
        fn stop(&mut self) -> Result<(), DiscoveryError> {
            self.running = false;
            Ok(())
        }
        
        fn announce(&mut self, info: PeerInfo) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DiscoveryError>> + Send>> {
            let id = info.id.clone();
            self.peers.insert(id, info);
            Box::pin(async { Ok(()) })
        }
        
        fn lookup_peer(&mut self, id: &[u8]) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<PeerInfo>, DiscoveryError>> + Send>> {
            let result = self.peers.get(id).cloned();
            Box::pin(async move { Ok(result) })
        }
        
        fn find_peers(&mut self, predicate: Option<String>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<PeerInfo>, DiscoveryError>> + Send>> {
            let peers: Vec<PeerInfo> = match predicate {
                Some(proto) => self.peers.values()
                    .filter(|p| p.protocols.contains(&proto))
                    .cloned()
                    .collect(),
                None => self.peers.values().cloned().collect(),
            };
            Box::pin(async move { Ok(peers) })
        }
        
        fn next_event(&mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<DiscoveryEvent>> + Send>> {
            Box::pin(async { None })
        }
        
        fn is_running(&self) -> bool {
            self.running
        }
    }

    #[tokio::test]
    async fn test_mock_discovery_lifecycle() {
        let mut discovery = MockDiscovery::new();
        
        // Test initial state
        assert!(!discovery.is_running());
        
        // Test start
        discovery.start().expect("Failed to start discovery");
        assert!(discovery.is_running());
        
        // Test stop
        discovery.stop().expect("Failed to stop discovery");
        assert!(!discovery.is_running());
    }
    
    #[tokio::test]
    async fn test_mock_discovery_announce_lookup() {
        let mut discovery = MockDiscovery::new();
        discovery.start().expect("Failed to start discovery");
        
        // Create peer info
        let id = vec![1, 2, 3, 4];
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();
        let info = create_test_peer_info(id.clone(), addr);
        
        // Announce
        discovery.announce(info.clone()).await.expect("Failed to announce");
        
        // Lookup
        let lookup_result = discovery.lookup_peer(&id).await.expect("Lookup failed");
        assert!(lookup_result.is_some());
        let found_info = lookup_result.unwrap();
        assert_eq!(found_info.id, id);
        assert_eq!(found_info.addresses, vec![addr]);
        
        // Find peers
        let find_result = discovery.find_peers(None).await.expect("Find failed");
        assert_eq!(find_result.len(), 1);
        
        // Find with predicate
        let find_result = discovery.find_peers(Some("test/1.0".to_string())).await.expect("Find failed");
        assert_eq!(find_result.len(), 1);
        
        let find_result = discovery.find_peers(Some("unknown/1.0".to_string())).await.expect("Find failed");
        assert_eq!(find_result.len(), 0);
    }

    // Placeholder tests for actual implementations
    #[test]
    fn test_bootstrap_config() {
        let config = BootstrapDiscoveryConfig {
            bootstrap_nodes: vec![SocketAddr::from_str("127.0.0.1:8080").unwrap()],
            event_buffer_size: 16,
            peer_expiration: 120,
            protocol_name: "test-protocol".to_string(),
            connect_timeout: 5,
            refresh_interval: 60,
        };
        
        assert_eq!(config.bootstrap_nodes.len(), 1);
        assert_eq!(config.event_buffer_size, 16);
        assert_eq!(config.peer_expiration, 120);
        assert_eq!(config.protocol_name, "test-protocol");
        assert_eq!(config.connect_timeout, 5);
        assert_eq!(config.refresh_interval, 60);
    }
    
    #[test]
    fn test_dht_config() {
        let config = DhtDiscoveryConfig {
            protocol_name: "test-dht".to_string(),
            event_buffer_size: 32,
            bootstrap_peers: vec![
                "/ip4/1.2.3.4/tcp/5678/p2p/QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N".parse().unwrap(),
            ],
            record_ttl: 3600,
            peer_expiration: 1800,
        };
        
        assert_eq!(config.protocol_name, "test-dht");
        assert_eq!(config.event_buffer_size, 32);
        assert_eq!(config.bootstrap_peers.len(), 1);
        assert_eq!(config.record_ttl, 3600);
        assert_eq!(config.peer_expiration, 1800);
    }
    
    #[test]
    fn test_mdns_config() {
        let config = MdnsDiscoveryConfig {
            ttl: 60,
            service_name: "test-service".to_string(),
            event_buffer_size: 16,
            peer_expiration: 120,
        };
        
        assert_eq!(config.service_name, "test-service");
        assert_eq!(config.ttl, 60);
        assert_eq!(config.event_buffer_size, 16);
        assert_eq!(config.peer_expiration, 120);
    }

    // Timeout for discovery operations in tests
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// Create a test peer info struct
    fn create_test_peer(id: u8, port: u16) -> PeerInfo {
        PeerInfo {
            id: vec![id],
            addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)],
            protocols: vec!["test/1.0.0".to_string()],
            metadata: HashMap::new(),
        }
    }

    /// Test creating and configuring the mDNS discovery
    #[test]
    fn test_mdns_discovery_creation() {
        // Test default config
        let discovery = MdnsDiscovery::new();
        assert!(!discovery.is_running());
        
        // Test custom config
        let config = MdnsDiscoveryConfig {
            ttl: 60,
            service_name: "test-service".to_string(),
            event_buffer_size: 16,
            peer_expiration: 120,
        };
        
        let discovery = MdnsDiscovery::with_config(config.clone());
        assert!(!discovery.is_running());
    }

    /// Test creating and configuring the DHT discovery
    #[test]
    fn test_dht_discovery_creation() {
        // Test default config
        let discovery = DhtDiscovery::new();
        assert!(!discovery.is_running());
        
        // Test custom config
        let config = DhtDiscoveryConfig {
            protocol_name: "test-dht".to_string(),
            event_buffer_size: 32,
            bootstrap_peers: vec![
                "/ip4/1.2.3.4/tcp/5678/p2p/QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N".parse().unwrap(),
            ],
            record_ttl: 3600,
            peer_expiration: 1800,
        };
        
        let discovery = DhtDiscovery::with_config(config);
        assert!(!discovery.is_running());
    }

    /// Test creating and configuring the Bootstrap discovery
    #[test]
    fn test_bootstrap_discovery_creation() {
        // Test default config
        let discovery = BootstrapDiscovery::new();
        assert!(!discovery.is_running());
        
        // Test custom config
        let config = BootstrapDiscoveryConfig {
            bootstrap_nodes: vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080),
            ],
            event_buffer_size: 16,
            peer_expiration: 120,
            protocol_name: "test-protocol".to_string(),
            connect_timeout: 5,
            refresh_interval: 60,
        };
        
        let discovery = BootstrapDiscovery::with_config(config);
        assert!(!discovery.is_running());
    }

    /// Test basic peer lookup operations
    #[tokio::test]
    async fn test_peer_lookup() {
        // Create a bootstrap discovery instance
        let config = BootstrapDiscoveryConfig {
            bootstrap_nodes: vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            ],
            event_buffer_size: 16,
            peer_expiration: 120,
            protocol_name: "test-protocol".to_string(),
            connect_timeout: 5,
            refresh_interval: 60,
        };
        
        let mut discovery = BootstrapDiscovery::with_config(config);
        
        // Manually add a peer to test lookup
        let test_peer = create_test_peer(1, 1234);
        let peer_id = test_peer.id.clone();
        
        // Use the internal mechanism to add the peer
        {
            let now = std::time::Instant::now();
            let mut peers = discovery.peers.lock().unwrap();
            peers.insert(peer_id.clone(), (test_peer.clone(), now));
        }
        
        // Now try to look up the peer
        let found_peer = discovery.lookup_peer(&peer_id).await.unwrap();
        assert!(found_peer.is_some());
        
        let found_peer = found_peer.unwrap();
        assert_eq!(found_peer.id, peer_id);
        assert_eq!(found_peer.addresses.len(), 1);
        assert_eq!(found_peer.addresses[0].port(), 1234);
        
        // Try looking up a non-existent peer
        let not_found = discovery.lookup_peer(&[99]).await.unwrap();
        assert!(not_found.is_none());
    }

    /// Test finding peers with protocol matching
    #[tokio::test]
    async fn test_find_peers() {
        // Create a bootstrap discovery instance
        let mut discovery = BootstrapDiscovery::new();
        
        // Add several test peers with different protocols
        let peers = vec![
            create_test_peer(1, 1234),
            {
                let mut p = create_test_peer(2, 2345);
                p.protocols = vec!["protocol-a/1.0".to_string()];
                p
            },
            {
                let mut p = create_test_peer(3, 3456);
                p.protocols = vec!["protocol-b/1.0".to_string(), "protocol-a/1.0".to_string()];
                p
            },
        ];
        
        // Add peers to the discovery cache
        {
            let now = std::time::Instant::now();
            let mut peers_lock = discovery.peers.lock().unwrap();
            for peer in peers {
                peers_lock.insert(peer.id.clone(), (peer, now));
            }
        }
        
        // Find all peers
        let all_peers = discovery.find_peers(None).await.unwrap();
        assert_eq!(all_peers.len(), 3);
        
        // Find peers with protocol-a
        let protocol_a_peers = discovery.find_peers(Some("protocol-a/1.0".to_string())).await.unwrap();
        assert_eq!(protocol_a_peers.len(), 2);
        
        // Find peers with protocol-b
        let protocol_b_peers = discovery.find_peers(Some("protocol-b/1.0".to_string())).await.unwrap();
        assert_eq!(protocol_b_peers.len(), 1);
        
        // Find peers with non-existent protocol
        let none_peers = discovery.find_peers(Some("non-existent".to_string())).await.unwrap();
        assert_eq!(none_peers.len(), 0);
    }

    /// Test announcing peer info
    #[tokio::test]
    async fn test_announce() {
        // Create a discovery instance
        let mut discovery = MdnsDiscovery::new();
        
        // Create peer info to announce
        let peer_info = create_test_peer(42, 4242);
        
        // Announce the peer
        let result = discovery.announce(peer_info.clone()).await;
        assert!(result.is_ok());
        
        // Verify that the peer info was stored
        assert_eq!(discovery.own_info.as_ref().unwrap().id, peer_info.id);
    }

    /// Test discovery events
    #[test]
    fn test_discovery_events() {
        // Create test peer info
        let peer1 = create_test_peer(1, 1234);
        let peer2 = create_test_peer(2, 2345);
        
        // Test event variants
        let events = vec![
            DiscoveryEvent::PeerDiscovered(peer1.clone()),
            DiscoveryEvent::PeerUpdated(peer1.clone()),
            DiscoveryEvent::PeerExpired(peer1.id.clone()),
            DiscoveryEvent::Error(crate::discovery::DiscoveryError::BootstrapError("Test error".to_string())),
        ];
        
        for event in events {
            match event {
                DiscoveryEvent::PeerDiscovered(p) => assert_eq!(p.id, peer1.id),
                DiscoveryEvent::PeerUpdated(p) => assert_eq!(p.id, peer1.id),
                DiscoveryEvent::PeerExpired(id) => assert_eq!(id, peer1.id),
                DiscoveryEvent::Error(_) => {},
            }
        }
    }
} 