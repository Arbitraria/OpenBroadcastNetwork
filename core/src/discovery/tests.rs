//! Tests for the discovery implementations

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use crate::discovery::{
        interface::{Discovery, DiscoveryError, DiscoveryEvent, PeerInfo, ConnectionStatus},
    };
    use std::{
        net::{SocketAddr, IpAddr, Ipv4Addr},
        time::{SystemTime, Duration},
        collections::HashMap,
        sync::Arc,
    };
    use tokio::sync::Mutex;
    use std::str::FromStr;

    /// Create a test peer info instance
    fn create_test_peer_info(id: Vec<u8>, addr: SocketAddr) -> PeerInfo {
        let now = SystemTime::now();
        PeerInfo {
            id,
            addresses: vec![addr],
            protocols: vec!["test/1.0".to_string()],
            metadata: HashMap::new(),
            first_seen: Some(now),
            last_seen: Some(now),
            connection_status: ConnectionStatus::Disconnected,
        }
    }

    /// A mock discovery implementation for testing
    pub struct MockDiscovery {
        peers: Arc<Mutex<HashMap<Vec<u8>, PeerInfo>>>,
        running: bool,
    }

    impl MockDiscovery {
        /// Create a new MockDiscovery instance
        pub fn new() -> Self {
            MockDiscovery {
                peers: Arc::new(Mutex::new(HashMap::new())),
                running: false,
            }
        }
        
        /// Add a peer to the mock discovery
        pub async fn add_peer(&mut self, info: PeerInfo) -> Result<(), DiscoveryError> {
            let mut peers = self.peers.lock().await;
            peers.insert(info.id.clone(), info);
            Ok(())
        }
        
        /// Helper to create a test peer
        pub fn create_test_peer(id: u8, port: u16) -> PeerInfo {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
            let now = SystemTime::now();
            PeerInfo {
                id: vec![id],
                addresses: vec![addr],
                protocols: vec!["/test/1.0.0".to_string()],
                metadata: HashMap::new(),
                first_seen: Some(now),
                last_seen: Some(now),
                connection_status: ConnectionStatus::Disconnected,
            }
        }
    }

    #[async_trait::async_trait]
    impl Discovery for MockDiscovery {
        async fn start(&mut self) -> Result<(), DiscoveryError> {
            self.running = true;
            Ok(())
        }

        async fn stop(&mut self) -> Result<(), DiscoveryError> {
            self.running = false;
            Ok(())
        }

        async fn announce(&mut self, info: PeerInfo) -> Result<(), DiscoveryError> {
            let mut peers = self.peers.lock().await;
            peers.insert(info.id.clone(), info);
            Ok(())
        }

        async fn lookup_peer(&self, id: &[u8]) -> Result<Option<PeerInfo>, DiscoveryError> {
            let peers = self.peers.lock().await;
            Ok(peers.get(id).cloned())
        }

        async fn find_peers(&self, criteria: &str) -> Result<Vec<PeerInfo>, DiscoveryError> {
            let peers = self.peers.lock().await;
            
            // If criteria is empty, return all peers
            if criteria.is_empty() {
                return Ok(peers.values().cloned().collect());
            }
            
            // Otherwise, filter peers by protocol
            let criteria = criteria.to_lowercase();
            let filtered_peers = peers.values()
                .filter(|peer| {
                    // Check if any protocol matches the criteria (case-insensitive)
                    peer.protocols.iter()
                        .any(|p| p.to_lowercase().contains(&criteria))
                })
                .cloned()
                .collect();
                
            Ok(filtered_peers)
        }

        async fn next_event(&mut self, _timeout: Option<Duration>) -> Result<Option<DiscoveryEvent>, DiscoveryError> {
            // Not implemented for mock
            Ok(None)
        }

        fn is_running(&self) -> bool {
            self.running
        }

        fn local_peer_id(&self) -> Option<Vec<u8>> {
            // Return a fixed peer ID for testing
            Some(vec![0x01, 0x02, 0x03, 0x04])
        }


        async fn discovered_peers(&self) -> Result<Vec<PeerInfo>, DiscoveryError> {
            let peers = self.peers.lock().await;
            Ok(peers.values().cloned().collect())
        }
    }

    /// Create a test peer with the given ID and port
    fn create_test_peer(id: u8, port: u16) -> PeerInfo {
        let addr = format!("127.0.0.1:{}", port).parse().unwrap();
        create_test_peer_info(vec![id], addr)
    }
    
    /// Helper function to create a socket address from IP and port
    fn create_socket_addr(ip: &str, port: u16) -> SocketAddr {
        format!("{}:{}", ip, port).parse().unwrap()
    }

    // Commenting out mDNS tests since they require mDNS service
    // #[tokio::test]
    // async fn test_mdns_discovery_integration() -> Result<(), DiscoveryError> {
    //     // This test requires an mDNS service to be running
    //     // and is therefore excluded from normal test runs
    //     Ok(())
    // }
    
    #[tokio::test]
    async fn test_mock_discovery_announce_lookup() {
        let mut discovery = MockDiscovery::new();
        
        // Test announce
        let peer = create_test_peer(1, 1234);
        discovery.start().await.expect("Failed to start discovery");
        discovery.announce(peer.clone()).await.expect("Failed to announce peer");
        
        // Test lookup
        let found = discovery.lookup_peer(&peer.id).await.expect("Lookup failed");
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.id, peer.id);
        assert_eq!(found.addresses, peer.addresses);
        
        // Test non-existent peer
        let not_found = discovery.lookup_peer(&[99]).await.expect("Lookup failed");
        assert!(not_found.is_none());
        
        // Test stop
        discovery.stop().await.expect("Failed to stop discovery");
        assert!(!discovery.is_running());
    }

    // Commenting out mDNS lifecycle test since it requires mDNS service
    // #[tokio::test]
    // async fn test_mdns_discovery_lifecycle() -> Result<(), DiscoveryError> {
    //     // This test requires an mDNS service to be running
    //     // and is therefore excluded from normal test runs
    //     Ok(())
    // }

    // Commenting out mDNS creation test since it requires mDNS module
    // #[test]
    // fn test_mdns_discovery_creation() {
    //     // This test requires the mDNS module
    //     // and is therefore excluded from normal test runs
    // }

    #[test]
    fn test_socket_addr_parsing() {
        // Test valid socket address
        let addr = "127.0.0.1:8080".parse::<SocketAddr>();
        assert!(addr.is_ok());
        assert_eq!(addr.unwrap().port(), 8080);
        
        // Test invalid socket address
        let invalid_addr = "not_an_address".parse::<SocketAddr>();
        assert!(invalid_addr.is_err());
    }

    /// Test basic peer lookup operations
    #[tokio::test]
    async fn test_peer_lookup() -> Result<(), DiscoveryError> {
        // Create a mock discovery instance
        let mut discovery = MockDiscovery::new();
        
        // Create peer info
        let peer = MockDiscovery::create_test_peer(1, 8080);
        let id = peer.id.clone();
        
        // Start the discovery service
        discovery.start().await?;
        
        // Announce the peer
        discovery.announce(peer).await?;
        
        // Lookup the peer
        let found_peer = discovery.lookup_peer(&id).await?;
        assert!(found_peer.is_some());
        let found_peer = found_peer.unwrap();
        assert_eq!(found_peer.id, id);
        assert_eq!(found_peer.addresses.len(), 1);
        assert_eq!(found_peer.addresses[0].port(), 8080);
        
        // Try looking up a non-existent peer
        let not_found = discovery.lookup_peer(&[99]).await?;
        assert!(not_found.is_none());
        
        // Stop the discovery service
        discovery.stop().await?;
        
        Ok(())
    }

    /// Test finding peers with protocol matching
    #[tokio::test]
    async fn test_find_peers() -> Result<(), DiscoveryError> {
        // Create a mock discovery instance
        let mut discovery = MockDiscovery::new();
        
        // Start the discovery service
        discovery.start().await?;
        
        // Create test peers with different protocols
        let mut peer1 = MockDiscovery::create_test_peer(1, 1234);
        let mut peer2 = MockDiscovery::create_test_peer(2, 2345);
        let mut peer3 = MockDiscovery::create_test_peer(3, 3456);
        
        // Set protocols for peers
        peer1.protocols = vec!["protocol-a/1.0".to_string()];
        peer2.protocols = vec!["protocol-b/1.0".to_string()];
        peer3.protocols = vec!["protocol-a/1.0".to_string(), "protocol-b/1.0".to_string()];
        
        // Announce the peers
        discovery.announce(peer1).await?;
        discovery.announce(peer2).await?;
        discovery.announce(peer3).await?;
        
        // Find all peers (empty criteria should return all)
        let all_peers = discovery.find_peers("").await?;
        assert_eq!(all_peers.len(), 3);
        
        // Find peers with protocol-a
        let proto_a_peers = discovery.find_peers("protocol-a/1.0").await?;
        assert_eq!(proto_a_peers.len(), 2);
        
        // Find peers with protocol-b
        let proto_b_peers = discovery.find_peers("protocol-b/1.0").await?;
        assert_eq!(proto_b_peers.len(), 2);
        
        // Find with non-existent protocol
        let no_peers = discovery.find_peers("nonexistent").await?;
        assert!(no_peers.is_empty());
        
        // Stop the discovery service
        discovery.stop().await?;
        
        Ok(())
    }
    
    /// Test announcing peer info
    #[tokio::test]
    async fn test_announce() -> Result<(), DiscoveryError> {
        // Create a mock discovery instance
        let mut discovery = MockDiscovery::new();
        
        // Start the discovery service
        discovery.start().await?;
        
        // Create peer info to announce
        let peer_info = MockDiscovery::create_test_peer(42, 4242);
        let peer_id = peer_info.id.clone();
        
        // Announce the peer
        discovery.announce(peer_info).await?;
        
        // Verify that the peer info was stored by looking it up
        let found_peer = discovery.lookup_peer(&peer_id).await?;
        assert!(found_peer.is_some());
        assert_eq!(found_peer.unwrap().id, peer_id);
        
        // Stop the discovery service
        discovery.stop().await?;
        
        Ok(())
    }

    /// Test discovery events
    #[test]
    fn test_discovery_events() {
        let peer1 = create_test_peer(1, 1234);
        
        // Test event variants
        let events = vec![
            DiscoveryEvent::PeerDiscovered(peer1.clone()),
            DiscoveryEvent::PeerUpdated(peer1.clone()),
            DiscoveryEvent::PeerExpired(peer1.id.clone()),
            DiscoveryEvent::Error("Test error".to_string()),
            DiscoveryEvent::PeerConnectionStatusChanged {
                peer_id: peer1.id.clone(),
                status: ConnectionStatus::Connected,
                error: None,
            },
            DiscoveryEvent::ServiceStarted,
            DiscoveryEvent::ServiceStopped,
        ];
        
        for event in events {
            match event {
                DiscoveryEvent::PeerDiscovered(p) => assert_eq!(p.id, peer1.id),
                DiscoveryEvent::PeerUpdated(p) => assert_eq!(p.id, peer1.id),
                DiscoveryEvent::PeerExpired(id) => assert_eq!(id, peer1.id),
                DiscoveryEvent::Error(_) => {},
                DiscoveryEvent::PeerConnectionStatusChanged { peer_id, status, error } => {
                    assert_eq!(peer_id, peer1.id);
                    assert_eq!(status, ConnectionStatus::Connected);
                    assert!(error.is_none());
                },
                DiscoveryEvent::ServiceStarted => {},
                DiscoveryEvent::ServiceStopped => {},
            }
        }
    }
} 