//! Tests for the discovery implementations

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        discovery::{
            mdns::{MdnsDiscovery, MdnsDiscoveryConfig},
            Discovery, DiscoveryEvent, DiscoveryError,
        },
        transport::Transport,
    };
    use crate::discovery::PeerInfo;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        str::FromStr,
        time::Duration,
        collections::HashMap,
    };
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

    /// Create a test peer with the given ID and port
    fn create_test_peer(id: u8, port: u16) -> PeerInfo {
        let id = vec![id];
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        create_test_peer_info(id, addr)
    }

    #[tokio::test]
    async fn test_mdns_discovery_integration() -> Result<(), DiscoveryError> {
        // This test would normally require an mDNS service to be running
        // For now, we'll just test the basic functionality without network calls
        
        let mut discovery = MdnsDiscovery::new();
        
        // Test with a mock peer ID
        let peer_id = vec![1, 2, 3, 4];
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let peer_info = create_test_peer_info(peer_id.clone(), peer_addr);
        
        // Test announcing a peer
        discovery.announce(peer_info.clone()).await?;
        
        // Test looking up the peer
        let found_peer = discovery.lookup_peer(&peer_id).await?;
        assert!(found_peer.is_some());
        assert_eq!(found_peer.unwrap().id, peer_id);
        
        // Test finding peers
        let peers = discovery.find_peers(None).await?;
        assert!(!peers.is_empty());
        
        // Test with a predicate that won't match
        let no_peers = discovery.find_peers(Some("nonexistent".to_string())).await?;
        assert!(no_peers.is_empty());
        
        Ok(())
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

    #[tokio::test]
    async fn test_mdns_discovery_lifecycle() -> Result<(), DiscoveryError> {
        let config = MdnsDiscoveryConfig {
            service_name: "_test-service._udp".to_string(),
            ttl: 60,
            event_buffer_size: 10,
            peer_expiration: 300,
        };
        
        let mut discovery = MdnsDiscovery::with_config(config);
        
        // Test starting
        discovery.start()?;
        assert!(discovery.is_running());
        
        // Test with a peer
        let peer_id = vec![1, 2, 3, 4];
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let peer_info = create_test_peer_info(peer_id.clone(), peer_addr);
        
        // Test announcing the peer
        discovery.announce(peer_info).await?;
        
        // Test looking up the peer
        let found_peer = discovery.lookup_peer(&peer_id).await?;
        assert!(found_peer.is_some());
        
        // Test stopping
        discovery.stop()?;
        assert!(!discovery.is_running());
        
        Ok(())
    }

    /// Test creating and configuring the mDNS discovery
    #[test]
    fn test_mdns_discovery_creation() {
        // Test default config
        let discovery = MdnsDiscovery::new();
        assert!(!discovery.is_running());
        
        // Test with custom config
        let config = MdnsDiscoveryConfig {
            service_name: "_test-service._udp".to_string(),
            ttl: 60,
            event_buffer_size: 16,
            peer_expiration: 300,
        };
        
        let discovery = MdnsDiscovery::with_config(config);
        assert!(!discovery.is_running());
    }

    /// Test basic peer lookup operations
    #[tokio::test]
    async fn test_peer_lookup() {
        // Create a mDNS discovery instance
        let mut discovery = MdnsDiscovery::new();
        
        // Create peer info
        let id = vec![1, 2, 3, 4];
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();
        let info = create_test_peer_info(id.clone(), addr);
        
        // Announce the peer
        discovery.announce(info.clone()).await.unwrap();
        
        // Lookup the peer
        let found_peer = discovery.lookup_peer(&id).await.unwrap();
        assert!(found_peer.is_some());
        let found_peer = found_peer.unwrap();
        assert_eq!(found_peer.id, id);
        assert_eq!(found_peer.addresses.len(), 1);
        assert_eq!(found_peer.addresses[0].port(), 8080);
        
        // Try looking up a non-existent peer
        let not_found = discovery.lookup_peer(&[99]).await.unwrap();
        assert!(not_found.is_none());
    }

    /// Test finding peers with protocol matching
    #[tokio::test]
    async fn test_find_peers() -> Result<(), DiscoveryError> {
        // Create an mDNS discovery instance
        let mut discovery = MdnsDiscovery::new();
        
        // Add several test peers with different protocols
        let peer1 = create_test_peer(1, 1234);
        let mut peer2 = create_test_peer(2, 2345);
        peer2.protocols = vec!["protocol-a/1.0".to_string()];
        let mut peer3 = create_test_peer(3, 3456);
        peer3.protocols = vec!["protocol-b/1.0".to_string(), "protocol-a/1.0".to_string()];
        
        // Announce the peers
        discovery.announce(peer1.clone()).await?;
        discovery.announce(peer2.clone()).await?;
        discovery.announce(peer3.clone()).await?;
        
        // Find all peers
        let all_peers = discovery.find_peers(None).await?;
        assert!(!all_peers.is_empty());
        
        // Find peers with protocol-a
        let proto_a_peers = discovery.find_peers(Some("protocol-a/1.0".to_string())).await?;
        assert_eq!(proto_a_peers.len(), 2);
        
        // Find peers with protocol-b
        let proto_b_peers = discovery.find_peers(Some("protocol-b/1.0".to_string())).await?;
        assert_eq!(proto_b_peers.len(), 1);
        
        // Find with non-existent protocol
        let no_peers = discovery.find_peers(Some("nonexistent".to_string())).await?;
        assert!(no_peers.is_empty());
        
        Ok(())
    }
    
    /// Test announcing peer info
    #[tokio::test]
    async fn test_announce() -> Result<(), DiscoveryError> {
        // Create a discovery instance
        let mut discovery = MdnsDiscovery::new();
        
        // Create peer info to announce
        let peer_info = create_test_peer(42, 4242);
        
        // Announce the peer
        discovery.announce(peer_info.clone()).await?;
        
        // Verify that the peer info was stored by looking it up
        let found_peer = discovery.lookup_peer(&peer_info.id).await?;
        assert!(found_peer.is_some());
        assert_eq!(found_peer.unwrap().id, peer_info.id);
        
        Ok(())
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