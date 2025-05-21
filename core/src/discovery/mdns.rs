//! mDNS-based peer discovery
//!
//! This module provides peer discovery via multicast DNS for local network peers.

use crate::discovery::interface::{Discovery, DiscoveryEvent, DiscoveryError, PeerInfo};
use std::future::Future;
use std::pin::Pin;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::{SinkExt, StreamExt};
use libp2p::core::multiaddr::{Multiaddr, Protocol};
use libp2p::core::PeerId;
use libp2p::mdns::{Mdns, MdnsConfig, MdnsEvent};
use libp2p::swarm::{Swarm, SwarmEvent};
use tokio::sync::Mutex as TokioMutex;
use tokio::time::interval;

/// Time to wait for mDNS query responses
const MDNS_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Controls how often we refresh our peer list
const PEER_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Configuration for mDNS discovery
#[derive(Debug, Clone)]
pub struct MdnsDiscoveryConfig {
    /// TTL for mDNS records in seconds
    pub ttl: u32,
    
    /// Service name for discovery
    pub service_name: String,
    
    /// Maximum number of events to buffer
    pub event_buffer_size: usize,
    
    /// Peer expiration time in seconds
    pub peer_expiration: u64,
}

impl Default for MdnsDiscoveryConfig {
    fn default() -> Self {
        Self {
            ttl: 120,
            service_name: "decentralized-stream".to_string(),
            event_buffer_size: 32,
            peer_expiration: 300, // 5 minutes
        }
    }
}

/// mDNS-based peer discovery
pub struct MdnsDiscovery {
    /// Configuration
    config: MdnsDiscoveryConfig,
    
    /// Channel for events
    event_sender: Option<Sender<DiscoveryEvent>>,
    event_receiver: Option<Receiver<DiscoveryEvent>>,
    
    /// Known peers
    peers: Arc<Mutex<HashMap<Vec<u8>, (PeerInfo, Instant)>>>,
    
    /// Is the discovery service running
    running: bool,
    
    /// Our own peer info for announcements
    own_info: Option<PeerInfo>,
    
    /// Task handle for background discovery
    #[allow(dead_code)]
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl MdnsDiscovery {
    /// Create a new mDNS discovery service with default configuration
    pub fn new() -> Self {
        Self::with_config(MdnsDiscoveryConfig::default())
    }
    
    /// Create a new mDNS discovery service with custom configuration
    pub fn with_config(config: MdnsDiscoveryConfig) -> Self {
        let (tx, rx) = channel(config.event_buffer_size);
        
        Self {
            config,
            event_sender: Some(tx),
            event_receiver: Some(rx),
            peers: Arc::new(Mutex::new(HashMap::new())),
            running: false,
            own_info: None,
            task_handle: None,
        }
    }
    
    /// Convert a PeerId to a byte vector
    fn peer_id_to_bytes(peer_id: &PeerId) -> Vec<u8> {
        peer_id.to_bytes()
    }
    
    /// Convert a byte vector to a PeerId
    fn bytes_to_peer_id(bytes: &[u8]) -> Result<PeerId, DiscoveryError> {
        PeerId::from_bytes(bytes)
            .map_err(|e| DiscoveryError::MdnsError(format!("Invalid peer ID: {}", e)))
    }
    
    /// Convert a Multiaddr to a SocketAddr if possible
    fn multiaddr_to_socket_addr(addr: &Multiaddr) -> Option<SocketAddr> {
        let mut iter = addr.iter();
        
        match (iter.next(), iter.next()) {
            (Some(Protocol::Ip4(ip)), Some(Protocol::Tcp(port))) => {
                Some(SocketAddr::new(ip.into(), port))
            },
            (Some(Protocol::Ip6(ip)), Some(Protocol::Tcp(port))) => {
                Some(SocketAddr::new(ip.into(), port))
            },
            _ => None,
        }
    }
    
    /// Convert a PeerInfo to a libp2p PeerId
    fn peer_info_to_peer_id(info: &PeerInfo) -> Result<PeerId, DiscoveryError> {
        Self::bytes_to_peer_id(&info.id)
    }
    
    /// Start the background discovery task
    async fn start_discovery_task(
        peers: Arc<Mutex<HashMap<Vec<u8>, (PeerInfo, Instant)>>>,
        event_sender: Sender<DiscoveryEvent>,
        config: MdnsDiscoveryConfig,
    ) -> Result<tokio::task::JoinHandle<()>, DiscoveryError> {
        // Create a keypair for our local peer ID
        let local_key = libp2p::identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        
        // Create an mDNS service
        let mdns_config = MdnsConfig::new().map_err(|e| DiscoveryError::MdnsError(e.to_string()))?;
        
        // Create a swarm with the mDNS behavior
        let mut swarm = libp2p::SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(Default::default(), libp2p::noise::Config::new, libp2p::yamux::Config::default)
            .map_err(|e| DiscoveryError::MdnsError(format!("Failed to build transport: {}", e)))?
            .with_behaviour(|_| Mdns::new(mdns_config).map_err(|e| e.to_string()))
            .map_err(|e| DiscoveryError::MdnsError(e))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();
        
        // Start listening
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
            .map_err(|e| DiscoveryError::MdnsError(format!("Failed to listen: {}", e)))?;
        
        // Clone needed values for the task
        let peers_clone = peers.clone();
        let mut event_sender = event_sender;
        let peer_expiration = Duration::from_secs(config.peer_expiration);
        
        // Spawn the discovery task
        let task = tokio::spawn(async move {
            let mut refresh_interval = interval(PEER_REFRESH_INTERVAL);
            let mut expired_peers = HashSet::new();
            
            loop {
                tokio::select! {
                    event = swarm.select_next_some() => {
                        match event {
                            SwarmEvent::Behaviour(MdnsEvent::Discovered(peers_map)) => {
                                for (peer_id, addrs) in peers_map {
                                    // Skip our own peer ID
                                    if peer_id == local_peer_id {
                                        continue;
                                    }
                                    
                                    let peer_id_bytes = Self::peer_id_to_bytes(&peer_id);
                                    let socket_addrs: Vec<SocketAddr> = addrs.iter()
                                        .filter_map(|addr| Self::multiaddr_to_socket_addr(addr))
                                        .collect();
                                    
                                    if socket_addrs.is_empty() {
                                        continue;
                                    }
                                    
                                    let peer_info = PeerInfo {
                                        id: peer_id_bytes.clone(),
                                        addresses: socket_addrs,
                                        protocols: vec!["libp2p/mdns".to_string()],
                                        metadata: HashMap::new(),
                                    };
                                    
                                    let now = Instant::now();
                                    let mut peers = peers_clone.lock().unwrap();
                                    
                                    if peers.contains_key(&peer_id_bytes) {
                                        peers.insert(peer_id_bytes.clone(), (peer_info.clone(), now));
                                        
                                        // Emit peer updated event
                                        let _ = event_sender.send(DiscoveryEvent::PeerUpdated(peer_info)).await;
                                    } else {
                                        peers.insert(peer_id_bytes.clone(), (peer_info.clone(), now));
                                        
                                        // Emit peer discovered event
                                        let _ = event_sender.send(DiscoveryEvent::PeerDiscovered(peer_info)).await;
                                    }
                                }
                            },
                            SwarmEvent::Behaviour(MdnsEvent::Expired(peers_map)) => {
                                for (peer_id, _) in peers_map {
                                    let peer_id_bytes = Self::peer_id_to_bytes(&peer_id);
                                    let mut peers = peers_clone.lock().unwrap();
                                    
                                    if peers.remove(&peer_id_bytes).is_some() {
                                        // Emit peer expired event
                                        let _ = event_sender.send(DiscoveryEvent::PeerExpired(peer_id_bytes)).await;
                                    }
                                }
                            },
                            _ => {}
                        }
                    },
                    _ = refresh_interval.tick() => {
                        // Check for expired peers
                        let now = Instant::now();
                        expired_peers.clear();
                        
                        {
                            let peers = peers_clone.lock().unwrap();
                            for (id, (_, last_seen)) in peers.iter() {
                                if now.duration_since(*last_seen) > peer_expiration {
                                    expired_peers.insert(id.clone());
                                }
                            }
                        }
                        
                        // Remove expired peers
                        if !expired_peers.is_empty() {
                            let mut peers = peers_clone.lock().unwrap();
                            for id in &expired_peers {
                                peers.remove(id);
                                // Emit peer expired event
                                let _ = event_sender.send(DiscoveryEvent::PeerExpired(id.clone())).await;
                            }
                        }
                    }
                }
            }
        });
        
        Ok(task)
    }
}

impl Discovery for MdnsDiscovery {
    fn start(&mut self) -> Result<(), DiscoveryError> {
        if self.running {
            return Ok(());
        }
        
        // Ensure we have event channels
        if self.event_sender.is_none() || self.event_receiver.is_none() {
            let (tx, rx) = channel(self.config.event_buffer_size);
            self.event_sender = Some(tx);
            self.event_receiver = Some(rx);
        }
        
        // Start the discovery task
        let peers = self.peers.clone();
        let event_sender = self.event_sender.as_ref().unwrap().clone();
        let config = self.config.clone();
        
        let task_handle = tokio::runtime::Handle::current().block_on(async {
            Self::start_discovery_task(peers, event_sender, config).await
        })?;
        
        self.task_handle = Some(task_handle);
        self.running = true;
        
        Ok(())
    }
    
    fn stop(&mut self) -> Result<(), DiscoveryError> {
        if !self.running {
            return Ok(());
        }
        
        // Cancel the discovery task
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
        
        self.running = false;
        Ok(())
    }
    
    fn announce(&mut self, info: PeerInfo) -> Pin<Box<dyn Future<Output = Result<(), DiscoveryError>> + Send>> {
        self.own_info = Some(info);
        
        // mDNS automatically announces our presence, so we don't need to do anything here
        Box::pin(async { Ok(()) })
    }
    
    fn lookup_peer(&mut self, id: &[u8]) -> Pin<Box<dyn Future<Output = Result<Option<PeerInfo>, DiscoveryError>> + Send>> {
        // Look up in our local cache first
        let peer_id = id.to_vec();
        let peers = self.peers.clone();
        
        Box::pin(async move {
            let peers = peers.lock().unwrap();
            if let Some((info, _)) = peers.get(&peer_id) {
                return Ok(Some(info.clone()));
            }
            
            // Not found in cache
            Ok(None)
        })
    }
    
    fn find_peers(&mut self, predicate: Option<String>) -> Pin<Box<dyn Future<Output = Result<Vec<PeerInfo>, DiscoveryError>> + Send>> {
        let peers = self.peers.clone();
        let protocol = predicate;
        
        Box::pin(async move {
            let mut result = Vec::new();
            let peers = peers.lock().unwrap();
            
            for (_, (info, _)) in peers.iter() {
                if let Some(ref proto) = protocol {
                    if info.protocols.contains(&proto) {
                        result.push(info.clone());
                    }
                } else {
                    result.push(info.clone());
                }
            }
            
            Ok(result)
        })
    }
    
    fn next_event(&mut self) -> Pin<Box<dyn Future<Output = Option<DiscoveryEvent>> + Send>> {
        if let Some(receiver) = &mut self.event_receiver {
            let mut rx = receiver.clone();
            Box::pin(async move { rx.next().await })
        } else {
        Box::pin(async { None })
        }
    }
    
    fn is_running(&self) -> bool {
        self.running
    }
} 