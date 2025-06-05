//! DHT-based peer discovery
//!
//! This module provides peer discovery via a Distributed Hash Table (Kademlia).
//! It integrates with libp2p's Kademlia implementation for real peer discovery.

use crate::discovery::interface::{Discovery, DiscoveryEvent, DiscoveryError, PeerInfo};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::net::SocketAddr;

use async_trait::async_trait;
use futures::channel::mpsc::{self, Sender, Receiver};
use futures::lock::Mutex;
use futures::{StreamExt, SinkExt};
use libp2p::core::multiaddr::Multiaddr;
use libp2p::kad::{QueryId, Behaviour as Kademlia, Event as KademliaEvent, store::MemoryStore, Config as KademliaConfig, QueryResult};
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::identity::Keypair;
use libp2p::{PeerId, Transport, SwarmBuilder};
use libp2p::tcp::Config as GenTcpConfig;
use libp2p::noise;
use tracing::{debug, warn, error};

/// Configuration for DHT discovery
#[derive(Debug, Clone)]
pub struct DhtConfig {
    /// Whether this peer is a bootstrap node for the DHT
    pub is_bootstrap: bool,
    /// How often to republish peer information (in seconds)
    pub republish_interval: u64,
    /// How often to refresh routing table (in seconds)
    pub refresh_interval: u64,
    /// Replication factor (k-parameter in Kademlia)
    pub replication_factor: u8,
}

/// Time between periodic DHT refreshes
const DHT_REFRESH_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// Time between provider announcements
const PROVIDER_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(120); // 2 minutes

/// Expiration time for DHT records
const RECORD_TTL: Duration = Duration::from_secs(7200); // 2 hours

/// Peer expiration time
const PEER_EXPIRATION: Duration = Duration::from_secs(3600); // 1 hour

/// Configuration for DHT discovery
#[derive(Debug, Clone)]
pub struct DhtDiscoveryConfig {
    /// Service protocol name
    pub protocol_name: String,
    
    /// Maximum number of events to buffer
    pub event_buffer_size: usize,
    
    /// Bootstrap peers to connect to
    pub bootstrap_peers: Vec<Multiaddr>,
    
    /// DHT record TTL in seconds
    pub record_ttl: u64,
    
    /// Peer record expiration time in seconds
    pub peer_expiration: u64,
}

impl Default for DhtDiscoveryConfig {
    fn default() -> Self {
        Self {
            protocol_name: "/OpenBroadcastNetwork/1.0.0".to_string(),
            event_buffer_size: 32,
            bootstrap_peers: Vec::new(),
            record_ttl: 7200, // 2 hours
            peer_expiration: 3600, // 1 hour
        }
    }
}

/// DHT-based peer discovery
pub struct DhtDiscovery {
    /// Configuration
    config: DhtDiscoveryConfig,
    
    /// Channel for events
    event_sender: Option<Sender<DiscoveryEvent>>,
    event_receiver: Option<Receiver<DiscoveryEvent>>,
    
    /// Known peers
    peers: Arc<Mutex<HashMap<Vec<u8>, (PeerInfo, Instant)>>>,
    
    /// Is the discovery service running
    running: Arc<AtomicBool>,
    
    /// Our own peer info for announcements
    own_info: Option<PeerInfo>,
    
    /// Active queries
    active_queries: Arc<Mutex<HashMap<QueryId, QueryType>>>,
    
    /// Our local peer ID
    local_peer_id: Option<PeerId>,
    
    /// Libp2p swarm for network operations
    swarm: Arc<Mutex<Option<Swarm<Kademlia<MemoryStore>>>>>,
    
    /// Task handle for background discovery
    task_handle: Option<tokio::task::JoinHandle<()>>,
    
    /// Keypair for authentication
    keypair: Option<Keypair>,
}

/// Types of DHT queries we can perform
#[derive(Debug, Clone)]
enum QueryType {
    /// Searching for a specific peer
    FindPeer(Vec<u8>),
    /// Searching for closest peers to a key
    FindClosestPeers(String),
    /// Getting a record from the DHT
    GetRecord(String),
    /// Putting a record to the DHT
    PutRecord(String),
    /// Refreshing the DHT routing table
    Refresh,
}

impl DhtDiscovery {
    /// Create a new DHT discovery instance
    pub fn new(config: DhtDiscoveryConfig) -> Self {
        Self::with_config(config)
    }
    
    /// Create a new DHT discovery instance with the given configuration
    pub fn with_config(config: DhtDiscoveryConfig) -> Self {
        let (event_sender, event_receiver) = mpsc::channel(32);
        
        DhtDiscovery {
            config,
            event_sender: Some(event_sender),
            event_receiver: Some(event_receiver),
            peers: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
            own_info: None,
            active_queries: Arc::new(Mutex::new(HashMap::new())),
            local_peer_id: None,
            swarm: Arc::new(Mutex::new(None)),
            task_handle: None,
            keypair: None,
        }
    }
    
    /// Initialize the libp2p swarm with Kademlia behavior
    async fn init_swarm(&mut self) -> Result<(), DiscoveryError> {
        // Generate or use existing keypair
        let keypair = self.keypair.take().unwrap_or_else(|| Keypair::generate_ed25519());
        let peer_id = keypair.public().to_peer_id();
        self.local_peer_id = Some(peer_id);
        
        // Create Kademlia behavior
        let store = MemoryStore::new(peer_id);
        let mut kademlia = Kademlia::new(peer_id, store);
        
        // Set Kademlia to server mode to participate in the DHT
        kademlia.set_mode(Some(libp2p::kad::Mode::Server));
        
        // Add bootstrap peers to Kademlia
        for addr in &self.config.bootstrap_peers {
            if let Some(peer_id) = extract_peer_id_from_multiaddr(addr) {
                kademlia.add_address(&peer_id, addr.clone());
            }
        }
        
        // Build the swarm
        let swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                GenTcpConfig::default().nodelay(true),
                noise::Config::new,
                libp2p::yamux::Config::default
            )
            .map_err(|e| DiscoveryError::Other(format!("Failed to build swarm: {}", e)))?
            .with_behaviour(|_| kademlia)
            .map_err(|e| DiscoveryError::Other(format!("Failed to create behavior: {}", e)))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();
        
        // Store the swarm
        let mut swarm_lock = self.swarm.lock().await;
        *swarm_lock = Some(swarm);
        
        Ok(())
    }
    
    /// Run the swarm event loop
    async fn run_swarm_loop(
        swarm: Arc<Mutex<Option<Swarm<Kademlia<MemoryStore>>>>>,
        event_sender: Sender<DiscoveryEvent>,
        peers: Arc<Mutex<HashMap<Vec<u8>, (PeerInfo, Instant)>>>,
        active_queries: Arc<Mutex<HashMap<QueryId, QueryType>>>,
        running: Arc<AtomicBool>,
    ) {
        let mut event_sender = event_sender;
        
        loop {
            if !running.load(Ordering::SeqCst) {
                break;
            }
            
            // Get the swarm
            let mut swarm_opt = {
                let mut swarm_lock = swarm.lock().await;
                swarm_lock.take()
            };
            
            if let Some(mut swarm_instance) = swarm_opt {
                // Listen on a random port
                if let Err(e) = swarm_instance.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap()) {
                    error!("Failed to start listening: {}", e);
                }
                
                // Bootstrap the DHT
                if let Err(e) = swarm_instance.behaviour_mut().bootstrap() {
                    debug!("Failed to bootstrap DHT: {}", e);
                }
                
                // Process events
                loop {
                    if !running.load(Ordering::SeqCst) {
                        break;
                    }
                    
                    match swarm_instance.select_next_some().await {
                        SwarmEvent::Behaviour(kad_event) => {
                            Self::handle_kademlia_event(
                                kad_event,
                                &mut event_sender,
                                &peers,
                                &active_queries,
                            ).await;
                        },
                        SwarmEvent::NewListenAddr { address, .. } => {
                            debug!("Listening on: {}", address);
                        },
                        SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                            debug!("Connected to peer: {}", peer_id);
                            
                            // Add peer to our local list
                            let peer_info = PeerInfo {
                                id: peer_id.to_bytes(),
                                addresses: vec![], // Will be populated from other events
                                protocols: vec![],
                                metadata: HashMap::new(),
                                first_seen: Some(std::time::SystemTime::now()),
                                last_seen: Some(std::time::SystemTime::now()),
                                connection_status: crate::discovery::interface::ConnectionStatus::Connected,
                            };
                            
                            // Store the peer
                            {
                                let mut peers_lock = peers.lock().await;
                                peers_lock.insert(peer_id.to_bytes(), (peer_info.clone(), Instant::now()));
                            }
                            
                            // Send event
                            let _ = event_sender.send(DiscoveryEvent::PeerDiscovered(peer_info)).await;
                        },
                        SwarmEvent::ConnectionClosed { peer_id, .. } => {
                            debug!("Disconnected from peer: {}", peer_id);
                            let _ = event_sender.send(DiscoveryEvent::PeerExpired(peer_id.to_bytes())).await;
                        },
                        SwarmEvent::IncomingConnection { .. } => {},
                        SwarmEvent::IncomingConnectionError { .. } => {},
                        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                            if let Some(peer_id) = peer_id {
                                warn!("Failed to connect to peer {}: {}", peer_id, error);
                            }
                        },
                        _ => {}
                    }
                }
                
                // Put the swarm back
                {
                    let mut swarm_lock = swarm.lock().await;
                    *swarm_lock = Some(swarm_instance);
                }
            } else {
                // No swarm available, wait a bit
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    
    /// Handle Kademlia events
    async fn handle_kademlia_event(
        event: KademliaEvent,
        event_sender: &mut Sender<DiscoveryEvent>,
        peers: &Arc<Mutex<HashMap<Vec<u8>, (PeerInfo, Instant)>>>,
        active_queries: &Arc<Mutex<HashMap<QueryId, QueryType>>>,
    ) {
        match event {
            KademliaEvent::OutboundQueryProgressed { id, result, .. } => {
                match result {
                    QueryResult::GetClosestPeers(Ok(ok)) => {
                        debug!("Found {} closest peers", ok.peers.len());
                        
                        for peer_id in ok.peers {
                            let peer_info = PeerInfo {
                                id: peer_id.to_bytes(),
                                addresses: vec![], // Addresses will be populated from other sources
                                protocols: vec![],
                                metadata: HashMap::new(),
                                first_seen: Some(std::time::SystemTime::now()),
                                last_seen: Some(std::time::SystemTime::now()),
                                connection_status: crate::discovery::interface::ConnectionStatus::Disconnected,
                            };
                            
                            // Store the peer
                            {
                                let mut peers_lock = peers.lock().await;
                                peers_lock.insert(peer_id.to_bytes(), (peer_info.clone(), Instant::now()));
                            }
                            
                            // Send discovery event
                            let _ = event_sender.send(DiscoveryEvent::PeerDiscovered(peer_info)).await;
                        }
                    },
                    QueryResult::GetClosestPeers(Err(e)) => {
                        debug!("Failed to get closest peers: {:?}", e);
                    },
                    QueryResult::Bootstrap(Ok(_)) => {
                        debug!("DHT bootstrap completed successfully");
                    },
                    QueryResult::Bootstrap(Err(e)) => {
                        debug!("DHT bootstrap failed: {:?}", e);
                    },
                    _ => {}
                }
                
                // Remove the query from active queries
                {
                    let mut queries_lock = active_queries.lock().await;
                    queries_lock.remove(&id);
                }
            },
            KademliaEvent::RoutingUpdated { peer, addresses, .. } => {
                debug!("Routing updated for peer: {}", peer);
                
                // Convert addresses to SocketAddr
                let socket_addrs: Vec<SocketAddr> = addresses.iter()
                    .filter_map(|addr| extract_socket_addr_from_multiaddr(addr))
                    .collect();
                
                let peer_info = PeerInfo {
                    id: peer.to_bytes(),
                    addresses: socket_addrs,
                    protocols: vec![],
                    metadata: HashMap::new(),
                    first_seen: Some(std::time::SystemTime::now()),
                    last_seen: Some(std::time::SystemTime::now()),
                    connection_status: crate::discovery::interface::ConnectionStatus::Disconnected,
                };
                
                // Store or update the peer
                {
                    let mut peers_lock = peers.lock().await;
                    peers_lock.insert(peer.to_bytes(), (peer_info.clone(), Instant::now()));
                }
                
                // Send event
                let _ = event_sender.send(DiscoveryEvent::PeerUpdated(peer_info)).await;
            },
            _ => {}
        }
    }
}

#[async_trait::async_trait]
impl Discovery for DhtDiscovery {
    async fn start(&mut self) -> Result<(), DiscoveryError> {
        if self.running.load(Ordering::SeqCst) {
            return Err(DiscoveryError::AlreadyRunning);
        }
        
        debug!(target: "dht", "Starting DHT discovery service");
        
        // Initialize the swarm
        self.init_swarm().await?;
        
        // Get the event sender for the background task
        let event_sender = self.event_sender.as_ref()
            .ok_or_else(|| DiscoveryError::Other("No event sender available".into()))?
            .clone();
        
        // Start the background swarm task
        let swarm = self.swarm.clone();
        let peers = self.peers.clone();
        let active_queries = self.active_queries.clone();
        let running = self.running.clone();
        
        let task_handle = tokio::spawn(async move {
            Self::run_swarm_loop(swarm, event_sender, peers, active_queries, running).await;
        });
        
        self.task_handle = Some(task_handle);
        
        // Mark as running
        self.running.store(true, Ordering::SeqCst);
        
        debug!(target: "dht", "DHT discovery service started with peer ID: {:?}", self.local_peer_id);
        Ok(())
    }
    
    async fn stop(&mut self) -> Result<(), DiscoveryError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(DiscoveryError::NotRunning);
        }
        
        debug!(target: "dht", "Stopping DHT discovery service");
        
        // Stop background tasks
        self.running.store(false, Ordering::SeqCst);
        
        // Wait for the background task to complete
        if let Some(handle) = self.task_handle.take() {
            let _ = handle.await;
        }
        
        // Clear the swarm
        {
            let mut swarm_lock = self.swarm.lock().await;
            *swarm_lock = None;
        }
        
        debug!(target: "dht", "DHT discovery service stopped");
        Ok(())
    }
    
    async fn announce(&mut self, info: PeerInfo) -> Result<(), DiscoveryError> {
        let peer_id = PeerId::from_bytes(&info.id)
            .map_err(|e| DiscoveryError::Other(format!("Invalid peer ID: {}", e)))?;
            
        debug!(target: "dht", "Announcing peer: {:?}", peer_id);
        
        // Store our own info
        self.own_info = Some(info.clone());
        
        // Store the peer in our local cache
        let mut peers = self.peers.lock().await;
        peers.insert(info.id.clone(), (info, Instant::now()));
        
        // If we have a swarm, start providing this peer's record in the DHT
        {
            let mut swarm_lock = self.swarm.lock().await;
            if let Some(ref mut swarm) = *swarm_lock {
                // Create a key for this peer
                let key = libp2p::kad::RecordKey::new(&format!("peer:{}", peer_id));
                
                // Start providing this key so other peers can find us
                let query_id = swarm.behaviour_mut().start_providing(key.clone())
                    .map_err(|e| DiscoveryError::Other(format!("Failed to start providing: {}", e)))?;
                    
                // Track this query
                let mut queries = self.active_queries.lock().await;
                queries.insert(query_id, QueryType::PutRecord(format!("peer:{}", peer_id)));
            }
        }
        
        Ok(())
    }
    
    async fn lookup_peer(&self, peer_id: &[u8]) -> Result<Option<PeerInfo>, DiscoveryError> {
        // Check local cache first
        {
            let peers = self.peers.lock().await;
            if let Some((info, _)) = peers.get(peer_id) {
                return Ok(Some(info.clone()));
            }
        }
        
        debug!(target: "dht", "Looking up peer: {:?}", peer_id);
        
        // Perform DHT lookup if we have a swarm
        {
            let mut swarm_lock = self.swarm.lock().await;
            if let Some(ref mut swarm) = *swarm_lock {
                let libp2p_peer_id = PeerId::from_bytes(peer_id)
                    .map_err(|e| DiscoveryError::Other(format!("Invalid peer ID: {}", e)))?;
                
                // Query the DHT for this specific peer
                let query_id = swarm.behaviour_mut().get_closest_peers(libp2p_peer_id);
                
                // Track this query
                let mut queries = self.active_queries.lock().await;
                queries.insert(query_id, QueryType::FindPeer(peer_id.to_vec()));
                
                // Note: The result will come back through the event loop
                // For now, return None and let the caller wait for events
            }
        }
        
        Ok(None)
    }
    
    async fn find_peers(&self, criteria: &str) -> Result<Vec<PeerInfo>, DiscoveryError> {
        debug!(target: "dht", "Finding peers with criteria: {}", criteria);
        
        // First, return any known peers that match the criteria
        let local_results = {
            let peers = self.peers.lock().await;
            peers.values()
                .filter_map(|(info, _)| {
                    if criteria.is_empty() || info.protocols.iter().any(|p| p.contains(criteria)) {
                        Some(info.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        
        // Also trigger a DHT query to find more peers
        {
            let mut swarm_lock = self.swarm.lock().await;
            if let Some(ref mut swarm) = *swarm_lock {
                // Use a random key to find closest peers, which will give us more peers in the network
                let random_key = PeerId::random();
                let query_id = swarm.behaviour_mut().get_closest_peers(random_key);
                
                // Track this query
                let mut queries = self.active_queries.lock().await;
                queries.insert(query_id, QueryType::FindClosestPeers(criteria.to_string()));
            }
        }
        
        Ok(local_results)
    }
    
    async fn next_event(&mut self, timeout: Option<Duration>) -> Result<Option<DiscoveryEvent>, DiscoveryError> {
        if let Some(ref mut receiver) = self.event_receiver {
            match timeout {
                Some(duration) => {
                    match tokio::time::timeout(duration, receiver.next()).await {
                        Ok(Some(event)) => Ok(Some(event)),
                        Ok(None) => Ok(None),
                        Err(_) => Ok(None), // Timeout
                    }
                }
                None => {
                    match receiver.next().await {
                        Some(event) => Ok(Some(event)),
                        None => Ok(None),
                    }
                }
            }
        } else {
            Ok(None)
        }
    }
    
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    
    fn local_peer_id(&self) -> Option<Vec<u8>> {
        self.local_peer_id.as_ref().map(|id| id.to_bytes())
    }
    
    async fn discovered_peers(&self) -> Result<Vec<PeerInfo>, DiscoveryError> {
        let peers = self.peers.lock().await;
        let now = Instant::now();
        
        // Filter out expired peers
        let result = peers.values()
            .filter(|(_, last_seen)| now.duration_since(*last_seen) < PEER_EXPIRATION)
            .map(|(info, _)| info.clone())
            .collect();
            
        Ok(result)
    }
}

/// Extract peer ID from a multiaddr if it contains one
fn extract_peer_id_from_multiaddr(addr: &Multiaddr) -> Option<PeerId> {
    for component in addr.iter() {
        if let libp2p::multiaddr::Protocol::P2p(peer_id_hash) = component {
            // The P2p protocol contains a PeerId directly, not a Multihash
            return Some(peer_id_hash);
        }
    }
    None
}

/// Extract socket address from a multiaddr
fn extract_socket_addr_from_multiaddr(addr: &Multiaddr) -> Option<SocketAddr> {
    let mut ip = None;
    let mut port = None;
    
    for component in addr.iter() {
        match component {
            libp2p::multiaddr::Protocol::Ip4(addr) => {
                ip = Some(std::net::IpAddr::V4(addr));
            },
            libp2p::multiaddr::Protocol::Ip6(addr) => {
                ip = Some(std::net::IpAddr::V6(addr));
            },
            libp2p::multiaddr::Protocol::Tcp(p) => {
                port = Some(p);
            },
            _ => {}
        }
    }
    
    match (ip, port) {
        (Some(ip), Some(port)) => Some(SocketAddr::new(ip, port)),
        _ => None,
    }
}