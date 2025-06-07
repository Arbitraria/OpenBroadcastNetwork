//! Bootstrap-based peer discovery
//!
//! This module provides peer discovery via well-known bootstrap nodes.
//! Updated to use real libp2p connections instead of simulated ones.

use crate::discovery::interface::{Discovery, DiscoveryEvent, DiscoveryError, PeerInfo};
use async_trait::async_trait;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::{interval, timeout};
use tracing::{debug, error, warn};
use libp2p::core::multiaddr::Multiaddr;
use libp2p::identity::Keypair;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::identify::{Behaviour as Identify, Event as IdentifyEvent, Config as IdentifyConfig};
use libp2p::{PeerId, Transport, SwarmBuilder};
use libp2p::tcp::Config as GenTcpConfig;
use libp2p::noise;

/// Connection timeout for bootstrap nodes
const BOOTSTRAP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Time between bootstrap refresh attempts
const BOOTSTRAP_REFRESH_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// Peer expiration time
const PEER_EXPIRATION: Duration = Duration::from_secs(3600); // 1 hour

/// Configuration for bootstrap discovery
#[derive(Debug, Clone)]
pub struct BootstrapDiscoveryConfig {
    /// List of bootstrap servers to connect to (as multiaddrs)
    pub bootstrap_nodes: Vec<Multiaddr>,
    
    /// Legacy socket addresses (converted to multiaddrs)
    pub bootstrap_nodes_legacy: Vec<SocketAddr>,
    
    /// Maximum number of events to buffer
    pub event_buffer_size: usize,
    
    /// Peer expiration time in seconds
    pub peer_expiration: u64,
    
    /// Protocol name/version
    pub protocol_name: String,
    
    /// Connection timeout in seconds
    pub connect_timeout: u64,
    
    /// Refresh interval in seconds
    pub refresh_interval: u64,
}

impl Default for BootstrapDiscoveryConfig {
    fn default() -> Self {
        Self {
            bootstrap_nodes: Vec::new(),
            bootstrap_nodes_legacy: Vec::new(),
            event_buffer_size: 32,
            peer_expiration: 3600, // 1 hour
            protocol_name: "OpenBroadcastNetwork/1.0.0".to_string(),
            connect_timeout: 10,
            refresh_interval: 300, // 5 minutes
        }
    }
}

/// Bootstrap-based peer discovery
pub struct BootstrapDiscovery {
    /// Configuration
    config: BootstrapDiscoveryConfig,
    
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
    
    /// Sender to signal the background task to stop
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
    
    /// libp2p swarm for network operations
    swarm: Arc<tokio::sync::Mutex<Option<Swarm<Identify>>>>,
    
    /// Keypair for authentication
    keypair: Option<Keypair>,
    
    /// Local peer ID
    local_peer_id: Option<PeerId>,
}

impl BootstrapDiscovery {
    /// Create a new bootstrap discovery service with default configuration
    pub fn new() -> Self {
        Self::with_config(BootstrapDiscoveryConfig::default())
    }
    
    /// Get the local peer ID if available
    fn get_local_peer_id(&self) -> Option<Vec<u8>> {
        self.own_info.as_ref().map(|info| info.id.clone())
    }
    
    /// Create a new bootstrap discovery service with custom configuration
    pub fn with_config(mut config: BootstrapDiscoveryConfig) -> Self {
        let (tx, rx) = channel(config.event_buffer_size);
        let (stop_tx, _) = tokio::sync::oneshot::channel();
        
        // Convert legacy socket addresses to multiaddrs
        for socket_addr in &config.bootstrap_nodes_legacy {
            let multiaddr = format!("/ip4/{}/tcp/{}", socket_addr.ip(), socket_addr.port())
                .parse::<Multiaddr>()
                .unwrap();
            config.bootstrap_nodes.push(multiaddr);
        }
        
        Self {
            config,
            event_sender: Some(tx),
            event_receiver: Some(rx),
            peers: Arc::new(Mutex::new(HashMap::new())),
            running: false,
            own_info: None,
            task_handle: None,
            stop_tx: Some(stop_tx),
            swarm: Arc::new(tokio::sync::Mutex::new(None)),
            keypair: None,
            local_peer_id: None,
        }
    }
    
    /// Initialize the libp2p swarm
    async fn init_swarm(&mut self) -> Result<(), DiscoveryError> {
        // Generate or use existing keypair
        let keypair = self.keypair.take().unwrap_or_else(|| Keypair::generate_ed25519());
        let peer_id = keypair.public().to_peer_id();
        self.local_peer_id = Some(peer_id);
        
        // Create identify behavior
        let identify = Identify::new(
            IdentifyConfig::new(
                self.config.protocol_name.clone(),
                keypair.public()
            )
        );
        
        // Build the swarm
        let swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                GenTcpConfig::default().nodelay(true),
                noise::Config::new,
                libp2p::yamux::Config::default
            )
            .map_err(|e| DiscoveryError::Other(format!("Failed to build swarm: {}", e)))?
            .with_behaviour(|_| identify)
            .map_err(|e| DiscoveryError::Other(format!("Failed to create behavior: {}", e)))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();
        
        // Store the swarm
        let mut swarm_lock = self.swarm.lock().await;
        *swarm_lock = Some(swarm);
        
        Ok(())
    }
    
    /// Generate a simple peer ID from a socket address
    fn generate_peer_id(addr: &SocketAddr) -> Vec<u8> {
        let mut bytes = Vec::new();
        match addr {
            SocketAddr::V4(addr_v4) => {
                bytes.extend_from_slice(&[0]);
                bytes.extend_from_slice(&addr_v4.ip().octets());
                bytes.extend_from_slice(&addr_v4.port().to_be_bytes());
            },
            SocketAddr::V6(addr_v6) => {
                bytes.extend_from_slice(&[1]);
                bytes.extend_from_slice(&addr_v6.ip().octets());
                bytes.extend_from_slice(&addr_v6.port().to_be_bytes());
            },
        }
        bytes
    }
    
    /// Connect to a bootstrap node using libp2p and discover peers
    async fn connect_to_bootstrap(
        swarm: Arc<tokio::sync::Mutex<Option<Swarm<Identify>>>>,
        addr: Multiaddr,
        connect_timeout: Duration,
        event_sender: &mut Sender<DiscoveryEvent>,
    ) -> Result<Vec<PeerInfo>, DiscoveryError> {
        debug!("Attempting to connect to bootstrap node: {}", addr);
        
        let mut discovered_peers = Vec::new();
        
        // Get the swarm
        let mut swarm_lock = swarm.lock().await;
        if let Some(ref mut swarm_instance) = *swarm_lock {
            // Dial the bootstrap node
            match swarm_instance.dial(addr.clone()) {
                Ok(()) => {
                    debug!("Initiated connection to bootstrap node: {}", addr);
                },
                Err(e) => {
                    return Err(DiscoveryError::BootstrapError(
                        format!("Failed to dial bootstrap node {}: {}", addr, e).into()
                    ));
                }
            }
            
            // Wait for connection events with timeout
            let start_time = Instant::now();
            let mut connected = false;
            
            while start_time.elapsed() < connect_timeout && !connected {
                let event_future = swarm_instance.select_next_some();
                
                match timeout(Duration::from_millis(100), event_future).await {
                    Ok(event) => {
                        match event {
                            SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                                debug!("Connected to bootstrap peer: {}", peer_id);
                                connected = true;
                                
                                // Extract address from endpoint
                                let mut addresses = vec![];
                                if let Some(socket_addr) = extract_socket_addr_from_multiaddr(&endpoint.get_remote_address()) {
                                    addresses.push(socket_addr);
                                }
                                
                                let peer_info = PeerInfo {
                                    id: peer_id.to_bytes(),
                                    addresses,
                                    protocols: vec![],
                                    metadata: HashMap::new(),
                                    first_seen: Some(std::time::SystemTime::now()),
                                    last_seen: Some(std::time::SystemTime::now()),
                                    connection_status: crate::discovery::interface::ConnectionStatus::Connected,
                                };
                                
                                discovered_peers.push(peer_info.clone());
                                let _ = event_sender.send(DiscoveryEvent::PeerDiscovered(peer_info)).await;
                            },
                            SwarmEvent::Behaviour(IdentifyEvent::Received { peer_id, info }) => {
                                debug!("Received identify info from {}: {:?}", peer_id, info);
                                
                                // Extract addresses from the identify info
                                let addresses: Vec<SocketAddr> = info.listen_addrs.iter()
                                    .filter_map(|addr| extract_socket_addr_from_multiaddr(addr))
                                    .collect();
                                
                                let peer_info = PeerInfo {
                                    id: peer_id.to_bytes(),
                                    addresses,
                                    protocols: info.protocols.iter().map(|p| p.to_string()).collect(),
                                    metadata: HashMap::new(),
                                    first_seen: Some(std::time::SystemTime::now()),
                                    last_seen: Some(std::time::SystemTime::now()),
                                    connection_status: crate::discovery::interface::ConnectionStatus::Connected,
                                };
                                
                                discovered_peers.push(peer_info.clone());
                                let _ = event_sender.send(DiscoveryEvent::PeerDiscovered(peer_info)).await;
                            },
                            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                                let peer_str = peer_id.map(|p| p.to_string()).unwrap_or_else(|| "unknown".to_string());
                                warn!("Failed to connect to bootstrap peer {}: {}", peer_str, error);
                                return Err(DiscoveryError::BootstrapError(
                                    format!("Connection failed to {}: {}", peer_str, error).into()
                                ));
                            },
                            SwarmEvent::IncomingConnection { .. } => {},
                            SwarmEvent::IncomingConnectionError { .. } => {},
                            SwarmEvent::NewListenAddr { .. } => {},
                            SwarmEvent::ExpiredListenAddr { .. } => {},
                            SwarmEvent::ListenerClosed { .. } => {},
                            SwarmEvent::ListenerError { .. } => {},
                            SwarmEvent::Dialing { .. } => {},
                            SwarmEvent::ConnectionClosed { .. } => {},
                            _ => {}
                        }
                    },
                    Err(_) => {
                        // Timeout on individual event, continue
                    }
                }
            }
            
            if !connected {
                return Err(DiscoveryError::BootstrapError(
                    format!("Timeout connecting to bootstrap node: {}", addr).into()
                ));
            }
        } else {
            return Err(DiscoveryError::Other("No swarm available".into()));
        }
        
        Ok(discovered_peers)
    }
    
    /// Start the background discovery task
    async fn start_discovery_task(
        peers: Arc<Mutex<HashMap<Vec<u8>, (PeerInfo, Instant)>>>,
        event_sender: Sender<DiscoveryEvent>,
        config: BootstrapDiscoveryConfig,
        swarm: Arc<tokio::sync::Mutex<Option<Swarm<Identify>>>>,
    ) -> Result<tokio::task::JoinHandle<()>, DiscoveryError> {
        // Clone needed values for the task
        let bootstrap_nodes = config.bootstrap_nodes.clone();
        let connect_timeout = Duration::from_secs(config.connect_timeout);
        let refresh_interval = Duration::from_secs(config.refresh_interval);
        let peer_expiration = Duration::from_secs(config.peer_expiration);
        let mut event_sender = event_sender;
        
        // Define the task for periodic bootstrap refresh
        let task = tokio::spawn(async move {
            let mut refresh_timer = interval(refresh_interval);
            
            loop {
                // Wait for the refresh interval
                refresh_timer.tick().await;
                
                // Connect to all bootstrap nodes
                for addr in &bootstrap_nodes {
                    match Self::connect_to_bootstrap(
                        swarm.clone(),
                        addr.clone(),
                        connect_timeout,
                        &mut event_sender,
                    ).await {
                        Ok(found_peers) => {
                            // Process the peers we got back
                            let now = Instant::now();
                            
                            // Update our peer list with the results
                            {
                                let mut peers_lock = peers.lock().unwrap();
                                
                                for peer_info in found_peers {
                                    let peer_id = peer_info.id.clone();
                                    peers_lock.insert(peer_id, (peer_info, now));
                                }
                            }
                        },
                        Err(e) => {
                            // Log the error but continue with other bootstrap nodes
                            error!("Failed to connect to bootstrap node {}: {}", addr, e);
                            let _ = event_sender.send(DiscoveryEvent::Error(e.to_string())).await;
                        },
                    }
                }
                
                // Check for expired peers
                let now = Instant::now();
                
                // Get a list of expired peer IDs
                let expired_peers: Vec<Vec<u8>> = {
                    let peers_guard = match peers.lock() {
                        Ok(guard) => guard,
                        Err(_) => {
                            error!("Failed to lock peers");
                            continue;
                        }
                    };
                    
                    peers_guard
                        .iter()
                        .filter_map(|(peer_id, (_, last_seen))| {
                            if now.duration_since(*last_seen) > peer_expiration {
                                Some(peer_id.clone())
                            } else {
                                None
                            }
                        })
                        .collect()
                };
                
                // Remove expired peers
                if !expired_peers.is_empty() {
                    let mut peers_guard = match peers.lock() {
                        Ok(guard) => guard,
                        Err(_) => {
                            error!("Failed to lock peers");
                            continue;
                        }
                    };
                    
                    for peer_id in &expired_peers {
                        peers_guard.remove(peer_id);
                    }
                }
                
                // Send all expired events after releasing the lock
                for peer_id in expired_peers {
                    if let Err(e) = event_sender.try_send(DiscoveryEvent::PeerExpired(peer_id)) {
                        error!(target: "bootstrap", "Failed to send peer expired event: {}", e);
                        break;
                    }
                }
            }
        });
        
        Ok(task)
    }
}

#[async_trait::async_trait]
impl Discovery for BootstrapDiscovery {
    async fn start(&mut self) -> Result<(), DiscoveryError> {
        if self.running {
            return Ok(());
        }
        
        debug!("Starting bootstrap discovery service");
        
        // Initialize the swarm
        self.init_swarm().await?;
        
        // Ensure we have event channels
        if self.event_sender.is_none() || self.event_receiver.is_none() {
            let (tx, rx) = channel(self.config.event_buffer_size);
            self.event_sender = Some(tx);
            self.event_receiver = Some(rx);
        }
        
        // If no bootstrap nodes configured, we can't do discovery
        if self.config.bootstrap_nodes.is_empty() && self.config.bootstrap_nodes_legacy.is_empty() {
            return Err(DiscoveryError::BootstrapError(
                anyhow::anyhow!("No bootstrap nodes configured").into()
            ));
        }
        
        // Start the discovery task
        let peers = self.peers.clone();
        let event_sender = self.event_sender.as_ref().unwrap().clone();
        let config = self.config.clone();
        let swarm = self.swarm.clone();
        
        let task_handle = Self::start_discovery_task(peers, event_sender, config, swarm).await?;
        
        self.task_handle = Some(task_handle);
        self.running = true;
        
        debug!("Bootstrap discovery service started with peer ID: {:?}", self.local_peer_id);
        Ok(())
    }
    
    async fn stop(&mut self) -> Result<(), DiscoveryError> {
        if !self.running {
            return Ok(());
        }
        
        self.running = false;
        
        // Signal the background task to stop
        if let Some(sender) = self.stop_tx.take() {
            // Ignore the error if the receiver is already dropped
            let _ = sender.send(());
        }
        
        // Wait for the background task to complete if it exists
        if let Some(handle) = self.task_handle.take() {
            if let Err(e) = handle.await {
                error!("Error in background task: {:?}", e);
                return Err(DiscoveryError::Other("Background task error".into()));
            }
        }
        
        Ok(())
    }
    
    async fn announce(&mut self, info: PeerInfo) -> Result<(), DiscoveryError> {
        // Store our own info for future reference
        let mut peers = self.peers.lock().map_err(|_| DiscoveryError::Other("Failed to lock peers".into()))?;
        peers.insert(info.id.clone(), (info, Instant::now()));
        Ok(())
    }
    
    async fn lookup_peer(&self, peer_id: &[u8]) -> Result<Option<PeerInfo>, DiscoveryError> {
        // Look up in our local cache
        let peers = self.peers.lock().map_err(|_| DiscoveryError::Other("Failed to lock peers".into()))?;
        if let Some((info, _)) = peers.get(peer_id) {
            return Ok(Some(info.clone()));
        }
        
        // Not found in cache
        Ok(None)
    }
    
    async fn find_peers(&self, criteria: &str) -> Result<Vec<PeerInfo>, DiscoveryError> {
        let peers = self.peers.lock().map_err(|_| DiscoveryError::Other("Failed to lock peers".into()))?;
        let now = Instant::now();
        
        // Filter peers based on the criteria
        let result = peers.values()
            .filter(|(info, last_seen)| {
                let not_expired = now.duration_since(*last_seen) < Duration::from_secs(self.config.peer_expiration as u64);
                let matches_criteria = criteria.is_empty() || 
                    info.protocols.iter().any(|proto| proto.contains(criteria));
                not_expired && matches_criteria
            })
            .map(|(info, _)| info.clone())
            .collect();
            
        Ok(result)
    }
    
    async fn next_event(&mut self, timeout: Option<Duration>) -> Result<Option<DiscoveryEvent>, DiscoveryError> {
        use futures::StreamExt;
        
        if let Some(receiver) = &mut self.event_receiver {
            match timeout {
                Some(duration) => {
                    match tokio::time::timeout(duration, receiver.next()).await {
                        Ok(Some(event)) => Ok(Some(event)),
                        Ok(None) => Ok(None),
                        Err(_) => Ok(None),
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
        self.running
    }
    
    fn local_peer_id(&self) -> Option<Vec<u8>> {
        self.local_peer_id.as_ref().map(|id| id.to_bytes())
    }
    
    async fn discovered_peers(&self) -> Result<Vec<PeerInfo>, DiscoveryError> {
        let peers = self.peers.lock().map_err(|_| DiscoveryError::Other("Failed to lock peers".into()))?;
        let now = Instant::now();
        
        // Return all non-expired peers
        let result = peers.values()
            .filter(|(_, last_seen)| now.duration_since(*last_seen) < Duration::from_secs(self.config.peer_expiration as u64))
            .map(|(info, _)| info.clone())
            .collect();
            
        Ok(result)
    }
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