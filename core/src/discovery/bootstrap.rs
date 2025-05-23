//! Bootstrap-based peer discovery
//!
//! This module provides peer discovery via well-known bootstrap nodes.

use crate::discovery::interface::{Discovery, DiscoveryEvent, DiscoveryError, PeerInfo};
use async_trait::async_trait;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::SinkExt;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::{interval, sleep, timeout};
use tracing::{debug, error, info};

/// Connection timeout for bootstrap nodes
const BOOTSTRAP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Time between bootstrap refresh attempts
const BOOTSTRAP_REFRESH_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// Peer expiration time
const PEER_EXPIRATION: Duration = Duration::from_secs(3600); // 1 hour

/// Configuration for bootstrap discovery
#[derive(Debug, Clone)]
pub struct BootstrapDiscoveryConfig {
    /// List of bootstrap servers to connect to
    pub bootstrap_nodes: Vec<SocketAddr>,
    
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
            event_buffer_size: 32,
            peer_expiration: 3600, // 1 hour
            protocol_name: "decentralized-stream/1.0.0".to_string(),
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
    pub fn with_config(config: BootstrapDiscoveryConfig) -> Self {
        let (tx, rx) = channel(config.event_buffer_size);
        let (stop_tx, _) = tokio::sync::oneshot::channel();
        
        Self {
            config,
            event_sender: Some(tx),
            event_receiver: Some(rx),
            peers: Arc::new(Mutex::new(HashMap::new())),
            running: false,
            own_info: None,
            task_handle: None,
            stop_tx: Some(stop_tx),
        }
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
    
    /// Connect to a bootstrap node and try to get peer information
    async fn connect_to_bootstrap(
        addr: SocketAddr,
        protocol_name: String,
        connect_timeout: Duration,
    ) -> Result<Vec<PeerInfo>, DiscoveryError> {
        // In a real implementation, we would:
        // 1. Establish a TCP connection to the bootstrap node
        // 2. Perform a handshake with protocol version
        // 3. Request a list of known peers
        // 4. Parse the response and return peer information
        
        // For this example, we'll simulate the process with a timeout
        let connect_future = async move {
            // Simulate connecting to bootstrap server
            debug!("Connecting to bootstrap node: {}", addr);
            sleep(Duration::from_millis(500)).await;
            
            // Create a fake list of peers
            let mut peers = Vec::new();
            
            // Add the bootstrap node itself
            let now = std::time::SystemTime::now();
            let bootstrap_peer = PeerInfo {
                id: Self::generate_peer_id(&addr),
                addresses: vec![addr],
                protocols: vec![protocol_name.clone()],
                metadata: HashMap::new(),
                first_seen: Some(now),
                last_seen: Some(now),
                connection_status: crate::discovery::interface::ConnectionStatus::Disconnected,
            };
            
            peers.push(bootstrap_peer);
            
            // Simulate 3 additional peers that might be returned
            for i in 1..4 {
                let port = 10000 + i;
                let peer_addr = SocketAddr::new(addr.ip(), port as u16);
                
                let now = std::time::SystemTime::now();
                let peer_info = PeerInfo {
                    id: Self::generate_peer_id(&peer_addr),
                    addresses: vec![peer_addr],
                    protocols: vec![protocol_name.clone()],
                    metadata: HashMap::new(),
                    first_seen: Some(now),
                    last_seen: Some(now),
                    connection_status: crate::discovery::interface::ConnectionStatus::Disconnected,
                };
                
                peers.push(peer_info);
            }
            
            Ok(peers)
        };
        
        // Apply timeout to the connection attempt
        match timeout(connect_timeout, connect_future).await {
            Ok(result) => result,
            Err(_) => Err(DiscoveryError::BootstrapError(
                format!("Connection timeout to bootstrap node: {}", addr).into()
            )),
        }
    }
    
    /// Start the background discovery task
    async fn start_discovery_task(
        peers: Arc<Mutex<HashMap<Vec<u8>, (PeerInfo, Instant)>>>,
        event_sender: Sender<DiscoveryEvent>,
        config: BootstrapDiscoveryConfig,
    ) -> Result<tokio::task::JoinHandle<()>, DiscoveryError> {
        // Clone needed values for the task
        let bootstrap_nodes = config.bootstrap_nodes.clone();
        let protocol_name = config.protocol_name.clone();
        let connect_timeout = Duration::from_secs(config.connect_timeout);
        let refresh_interval = Duration::from_secs(config.refresh_interval);
        let peer_expiration = Duration::from_secs(config.peer_expiration);
        let mut event_sender = event_sender;
        
        // Define the task for periodic bootstrap refresh
        let task = tokio::spawn(async move {
            let mut refresh_timer = interval(refresh_interval);
            let _expired_peers: HashSet<Vec<u8>> = HashSet::new();
            
            loop {
                // Wait for the refresh interval
                refresh_timer.tick().await;
                
                // Connect to all bootstrap nodes
                for &addr in &bootstrap_nodes {
                    match Self::connect_to_bootstrap(addr, protocol_name.clone(), connect_timeout).await {
                        Ok(found_peers) => {
                            // Process the peers we got back
                            let now = Instant::now();
                            
                            // Collect events to send after releasing the lock
                            let mut events_to_send = Vec::new();
                            
                            // Update our peer list with the results
                            {
                                let mut peers_lock = peers.lock().unwrap();
                                
                                for peer_info in found_peers {
                                    let peer_id = peer_info.id.clone();
                                    
                                    if peers_lock.contains_key(&peer_id) {
                                        // Update existing peer
                                        peers_lock.insert(peer_id.clone(), (peer_info.clone(), now));
                                        events_to_send.push(DiscoveryEvent::PeerUpdated(peer_info));
                                    } else {
                                        // Add new peer
                                        peers_lock.insert(peer_id, (peer_info.clone(), now));
                                        events_to_send.push(DiscoveryEvent::PeerDiscovered(peer_info));
                                    }
                                }
                            }
                            
                            // Send all events after releasing the lock
                            for event in events_to_send {
                                let _ = event_sender.send(event).await;
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
        
        // Ensure we have event channels
        if self.event_sender.is_none() || self.event_receiver.is_none() {
            let (tx, rx) = channel(self.config.event_buffer_size);
            self.event_sender = Some(tx);
            self.event_receiver = Some(rx);
        }
        
        // If no bootstrap nodes configured, we can't do discovery
        if self.config.bootstrap_nodes.is_empty() {
            return Err(DiscoveryError::BootstrapError(
                anyhow::anyhow!("No bootstrap nodes configured").into()
            ));
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
    
    async fn announce(&self, info: PeerInfo) -> Result<(), DiscoveryError> {
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
        self.get_local_peer_id()
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