//! mDNS-based peer discovery
//!
//! This module provides peer discovery via multicast DNS for local network peers.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::channel::mpsc::{channel, Sender};
use futures::{SinkExt, StreamExt};
use libp2p::core::multiaddr::{Multiaddr, Protocol};
use libp2p::core::PeerId as Libp2pPeerId;
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::swarm::SwarmEvent;
use parking_lot::Mutex;
use tokio::sync::oneshot::{self, Sender as OneshotSender};
use tokio::time::interval;
use tracing::{debug, error, info, trace, warn};

use crate::discovery::interface::{
    ConnectionStatus, Discovery, DiscoveryError, DiscoveryEvent, PeerInfo,
};

/// Re-export for convenience
pub use libp2p::mdns::Config as MdnsConfig;

/// Default service name for mDNS discovery
const DEFAULT_SERVICE_NAME: &str = "_decentralized-stream._udp";

/// Default TTL for mDNS records in seconds
const DEFAULT_TTL: u32 = 120;

/// Default buffer size for discovery events
const DEFAULT_EVENT_BUFFER_SIZE: usize = 100;

/// Default peer expiration time in seconds
const DEFAULT_PEER_EXPIRATION: u64 = 300; // 5 minutes

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
    
    /// Sender for the shutdown signal (used internally)
    shutdown_sender: Option<OneshotSender<()>>,
}

/// Time to wait for mDNS query responses
const MDNS_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Controls how often we refresh our peer list
const PEER_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

impl Default for MdnsDiscoveryConfig {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_TTL,
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            event_buffer_size: DEFAULT_EVENT_BUFFER_SIZE,
            peer_expiration: DEFAULT_PEER_EXPIRATION,
            shutdown_sender: None,
        }
    }
}

impl MdnsDiscoveryConfig {
    /// Create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Set the TTL for mDNS records
    pub fn with_ttl(mut self, ttl: u32) -> Self {
        self.ttl = ttl;
        self
    }
    
    /// Set the service name for discovery
    pub fn with_service_name<S: Into<String>>(mut self, service_name: S) -> Self {
        self.service_name = service_name.into();
        self
    }
    
    /// Set the event buffer size
    pub fn with_event_buffer_size(mut self, size: usize) -> Self {
        self.event_buffer_size = size;
        self
    }
    
    /// Set the peer expiration time in seconds
    pub fn with_peer_expiration(mut self, expiration_secs: u64) -> Self {
        self.peer_expiration = expiration_secs;
        self
    }
    
    /// Build the configuration
    pub fn build(self) -> Self {
        self
    }
}

/// mDNS-based peer discovery
pub struct MdnsDiscovery {
    /// The mDNS service instance
    mdns: Option<Mdns>,
    
    /// Configuration for the mDNS service
    config: MdnsDiscoveryConfig,
    
    /// Sender for discovery events
    event_sender: Option<Sender<DiscoveryEvent>>,
    
    /// Map of discovered peers and their last seen timestamp
    peers: Arc<Mutex<HashMap<Vec<u8>, (PeerInfo, Instant)>>>,
    
    /// Flag indicating if the discovery service is running
    running: Arc<AtomicBool>,
    
    /// Local peer ID
    local_peer_id: Option<Vec<u8>>,
    
    /// Handle to the background task
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl MdnsDiscovery {
    /// Create a new mDNS discovery service with default configuration
    pub fn new() -> Self {
        Self::with_config(MdnsDiscoveryConfig::default())
    }
    
    /// Create a new mDNS discovery service with the given configuration
    pub fn with_config(config: MdnsDiscoveryConfig) -> Self {
        // Create a channel for discovery events
        let (event_sender, _) = channel(config.event_buffer_size);
        
        Self {
            mdns: None,
            config,
            event_sender: Some(event_sender),
            peers: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
            local_peer_id: None,
            task_handle: None,
        }
    }
    
    /// Initialize the mDNS service
    async fn init_mdns(&mut self) -> Result<(), DiscoveryError> {
        if self.mdns.is_some() {
            return Ok(());
        }
        
        let mdns = Mdns::new(MdnsConfig::default())
            .await
            .map_err(|e| DiscoveryError::MdnsError(Box::new(e)))?;
            
        self.mdns = Some(mdns);
        Ok(())
    }
    
    /// Start the background discovery task
    async fn start_discovery_task(
        &mut self,
    ) -> Result<(), DiscoveryError> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(DiscoveryError::AlreadyRunning);
        }
        
        // Initialize mDNS if not already done
        self.init_mdns().await?;
        
        // Create a channel for the shutdown signal
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        
        // Update config with the shutdown sender
        self.config.shutdown_sender = Some(shutdown_sender);
        
        // Clone the necessary state
        let peers = self.peers.clone();
        let running = self.running.clone();
        let event_sender = self.event_sender.take().ok_or_else(|| {
            DiscoveryError::ConfigError("Event sender not initialized".to_string())
        })?;
        
        // Create a new channel for events
        let (new_event_sender, mut event_receiver) = channel(self.config.event_buffer_size);
        self.event_sender = Some(new_event_sender);
        
        // Spawn the background task
        let task_handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10));
            
            // Mark as running
            running.store(true, Ordering::SeqCst);
            
            loop {
                tokio::select! {
                    // Handle shutdown signal
                    _ = &mut shutdown_receiver => {
                        debug!("Received shutdown signal, stopping mDNS discovery");
                        break;
                    }
                    
                    // Handle mDNS events
                    event = event_receiver.next() => {
                        if let Some(event) = event {
                            if let Err(e) = event_sender.send(event).await {
                                error!("Failed to send discovery event: {}", e);
                            }
                        } else {
                            // Channel was closed
                            break;
                        }
                    }
                    
                    // Periodic cleanup of expired peers
                    _ = interval.tick() => {
                        let now = Instant::now();
                        let mut peers_guard = peers.lock();
                        let expired_peers: Vec<Vec<u8>> = peers_guard
                            .iter()
                            .filter(|(_, (_, last_seen))| {
                                now.duration_since(*last_seen).as_secs() > 300 // 5 minutes
                            })
                            .map(|(id, _)| id.clone())
                            .collect();
                            
                        for peer_id in expired_peers {
                            if let Some((peer_info, _)) = peers_guard.remove(&peer_id) {
                                debug!("Peer expired: {:?}", peer_id);
                                let _ = event_sender.send(DiscoveryEvent::PeerExpired(peer_id)).await;
                            }
                        }
                    }
                }
            }
            
            // Mark as not running
            running.store(false, Ordering::SeqCst);
        });
        
        Ok(())
    }
    
    /// Convert a PeerId to a byte vector
    fn peer_id_to_bytes(peer_id: &Libp2pPeerId) -> Vec<u8> {
        peer_id.to_bytes()
    }
    
    /// Convert a byte vector to a PeerId
    fn bytes_to_peer_id(bytes: &[u8]) -> Result<Libp2pPeerId, DiscoveryError> {
        Libp2pPeerId::from_bytes(bytes)
            .map_err(|e| DiscoveryError::PeerIdError(e.to_string()))
    }
    
    /// Convert a Multiaddr to a SocketAddr if possible
    fn multiaddr_to_socket_addr(addr: &Multiaddr) -> Option<SocketAddr> {
        addr.iter().find_map(|p| match p {
            Protocol::Ip4(ip) => Some(SocketAddr::new(ip.into(), 0)),
            Protocol::Ip6(ip) => Some(SocketAddr::new(ip.into(), 0)),
            Protocol::Dns4(host) => None, // Can't resolve DNS names here
            Protocol::Dns6(host) => None, // Can't resolve DNS names here
            _ => None,
        })
    }
    
    /// Convert a PeerInfo to a libp2p PeerId
    fn peer_info_to_peer_id(info: &PeerInfo) -> Result<Libp2pPeerId, DiscoveryError> {
        Self::bytes_to_peer_id(&info.id)
    }
    
    /// Handle an mDNS event
    async fn handle_mdns_event(&self, event: MdnsEvent) -> Result<(), DiscoveryError> {
        match event {
            MdnsEvent::Discovered(discovered_list) => {
                for (peer_id, addr) in discovered_list {
                    self.handle_peer_discovered(peer_id, vec![addr]).await?;
                }
            }
            MdnsEvent::Expired(expired_list) => {
                for (peer_id, addr) in expired_list {
                    self.handle_peer_expired(peer_id).await?;
                }
            }
        }
        Ok(())
    }
    
    /// Handle a discovered peer
    async fn handle_peer_discovered(
        &self,
        peer_id: Libp2pPeerId,
        addresses: Vec<Multiaddr>,
    ) -> Result<(), DiscoveryError> {
        let peer_id_bytes = Self::peer_id_to_bytes(&peer_id);
        let socket_addrs: Vec<SocketAddr> = addresses
            .iter()
            .filter_map(Self::multiaddr_to_socket_addr)
            .collect();
        
        if socket_addrs.is_empty() {
            return Err(DiscoveryError::InvalidPeerInfo(
                "No valid socket addresses for peer".to_string(),
            ));
        }
        
        let peer_info = PeerInfo {
            id: peer_id_bytes.clone(),
            addresses: socket_addrs,
            protocols: vec!["mdns".to_string()],
            metadata: HashMap::new(),
            first_seen: Some(std::time::SystemTime::now()),
            last_seen: Some(std::time::SystemTime::now()),
            connection_status: ConnectionStatus::Disconnected,
        };
        
        let mut peers = self.peers.lock();
        let is_new = !peers.contains_key(&peer_id_bytes);
        peers.insert(peer_id_bytes.clone(), (peer_info.clone(), Instant::now()));
        
        // Send appropriate event
        if is_new {
            self.send_event(DiscoveryEvent::PeerDiscovered(peer_info)).await?;
        } else {
            self.send_event(DiscoveryEvent::PeerUpdated(peer_info)).await?;
        }
        
        Ok(())
    }
    
    /// Handle an expired peer
    async fn handle_peer_expired(&self, peer_id: Libp2pPeerId) -> Result<(), DiscoveryError> {
        let peer_id_bytes = Self::peer_id_to_bytes(&peer_id);
        
        let mut peers = self.peers.lock();
        if peers.remove(&peer_id_bytes).is_some() {
            self.send_event(DiscoveryEvent::PeerExpired(peer_id_bytes)).await?;
        }
        
        Ok(())
    }
    
    /// Send an event through the event channel
    async fn send_event(&self, event: DiscoveryEvent) -> Result<(), DiscoveryError> {
        if let Some(sender) = &self.event_sender {
            sender.clone()
                .send(event)
                .await
                .map_err(|e| DiscoveryError::IoError(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    format!("Failed to send event: {}", e),
                )))?;
        }
        Ok(())
    }
    
    /// Start the background discovery task
    async fn start_discovery_task(
        &mut self,
    ) -> Result<(), DiscoveryError> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(DiscoveryError::AlreadyRunning);
        }
        
        // Initialize mDNS if not already done
        self.init_mdns().await?;
        
        // Create a channel for the shutdown signal
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        
        // Update config with the shutdown sender
        self.config.shutdown_sender = Some(shutdown_sender);
        
        // Clone the necessary state
        let peers = self.peers.clone();
        let running = self.running.clone();
        let event_sender = self.event_sender.take().ok_or_else(|| {
            DiscoveryError::ConfigError("Event sender not initialized".to_string())
        })?;
        
        // Create a new channel for events
        let (new_event_sender, mut event_receiver) = channel(self.config.event_buffer_size);
        self.event_sender = Some(new_event_sender);
        
        // Get the mDNS instance
        let mut mdns = self.mdns.take().ok_or_else(|| {
            DiscoveryError::ConfigError("mDNS service not initialized".to_string())
        })?;
        // Spawn the discovery task
        let task = tokio::spawn(async move {
            // Create a future that resolves when the shutdown signal is received
            let shutdown_future = async {
                let _ = shutdown_receiver.await;
                log::info!("Received shutdown signal, stopping mDNS discovery...");
            };
            
            // Create a future for the main event loop
            let discovery_future = async {
                let mut refresh_interval = interval(PEER_REFRESH_INTERVAL);
                let mut expired_peers = HashSet::new();
                
                loop {
                    tokio::select! {
                        // Handle swarm events
                        event = mdns.next() => {
                            if let Some(event) = event {
                                if let Err(e) = Self::handle_mdns_event(&peers, &event_sender, event).await {
                                    log::error!("Error handling mDNS event: {}", e);
                                }
                            }
                        },
                        // Handle peer expiration
                        _ = refresh_interval.tick() => {
                            let now = Instant::now();
                            let mut peers_guard = match peers.lock() {
                                Ok(guard) => guard,
                                Err(e) => {
                                    log::error!("Failed to acquire lock on peers: {}", e);
                                    continue;
                                }
                            };
                            
                            // Find expired peers
                            let expired: Vec<Vec<u8>> = peers_guard
                                .iter()
                                .filter(|(_, (_, last_seen))| now.duration_since(*last_seen) > Duration::from_secs(300)) // 5 minutes
                                .map(|(id, _)| id.clone())
                                .collect();
                            
                            // Remove expired peers
                            for peer_id in expired {
                                if let Some((info, _)) = peers_guard.remove(&peer_id) {
                                    log::info!("Peer expired: {:?}", info.id);
                                    if let Err(e) = event_sender.send(DiscoveryEvent::PeerExpired(info.id.clone())).await {
                                        log::error!("Failed to send peer expired event: {}", e);
                                    }
                                }
                            }
                        },
                        // Handle shutdown signal
                        _ = &mut shutdown_receiver => {
                            log::info!("Shutting down mDNS discovery...");
                            break;
                        }
                    }
                }
                
                // Cleanup resources
                log::info!("mDNS discovery task finished");
                Ok::<(), DiscoveryError>(())
            };
            
            // Run the discovery future with a timeout
            match tokio::time::timeout(
                Duration::from_secs(5),
                discovery_future
            ).await {
                Ok(Ok(())) => log::info!("mDNS discovery finished successfully"),
                Ok(Err(e)) => log::error!("mDNS discovery error: {}", e),
                Err(_) => log::warn!("mDNS discovery timed out"),
            }
            
            // Ensure the running flag is reset
            running.store(false, Ordering::SeqCst);
        });
        
        self.task_handle = Some(task);
        log::info!("mDNS discovery task started successfully");
        Ok(())
            .multiplex(yamux::YamuxConfig::default())
            .boxed();
        
        // Create mDNS behaviour
            
            // Create a future for the main event loop
            let discovery_future = async {
                let mut refresh_interval = interval(PEER_REFRESH_INTERVAL);
                let mut expired_peers = HashSet::new();
                
                loop {
                    tokio::select! {
                        // Handle swarm events
                        event = swarm.select_next_some() => {
                        match event {
                            libp2p::swarm::SwarmEvent::Behaviour(mdns::Event::Discovered(discovered_list)) => {
                                log::debug!("Discovered {} peers via mDNS", discovered_list.len());
                                
                                for (peer_id, addr) in discovered_list {
                                    // Skip our own peer ID
                                    if peer_id == local_peer_id {
                                        log::trace!("Skipping our own peer ID: {}", peer_id);
                                        continue;
                                    }
                                    
                                    log::debug!("Discovered peer: {} at {}", peer_id, addr);
                                    
                                    // Convert the peer ID to bytes
                                    let peer_id_bytes = Self::peer_id_to_bytes(&peer_id);
                                    
                                    // Convert the multiaddress to a socket address
                                    let socket_addrs: Vec<SocketAddr> = std::iter::once(addr)
                                        .filter_map(|addr| {
                                            let socket_addr = Self::multiaddr_to_socket_addr(&addr);
                                            if socket_addr.is_none() {
                                                log::warn!("Could not convert multiaddr to socket addr: {}", addr);
                                            }
                                            socket_addr
                                        })
                                        .collect();
                                    
                                    if socket_addrs.is_empty() {
                                        log::warn!("No valid socket addresses for peer: {}", peer_id);
                                        continue;
                                    }
                                    
                                    // Create a new peer info object
                                    let peer_info = PeerInfo {
                                        id: peer_id_bytes.clone(),
                                        addresses: socket_addrs,
                                        protocols: vec!["libp2p/mdns".to_string()],
                                        metadata: HashMap::new(),
                                    };
                                    
                                    let now = Instant::now();
                                    
                                    // Update the peer in our cache
                                    let is_new_peer = {
                                        let mut peers = match peers_clone.lock() {
                                            Ok(peers) => peers,
                                            Err(err) => {
                                                log::error!("Failed to acquire lock on peers: {}", err);
                                                continue;
                                            }
                                        };
                                        
                                        let is_new = !peers.contains_key(&peer_id_bytes);
                                        peers.insert(peer_id_bytes.clone(), (peer_info.clone(), now));
                                        is_new
                                    };
                                    
                                    // Send the appropriate event
                                    let event = if is_new_peer {
                                        log::info!("New peer discovered: {} at {:?}", peer_id, peer_info.addresses);
                                        DiscoveryEvent::PeerDiscovered(peer_info)
                                    } else {
                                        log::debug!("Peer updated: {} at {:?}", peer_id, peer_info.addresses);
                                        DiscoveryEvent::PeerUpdated(peer_info)
                                    };
                                    
                                    if let Err(e) = event_sender.send(event).await {
                                        log::error!("Failed to send peer event: {}", e);
                                        break;
                                    }
                                }
                            },
                            libp2p::swarm::SwarmEvent::Behaviour(mdns::Event::Expired(expired_list)) => {
                                log::debug!("{} peers expired via mDNS", expired_list.len());
                                
                                for (peer_id, _) in expired_list {
                                    let peer_id_bytes = Self::peer_id_to_bytes(&peer_id);
                                    
                                    // Remove the peer from our cache
                                    let was_removed = {
                                        let mut peers = match peers_clone.lock() {
                                            Ok(peers) => peers,
                                            Err(err) => {
                                                log::error!("Failed to acquire lock on peers: {}", err);
                                                continue;
                                            }
                                        };
                                        
                                        peers.remove(&peer_id_bytes).is_some()
                                    };
                                    
                                    if was_removed {
                                        log::info!("Peer expired: {}", peer_id);
                                        
                                        // Emit peer expired event
                                        if let Err(e) = event_sender.send(DiscoveryEvent::PeerExpired(peer_id_bytes)).await {
                                            log::error!("Failed to send peer expired event: {}", e);
                                            break;
                                        }
                                    }
                                }
                            },
                            libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } => {
                                log::info!("Now listening on {}", address);
                            },
                            libp2p::swarm::SwarmEvent::IncomingConnection { local_addr, send_back_addr, .. } => {
                                log::debug!("Incoming connection from {} to {}", send_back_addr, local_addr);
                            },
                            libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                                log::debug!("Connection established with {} via {}", peer_id, endpoint.get_remote_address());
                            },
                            libp2p::swarm::SwarmEvent::ConnectionClosed { peer_id, endpoint, .. } => {
                                log::debug!("Connection closed with {} via {}", peer_id, endpoint.get_remote_address());
                            },
                            libp2p::swarm::SwarmEvent::Dialing(peer_id) => {
                                log::debug!("Dialing peer: {}", peer_id);
                            },
                            other => {
                                log::trace!("Unhandled swarm event: {:?}", other);
                            }
                        }
                    },
                    // Handle periodic peer expiration check
                    _ = refresh_interval.tick() => {
                        let now = Instant::now();
                        expired_peers.clear();
                        
                        // Find expired peers
                        {
                            let peers = match peers_clone.lock() {
                                Ok(peers) => peers,
                                Err(err) => {
                                    log::error!("Failed to acquire lock on peers: {}", err);
                                    continue;
                                }
                            };
                            
                            for (id, (_, last_seen)) in peers.iter() {
                                if now.duration_since(*last_seen) > peer_expiration {
                                    expired_peers.push(id.clone());
                                }
                            }
                        }
                        
                        // Remove expired peers
                        if !expired_peers.is_empty() {
                            log::info!("Removing {} expired peers", expired_peers.len());
                            
                            let mut peers = match peers_clone.lock() {
                                Ok(peers) => peers,
                                Err(err) => {
                                    log::error!("Failed to acquire lock on peers: {}", err);
                                    continue;
                                }
                            };
                            
                            for id in &expired_peers {
                                if let Some((info, _)) = peers.remove(id) {
                                    log::info!("Removed expired peer: {:?}", info);
                                    
                                    // Emit peer expired event
                                    if let Err(e) = event_sender.send(DiscoveryEvent::PeerExpired(id.clone())).await {
                                        log::error!("Failed to send peer expired event: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                
                log::info!("mDNS discovery event loop finished");
                Ok::<(), DiscoveryError>(())
            };
            
            // Wait for either the shutdown signal or the discovery to complete
            tokio::select! {
                _ = shutdown_future => {
                    log::info!("Shutting down mDNS discovery...");
                },
                result = discovery_future => {
                    if let Err(e) = result {
                        log::error!("mDNS discovery error: {}", e);
                        // Try to send an error event if possible
                        let _ = event_sender.send(DiscoveryEvent::Error(e)).await;
                    }
                }
            }
            
            // Perform any necessary cleanup
            log::info!("mDNS discovery task cleanup complete");
        });
        
        log::info!("mDNS discovery task started successfully");
        Ok(())
    }
    
    /// Stop the discovery service
    pub async fn stop(&mut self) -> Result<(), DiscoveryError> {
        log::info!("Stopping mDNS discovery...");
        
        // If we have a shutdown sender, send the shutdown signal
        if let Some(shutdown_sender) = self.config.shutdown_sender.take() {
            if let Err(e) = shutdown_sender.send(()) {
                log::warn!("Failed to send shutdown signal: {}", e);
            }
        }
        
        // Wait for the task to complete if it exists
        if let Some(handle) = self.task_handle.take() {
            log::debug!("Waiting for mDNS discovery task to complete...");
            
            // Wait for the task to complete with a timeout
            match tokio::time::timeout(
                Duration::from_secs(5), // 5 second timeout
                handle
            ).await {
                Ok(Ok(_)) => {
                    log::debug!("mDNS discovery task finished successfully");
                },
                Ok(Err(e)) => {
                    log::error!("Error in mDNS discovery task: {}", e);
                    
                    // Try to send an error event if possible
                    if let Some(sender) = self.event_sender.as_mut() {
                        if let Err(e) = sender.send(DiscoveryEvent::Error(
                            DiscoveryError::MdnsError("mDNS discovery task failed".to_string())
                        )).await {
                            log::error!("Failed to send error event: {}", e);
                        }
                    }
                    
                    return Err(DiscoveryError::MdnsError("mDNS discovery task failed".to_string()));
                },
                Err(_) => {
                    log::warn!("mDNS discovery task did not finish in time, forcing shutdown...");
                    handle.abort();
                    
                    // Try to wait for the task to be aborted
                    if let Err(e) = handle.await {
                        log::warn!("Error waiting for task to abort: {}", e);
                    }
                }
            }
        }
        
        // Clear the peer cache
        log::debug!("Clearing peer cache...");
        self.peers.lock().clear();
        
        // Reset the running flag
        self.running.store(false, Ordering::SeqCst);
        
        log::info!("mDNS discovery stopped successfully");
        Ok(())
    }
    
    fn announce(&mut self, info: PeerInfo) -> Pin<Box<dyn Future<Output = Result<(), DiscoveryError>> + Send>> {
        log::info!("Announcing peer with ID: {:?}", info.id);
        log::debug!("Peer info: {:?}", info);
        
        // Validate the peer info
        if info.id.is_empty() {
            let err_msg = "Cannot announce peer with empty ID".to_string();
            log::error!("{}", err_msg);
            return Box::pin(async { Err(DiscoveryError::MdnsError(err_msg)) });
        }
        
        if info.addresses.is_empty() {
            log::warn!("Announcing peer with no addresses, this peer may not be reachable");
        }
        
        // Store our own info
        self.own_info = Some(info.clone());
        
        // Log the announcement
        log::info!("Successfully announced peer with ID: {:?}", info.id);
        log::debug!("Peer addresses: {:?}", info.addresses);
        
        // No additional action needed as mDNS handles the announcement automatically
        Box::pin(async { Ok(()) })
    }
    
    fn lookup_peer(&mut self, id: &[u8]) -> Pin<Box<dyn Future<Output = Result<Option<PeerInfo>, DiscoveryError>> + Send>> {
        // Clone the necessary values to move into the future
        let peers = self.peers.clone();
        let id = id.to_vec();
        
        Box::pin(async move {
            log::debug!("Looking up peer with ID: {:?}", id);
            
            // Acquire the lock on the peers map
            let peers_guard = match peers.lock() {
                Ok(guard) => guard,
                Err(err) => {
                    let err_msg = format!("Failed to acquire lock on peers: {}", err);
                    log::error!("{}", err_msg);
                    return Err(DiscoveryError::MdnsError(err_msg));
                }
            };
            
            // Look up the peer by ID
            let peer_info = peers_guard.get(&id).map(|(info, _)| info.clone());
            
            match &peer_info {
                Some(info) => log::debug!("Found peer with ID: {:?}", info.id),
                None => log::debug!("Peer not found with ID: {:?}", id),
            }
            
            Ok(peer_info)
        })
    }
    
    fn find_peers(
        &mut self,
        predicate: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PeerInfo>, DiscoveryError>> + Send>> {
        // Clone the necessary values to move into the future
        let peers = self.peers.clone();
        
        Box::pin(async move {
            log::debug!("Finding peers with predicate: {:?}", predicate);
            
            // Acquire the lock on the peers map
            let peers_guard = match peers.lock() {
                Ok(guard) => guard,
                Err(err) => {
                    let err_msg = format!("Failed to acquire lock on peers: {}", err);
                    log::error!("{}", err_msg);
                    return Err(DiscoveryError::MdnsError(err_msg));
                }
            };
            
            // If no predicate is provided, return all peers
            let pred = match predicate {
                Some(p) => p,
                None => {
                    log::debug!("Returning all {} peers", peers_guard.len());
                    return Ok(peers_guard.values()
                        .map(|(info, _)| info.clone())
                        .collect());
                }
            };
            
            // Convert predicate to lowercase once for case-insensitive matching
            let pred_lower = pred.to_lowercase();
            
            // Filter peers based on the predicate
            let result: Vec<PeerInfo> = peers_guard
                .values()
                .filter_map(|(info, _)| {
                    // Check if any protocol matches the predicate (case-insensitive)
                    let protocol_match = info.protocols.iter()
                        .any(|p| p.to_lowercase().contains(&pred_lower));
                    
                    // Check if any metadata value matches the predicate (case-insensitive)
                    let metadata_match = info.metadata.values()
                        .any(|v| v.to_lowercase().contains(&pred_lower));
                    
                    if protocol_match || metadata_match {
                        Some(info.clone())
                    } else {
                        None
                    }
                })
                .collect();
            
            log::debug!("Found {} peers matching predicate '{}'", result.len(), pred);
            
            Ok(result)
        })
    }
    
    fn next_event(&mut self) -> Pin<Box<dyn Future<Output = Option<DiscoveryEvent>> + Send>> {
        // If we don't have a receiver, create a new one
        if self.event_receiver.is_none() {
            let (tx, rx) = channel(self.config.event_buffer_size);
            self.event_sender = Some(tx);
            self.event_receiver = Some(rx);
        }
        
        // Clone the receiver for this call
        let mut receiver = match &mut self.event_receiver {
            Some(rx) => rx.clone(),
            None => return Box::pin(async { None }),
        };
        
        // Create a oneshot channel to send the event back
        let (tx, rx) = tokio::sync::oneshot::channel();
        
        // Spawn a task to wait for the next event
        tokio::spawn(async move {
            let event = receiver.next().await;
            let _ = tx.send(event);
        });
        
        // Return a future that resolves when we get the event
        Box::pin(async move {
            match rx.await {
                Ok(event) => event,
                Err(_) => {
                    log::error!("Failed to receive event from oneshot channel");
                    None
                }
            }
        })
    }
    
    fn is_running(&self) -> bool {
        use std::sync::atomic::Ordering;
        self.running.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Discovery for MdnsDiscovery {
    async fn start(&mut self) -> Result<(), DiscoveryError> {
        if self.running.load(Ordering::SeqCst) {
            return Err(DiscoveryError::AlreadyRunning);
        }

        // Initialize mDNS
        self.init_mdns().await?;
        
        // Start the background task
        self.start_discovery_task().await?;
        
        // Send started event
        self.send_event(DiscoveryEvent::ServiceStarted).await?;
        
        Ok(())
    }
    
    async fn stop(&mut self) -> Result<(), DiscoveryError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(DiscoveryError::NotRunning);
        }
        
        // Send shutdown signal
        if let Some(sender) = self.config.shutdown_sender.take() {
            let _ = sender.send(());
        }
        
        // Wait for the task to finish
        if let Some(handle) = self.task_handle.take() {
            if let Err(e) = handle.await {
                error!("Error in discovery task: {}", e);
            }
        }
        
        // Clear peers
        self.peers.lock().clear();
        
        // Send stopped event
        self.send_event(DiscoveryEvent::ServiceStopped).await?;
        
        Ok(())
    }
    
    async fn announce(&mut self, info: PeerInfo) -> Result<(), DiscoveryError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(DiscoveryError::NotRunning);
        }
        
        // Store the local peer ID
        let _ = Self::peer_info_to_peer_id(&info)?; // Validate the peer ID
        self.local_peer_id = Some(info.id.clone());
        
        // Update our peer info
        let mut peers = self.peers.lock();
        peers.insert(info.id.clone(), (info, Instant::now()));
        
        Ok(())
    }
    
    async fn lookup_peer(&self, peer_id: &[u8]) -> Result<Option<PeerInfo>, DiscoveryError> {
        let peers = self.peers.lock();
        Ok(peers.get(peer_id).map(|(info, _)| info.clone()))
    }
    
    async fn find_peers(&self, _criteria: &str) -> Result<Vec<PeerInfo>, DiscoveryError> {
        let peers = self.peers.lock();
        Ok(peers.values().map(|(info, _)| info.clone()).collect())
    }
    
    async fn next_event(&mut self) -> Result<Option<DiscoveryEvent>, DiscoveryError> {
        use futures::StreamExt;
        
        match &mut self.event_receiver {
            Some(receiver) => {
                match receiver.next().await {
                    Some(event) => Ok(Some(event)),
                    None => Ok(None),
                }
            },
            None => Ok(None),
        }
    }
    
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    
    fn local_peer_id(&self) -> Option<Vec<u8>> {
        self.local_peer_id.clone()
    }
    
    async fn discovered_peers(&self) -> Result<Vec<PeerInfo>, DiscoveryError> {
        let peers = self.peers.lock();
        Ok(peers.values().map(|(info, _)| info.clone()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;
    use libp2p::PeerId;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::time::Duration;
    
    // Helper function to create a test peer
    fn create_test_peer() -> (PeerId, PeerInfo) {
        let keypair = Keypair::generate_ed25519();
        let peer_id = PeerId::from_public_key(&keypair.public());
        let peer_info = PeerInfo {
            id: peer_id.to_bytes(),
            addresses: vec![SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))],
            protocols: vec!["test".to_string()],
            metadata: HashMap::new(),
            first_seen: None,
            last_seen: None,
            connection_status: ConnectionStatus::Disconnected,
        };
        (peer_id, peer_info)
    }
    
    #[tokio::test]
    async fn test_mdns_discovery_lifecycle() {
        // Create a new mDNS discovery instance with a short peer expiration time
        let config = MdnsDiscoveryConfig::new()
            .with_peer_expiration(1) // 1 second for testing
            .with_event_buffer_size(10);
            
        let mut discovery = MdnsDiscovery::with_config(config);
        
        // Discovery should start successfully
        assert!(!discovery.is_running());
        discovery.start().await.expect("Failed to start discovery");
        assert!(discovery.is_running());
        
        // Should be able to announce a peer
        let (_, peer_info) = create_test_peer();
        discovery.announce(peer_info.clone()).await.expect("Failed to announce peer");
        
        // Should be able to look up the announced peer
        let found_peer = discovery.lookup_peer(&peer_info.id).await.expect("Lookup failed");
        assert!(found_peer.is_some());
        assert_eq!(found_peer.unwrap().id, peer_info.id);
        
        // Should be able to find all peers
        let all_peers = discovery.find_peers("").await.expect("Failed to find peers");
        assert!(!all_peers.is_empty());
        assert!(all_peers.iter().any(|p| p.id == peer_info.id));
        
        // Should be able to get events
        if let Some(event) = discovery.next_event().await.expect("Failed to get next event") {
            match event {
                DiscoveryEvent::PeerDiscovered(info) => {
                    assert_eq!(info.id, peer_info.id);
                },
                _ => panic!("Unexpected event: {:?}", event),
            }
        }
        
        // Should be able to stop the discovery service
        discovery.stop().await.expect("Failed to stop discovery");
        assert!(!discovery.is_running());
    }
    
    #[tokio::test]
    async fn test_peer_expiration() {
        // Create a new mDNS discovery instance with a very short peer expiration time
        let config = MdnsDiscoveryConfig::new()
            .with_peer_expiration(1) // 1 second for testing
            .with_event_buffer_size(10);
            
        let mut discovery = MdnsDiscovery::with_config(config);
        discovery.start().await.expect("Failed to start discovery");
        
        // Announce a peer
        let (_, peer_info) = create_test_peer();
        discovery.announce(peer_info.clone()).await.expect("Failed to announce peer");
        
        // Peer should be immediately discoverable
        assert!(discovery.lookup_peer(&peer_info.id).await.expect("Lookup failed").is_some());
        
        // Wait for the peer to expire (1 second + buffer)
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        // Peer should no longer be discoverable
        assert!(discovery.lookup_peer(&peer_info.id).await.expect("Lookup failed").is_none());
        
        discovery.stop().await.expect("Failed to stop discovery");
    }
    
    #[tokio::test]
    async fn test_duplicate_announce() {
        let config = MdnsDiscoveryConfig::new()
            .with_peer_expiration(60)
            .with_event_buffer_size(10);
            
        let mut discovery = MdnsDiscovery::with_config(config);
        discovery.start().await.expect("Failed to start discovery");
        
        let (_, mut peer_info) = create_test_peer();
        
        // First announcement
        discovery.announce(peer_info.clone()).await.expect("First announce failed");
        
        // Update peer info and announce again
        peer_info.protocols.push("updated".to_string());
        discovery.announce(peer_info.clone()).await.expect("Second announce failed");
        
        // Should get the updated peer info
        let found_peer = discovery.lookup_peer(&peer_info.id).await.expect("Lookup failed").unwrap();
        assert!(found_peer.protocols.contains(&"updated".to_string()));
        
        discovery.stop().await.expect("Failed to stop discovery");
    }
}