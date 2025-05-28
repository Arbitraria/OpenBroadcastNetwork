//! mDNS service implementation for peer discovery

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::channel::mpsc::{channel, Sender};
use futures::{SinkExt, StreamExt};
// use libp2p::core::multiaddr::Multiaddr; // Commented out until needed
// TODO: Update with proper libp2p mdns imports once integrated
// use libp2p::mdns::{Mdns, MdnsEvent};

// Placeholder types until we properly integrate with libp2p mdns
#[derive(Debug)]
struct Mdns {
    // Add fields as needed
    _config: MdnsDiscoveryConfig,
}

impl Mdns {
    fn new(config: MdnsDiscoveryConfig) -> Self {
        Mdns { _config: config }
    }
    
    // Simple stub for the next method
    async fn next(&mut self) -> Option<MdnsEvent> {
        // In a real implementation, this would wait for mDNS events
        // For now, just return None to indicate no events
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        None
    }
}

#[derive(Debug)]
enum MdnsEvent {
    Discovered(Vec<(libp2p::PeerId, libp2p::core::multiaddr::Multiaddr)>),
    Expired(Vec<(libp2p::PeerId, libp2p::core::multiaddr::Multiaddr)>),
}
use libp2p::swarm::SwarmEvent;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::discovery::interface::{ConnectionStatus, DiscoveryError, DiscoveryEvent, PeerInfo};
use crate::discovery::mdns::config::MdnsDiscoveryConfig;

// Utility functions that were previously in discovery/utils

/// Convert bytes to a PeerId
fn bytes_to_peer_id(bytes: &[u8]) -> Result<libp2p::PeerId, String> {
    libp2p::PeerId::from_bytes(bytes)
        .map_err(|e| format!("Failed to convert bytes to PeerId: {}", e))
}

/// Convert PeerId to bytes
fn peer_id_to_bytes(peer_id: &libp2p::PeerId) -> Vec<u8> {
    peer_id.to_bytes()
}

/// Convert a Multiaddr to a SocketAddr
fn multiaddr_to_socket_addr(addr: &libp2p::core::multiaddr::Multiaddr) -> Option<std::net::SocketAddr> {
    let mut ip = None;
    let mut tcp = None;
    let mut udp = None;
    
    // Extract IP and port components
    for protocol in addr.iter() {
        match protocol {
            libp2p::core::multiaddr::Protocol::Ip4(ipv4) => ip = Some(std::net::IpAddr::V4(ipv4)),
            libp2p::core::multiaddr::Protocol::Ip6(ipv6) => ip = Some(std::net::IpAddr::V6(ipv6)),
            libp2p::core::multiaddr::Protocol::Tcp(port) => tcp = Some(port),
            libp2p::core::multiaddr::Protocol::Udp(port) => udp = Some(port),
            _ => {}
        }
    }
    
    // Prefer TCP over UDP if both are present
    if let Some(ip) = ip {
        if let Some(port) = tcp.or(udp) {
            return Some(std::net::SocketAddr::new(ip, port));
        }
    }
    
    None
}

/// Time between peer expiration checks
const PEER_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// mDNS-based peer discovery service
pub struct MdnsService {
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
    pub local_peer_id: Option<Vec<u8>>,
    
    /// Handle to the background task
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl MdnsService {
    /// Create a new mDNS service with the given configuration
    pub fn new(config: MdnsDiscoveryConfig) -> Self {
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
    pub async fn init(&mut self) -> Result<(), DiscoveryError> {
        if self.mdns.is_some() {
            return Ok(());
        }
        
        let _config = libp2p::mdns::Config {
            ttl: Duration::from_secs(self.config.ttl.into()),
            ..Default::default()
        };
            
        // Create a new Mdns instance with our config
        let mdns = Mdns::new(self.config.clone());
            
        self.mdns = Some(mdns);
        Ok(())
    }
    
    /// Start the mDNS service
    pub async fn start(&mut self) -> Result<(), DiscoveryError> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(DiscoveryError::AlreadyRunning);
        }
        
        // Initialize mDNS if not already done
        self.init().await?;
        
        // Create a channel for the shutdown signal
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        
        // Make a copy of config values we need in the task
        let peer_expiration_duration = self.config.peer_expiration_duration();
        let event_buffer_size = self.config.event_buffer_size;
        
        // Store the shutdown sender
        self.config.shutdown_sender = Some(shutdown_sender);
        
        // Clone the necessary state
        let peers = self.peers.clone();
        let running = self.running.clone();
        
        // Create the event channel and initialize discovery
        let mut event_sender = self.event_sender.take().ok_or_else(|| {
            DiscoveryError::AlreadyStarted("MdnsService already started".to_string())
        })?;
        
        // Create a new channel for events
        let (new_event_sender, mut event_receiver) = channel(event_buffer_size);
        self.event_sender = Some(new_event_sender);
        
        // Get the mDNS instance
        let mut mdns = self.mdns.take().ok_or_else(|| {
            DiscoveryError::ConfigError("mDNS service not initialized".to_string())
        })?;
        
        // Spawn the discovery task
        let task_handle = tokio::spawn(async move {
            let mut interval = interval(PEER_REFRESH_INTERVAL);
            
            loop {
                tokio::select! {
                    // Handle shutdown signal
                    _ = &mut shutdown_receiver => {
                        debug!("Received shutdown signal, stopping mDNS discovery");
                        break;
                    }
                    
                    // Handle mDNS events
                    Some(event) = mdns.next() => {
                        // Create a cloned sender to avoid mutable reference issues
                        let mut event_sender_clone = event_sender.clone();
                        if let Err(e) = Self::process_mdns_event(&peers, &mut event_sender_clone, event).await {
                            error!("Error processing mDNS event: {}", e);
                        }
                    }
                    
                    // Handle periodic peer expiration check
                    _ = interval.tick() => {
                        let mut event_sender_clone = event_sender.clone();
                        if let Err(e) = Self::check_peer_expiration(
                            &peers, 
                            &mut event_sender_clone, 
                            peer_expiration_duration
                        ).await {
                            error!("Error checking peer expiration: {}", e);
                        }
                    }
                    
                    // Forward events from the internal channel
                    event = event_receiver.next() => {
                        match event {
                            Some(event) => {
                                if let Err(e) = event_sender.send(event).await {
                                    error!("Failed to forward event: {}", e);
                                    break;
                                }
                            }
                            None => {
                                // Channel was closed
                                break;
                            }
                        }
                    }
                }
            }
            
            // Cleanup
            running.store(false, Ordering::SeqCst);
            debug!("mDNS discovery task finished");
        });
        
        self.task_handle = Some(task_handle);
        info!("mDNS discovery service started");
        Ok(())
    }
    
    /// Process an mDNS event
    async fn process_mdns_event(
        peers: &Arc<Mutex<HashMap<Vec<u8>, (PeerInfo, Instant)>>>,
        event_sender: &mut Sender<DiscoveryEvent>,
        event: MdnsEvent,
    ) -> Result<(), DiscoveryError> {
        match event {
            MdnsEvent::Discovered(discovered_list) => {
                // Process all discovered peers in batches
                let mut peers_to_process = Vec::new();
                
                // First, prepare all peer info without holding locks
                for (peer_id, addr) in discovered_list {
                    let peer_id_bytes = peer_id_to_bytes(&peer_id);
                    
                    // Convert the multiaddress to a socket address
                    let socket_addrs: Vec<SocketAddr> = std::iter::once(addr)
                        .filter_map(|addr| {
                            let socket_addr = multiaddr_to_socket_addr(&addr);
                            if socket_addr.is_none() {
                                warn!("Could not convert multiaddr to socket addr: {}", addr);
                            }
                            socket_addr
                        })
                        .collect();
                    
                    if socket_addrs.is_empty() {
                        warn!("No valid socket addresses for peer: {}", peer_id);
                        continue;
                    }
                    
                    // Create a new peer info object
                    let peer_info = PeerInfo {
                        id: peer_id_bytes.clone(),
                        addresses: socket_addrs,
                        protocols: vec!["libp2p/mdns".to_string()],
                        metadata: HashMap::new(),
                        first_seen: None,
                        last_seen: None,
                        connection_status: ConnectionStatus::Disconnected,
                    };
                    
                    peers_to_process.push((peer_id, peer_id_bytes, peer_info));
                }
                
                // Now process each peer, updating the cache and creating events
                for (peer_id, peer_id_bytes, peer_info) in peers_to_process {
                    let now = Instant::now();
                    
                    // Update the peer in our cache
                    let is_new_peer = {
                        let mut peers_lock = peers.lock();
                        let is_new = !peers_lock.contains_key(&peer_id_bytes);
                        peers_lock.insert(peer_id_bytes.clone(), (peer_info.clone(), now));
                        is_new
                    }; // lock is dropped here
                    
                    // Create the appropriate event
                    let event = if is_new_peer {
                        info!("New peer discovered: {} at {:?}", peer_id, peer_info.addresses);
                        DiscoveryEvent::PeerDiscovered(peer_info)
                    } else {
                        debug!("Peer updated: {} at {:?}", peer_id, peer_info.addresses);
                        DiscoveryEvent::PeerUpdated(peer_info)
                    };
                    
                    // Send the event
                    if let Err(e) = event_sender.send(event).await {
                        error!("Failed to send peer event: {}", e);
                        return Err(DiscoveryError::ChannelError(e.to_string()));
                    }
                }
            }
            MdnsEvent::Expired(expired_list) => {
                // Process all expired peers in a batch to minimize lock contention
                let peers_to_remove = {
                    let mut result = Vec::new();
                    for (peer_id, _) in expired_list {
                        let peer_id_bytes = peer_id_to_bytes(&peer_id);
                        
                        // Check if peer exists in our cache
                        let mut peers_lock = peers.lock();
                        if peers_lock.remove(&peer_id_bytes).is_some() {
                            info!("Peer expired: {}", peer_id);
                            result.push((peer_id, peer_id_bytes));
                        }
                    }
                    result
                }; // lock is dropped here
                
                // Send events without holding the lock
                for (_peer_id, peer_id_bytes) in peers_to_remove {
                    // Emit peer expired event
                    if let Err(e) = event_sender.send(DiscoveryEvent::PeerExpired(peer_id_bytes)).await {
                        error!("Failed to send peer expired event: {}", e);
                        return Err(DiscoveryError::ChannelError(e.to_string()));
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Check for expired peers and remove them
    async fn check_peer_expiration(
        peers: &Arc<Mutex<HashMap<Vec<u8>, (PeerInfo, Instant)>>>,
        event_sender: &mut Sender<DiscoveryEvent>,
        peer_expiration: Duration,
    ) -> Result<(), DiscoveryError> {
        let now = Instant::now();
        let mut expired_peers = Vec::new();
        
        // Find expired peers
        {
            let peers_guard = peers.lock();
            for (id, (_, last_seen)) in peers_guard.iter() {
                if now.duration_since(*last_seen) > peer_expiration {
                    expired_peers.push(id.clone());
                }
            }
        }
        
        // Remove expired peers
        if !expired_peers.is_empty() {
            debug!("Removing {} expired peers", expired_peers.len());
            
            // Collect the information about peers to remove while holding the lock
            let peers_to_remove = {
                let mut peers_guard = peers.lock();
                let mut to_remove = Vec::new();
                
                for id in expired_peers {
                    if let Some((info, _)) = peers_guard.remove(&id) {
                        info!("Removed expired peer: {:?}", info);
                        to_remove.push((id, info));
                    }
                }
                
                to_remove
            }; // The mutex guard is dropped here
            
            // Now send events without holding the lock
            for (id, _info) in peers_to_remove {
                // Emit peer expired event
                if let Err(e) = event_sender.send(DiscoveryEvent::PeerExpired(id)).await {
                    error!("Failed to send peer expired event: {}", e);
                    return Err(DiscoveryError::ChannelError(e.to_string()));
                }
            }
        }
        
        Ok(())
    }
    
    /// Stop the mDNS service
    pub async fn stop(&mut self) -> Result<(), DiscoveryError> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Err(DiscoveryError::NotRunning);
        }
        
        // Send shutdown signal
        if let Some(sender) = self.config.shutdown_sender.take() {
            if let Err(e) = sender.send(()) {
                warn!("Failed to send shutdown signal: {:?}", e);
            }
        }
        
        // Wait for the task to finish
        if let Some(handle) = self.task_handle.take() {
            if let Err(e) = handle.await {
                error!("Error in mDNS discovery task: {}", e);
                return Err(DiscoveryError::TaskError(e.to_string()));
            }
        }
        
        // Clear peers
        self.peers.lock().clear();
        
        info!("mDNS discovery service stopped");
        Ok(())
    }
    
    /// Check if the service is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    
    /// Get the local peer ID
    pub fn local_peer_id(&self) -> Option<Vec<u8>> {
        self.local_peer_id.clone()
    }
    
    /// Set the local peer ID
    pub fn set_local_peer_id(&mut self, peer_id: Vec<u8>) {
        self.local_peer_id = Some(peer_id);
    }
    
    /// Get the event sender
    pub fn event_sender(&self) -> Option<Sender<DiscoveryEvent>> {
        self.event_sender.clone()
    }
    
    /// Get the list of discovered peers
    pub async fn discovered_peers(&self) -> Result<Vec<PeerInfo>, DiscoveryError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(DiscoveryError::NotRunning);
        }
        
        Ok(self.peers.lock().values().map(|(info, _)| info.clone()).collect())
    }
}

impl Drop for MdnsService {
    fn drop(&mut self) {
        if self.running.load(Ordering::SeqCst) {
            if let Err(e) = futures::executor::block_on(self.stop()) {
                error!("Error stopping mDNS service during drop: {}", e);
            }
        }
    }
}
