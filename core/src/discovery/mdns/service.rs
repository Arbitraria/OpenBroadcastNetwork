//! mDNS service implementation for peer discovery

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::channel::mpsc::{channel, Sender};
use futures::{SinkExt, StreamExt};
use libp2p::core::multiaddr::Multiaddr;
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::swarm::SwarmEvent;
use parking_lot::Mutex;
use tokio::sync::oneshot::{self, Sender as OneshotSender};
use tokio::time::interval;
use tracing::{debug, error, info, trace, warn};

use crate::discovery::interface::{ConnectionStatus, DiscoveryError, DiscoveryEvent, PeerInfo};
use crate::discovery::mdns::config::MdnsDiscoveryConfig;
use crate::discovery::utils::{bytes_to_peer_id, multiaddr_to_socket_addr, peer_id_to_bytes};

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
        
        let config = libp2p::mdns::Config {
            ttl: Duration::from_secs(self.config.ttl.into()),
            ..Default::default()
        };
            
        let mdns = Mdns::new(config)
            .await
            .map_err(|e| DiscoveryError::MdnsError(Box::new(e)))?;
            
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
        let task_handle = tokio::spawn(async move {
            let mut interval = interval(PEER_REFRESH_INTERVAL);
            let mut expired_peers = HashSet::new();
            
            loop {
                tokio::select! {
                    // Handle shutdown signal
                    _ = &mut shutdown_receiver => {
                        debug!("Received shutdown signal, stopping mDNS discovery");
                        break;
                    }
                    
                    // Handle mDNS events
                    Some(event) = mdns.next() => {
                        if let Err(e) = Self::process_mdns_event(&peers, &event_sender, event).await {
                            error!("Error processing mDNS event: {}", e);
                        }
                    }
                    
                    // Handle periodic peer expiration check
                    _ = interval.tick() => {
                        if let Err(e) = Self::check_peer_expiration(
                            &peers, 
                            &mut event_sender.clone(), 
                            self.config.peer_expiration_duration()
                        ).await {
                            error!("Error checking peer expiration: {}", e);
                        }
                    }
                    
                    // Forward events from the internal channel
                    event = event_receiver.next() => {
                        if let Some(event) = event {
                            if let Err(e) = event_sender.send(event).await {
                                error!("Failed to forward event: {}", e);
                                break;
                            }
                        } else {
                            // Channel was closed
                            break;
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
        event_sender: &Sender<DiscoveryEvent>,
        event: MdnsEvent,
    ) -> Result<(), DiscoveryError> {
        match event {
            MdnsEvent::Discovered(discovered_list) => {
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
                    
                    let now = Instant::now();
                    
                    // Update the peer in our cache
                    let is_new_peer = {
                        let mut peers = peers.lock();
                        let is_new = !peers.contains_key(&peer_id_bytes);
                        peers.insert(peer_id_bytes.clone(), (peer_info.clone(), now));
                        is_new
                    };
                    
                    // Send the appropriate event
                    let event = if is_new_peer {
                        info!("New peer discovered: {} at {:?}", peer_id, peer_info.addresses);
                        DiscoveryEvent::PeerDiscovered(peer_info)
                    } else {
                        debug!("Peer updated: {} at {:?}", peer_id, peer_info.addresses);
                        DiscoveryEvent::PeerUpdated(peer_info)
                    };
                    
                    if let Err(e) = event_sender.send(event).await {
                        error!("Failed to send peer event: {}", e);
                        return Err(DiscoveryError::ChannelError(e.to_string()));
                    }
                }
            }
            MdnsEvent::Expired(expired_list) => {
                for (peer_id, _) in expired_list {
                    let peer_id_bytes = peer_id_to_bytes(&peer_id);
                    
                    // Remove the peer from our cache
                    let was_removed = {
                        let mut peers = peers.lock();
                        peers.remove(&peer_id_bytes).is_some()
                    };
                    
                    if was_removed {
                        info!("Peer expired: {}", peer_id);
                        
                        // Emit peer expired event
                        if let Err(e) = event_sender.send(DiscoveryEvent::PeerExpired(peer_id_bytes)).await {
                            error!("Failed to send peer expired event: {}", e);
                            return Err(DiscoveryError::ChannelError(e.to_string()));
                        }
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
            
            let mut peers_guard = peers.lock();
            for id in expired_peers {
                if let Some((info, _)) = peers_guard.remove(&id) {
                    info!("Removed expired peer: {:?}", info);
                    
                    // Emit peer expired event
                    if let Err(e) = event_sender.send(DiscoveryEvent::PeerExpired(id)).await {
                        error!("Failed to send peer expired event: {}", e);
                        return Err(DiscoveryError::ChannelError(e.to_string()));
                    }
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
