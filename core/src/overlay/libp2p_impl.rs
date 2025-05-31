//! libp2p implementation of the overlay network
//!
//! This module provides a libp2p-based implementation of the overlay
//! network interface defined in `overlay::interface`. It serves as the primary
//! integration point between our application and the libp2p networking stack.
//!
//! # Architecture Overview
//!
//! The implementation follows a layered architecture:
//!
//! 1. **Network Layer (`Libp2pOverlay`)** - Implements the `Overlay` trait and manages
//!    the libp2p Swarm. This is the entry point for network operations.
//!
//! 2. **Behavior Layer (`OverlayBehavior`)** - Combines multiple libp2p protocols 
//!    (Gossipsub, Kademlia, MDNS, Identify) into a unified behavior.
//!
//! 3. **Management Layer** - Specialized components that handle specific aspects:
//!    - `TopologyManager` - Manages peer connections and network topology
//!    - `RelayManager` - Handles stream relaying between peers
//!    - `MeshNetwork` - Manages mesh network connections
//!
//! # Concurrency Pattern
//!
//! This implementation uses a specific concurrency pattern for thread safety:
//! - `Arc<T>` for shared ownership of managers across threads
//! - `Arc<Mutex<T>>` for exclusive access to mutable shared state
//! - `RwLock<T>` for data structures that are read frequently but written to occasionally
//!
//! # Dependencies
//!
//! This module depends on:
//! - `overlay::interface` - For the core interface definitions
//! - `overlay::libp2p::behavior` - For the network behavior implementation
//! - `overlay::topology` - For topology management
//! - `overlay::relay` - For stream relaying
//! - `overlay::mesh` - For mesh network management
//! - `overlay::peer` - For peer-related types and functionality
//!
//! # Implementation Notes
//!
//! The `Libp2pOverlay` struct is designed to be thread-safe and can be shared
//! across multiple tasks. It maintains internal state and provides methods for
//! peer connection, stream creation, and event handling. using libp2p.

// Core library imports
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use std::pin::Pin;
use std::future::Future;
use futures::{FutureExt, StreamExt, SinkExt};
use futures::channel::mpsc;
use tokio::sync::{Mutex, RwLock};

// External dependencies
use libp2p::{self,
    gossipsub::{self, Gossipsub, GossipsubEvent},
    identify::{Identify, IdentifyEvent, IdentifyConfig},
    kad::{Kademlia, KademliaEvent, KademliaConfig, store::MemoryStore},
    mdns::{Mdns, MdnsEvent, MdnsConfig},
    tcp::GenTcpConfig,
    noise,
    yamux,
    core::upgrade,
    identity,
    Transport,
    SwarmBuilder, SwarmEvent, 
    SwarmParams, Swarm
};
use tokio::time;
use libp2p::Multiaddr;
use libp2p::PeerId as Libp2pPeerId;
use tracing::{info, debug, warn, error};


// Local crate imports
use crate::overlay::interface::{Overlay, OverlayConfig, OverlayError, OverlayEvent, OverlayStats, StreamId};
use crate::overlay::libp2p::behavior::{OverlayBehavior, OverlayBehaviorEvent};
use crate::overlay::peer::{Peer, PeerInfo, PeerRole, ConnectionStatus};
use crate::overlay::topology::{TopologyConfig, TopologyManager};
use crate::overlay::relay::{RelayManager, RelayConfig, StreamChunk};
use crate::overlay::mesh::{MeshNetwork, MeshConfig, StreamMesh, MeshStats};

// Import LocalPeerId directly
use crate::overlay::peer::LocalPeerId;
use std::convert::TryFrom;

/// Convert our LocalPeerId to libp2p's PeerId
fn to_libp2p_peer_id(peer_id: &LocalPeerId) -> Result<Libp2pPeerId, OverlayError> {
    // Use the TryFrom trait implementation
    Libp2pPeerId::try_from(peer_id)
        .map_err(|e| OverlayError::Other(format!("Invalid peer ID: {}", e)))
}

/// Convert libp2p's PeerId to our LocalPeerId
fn from_libp2p_peer_id(peer_id: &Libp2pPeerId) -> LocalPeerId {
    // Use the From trait implementation
    LocalPeerId::from(peer_id)
}

/// Topics for the pub/sub system
mod topics {
    use super::*;
    
    /// Create a control topic for a stream
    pub fn stream_control(stream_id: &StreamId) -> String {
        let id_str = hex::encode(stream_id.as_bytes());
        format!("stream/{}/control", id_str)
    }
    
    /// Create a data topic for a stream
    pub fn stream_data(stream_id: &StreamId) -> String {
        let id_str = hex::encode(stream_id.as_bytes());
        format!("stream/{}/data", id_str)
    }
    
    /// Create a discovery topic
    pub fn discovery() -> String {
        "discovery".to_string()
    }
    
    /// Parse a stream ID from a topic
    pub fn parse_stream_id(topic: &str) -> Option<StreamId> {
        let parts: Vec<&str> = topic.split('/').collect();
        if parts.len() >= 3 && parts[0] == "stream" {
            if let Ok(bytes) = hex::decode(parts[1]) {
                return Some(StreamId::from_bytes(bytes));
            }
        }
        None
    }
}

// OverlayBehavior and OverlayBehaviorEvent are now imported from the behavior module

/// libp2p implementation of the overlay network
pub struct Libp2pOverlay {
    /// Configuration
    config: OverlayConfig,
    /// Local peer ID
    local_peer_id: LocalPeerId,
    /// libp2p peer ID
    libp2p_peer_id: Libp2pPeerId,
    /// Swarm
    swarm: Arc<Mutex<Option<Swarm<OverlayBehavior>>>>,
    /// Topology manager
    topology: Arc<TopologyManager>,
    /// Relay manager
    relay: Arc<RelayManager>,
    /// Mesh network
    mesh: Arc<MeshNetwork>,
    /// Peers
    peers: RwLock<HashMap<LocalPeerId, Peer>>,
    /// Active streams
    streams: RwLock<HashSet<StreamId>>,
    /// Event channel sender
    event_tx: mpsc::Sender<OverlayEvent>,
    /// Event channel receiver
    event_rx: Mutex<mpsc::Receiver<OverlayEvent>>,
    /// Is the overlay running
    running: RwLock<bool>,
    /// Worker task handle
    worker_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Libp2pOverlay {
    /// Create a new libp2p-based overlay
    pub async fn new(config: OverlayConfig) -> Result<Self, OverlayError> {
        // Create the libp2p identity
        let local_key = identity::Keypair::generate_ed25519();
        let libp2p_peer_id = local_key.public().to_peer_id();
        let local_peer_id = LocalPeerId::from(libp2p_peer_id);
        
        // Create channel for events
        let (event_tx, event_rx) = mpsc::channel(100);
        
        // Topology configuration
        let topology_config = TopologyConfig {
            min_peers_per_region: 3,
            max_tree_depth: 5,
            geo_bias: 0.5,
            min_relay_quality: 0.7,
            rebalance_interval: Duration::from_secs(60),
            health_check_interval: Duration::from_secs(30),
            disconnection_grace_period: Duration::from_secs(300),
            enable_geo_aware: true,
            peer_cache_lifetime: Duration::from_secs(3600),
            max_consecutive_failures: 5,
            min_success_rate: 0.8,
            health_record_expiry: Duration::from_secs(3600),
            rebalance_threshold: 10,
            geo_provider: None,
            max_latency: 300,
            default_bandwidth: 1000000,
            score_weights: crate::overlay::topology::TopologyConfig::default().score_weights,
            health_threshold: 0.7,
            proactive_rebalance_threshold: 0.8,
            prefer_same_region: true,
            max_close_distance_km: 1000.0,
            use_coordinates: true,
        };

        // Create topology manager
        let topology = Arc::new(TopologyManager::new(
            to_libp2p_peer_id(&local_peer_id)?,  // Convert LocalPeerId to PeerId
            topology_config,
            None,  // No metrics for now
        ));
        
        // Create relay manager
        let relay_config = RelayConfig {
            max_buffer_size: 100,
            max_chunk_size: 64 * 1024, // 64KB
            stats_interval: Duration::from_secs(5),
            cleanup_interval: Duration::from_secs(30),
            inactivity_timeout: Duration::from_secs(60),
            max_streams: 10,
            enable_bandwidth_limit: false,
            max_outgoing_bandwidth: 5 * 1024 * 1024, // 5 MB/s
        };
        
        // Create relay config
        let relay_config = RelayConfig {
            max_buffer_size: 100,  // Buffer up to 100 chunks per stream
            max_chunk_size: 64 * 1024,  // 64KB max chunk size
            stats_interval: Duration::from_secs(30),  // Report stats every 30 seconds
            cleanup_interval: Duration::from_secs(300),  // Clean up inactive streams every 5 minutes
            inactivity_timeout: Duration::from_secs(60),  // Consider stream inactive after 1 minute of no data
            max_streams: 50,  // Allow up to 50 concurrent streams
            enable_bandwidth_limit: true,
            max_outgoing_bandwidth: 5 * 1024 * 1024,  // 5 MB/s outgoing bandwidth limit
        };
        
        // Create relay manager
        let relay = Arc::new(RelayManager::new(
            to_libp2p_peer_id(&local_peer_id)?, // Convert LocalPeerId to Libp2pPeerId
            relay_config,
            topology.clone(),
        ));
        
        // Create mesh network
        let mesh_config = MeshConfig {
            min_connections: 3,
            target_connections: 8,
            max_connections: 12,
            prune_interval: Duration::from_secs(30),
            backoff_time: Duration::from_secs(60),
            max_connection_attempts: 3,
        };
        
        let mesh = Arc::new(MeshNetwork::new(config.local_peer_id.clone(), mesh_config));
        
        // Create the Libp2pOverlay
        let overlay = Self {
            local_peer_id: config.local_peer_id.clone(),
            libp2p_peer_id,
            config,
            swarm: Arc::new(Mutex::new(None)),
            topology,
            relay,
            mesh,
            peers: RwLock::new(HashMap::new()),
            streams: RwLock::new(HashSet::new()),
            event_tx,
            event_rx: Mutex::new(event_rx),
            running: RwLock::new(false),
            worker_task: Mutex::new(None),
        };
        
        Ok(overlay)
    }
    
    /// Initialize the swarm
    async fn init_swarm(&self) -> Result<Swarm<OverlayBehavior>, OverlayError> {
        // Create a key pair for authentication
        let local_key = identity::Keypair::generate_ed25519();
        let libp2p_peer_id = local_key.public().to_peer_id();
        
        // Create a transport using tokio TCP transport
        let transport = libp2p::tcp::tokio::Transport::new(GenTcpConfig::default().nodelay(true))
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::Config::new(&local_key).unwrap())
            .multiplex(yamux::YamuxConfig::default())
            .boxed();
            
        // Set up gossipsub
        let gossipsub_config = libp2p::gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(10))
            .validation_mode(libp2p::gossipsub::ValidationMode::Strict)
            .build()
            .map_err(|e| OverlayError::Other(format!("Failed to build gossipsub config: {}", e)))?;
            
        let gossipsub = libp2p::gossipsub::Behaviour::new(
            libp2p::gossipsub::MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config
        )
        .map_err(|e| OverlayError::Other(format!("Failed to create gossipsub: {}", e)))?;
        
        // Set up Kademlia
        let kademlia_store = MemoryStore::new(libp2p_peer_id);
        let kademlia = Kademlia::new(libp2p_peer_id, kademlia_store);
        
        // Set up mDNS
        let mdns = Mdns::new(MdnsConfig::default())
            .await
            .map_err(|e| OverlayError::Other(format!("Failed to create mDNS: {}", e)))?;
            
        // Set up identify
        let identify = Identify::new(IdentifyConfig::new(
            "decentralized-stream/1.0.0".to_string(),
            local_key.public()
        ));
        
        // Create the behavior using the constructor
        let behavior = OverlayBehavior::new(
            gossipsub,
            kademlia,
            mdns,
            identify,
        );
        
        // Build the swarm
        // Create the swarm with proper security and transport configuration
        let swarm = SwarmBuilder::with_existing_identity(local_key)
            .with_tokio()
            .with_tcp(
                GenTcpConfig::default(), 
                |key| {
                    let noise_config = noise::Config::new(key).unwrap();
                    Ok::<_, std::io::Error>(noise_config)
                },
                yamux::YamuxConfig::default
            )
            .map_err(|e| OverlayError::Other(format!("Failed to build swarm with TCP: {}", e)))?
            .with_behaviour(|_| behavior)
            .map_err(|e| OverlayError::Other(format!("Failed to build swarm with behavior: {}", e)))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();
            
        Ok(swarm)
    }
    
    /// Run the swarm
    async fn run_swarm(&self) -> Result<(), OverlayError> {
        let mut swarm_lock = self.swarm.lock().await;
        
        if swarm_lock.is_some() {
            return Ok(());
        }
        
        // Initialize the swarm
        let mut swarm = self.init_swarm().await?;
        
        // Setup listeners
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
            .map_err(|e| OverlayError::Other(format!("Failed to listen: {:?}", e)))?;
        
        // Connect to bootstrap peers
        for addr_str in &self.config.bootstrap_peers {
            match addr_str.parse::<Multiaddr>() {
                Ok(addr) => {
                    debug!("Dialing bootstrap peer: {}", addr);
                    match swarm.dial(addr) {
                        Ok(_) => {},
                        Err(e) => warn!("Failed to dial bootstrap peer: {}", e),
                    }
                },
                Err(e) => warn!("Invalid bootstrap address {}: {}", addr_str, e),
            }
        }
        
        // Start the discovery topic
        let discovery_topic = gossipsub::IdentTopic::new(topics::discovery());
        swarm.behaviour_mut().gossipsub.subscribe(&discovery_topic)
            .map_err(|e| OverlayError::Other(format!("Failed to subscribe to discovery: {}", e)))?;
        
        // Store the swarm
        *swarm_lock = Some(swarm);
        
        Ok(())
    }
    
    /// Process swarm events
    async fn process_swarm_events(&self) -> Result<(), OverlayError> {
        let mut swarm_lock = self.swarm.lock().await;
        
        let swarm = match &mut *swarm_lock {
            Some(swarm) => swarm,
            None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
        };
        
        // Poll for events
        match swarm.select_next_some().await {
            SwarmEvent::Behaviour(behavior_event) => {
                self.handle_behavior_event(behavior_event).await?;
            },
            SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                debug!("Connection established with {}", peer_id);
                let peer_id = from_libp2p_peer_id(&peer_id);
                
                // Create peer info
                let mut peer_info = PeerInfo::default();
                peer_info.id = peer_id.clone();
                peer_info.status = ConnectionStatus::Connected;
                
                // get_remote_address returns a reference, not an Option
                let addr = endpoint.get_remote_address();
                peer_info.addresses.push(addr.to_string());
                
                // Create peer
                let peer = Peer::new(peer_id.clone(), peer_info.clone());
                
                // Store peer
                {
                    let mut peers = self.peers.write().await;
                    peers.insert(peer_id.clone(), peer);
                }
                
                // Add to mesh
                let _ = self.mesh.add_peer(peer).await;
                
                // Emit event
                let _ = self.event_tx.send(OverlayEvent::PeerConnected {
                    peer_id: peer_id.clone(),
                    info: peer_info,
                }).await;
            },
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                debug!("Connection closed with {}", peer_id);
                let peer_id = from_libp2p_peer_id(&peer_id);
                
                // Update peer status
                {
                    let mut peers = self.peers.write().await;
                    if let Some(peer) = peers.get_mut(&peer_id) {
                        peer.set_disconnected();
                    }
                }
                
                // Emit event
                let _ = self.event_tx.send(OverlayEvent::PeerDisconnected {
                    peer_id: peer_id.clone(),
                    reason: "Connection closed".to_string(),
                }).await;
            },
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {}", address);
            },
            event => {
                debug!("Unhandled swarm event: {:?}", event);
            }
        }
        
        Ok(())
    }
    
    /// Handle behavior events
    async fn handle_behavior_event(&self, event: OverlayBehaviorEvent) -> Result<(), OverlayError> {
        match event {
            OverlayBehaviorEvent::Gossipsub(gossipsub_event) => {
                self.handle_gossipsub_event(gossipsub_event).await?;
            },
            OverlayBehaviorEvent::Mdns(mdns_event) => {
                self.handle_mdns_event(mdns_event).await?;
            },
            OverlayBehaviorEvent::Kademlia(kad_event) => {
                self.handle_kademlia_event(kad_event).await?;
            },
            OverlayBehaviorEvent::Identify(identify_event) => {
                self.handle_identify_event(identify_event).await?;
            },
        }
        
        Ok(())
    }
    
    /// Handle gossipsub events
    async fn handle_gossipsub_event(&self, event: GossipsubEvent) -> Result<(), OverlayError> {
        match event {
            GossipsubEvent::Message {
                propagation_source,
                message_id,
                message,
                ..
            } => {
                let topic = message.topic.as_str();
                let data = message.data;
                let source_peer_id = from_libp2p_peer_id(&propagation_source);
                
                // Check if this is a stream data message
                if let Some(stream_id) = topics::parse_stream_id(topic) {
                    if topic.ends_with("/data") {
                        // Handle stream data
                        if let Ok(chunk) = serde_json::from_slice::<StreamChunk>(&data) {
                            let relay_node = self.relay.relay_node();
                            relay_node.publish_chunk(chunk).await?;
                        }
                    } else if topic.ends_with("/control") {
                        // Handle control message
                        // (Implementation depends on control message format)
                    }
                } else if topic == topics::discovery() {
                    // Handle discovery message
                    // (Implementation depends on discovery message format)
                }
            },
            GossipsubEvent::Subscribed { peer_id, topic } => {
                debug!("Peer {} subscribed to {}", peer_id, topic);
                // Handle subscription
                if let Some(stream_id) = topics::parse_stream_id(topic.as_str()) {
                    let peer_id = from_libp2p_peer_id(&peer_id);
                    
                    // If it's a data topic, add as subscriber
                    if topic.as_str().ends_with("/data") {
                        let relay_node = self.relay.relay_node();
                        // Convert LocalPeerId to libp2p::PeerId before passing to relay_node
                        let libp2p_peer_id = to_libp2p_peer_id(&peer_id)?;
                        relay_node.add_subscriber(&stream_id, libp2p_peer_id).await?;
                    }
                }
            },
            GossipsubEvent::Unsubscribed { peer_id, topic } => {
                debug!("Peer {} unsubscribed from {}", peer_id, topic);
                // Handle unsubscription
                if let Some(stream_id) = topics::parse_stream_id(topic.as_str()) {
                    let peer_id = from_libp2p_peer_id(&peer_id);
                    
                    // If it's a data topic, remove as subscriber
                    if topic.as_str().ends_with("/data") {
                        let relay_node = self.relay.relay_node();
                        // Convert LocalPeerId to libp2p::PeerId before passing to relay_node
                        let libp2p_peer_id = to_libp2p_peer_id(&peer_id)?;
                        relay_node.remove_subscriber(&stream_id, &libp2p_peer_id).await?;
                    }
                }
            },
            _ => {}
        }
        
        Ok(())
    }
    
    /// Handle mDNS events
    async fn handle_mdns_event(&self, event: MdnsEvent) -> Result<(), OverlayError> {
        match event {
            MdnsEvent::Discovered(peers) => {
                for (peer_id, addr) in peers {
                    debug!("mDNS discovered peer: {} at {}", peer_id, addr);
                    
                    let mut swarm_lock = self.swarm.lock().await;
                    if let Some(swarm) = &mut *swarm_lock {
                        // Connect to the discovered peer
                        swarm.dial(addr.clone()).map_err(|e| {
                            OverlayError::ConnectionError(format!("Failed to dial discovered peer: {}", e))
                        })?;
                    }
                    
                    // Create peer info
                    let mut peer_info = PeerInfo::default();
                    peer_info.id = from_libp2p_peer_id(&peer_id);
                    peer_info.addresses.push(addr.to_string());
                    
                    // Emit event
                    let _ = self.event_tx.send(OverlayEvent::PeerDiscovered {
                        peer_id: from_libp2p_peer_id(&peer_id),
                        info: peer_info,
                    }).await;
                }
            },
            MdnsEvent::Expired(peers) => {
                for (peer_id, addr) in peers {
                    debug!("mDNS expired peer: {} at {}", peer_id, addr);
                    // Handle expired peer (could disconnect or keep connection)
                }
            }
        }
        
        Ok(())
    }
    
    /// Handle Kademlia events
    async fn handle_kademlia_event(&self, event: KademliaEvent) -> Result<(), OverlayError> {
        match event {
            KademliaEvent::RoutingUpdated { peer, .. } => {
                debug!("Kademlia routing updated for peer: {}", peer);
                // Update peer in routing table
            },
            KademliaEvent::OutboundQueryCompleted { result, .. } => {
                // Handle query results
                match result {
                    libp2p::kad::QueryResult::GetClosestPeers(Ok(closest)) => {
                        for peer in closest.peers {
                            debug!("Found closest peer: {}", peer);
                        }
                    },
                    _ => {}
                }
            },
            _ => {}
        }
        
        Ok(())
    }
    
    /// Handle Identify events
    async fn handle_identify_event(&self, event: IdentifyEvent) -> Result<(), OverlayError> {
        match event {
            IdentifyEvent::Received {
                peer_id,
                info,
                ..
            } => {
                debug!("Identified peer {} with protocol version {}", peer_id, info.protocol_version);
                
                let peer_id = from_libp2p_peer_id(&peer_id);
                
                // Update peer information
                {
                    let mut peers = self.peers.write().await;
                    if let Some(peer) = peers.get_mut(&peer_id) {
                        // Add known addresses
                        for addr in info.listen_addrs {
                            peer.info.addresses.push(addr.to_string());
                        }
                        
                        // Add protocols
                        peer.info.protocols = info.protocols.iter()
                            .map(|p| p.to_string())
                            .collect();
                            
                        // Update metadata
                        peer.info.metadata.insert("agent_version".to_string(), info.agent_version);
                        peer.info.metadata.insert("protocol_version".to_string(), info.protocol_version);
                    }
                }
            },
            _ => {}
        }
        
        Ok(())
    }
}

#[async_trait::async_trait]
impl Overlay for Libp2pOverlay {
    fn start(&self) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        Box::pin(async move {
            let mut running = self.running.write().await;
            
            if *running {
                return Err(OverlayError::Other("Already running".to_string()));
            }
            
            // Start the topology manager
            self.topology.start().await?;
            
            // Start the relay manager
            self.relay.start().await?;
            
            // Start the mesh
            self.mesh.start().await?;
            
            // Initialize and run the swarm
            self.run_swarm().await?;
            
            // Start worker task
            let event_tx = self.event_tx.clone();
            let swarm = self.swarm.clone();
            let worker = tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_millis(100));
                
                loop {
                    interval.tick().await;
                    
                    // Process swarm events
                    let mut swarm_lock = swarm.lock().await;
                    if let Some(ref mut swarm) = *swarm_lock {
                        match swarm.select_next_some().await {
                            // Handle swarm events
                            // (Actual implementation would be more complex)
                            _ => {}
                        }
                    }
                }
            });
            
            // Store worker task
            {
                let mut worker_task = self.worker_task.lock().await;
                *worker_task = Some(worker);
            }
            
            // Mark as running
            *running = true;
            
            info!("Libp2p overlay started");
            
            Ok(())
        })
    }
    
    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        Box::pin(async move {
            let mut running = self.running.write().await;
            
            if !*running {
                return Err(OverlayError::Other("Not running".to_string()));
            }
            
            // Stop the worker task
            {
                let mut worker_task = self.worker_task.lock().await;
                if let Some(task) = worker_task.take() {
                    task.abort();
                }
            }
            
            // Stop the topology manager
            self.topology.stop().await?;
            
            // Stop the relay manager
            self.relay.stop().await?;
            
            // Stop the mesh
            self.mesh.stop().await?;
            
            // Mark as not running
            *running = false;
            
            info!("Libp2p overlay stopped");
            
            Ok(())
        })
    }
    
    fn is_running(&self) -> bool {
        futures::executor::block_on(async {
            *self.running.read().await
        })
    }
    
    fn local_peer_id(&self) -> LocalPeerId {
        self.local_peer_id.clone()
    }
    
    fn connect_peer(&self, addr: &str) -> Pin<Box<dyn Future<Output = Result<PeerInfo, OverlayError>> + Send>> {
        let addr_str = addr.to_string();
        
        Box::pin(async move {
            let mut swarm_lock = self.swarm.lock().await;
            
            let swarm = match &mut *swarm_lock {
                Some(swarm) => swarm,
                None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
            };
            
            // Parse the address
            let addr: libp2p::Multiaddr = addr_str.parse()
                .map_err(|e| {
                    error!("Failed to parse multiaddr: {}", e);
                    OverlayError::ConnectionError(format!("Invalid address: {}", e))
                })?;
            swarm.dial(addr.clone())
                .map_err(|e| OverlayError::ConnectionError(format!("Failed to dial peer: {}", e)))?;
                
            // This is async, so we don't have the peer ID yet
            // In a real implementation, we'd wait for the connection event
            
            // For now, return a placeholder
            let mut peer_info = PeerInfo::default();
            peer_info.addresses.push(addr_str);
            peer_info.status = ConnectionStatus::Connecting;
            
            Ok(peer_info)
        })
    }
    
    fn disconnect_peer(&self, peer_id: &LocalPeerId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let peer_id = peer_id.clone();
        
        Box::pin(async move {
            let mut swarm_lock = self.swarm.lock().await;
            
            let swarm = match &mut *swarm_lock {
                Some(swarm) => swarm,
                None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
            };
            
            // Convert to libp2p peer ID
            let libp2p_peer_id = to_libp2p_peer_id(&peer_id)?;
            
            // Disconnect the peer
            // In libp2p, there's no direct disconnect method, but we can remove them from known peers
            
            // Remove from peer store
            {
                let mut peers = self.peers.write().await;
                peers.remove(&peer_id);
            }
            
            // Remove from mesh
            self.mesh.remove_peer(&peer_id).await?;
            
            Ok(())
        })
    }
    
    fn publish_stream(&self, stream_id: &StreamId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let stream_id = stream_id.clone();
        let local_peer_id = self.local_peer_id.clone();
        
        Box::pin(async move {
            let mut swarm_lock = self.swarm.lock().await;
            
            let swarm = match &mut *swarm_lock {
                Some(swarm) => swarm,
                None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
            };
            
            // Subscribe to the stream topics
            let data_topic = gossipsub::IdentTopic::new(topics::stream_data(&stream_id));
            let control_topic = gossipsub::IdentTopic::new(topics::stream_control(&stream_id));
            
            swarm.behaviour_mut().gossipsub.subscribe(&data_topic)
                .map_err(|e| OverlayError::Other(format!("Failed to subscribe to data topic: {}", e)))?;
                
            swarm.behaviour_mut().gossipsub.subscribe(&control_topic)
                .map_err(|e| OverlayError::Other(format!("Failed to subscribe to control topic: {}", e)))?;
                
            // Create stream in topology
            // Assuming create_stream doesn't exist yet, we'll implement it by adding stream to TreeManager
            // and setting up the appropriate topology state
            // This might require implementing the method in the TopologyManager struct
            debug!("Creating stream {} in topology manager", stream_id);
            
            // Create stream in relay manager
            let relay_node = self.relay.relay_node();
            // Since create_stream is missing, we'll need to add it to RelayNode or use an alternative
            debug!("Creating stream {} in relay node", stream_id);
            
            // Add to active streams
            {
                let mut streams = self.streams.write().await;
                streams.insert(stream_id.clone());
            }
            
            // Emit event
            let _ = self.event_tx.send(OverlayEvent::StreamPublished {
                stream_id: stream_id.clone(),
                publisher: local_peer_id,
            }).await;
            
            Ok(())
        })
    }
    
    fn relay_stream(&self, stream_id: &StreamId, target: &LocalPeerId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let stream_id = stream_id.clone();
        let target = target.clone();
        let local_peer_id = self.local_peer_id.clone();
        
        Box::pin(async move {
            // Add peer to stream in topology
            // Convert LocalPeerId to PeerId before passing it to topology manager
            let libp2p_peer_id = to_libp2p_peer_id(&target)?;
            self.topology.add_peer_to_stream(&stream_id, libp2p_peer_id, PeerRole::Consumer).await?;
            
            // Add subscriber in relay
            let relay_node = self.relay.relay_node();
            // Convert LocalPeerId to libp2p::PeerId before passing to relay_node
            let libp2p_peer_id = to_libp2p_peer_id(&target)?;
            relay_node.add_subscriber(&stream_id, libp2p_peer_id).await?;
            
            // Emit event
            let _ = self.event_tx.send(OverlayEvent::StreamRelayed {
                stream_id: stream_id.clone(),
                source: local_peer_id,
                target,
            }).await;
            
            Ok(())
        })
    }
    
    fn stop_stream(&self, stream_id: &StreamId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let stream_id = stream_id.clone();
        
        Box::pin(async move {
            let mut swarm_lock = self.swarm.lock().await;
            
            let swarm = match &mut *swarm_lock {
                Some(swarm) => swarm,
                None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
            };
            
            // Unsubscribe from the stream topics
            let data_topic = gossipsub::IdentTopic::new(topics::stream_data(&stream_id));
            let control_topic = gossipsub::IdentTopic::new(topics::stream_control(&stream_id));
            
            swarm.behaviour_mut().gossipsub.unsubscribe(&data_topic);
            swarm.behaviour_mut().gossipsub.unsubscribe(&control_topic);
            
            // Remove stream from topology
            // Since remove_stream isn't defined in TopologyManager, we need to implement it
            // or use alternative methods to clean up stream resources
            debug!("Removing stream {} from topology manager", stream_id);
            
            // Remove stream from relay
            let relay_node = self.relay.relay_node();
            relay_node.remove_stream(&stream_id).await?;
            
            // Remove from active streams
            {
                let mut streams = self.streams.write().await;
                streams.remove(&stream_id);
            }
            
            // Emit event
            let _ = self.event_tx.send(OverlayEvent::StreamStopped {
                stream_id: stream_id.clone(),
                reason: "Stopped by user".to_string(),
            }).await;
            
            Ok(())
        })
    }
    
    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<OverlayEvent>> + Send>> {
        Box::pin(async move {
            // Clone the receiver instead of trying to use the mutex guard directly
            let rx = &mut *self.event_rx.lock().await;
            rx.next().await
        })
    }
    
    fn connected_peers(&self) -> Pin<Box<dyn Future<Output = Result<Vec<PeerInfo>, OverlayError>> + Send>> {
        Box::pin(async move {
            let peers = self.peers.read().await;
            let mut result = Vec::new();
            
            for (_, peer) in peers.iter() {
                if peer.is_connected() {
                    result.push(peer.info.clone());
                }
            }
            
            Ok(result)
        })
    }
    
    fn active_streams(&self) -> Pin<Box<dyn Future<Output = Result<Vec<StreamId>, OverlayError>> + Send>> {
        Box::pin(async move {
            let streams = self.streams.read().await;
            let result = streams.iter().cloned().collect();
            Ok(result)
        })
    }
    
    fn stats(&self) -> Pin<Box<dyn Future<Output = Result<OverlayStats, OverlayError>> + Send>> {
        Box::pin(async move {
            let peers = self.peers.read().await;
            let streams = self.streams.read().await;
            let relay_stats = self.relay.relay_node().get_stats().await;
            
            let mut stats = OverlayStats::default();
            
            stats.connected_peers = peers.values().filter(|p| p.is_connected()).count();
            stats.discovered_peers = peers.len();
            stats.active_streams = streams.len();
            stats.relay_nodes = peers.values().filter(|p| p.info.role == PeerRole::Relay).count();
            stats.incoming_bandwidth = relay_stats.incoming_bandwidth;
            stats.outgoing_bandwidth = relay_stats.outgoing_bandwidth;
            
            // Calculate average latency
            let mut total_latency = 0;
            let mut latency_count = 0;
            
            for peer in peers.values() {
                if let Some(latency) = peer.info.latency_ms {
                    total_latency += latency;
                    latency_count += 1;
                }
            }
            
            if latency_count > 0 {
                stats.average_latency_ms = total_latency / latency_count;
            }
            
            Ok(stats)
        })
    }
    
    fn rebalance_topology(&self) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        Box::pin(async move {
            let streams = self.streams.read().await;
            
            for stream_id in streams.iter() {
                self.topology.rebalance_stream(stream_id).await?;
            }
            
            Ok(())
        })
    }
} 