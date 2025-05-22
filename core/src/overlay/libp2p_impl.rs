//! libp2p implementation of the overlay network
//!
//! This module implements the Overlay trait using libp2p.

use crate::overlay::interface::{Overlay, OverlayEvent, OverlayError, OverlayConfig, OverlayStats, StreamId};
use crate::overlay::peer::{Peer, PeerId, PeerInfo, PeerRole, ConnectionStatus};
use crate::overlay::topology::{TopologyManager, TopologyConfig};
use crate::overlay::relay::{RelayManager, RelayConfig, StreamChunk};
use crate::overlay::mesh::{MeshNetwork, MeshConfig};

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Mutex};
use tokio::time;
use futures::{StreamExt, FutureExt};
use libp2p::{
    core::upgrade,
    gossipsub::{self, Gossipsub, GossipsubEvent, MessageId, TopicHash},
    identify::{Identify, IdentifyConfig, IdentifyEvent},
    kad::{record::store::MemoryStore, Kademlia, KademliaConfig, KademliaEvent},
    mdns::{Mdns, MdnsConfig, MdnsEvent},
    noise,
    swarm::{
        NetworkBehaviour, SwarmBuilder, SwarmEvent, ConnectionHandlerUpgrErr, ListenError, 
        ConnectionId, DialError, SwarmParams, SwarmBuilder, Swarm, ConnectionHandler
    },
    tcp::{GenTcpConfig, TcpTransport},
    Multiaddr, PeerId as Libp2pPeerId, Transport, identity, yamux
};
use tracing::{debug, info, warn, error};
use std::pin::Pin;
use std::future::Future;

/// Convert our PeerId to libp2p's PeerId
fn to_libp2p_peer_id(peer_id: &PeerId) -> Result<Libp2pPeerId, OverlayError> {
    let bytes = peer_id.as_bytes();
    let libp2p_peer_id = Libp2pPeerId::from_bytes(bytes)
        .map_err(|e| OverlayError::Other(format!("Invalid peer ID: {}", e)))?;
    Ok(libp2p_peer_id)
}

/// Convert libp2p's PeerId to our PeerId
fn from_libp2p_peer_id(peer_id: &Libp2pPeerId) -> PeerId {
    PeerId::from_bytes(peer_id.to_bytes())
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

/// Combined network behavior for the overlay
#[derive(NetworkBehaviour)]
struct OverlayBehavior {
    /// Gossipsub for pub/sub communication
    gossipsub: Gossipsub,
    /// Kademlia for DHT and peer discovery
    kademlia: Kademlia<MemoryStore>,
    /// mDNS for local peer discovery
    mdns: Mdns,
    /// Identify protocol
    identify: Identify,
}

/// libp2p implementation of the overlay network
pub struct Libp2pOverlay {
    /// Local peer ID
    local_peer_id: PeerId,
    /// libp2p peer ID
    libp2p_peer_id: Libp2pPeerId,
    /// Configuration
    config: OverlayConfig,
    /// Swarm
    swarm: Mutex<Option<Swarm<OverlayBehavior>>>,
    /// Topology manager
    topology: Arc<TopologyManager>,
    /// Relay manager
    relay: Arc<RelayManager>,
    /// Mesh network
    mesh: Arc<MeshNetwork>,
    /// Known peers
    peers: RwLock<HashMap<PeerId, Peer>>,
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
        
        // Create channel for events
        let (event_tx, event_rx) = mpsc::channel(100);
        
        // Create topology manager
        let topology_config = TopologyConfig {
            max_fanout: 3,
            max_depth: 5,
            rebalance_interval: Duration::from_secs(60),
            region_aware: true,
            max_latency_ms: 300,
            ..Default::default()
        };
        
        let topology = Arc::new(TopologyManager::new(topology_config));
        
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
        
        let relay = Arc::new(RelayManager::new(
            config.local_peer_id.clone(),
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
            swarm: Mutex::new(None),
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
        
        // Create a transport
        let transport = libp2p::tcp::async_std::Transport::new(GenTcpConfig::default().nodelay(true))
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::NoiseAuthenticated::xx(&local_key).unwrap())
            .multiplex(yamux::YamuxConfig::default())
            .boxed();
            
        // Set up gossipsub
        let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(10))
            .validation_mode(gossipsub::ValidationMode::Strict)
            .build()
            .map_err(|e| OverlayError::Other(format!("Failed to build gossipsub config: {}", e)))?;
            
        let gossipsub = gossipsub::Gossipsub::new(
            gossipsub::MessageAuthenticity::Signed(local_key.clone()),
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
        
        // Create the behavior
        let behavior = OverlayBehavior {
            gossipsub,
            kademlia,
            mdns,
            identify,
        };
        
        // Build the swarm
        let swarm = SwarmBuilder::with_existing_identity(local_key)
            .with_tokio()
            .with_tcp(
                GenTcpConfig::default(),
                noise::NoiseAuthenticated::xx,
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
                
                if let Some(addr) = endpoint.get_remote_address() {
                    peer_info.addresses.push(addr.to_string());
                }
                
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
                        relay_node.subscribe_peer(stream_id, peer_id).await?;
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
                        relay_node.unsubscribe_peer(stream_id, peer_id).await?;
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
    
    fn local_peer_id(&self) -> PeerId {
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
            let addr = addr_str.parse()
                .map_err(|e| OverlayError::ConnectionError(format!("Invalid address: {}", e)))?;
                
            // Dial the peer
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
    
    fn disconnect_peer(&self, peer_id: &PeerId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
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
            self.topology.create_stream(stream_id.clone(), local_peer_id.clone()).await?;
            
            // Create stream in relay manager
            let relay_node = self.relay.relay_node();
            relay_node.create_stream(stream_id.clone(), local_peer_id.clone()).await?;
            
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
    
    fn relay_stream(&self, stream_id: &StreamId, target: &PeerId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let stream_id = stream_id.clone();
        let target = target.clone();
        let local_peer_id = self.local_peer_id.clone();
        
        Box::pin(async move {
            // Add peer to stream in topology
            self.topology.add_peer_to_stream(&stream_id, target.clone()).await?;
            
            // Add subscriber in relay
            let relay_node = self.relay.relay_node();
            relay_node.subscribe_peer(stream_id.clone(), target.clone()).await?;
            
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
            self.topology.remove_stream(&stream_id).await?;
            
            // Remove stream from relay
            let relay_node = self.relay.relay_node();
            relay_node.remove_stream(stream_id.clone()).await?;
            
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
            let mut rx = self.event_rx.lock().await;
            rx.recv().await
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