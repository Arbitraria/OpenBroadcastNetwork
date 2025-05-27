//! Hybrid tree-mesh overlay implementation
//!
//! This module combines the tree and mesh overlays to provide an efficient
//! and resilient distribution network.

use crate::overlay::interface::{Overlay, OverlayEvent, OverlayError, OverlayStats, StreamId};
use crate::overlay::peer::{Peer, LocalPeerId, PeerInfo, PeerRole};
use crate::overlay::tree::{StreamTree, TreeNode, TreeStats};
use crate::overlay::mesh::{StreamMesh, MeshStats};
use crate::overlay::topology::{TopologyManager, TopologyConfig, RelayScoreWeights};
use crate::overlay::network::{Network, NetworkEvent, NetworkConfig};

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use futures::future::BoxFuture;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{self, sleep};
use tracing::{debug, error, info, trace, warn};

/// Configuration for the hybrid overlay
#[derive(Debug, Clone)]
pub struct HybridOverlayConfig {
    /// Network configuration
    pub network_config: NetworkConfig,
    /// Topology configuration
    pub topology_config: TopologyConfig,
    /// How often to rebalance the tree (in seconds)
    pub rebalance_interval: u64,
    /// Maximum fan-out for tree nodes
    pub max_fan_out: usize,
    /// Heartbeat interval (in seconds)
    pub heartbeat_interval: u64,
    /// Estimated bandwidth of this node
    pub bandwidth: u64,
    /// Default role for this node
    pub default_role: PeerRole,
    /// Whether to enable geolocation-aware balancing
    pub geo_aware: bool,
    /// Channel buffer sizes
    pub channel_buffer_size: usize,
}

impl Default for HybridOverlayConfig {
    fn default() -> Self {
        Self {
            network_config: NetworkConfig::default(),
            topology_config: TopologyConfig::default(),
            rebalance_interval: 30,
            max_fan_out: 3,
            heartbeat_interval: 5,
            bandwidth: 1000000, // 1 Mbps
            default_role: PeerRole::Relay,
            geo_aware: true,
            channel_buffer_size: 100,
        }
    }
}

/// Stream metadata
#[derive(Debug, Clone)]
pub struct StreamMetadata {
    /// Unique stream ID
    pub stream_id: StreamId,
    /// Publisher peer ID
    pub publisher: LocalPeerId,
    /// Stream metadata (codec, bitrate, etc.)
    pub metadata: HashMap<String, String>,
    /// Timestamp when the stream was published
    pub timestamp: u64,
    /// List of relay peers
    pub relay_peers: Vec<LocalPeerId>,
    /// Stream quality metrics
    pub quality: StreamQuality,
    /// Whether the stream is active
    pub is_active: bool,
    /// Whether this node is subscribed
    pub is_subscribed: bool,
}

/// Stream quality metrics
#[derive(Debug, Clone, Default)]
pub struct StreamQuality {
    /// Bandwidth in bytes per second
    pub bandwidth_bps: u64,
    /// Latency in milliseconds
    pub latency_ms: u64,
    /// Packet loss percentage
    pub packet_loss: f32,
    /// Frame rate
    pub framerate: f32,
}

/// A hybrid overlay implementation that combines tree and mesh structures
pub struct HybridOverlay {
    /// Stream trees (primary distribution)
    trees: HashMap<StreamId, StreamTree>,
    /// Configuration
    config: HybridOverlayConfig,
    
    /// Local peer ID
    local_peer_id: Option<LocalPeerId>,
    
    /// Network layer
    network: Network,
    
    /// Active streams
    streams: Arc<RwLock<HashMap<StreamId, StreamMetadata>>>,
    
    /// Known peers
    peers: Arc<RwLock<HashMap<LocalPeerId, Peer>>>,
    
    /// Event sender
    event_tx: mpsc::Sender<OverlayEvent>,
    
    /// Event receiver
    event_rx: Option<mpsc::Receiver<OverlayEvent>>,
    
    /// Whether the overlay is running
    is_running: Arc<std::sync::atomic::AtomicBool>,
    
    /// Topology manager
    topology: TopologyManager,
    
    /// Tree overlays
    trees: Arc<RwLock<HashMap<StreamId, StreamTree>>>,
    
    /// Mesh overlays
    meshes: Arc<RwLock<HashMap<StreamId, StreamMesh>>>,
}

impl HybridOverlay {
    /// Create a new hybrid overlay
    pub async fn new(config: HybridOverlayConfig) -> Result<Self, OverlayError> {
        let network = Network::new(config.network_config.clone())
            .await
            .map_err(|e| OverlayError::ConnectionError(format!("Failed to create network: {}", e)))?;
            
        let local_peer_id = network.local_peer_id();
        
        let topology = Arc::new(TopologyManager::new(config.topology_config.clone()));
        
        let (event_sender, event_receiver) = mpsc::channel(config.channel_buffer_size);
        
        // Create local peer info
        let local_peer_info = PeerInfo {
            id: local_peer_id.clone(),
            addresses: network.local_addresses(),
            role: config.default_role,
            protocols: vec!["stream/1.0.0".to_string()],
            metadata: HashMap::new(),
            latency_ms: None,
            last_seen: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            region: None,
            bandwidth_capacity: Some(config.bandwidth),
        };
        
        Ok(Self {
            trees: HashMap::new(),
            meshes: HashMap::new(),
            streams: HashMap::new(),
            topology,
            network: Some(network),
            config,
            local_peer_id: Some(local_peer_id),
            local_peer_info: Some(local_peer_info),
            peers: Arc::new(RwLock::new(HashMap::new())),
            event_sender,
            event_receiver,
            running: false,
            last_rebalance: Instant::now(),
            task_handles: Vec::new(),
        })
    }
    
    /// Handle network events
    async fn handle_network_event(&mut self, event: NetworkEvent) {
        match event {
            NetworkEvent::PeerConnected(peer_id) => {
                debug!("Peer connected: {}", peer_id);
                
                // Update peer status
                if let Some(peer_info) = self.update_peer_connection_status(&peer_id, true).await {
                    // Notify about the connection
                    let _ = self.event_sender.send(OverlayEvent::PeerConnected { 
                        peer_id: peer_id.clone(), 
                        info: peer_info 
                    }).await;
                }
            },
            NetworkEvent::PeerDisconnected(peer_id) => {
                debug!("Peer disconnected: {}", peer_id);
                
                // Update peer status
                if let Some(peer_info) = self.update_peer_connection_status(&peer_id, false).await {
                    // Notify about the disconnection
                    let _ = self.event_sender.send(OverlayEvent::PeerDisconnected { 
                        peer_id: peer_id.clone(), 
                        reason: "Disconnected".to_string()
                    }).await;
                            }
                
                // Handle disconnection in trees and meshes
                self.handle_peer_disconnection(&peer_id).await;
            },
            NetworkEvent::MessageReceived(topic, source_peer, data) => {
                trace!("Message received from {} on topic {:?}", source_peer, topic);
                
                // Process the message based on stream and message type
                self.process_stream_message(topic, source_peer, data).await;
            },
            NetworkEvent::Error(error) => {
                warn!("Network error: {}", error);
                let _ = self.event_sender.send(OverlayEvent::Error(
                    OverlayError::ConnectionError(error)
                )).await;
            },
        }
    }
    
    /// Update a peer's connection status
    fn update_peer_connection_status(&self, peer_id: &LocalPeerId, connected: bool) -> Option<PeerInfo> {
        let mut peers = match self.peers.write() {
            Ok(peers) => peers,
            Err(_) => return None,
        };
        
        if let Some(peer) = peers.get_mut(peer_id) {
            peer.connected = connected;
            peer.last_seen = Some(Instant::now());
            
            // Create a copy of the peer info to return
            let peer_info = PeerInfo {
                peer_id: peer.peer_id.clone(),
                role: peer.role,
                connected: peer.connected,
                last_seen: peer.last_seen,
                metadata: peer.metadata.clone(),
            };
            
            Some(peer_info)
        } else {
            None
        }
        None
    }
    
    /// Handle peer disconnection in trees and meshes
    async fn handle_peer_disconnection(&mut self, peer_id: &LocalPeerId) {
        // Update peer status
        self.update_peer_connection_status(peer_id, false);
        
        // Handle disconnection in trees
        for tree in self.trees.values_mut() {
            if let Some(parent) = tree.get_parent(peer_id) {
                // If the disconnected peer was a parent, try to reconnect to the tree
                if let Err(e) = tree.reconnect(peer_id, &parent) {
                    warn!("Failed to reconnect to tree after peer disconnection: {}", e);
                }
            }
            
            // Remove the peer from the tree
            tree.remove_peer(peer_id);
        }
        
        // Handle disconnection in meshes
        for mesh in self.meshes.values_mut() {
            mesh.remove_peer(peer_id);
        }
        
        // Notify about the disconnection
        if let Err(e) = self.event_tx.try_send(OverlayEvent::PeerDisconnected(peer_id.clone())) {
            warn!("Failed to send peer disconnected event: {}", e);
        }
    }
    
    /// Process a stream message
    async fn process_stream_message(&mut self, topic: String, source_peer: PeerId, data: Vec<u8>) {
        // Parse the topic to extract stream ID and message type
        let parts: Vec<&str> = topic.split('/').collect();
        if parts.len() < 3 {
            warn!("Invalid topic format: {}", topic);
            return;
        }
        
        // Extract stream ID from topic
        let stream_id_str = parts[1];
        let stream_id = StreamId::from_string(stream_id_str);
        
        // Check if this is a stream we're tracking
        if !self.streams.contains_key(&stream_id) {
            debug!("Received message for unknown stream: {}", stream_id_str);
            return;
        }
        
        // Handle based on message type (data or control)
        let message_type = parts[2];
        match message_type {
            "data" => {
                // Handle stream data packet
                self.handle_stream_data(stream_id, source_peer, data).await;
            },
            "control" => {
                // Handle control message
                self.handle_control_message(stream_id, source_peer, data).await;
            },
            _ => {
                warn!("Unknown message type: {}", message_type);
            }
        }
    }
    
    /// Handle stream data packet
    async fn handle_stream_data(
        &mut self,
        stream_id: StreamId,
        source_peer: LocalPeerId,
        data: Vec<u8>,
    ) {
        // Forward the data to subscribers in the tree
        if let Some(tree) = self.trees.get_mut(&stream_id) {
            // Forward to children in the tree
            if let Some(children) = tree.get_children(&source_peer) {
                for child in children {
                    if let Err(e) = self.network.send_data(&child, &data).await {
                        warn!("Failed to forward data to child {}: {}", child, e);
                    }
                }
            }
            
            // Forward to peers in the mesh (if any)
            if let Some(mesh) = self.meshes.get_mut(&stream_id) {
                for peer in mesh.get_peers() {
                    if peer != &source_peer {
                        if let Err(e) = self.network.send_data(peer, &data).await {
                            warn!("Failed to forward data to mesh peer {}: {}", peer, e);
                        }
                    }
                }
            }
            
            // Notify about the received data
            if let Err(e) = self.event_tx.send(OverlayEvent::StreamData {
                stream_id: stream_id.clone(),
                source: source_peer,
                data: data.clone(),
            }).await {
                warn!("Failed to send stream data event: {}", e);
            }
        } else {
            debug!("Received data for unknown stream: {}", stream_id);
        }
    }
    
    /// Handle control message
    async fn handle_control_message(
        &mut self,
        stream_id: StreamId,
        source_peer: LocalPeerId,
        data: Vec<u8>,
    ) {
        // Parse the control message
        let control_msg: Result<ControlMessage, _> = serde_json::from_slice(&data);
        let control_msg = match control_msg {
            Ok(msg) => msg,
            Err(e) => {
                warn!("Failed to parse control message: {}", e);
                return;
            }
        };

        match control_msg {
            ControlMessage::JoinRequest => {
                debug!("Received join request from peer {} for stream {}", source_peer, stream_id);
                
                // Add the peer to the tree
                if let Some(tree) = self.trees.get_mut(&stream_id) {
                    if let Err(e) = tree.add_child(&source_peer, &self.local_peer_id.unwrap()) {
                        warn!("Failed to add peer {} to tree: {}", source_peer, e);
                    }
                }
                
                // Add the peer to the mesh (if any)
                if let Some(mesh) = self.meshes.get_mut(&stream_id) {
                    if let Err(e) = mesh.add_peer(source_peer.clone()) {
                        warn!("Failed to add peer {} to mesh: {}", source_peer, e);
                    }
                }
                
                // Notify about the new peer
                if let Err(e) = self.event_tx.send(OverlayEvent::PeerJoined {
                    stream_id: stream_id.clone(),
                    peer_id: source_peer,
                }).await {
                    warn!("Failed to send peer joined event: {}", e);
                }
            }
            ControlMessage::LeaveRequest => {
                debug!("Received leave request from peer {} for stream {}", source_peer, stream_id);
                
                // Remove the peer from the tree
                if let Some(tree) = self.trees.get_mut(&stream_id) {
                    tree.remove_peer(&source_peer);
                }
                
                // Remove the peer from the mesh (if any)
                if let Some(mesh) = self.meshes.get_mut(&stream_id) {
                    mesh.remove_peer(&source_peer);
                }
                
                // Notify about the peer leaving
                if let Err(e) = self.event_tx.send(OverlayEvent::PeerLeft {
                    stream_id: stream_id.clone(),
                    peer_id: source_peer,
                }).await {
                    warn!("Failed to send peer left event: {}", e);
                }
            }
        }
        // Parse control message
        // In a real implementation, we'd deserialize the message and handle various control commands
    /// Rebalance a tree
    async fn rebalance_tree(&mut self, stream_id: &StreamId) -> Result<bool, OverlayError> {
        // Get the tree for this stream
        let Some(tree) = self.trees.get_mut(stream_id) else {
            return Ok(false);
        };
        
        // Get all peers in the tree
        let peers = tree.get_all_peers();
        
        // If there are no peers, nothing to rebalance
        if peers.is_empty() {
            return Ok(false);
        }
        
        // Rebuild the tree with the same root but rebalanced structure
        let root = tree.get_root().ok_or_else(|| OverlayError::TreeError("Tree has no root".to_string()))?;
        let mut new_tree = StreamTree::new(stream_id.clone(), root.clone());
        
        // Add peers back to the tree in a balanced way
        for peer_id in peers {
            if peer_id != root {
                if let Err(e) = new_tree.add_peer(&peer_id) {
                    warn!("Failed to add peer {} to rebalanced tree: {}", peer_id, e);
                }
            }
        }
        
        // Replace the old tree with the new one
        self.trees.insert(stream_id.clone(), new_tree);
        
        Ok(true)
    }

    /// Rebalance all overlays
    async fn rebalance_overlays(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_rebalance).as_secs() < self.config.rebalance_interval {
            return;
        }
        
        debug!("Rebalancing overlay networks");
        
        // Rebalance all trees
        for stream_id in self.trees.keys().cloned().collect::<Vec<_>>() {
            if let Err(e) = self.rebalance_tree(&stream_id).await {
                warn!("Failed to rebalance tree for stream {:?}: {}", stream_id, e);
            }
        }
        
        // Rebalance all meshes
        for mesh in self.meshes.values_mut() {
            mesh.rebalance();
        }
        
        self.last_rebalance = now;
    }
    
    /// Main event loop
    async fn run_event_loop(self) -> Result<(), OverlayError> {
        let mut this = self;
        let mut rebalance_interval = time::interval(Duration::from_secs(this.config.rebalance_interval));
        let mut heartbeat_interval = time::interval(Duration::from_secs(this.config.heartbeat_interval));
        
        while this.running {
            tokio::select! {
                Some(network_event) = async { 
                    if let Some(network) = &mut this.network {
                        network.next_event().await
                    } else {
                        None
                    }
                } => {
                    this.handle_network_event(network_event).await;
                }
                
                _ = rebalance_interval.tick() => {
                    this.rebalance_overlays().await;
                }
                
                _ = heartbeat_interval.tick() => {
                    this.send_heartbeats().await;
                }
                
                Some(event) = this.event_receiver.recv() => {
                    // Handle internal events
                    match event {
                        OverlayEvent::PeerConnected { peer_id } => {
                            debug!("Peer connected: {}", peer_id);
                        }
                        OverlayEvent::PeerDisconnected { peer_id } => {
                            debug!("Peer disconnected: {}", peer_id);
                            this.handle_peer_disconnection(&peer_id).await;
                        }
                        _ => {}
                    }
                }
                
                else => {
                    break;
                }
            }
        }
        
        Ok(())
    }
}

impl Overlay for HybridOverlay {
    fn start(&self) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let this = self.clone();
        
        Box::pin(async move {
            if this.running {
            return Ok(());
        }
        
            let mut cloned = this.clone();
            cloned.running = true;
        
            // Start the network
            if let Some(network) = &mut cloned.network {
                network.start().await.map_err(|e| {
                    OverlayError::ConnectionError(format!("Failed to start network: {}", e))
                })?;
            }
            
            // Start the topology manager
            cloned.topology.start().await?;
            
            // Spawn the event loop task
            let event_loop_handle = tokio::spawn(async move {
                if let Err(e) = cloned.run_event_loop().await {
                    error!("Event loop error: {}", e);
                }
            });
            
            // Store the task handle
            // Note: In a real implementation, we would need to mutably borrow self
            // which would require redesigning this to use Arc<RwLock<Self>>
        
        Ok(())
        })
    }
    
    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        Box::pin(async {
            // In a real implementation, we would stop all tasks and clean up resources
            Ok(())
        })
    }
    
    fn is_running(&self) -> bool {
        self.running
    }
    
    fn local_peer_id(&self) -> LocalPeerId {
        self.local_peer_id.clone().unwrap_or_else(|| LocalPeerId::random())
    }
    
    fn connect_peer(&self, addr: &str) -> Pin<Box<dyn Future<Output = Result<PeerInfo, OverlayError>> + Send>> {
        let addr = addr.to_string();
        let network = self.network.clone();
        
        Box::pin(async move {
            if let Some(network) = network {
                let peer_info = network.connect(addr.as_str()).await
                    .map_err(|e| OverlayError::ConnectionError(format!("Connect failed: {}", e)))?;
                
                Ok(peer_info)
            } else {
                Err(OverlayError::ConnectionError("Network not initialized".to_string()))
            }
        })
    }
    
    fn disconnect_peer(&self, peer_id: &LocalPeerId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let peer_id = peer_id.clone();
        let network = self.network.clone();
        
        Box::pin(async move {
            if let Some(network) = network {
                network.disconnect_peer(&peer_id).await.map_err(|e| {
                    OverlayError::ConnectionError(format!("Failed to disconnect peer: {}", e))
                })
            } else {
                Err(OverlayError::NotStarted)
            }
        })
    }
    
    fn publish_stream(&self, stream_id: &StreamId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let stream_id = stream_id.clone();
        let this = self.clone();
        let local_peer_id = self.local_peer_id.clone().unwrap();
        
        Box::pin(async move {
            // Create tree and mesh for the stream
            let tree = StreamTree::new(stream_id.clone());
            tree.set_source(local_peer_id.clone());
            
            let mesh = StreamMesh::new(stream_id.clone(), local_peer_id.clone());
            
            // Create stream metadata
            let metadata = StreamMetadata {
                id: stream_id.clone(),
                created_at: Instant::now(),
                publisher: local_peer_id.clone(),
                subscribers: 0,
                is_publisher: true,
                is_subscribed: false,
                quality: StreamQuality::default(),
            };
            
            // Register with topology manager
            this.topology.create_stream(stream_id.clone(), local_peer_id.clone()).await?;
            
            // Update local state
            // Note: In a real implementation, we'd need to mutably borrow self
            // this.trees.insert(stream_id.clone(), tree);
            // this.meshes.insert(stream_id.clone(), mesh);
            // this.streams.insert(stream_id.clone(), metadata);
            
            // Create and subscribe to topics
            if let Some(network) = &this.network {
                let data_topic = format!("stream/{}/data", stream_id);
                let control_topic = format!("stream/{}/control", stream_id);
                
                network.subscribe(&data_topic).await
                    .map_err(|e| OverlayError::ProtocolError(format!("Subscribe failed: {}", e)))?;
                    
                network.subscribe(&control_topic).await
                    .map_err(|e| OverlayError::ProtocolError(format!("Subscribe failed: {}", e)))?;
            }
            
            // Announce stream creation
            let _ = this.event_sender.send(OverlayEvent::StreamPublished {
                stream_id: stream_id.clone(),
                publisher: local_peer_id.clone(),
            }).await;
            
            Ok(())
        })
    }
    
    fn relay_stream(&self, stream_id: &StreamId, target: &LocalPeerId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let stream_id = stream_id.clone();
        let target = target.clone();
        let this = self.clone();
        
        Box::pin(async move {
            // Check if we know about this stream
            if !this.streams.contains_key(&stream_id) {
                return Err(OverlayError::TopologyError(format!("Unknown stream: {:?}", stream_id)));
            }
            
            // Add target peer to the tree
            if let Some(_tree) = this.trees.get_mut(&stream_id) {
                // In a real implementation, we'd add the peer to the tree
                // tree.add_peer(target.clone(), PeerRole::Consumer, 0);
                
                // Add to topology manager
                this.topology.add_peer_to_stream(&stream_id, target.clone()).await?;
                
                // Start relaying data to this peer
                let local_peer_id = this.local_peer_id();
                
                // Notify about the new relay
                if let Some(sender) = &this.event_sender {
                    let _ = sender.send(OverlayEvent::StreamRelayed {
                        stream_id: stream_id.clone(),
                        source: local_peer_id,
                        target: target.clone(),
                    }).await;
                }
            
            Ok(())
            } else {
                Err(OverlayError::TopologyError(format!("No tree for stream: {:?}", stream_id)))
            }
        })
    }
    
    fn stop_stream(&self, stream_id: &StreamId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let stream_id = stream_id.clone();
        let this = self.clone();
        
        Box::pin(async move {
            // Remove stream from topology manager
            this.topology.remove_stream(&stream_id).await?;
            
            // Unsubscribe from topics
            if let Some(network) = &this.network {
                let data_topic = format!("stream/{}/data", stream_id);
                let control_topic = format!("stream/{}/control", stream_id);
                
                if let Err(e) = network.unsubscribe(&data_topic).await {
                    warn!("Failed to unsubscribe from data topic: {}", e);
                }
                
                if let Err(e) = network.unsubscribe(&control_topic).await {
                    warn!("Failed to unsubscribe from control topic: {}", e);
                }
            }
            
            // Clean up local state
            // Note: In a real implementation, we'd need to mutably borrow self
            // this.trees.remove(&stream_id);
            // this.meshes.remove(&stream_id);
            // this.streams.remove(&stream_id);
            
            // Announce stream stop
            let _ = this.event_sender.send(OverlayEvent::StreamStopped {
                stream_id: stream_id.clone(),
                reason: "Publisher stopped the stream".to_string(),
            }).await;
            
            Ok(())
        })
    }
    
    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<OverlayEvent>> + Send>> {
        let mut receiver = self.event_receiver.clone();
        
        Box::pin(async move {
            receiver.recv().await
        })
    }
    
    fn connected_peers(&self) -> Pin<Box<dyn Future<Output = Result<Vec<PeerInfo>, OverlayError>> + Send>>> {
        let this = self.clone();
        
        Box::pin(async move {
            let mut peers = Vec::new();
            
            // Get connected peers from the network
            if let Some(network) = &this.network {
                for peer_id in network.connected_peers().await? {
                    if let Some(info) = this.peer_store.get_peer(&peer_id) {
                        peers.push(info.clone());
                    }
                }
            }
            
            Ok(peers)
        })
    }
    
    fn active_streams(&self) -> Pin<Box<dyn Future<Output = Result<Vec<StreamId>, OverlayError>> + Send>>> {
        let streams = self.streams.keys().cloned().collect();
        
        Box::pin(async move {
            Ok(streams)
        })
    }
    
    fn stats(&self) -> Pin<Box<dyn Future<Output = Result<OverlayStats, OverlayError>> + Send>>> {
        let this = self.clone();
        
        Box::pin(async move {
            let mut stats = OverlayStats::default();
            
            // Get network stats if available
            if let Some(network) = &this.network {
                let network_stats = network.stats().await?;
                stats.connected_peers = network_stats.connected_peers_count;
                stats.total_bytes_sent = network_stats.total_bytes_sent;
                stats.total_bytes_received = network_stats.total_bytes_received;
            }
            
            // Add stream stats
            stats.active_streams = this.streams.len() as u32;
            
            // Add tree stats
            stats.tree_peers = this.trees.values()
                .map(|t| t.size() as u32)
                .sum();
                
            // Add mesh stats
            stats.mesh_peers = this.meshes.values()
                .map(|m| m.size() as u32)
                .sum();
            
            Ok(stats)
        })
    }
    
    fn rebalance_topology(&self) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>>> {
        let this = self.clone();
        
        Box::pin(async move {
            // Trigger rebalancing of all overlays
            this.rebalance_overlays().await;
            
            // In a real implementation, we'd also rebalance the topology manager
            this.topology.rebalance().await?;
            
            Ok(())
        })
    }
}

// We need to implement Clone for HybridOverlay to make the code compile
// This is a simplified implementation for demonstration purposes
impl Clone for HybridOverlay {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            local_peer_id: self.local_peer_id.clone(),
            network: self.network.clone(),
            peer_store: self.peer_store.clone(),
            streams: self.streams.clone(),
            trees: self.trees.clone(),
            meshes: self.meshes.clone(),
            topology: self.topology.clone(),
            event_sender: self.event_sender.clone(),
            event_receiver: self.event_receiver.clone(),
            running: self.running,
            last_rebalance: self.last_rebalance,
        }
    }
}