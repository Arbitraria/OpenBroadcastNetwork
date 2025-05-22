//! Hybrid tree-mesh overlay implementation
//!
//! This module combines the tree and mesh overlays to provide an efficient
//! and resilient distribution network.

use crate::overlay::interface::{Overlay, OverlayEvent, OverlayError, OverlayStats, StreamId};
use crate::overlay::peer::{Peer, PeerId, PeerInfo, PeerRole};
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
    /// The stream ID
    pub id: StreamId,
    /// When the stream was created
    pub created_at: Instant,
    /// The publisher's peer ID
    pub publisher: PeerId,
    /// Current active subscribers count
    pub subscribers: usize,
    /// Whether this node is the publisher
    pub is_publisher: bool,
    /// Whether this node is subscribed
    pub is_subscribed: bool,
    /// Stream quality metrics
    pub quality: StreamQuality,
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
    /// Stream meshes (backup/resilience)
    meshes: HashMap<StreamId, StreamMesh>,
    /// Stream metadata
    streams: HashMap<StreamId, StreamMetadata>,
    /// Topology manager for organizing peers
    topology: Arc<TopologyManager>,
    /// Network layer
    network: Option<Network>,
    /// Configuration
    config: HybridOverlayConfig,
    /// Local peer ID
    local_peer_id: Option<PeerId>,
    /// Local peer info
    local_peer_info: Option<PeerInfo>,
    /// Known peers
    peers: Arc<RwLock<HashMap<PeerId, Peer>>>,
    /// Event channel sender
    event_sender: mpsc::Sender<OverlayEvent>,
    /// Event channel receiver
    event_receiver: mpsc::Receiver<OverlayEvent>,
    /// Is the overlay running
    running: bool,
    /// Last rebalance time
    last_rebalance: Instant,
    /// Background task handles
    task_handles: Vec<tokio::task::JoinHandle<()>>,
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
    async fn update_peer_connection_status(&self, peer_id: &PeerId, connected: bool) -> Option<PeerInfo> {
        let mut peers = self.peers.write().await;
        
        if let Some(peer) = peers.get_mut(peer_id) {
            if connected {
                // Update connection information
                peer.info.last_seen = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                
                return Some(peer.info.clone());
            } else {
                // Mark as disconnected
                peer.set_disconnected();
                return Some(peer.info.clone());
            }
        }
        
        None
    }
    
    /// Handle peer disconnection in trees and meshes
    async fn handle_peer_disconnection(&mut self, peer_id: &PeerId) {
        // Remove peer from all trees
        for (stream_id, tree) in &mut self.trees {
            if tree.contains_peer(peer_id) {
                if tree.remove_peer(peer_id) {
                    debug!("Removed peer {} from tree for stream {}", peer_id, stream_id);
                    
                    // Rebalance the tree
                    let _ = self.rebalance_tree(stream_id).await;
                }
            }
        }
        
        // Remove peer from all meshes
        for (stream_id, mesh) in &mut self.meshes {
            if mesh.contains_peer(peer_id) {
                if mesh.remove_peer(peer_id) {
                    debug!("Removed peer {} from mesh for stream {}", peer_id, stream_id);
                }
            }
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
    async fn handle_stream_data(&mut self, stream_id: StreamId, source_peer: PeerId, data: Vec<u8>) {
        // If we're a relay node, forward the data to our children
        if let Some(tree) = self.trees.get(&stream_id) {
            if let Some(node) = tree.nodes.get(&self.local_peer_id.unwrap()) {
                for child_id in &node.children {
                    if let Some(network) = &self.network {
                        let topic = format!("stream/{}/data", stream_id);
                        if let Err(e) = network.publish(&topic, data.clone()).await {
                            warn!("Failed to forward stream data to {}: {}", child_id, e);
                        }
                    }
                }
            }
        }
        
        // Notify about the data reception
        let _ = self.event_sender.send(OverlayEvent::StreamRelayed {
            stream_id: stream_id,
            source: source_peer,
            target: self.local_peer_id.unwrap(),
        }).await;
    }
    
    /// Handle control message
    async fn handle_control_message(&mut self, stream_id: StreamId, source_peer: PeerId, data: Vec<u8>) {
        // Parse control message
        // In a real implementation, we'd deserialize the message and handle various control commands
        // For now, we'll just log it
        debug!("Received control message for stream {} from {}", stream_id, source_peer);
    }
    
    /// Rebalance a tree
    async fn rebalance_tree(&mut self, stream_id: &StreamId) -> Result<bool, OverlayError> {
        if let Some(tree) = self.trees.get_mut(stream_id) {
            // Perform tree rebalancing
            let changed = tree.rebalance();
            
            if changed {
                // Notify about tree reorganization
                let _ = self.event_sender.send(OverlayEvent::TopologyChanged {
                    peer_count: tree.nodes.len(),
                    relay_count: tree.nodes.values().filter(|n| !n.children.is_empty()).count(),
                }).await;
            }
            
            Ok(changed)
        } else {
            Err(OverlayError::TopologyError(format!("Stream not found: {:?}", stream_id)))
        }
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
                    // Send heartbeats to connected peers
                    this.send_heartbeats().await;
                }
                else => {
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    /// Send heartbeats to connected peers
    async fn send_heartbeats(&self) {
        // Implement heartbeat mechanism for connection maintenance
        if let Some(network) = &self.network {
            // For each stream
            for (stream_id, _) in &self.streams {
                // Send a heartbeat on the control channel
                let topic = format!("stream/{}/control", stream_id);
                let heartbeat = serde_json::json!({
                    "type": "heartbeat",
                    "peer_id": self.local_peer_id.as_ref().unwrap().to_string(),
                    "timestamp": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                }).to_string().into_bytes();
                
                if let Err(e) = network.publish(&topic, heartbeat).await {
                    warn!("Failed to send heartbeat for stream {}: {}", stream_id, e);
                }
            }
        }
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
    
    fn local_peer_id(&self) -> PeerId {
        self.local_peer_id.clone().unwrap()
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
    
    fn disconnect_peer(&self, peer_id: &PeerId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let peer_id = peer_id.clone();
        let network = self.network.clone();
        
        Box::pin(async move {
            if let Some(network) = network {
                network.disconnect(&peer_id).await
                    .map_err(|e| OverlayError::ConnectionError(format!("Disconnect failed: {}", e)))?;
                
        Ok(())
            } else {
                Err(OverlayError::ConnectionError("Network not initialized".to_string()))
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
    
    fn relay_stream(&self, stream_id: &StreamId, target: &PeerId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let stream_id = stream_id.clone();
        let target = target.clone();
        let this = self.clone();
        
        Box::pin(async move {
            // Check if we know about this stream
            if !this.streams.contains_key(&stream_id) {
                return Err(OverlayError::TopologyError(format!("Unknown stream: {:?}", stream_id)));
            }
            
            // Add target peer to the tree
            if let Some(tree) = this.trees.get_mut(&stream_id) {
                // In a real implementation, we'd add the peer to the tree
                // tree.add_peer(target.clone(), PeerRole::Consumer, 0);
                
                // Add to topology manager
                this.topology.add_peer_to_stream(&stream_id, target.clone()).await?;
                
                // Start relaying data to this peer
                let local_peer_id = this.local_peer_id.clone().unwrap();
                let _ = this.event_sender.send(OverlayEvent::StreamRelayed {
                    stream_id: stream_id.clone(),
                    source: local_peer_id,
                    target: target.clone(),
                }).await;
            
            Ok(())
            } else {
                Err(OverlayError::TopologyError(format!("Stream tree not found: {:?}", stream_id)))
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
    
    fn connected_peers(&self) -> Pin<Box<dyn Future<Output = Result<Vec<PeerInfo>, OverlayError>> + Send>> {
        let peers = self.peers.clone();
        
        Box::pin(async move {
            let peers_lock = peers.read().await;
            let connected: Vec<PeerInfo> = peers_lock.values()
                .filter(|p| p.is_connected())
                .map(|p| p.info.clone())
                .collect();
                
            Ok(connected)
        })
    }
    
    fn active_streams(&self) -> Pin<Box<dyn Future<Output = Result<Vec<StreamId>, OverlayError>> + Send>> {
        let streams = self.streams.keys().cloned().collect();
        
        Box::pin(async move {
            Ok(streams)
        })
    }
    
    fn stats(&self) -> Pin<Box<dyn Future<Output = Result<OverlayStats, OverlayError>> + Send>> {
        Box::pin(async {
            // In a real implementation, we'd gather actual metrics
            let stats = OverlayStats {
                connected_peers: self.peers.read().await.values()
                    .filter(|p| p.is_connected())
                    .count(),
                discovered_peers: self.peers.read().await.len(),
                active_streams: self.streams.len(),
                relay_nodes: self.peers.read().await.values()
                    .filter(|p| matches!(p.info.role, PeerRole::Relay))
                    .count(),
                incoming_bandwidth: 0, // Would track actual bandwidth
                outgoing_bandwidth: 0,
                average_latency_ms: 0, // Would calculate from actual measurements
            };
            
            Ok(stats)
        })
    }
    
    fn rebalance_topology(&self) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let this = self.clone();
        
        Box::pin(async move {
            for stream_id in this.streams.keys() {
                this.topology.rebalance_stream(stream_id).await?;
            }
            
            Ok(())
        })
    }
}

// We need to implement Clone for HybridOverlay to make the code compile
// This is a simplified implementation for demonstration purposes
impl Clone for HybridOverlay {
    fn clone(&self) -> Self {
        Self {
            trees: self.trees.clone(),
            meshes: self.meshes.clone(),
            streams: self.streams.clone(),
            topology: self.topology.clone(),
            network: self.network.clone(),
            config: self.config.clone(),
            local_peer_id: self.local_peer_id.clone(),
            local_peer_info: self.local_peer_info.clone(),
            peers: self.peers.clone(),
            event_sender: self.event_sender.clone(),
            event_receiver: self.event_receiver.clone(),
            running: self.running,
            last_rebalance: self.last_rebalance,
            task_handles: Vec::new(), // Note: We don't clone task handles
        }
    }
} 