//! Hybrid tree-mesh overlay implementation
//!
//! This module combines the tree and mesh overlays to provide an efficient
//! and resilient distribution network.

use crate::overlay::interface::{Overlay, OverlayEvent, OverlayError, PeerRole, StreamId, StreamStats};
use crate::overlay::tree::{StreamTree, TreeStats};
use crate::overlay::mesh::{StreamMesh, MeshStats};
use crate::overlay::network::{Network, NetworkEvent, NetworkConfig};

use libp2p::{PeerId, gossipsub::TopicHash};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info, warn};

/// Configuration for the hybrid overlay
#[derive(Debug, Clone)]
pub struct HybridOverlayConfig {
    /// Network configuration
    pub network_config: NetworkConfig,
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
}

impl Default for HybridOverlayConfig {
    fn default() -> Self {
        Self {
            network_config: NetworkConfig::default(),
            rebalance_interval: 30,
            max_fan_out: 3,
            heartbeat_interval: 5,
            bandwidth: 1000000, // 1 Mbps
            default_role: PeerRole::Relay,
        }
    }
}

/// A hybrid overlay implementation that combines tree and mesh structures
pub struct HybridOverlay {
    /// Stream trees (primary distribution)
    trees: HashMap<StreamId, StreamTree>,
    /// Stream meshes (backup/resilience)
    meshes: HashMap<StreamId, StreamMesh>,
    /// Network layer
    network: Option<Network>,
    /// Configuration
    config: HybridOverlayConfig,
    /// Local peer ID
    local_peer_id: Option<PeerId>,
    /// Topic to StreamId mapping
    topics: HashMap<TopicHash, StreamId>,
    /// StreamId to Topic mapping
    stream_topics: HashMap<StreamId, TopicHash>,
    /// Event channel sender
    event_sender: mpsc::Sender<OverlayEvent>,
    /// Event channel receiver
    event_receiver: mpsc::Receiver<OverlayEvent>,
    /// Is the overlay running
    running: bool,
    /// Last rebalance time
    last_rebalance: Instant,
}

impl HybridOverlay {
    /// Create a new hybrid overlay
    pub async fn new(config: HybridOverlayConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let network = Network::new(config.network_config.clone()).await?;
        let local_peer_id = *network.local_peer_id();
        
        let (event_sender, event_receiver) = mpsc::channel(100);
        
        Ok(Self {
            trees: HashMap::new(),
            meshes: HashMap::new(),
            network: Some(network),
            config,
            local_peer_id: Some(local_peer_id),
            topics: HashMap::new(),
            stream_topics: HashMap::new(),
            event_sender,
            event_receiver,
            running: false,
            last_rebalance: Instant::now(),
        })
    }
    
    /// Get topic name for a stream
    fn stream_topic_name(&self, stream_id: &StreamId) -> String {
        format!("stream/{}", stream_id)
    }
    
    /// Get control topic name for a stream
    fn stream_control_topic_name(&self, stream_id: &StreamId) -> String {
        format!("stream/{}/control", stream_id)
    }
    
    /// Handle network events
    async fn handle_network_event(&mut self, event: NetworkEvent) {
        match event {
            NetworkEvent::PeerConnected(peer_id) => {
                debug!("Peer connected: {}", peer_id);
            },
            NetworkEvent::PeerDisconnected(peer_id) => {
                debug!("Peer disconnected: {}", peer_id);
                // Remove peer from all trees and meshes
                for (stream_id, tree) in &mut self.trees {
                    if tree.contains_peer(&peer_id) {
                        if tree.remove_peer(&peer_id) {
                            if let Err(e) = self.event_sender.send(OverlayEvent::PeerLeft(stream_id.clone(), peer_id.to_string())).await {
                                error!("Failed to send overlay event: {}", e);
                            }
                        }
                    }
                }
                
                for (stream_id, mesh) in &mut self.meshes {
                    if mesh.contains_peer(&peer_id) {
                        if mesh.remove_peer(&peer_id) {
                            if let Err(e) = self.event_sender.send(OverlayEvent::PeerLeft(stream_id.clone(), peer_id.to_string())).await {
                                error!("Failed to send overlay event: {}", e);
                            }
                        }
                    }
                }
            },
            NetworkEvent::MessageReceived(topic_hash, source_peer, _data) => {
                debug!("Message received from {} on topic {:?}", source_peer, topic_hash);
                
                // Check if this is a topic we're tracking
                if let Some(_stream_id) = self.topics.get(&topic_hash) {
                    // TODO: Process the message based on topic type (data or control)
                    // For now, just forward to any subscribers
                    
                    // ...
                }
            },
            NetworkEvent::Error(error) => {
                warn!("Network error: {}", error);
                if let Err(e) = self.event_sender.send(OverlayEvent::Error(OverlayError::ConnectionError(error))).await {
                    error!("Failed to send overlay event: {}", e);
                }
            },
        }
    }
    
    /// Rebalance all trees and meshes
    async fn rebalance_overlays(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_rebalance).as_secs() < self.config.rebalance_interval {
            return;
        }
        
        debug!("Rebalancing overlay networks");
        
        for (stream_id, tree) in &mut self.trees {
            if tree.rebalance() {
                if let Err(e) = self.event_sender.send(OverlayEvent::TreeReorganized(stream_id.clone())).await {
                    error!("Failed to send overlay event: {}", e);
                }
            }
        }
        
        for (_stream_id, mesh) in &mut self.meshes {
            mesh.rebalance();
        }
        
        self.last_rebalance = now;
    }
    
    /// Main event loop
    async fn run_event_loop(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut rebalance_interval = time::interval(Duration::from_secs(self.config.rebalance_interval));
        
        while self.running {
            tokio::select! {
                Some(network_event) = async { 
                    if let Some(network) = &mut self.network {
                        network.next_event().await
                    } else {
                        None
                    }
                } => {
                    self.handle_network_event(network_event).await;
                }
                _ = rebalance_interval.tick() => {
                    self.rebalance_overlays().await;
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
    fn start(&mut self) -> Result<(), OverlayError> {
        if self.running {
            return Ok(());
        }
        
        self.running = true;
        
        // In a real implementation, we would spawn the network and event loops here
        
        Ok(())
    }
    
    fn stop(&mut self) -> Result<(), OverlayError> {
        self.running = false;
        if let Some(network) = &mut self.network {
            network.stop();
        }
        Ok(())
    }
    
    fn create_stream(&mut self, stream_id: StreamId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let _peer_id = self.local_peer_id.expect("Local peer ID not set");
        let _topic_name = self.stream_topic_name(&stream_id);
        let _control_topic_name = self.stream_control_topic_name(&stream_id);
        let event_sender = self.event_sender.clone();
        
        Box::pin(async move {
            // TODO: Subscribe to topics and set up the stream
            
            // Notify of stream creation
            if let Err(e) = event_sender.send(OverlayEvent::StreamCreated(stream_id.clone())).await {
                error!("Failed to send overlay event: {}", e);
            }
            
            Ok(())
        })
    }
    
    fn join_stream(&mut self, stream_id: StreamId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let peer_id = self.local_peer_id.expect("Local peer ID not set");
        let role = self.config.default_role;
        let _bandwidth = self.config.bandwidth;
        let _topic_name = self.stream_topic_name(&stream_id);
        let _control_topic_name = self.stream_control_topic_name(&stream_id);
        let event_sender = self.event_sender.clone();
        
        Box::pin(async move {
            // TODO: Subscribe to topics and join the stream
            
            // Notify of joining
            if let Err(e) = event_sender.send(OverlayEvent::PeerJoined(
                stream_id.clone(), 
                peer_id.to_string(),
                role
            )).await {
                error!("Failed to send overlay event: {}", e);
            }
            
            Ok(())
        })
    }
    
    fn leave_stream(&mut self, stream_id: StreamId) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let peer_id = self.local_peer_id.expect("Local peer ID not set");
        let event_sender = self.event_sender.clone();
        
        Box::pin(async move {
            // TODO: Unsubscribe from topics and leave the stream
            
            // Notify of leaving
            if let Err(e) = event_sender.send(OverlayEvent::PeerLeft(
                stream_id.clone(), 
                peer_id.to_string()
            )).await {
                error!("Failed to send overlay event: {}", e);
            }
            
            Ok(())
        })
    }
    
    fn is_running(&self) -> bool {
        self.running
    }
    
    fn next_event(&mut self) -> Pin<Box<dyn Future<Output = Option<OverlayEvent>> + Send>> {
        let event_receiver = self.event_receiver.clone();
        Box::pin(async move {
            // We can't clone the receiver, so use a new one from the clone of the sender
            // This is a workaround for testing, in production this would be handled differently
            let (sender, mut rx) = mpsc::channel(100);
            let _ = sender;  // Unused but needed to create the channel
            
            // For testing, always return None
            // In real implementation, we would have a proper way to forward events
            rx.recv().await
        })
    }
    
    fn get_stream_stats(&self, stream_id: &StreamId) -> Result<StreamStats, OverlayError> {
        // Get stats from both tree and mesh
        let tree_stats = if let Some(tree) = self.trees.get(stream_id) {
            tree.get_stats()
        } else {
            return Err(OverlayError::StreamNotFound(stream_id.clone()));
        };
        
        let _mesh_stats = if let Some(mesh) = self.meshes.get(stream_id) {
            mesh.get_stats()
        } else {
            return Err(OverlayError::StreamNotFound(stream_id.clone()));
        };
        
        // Combine stats
        Ok(StreamStats {
            peer_count: tree_stats.peer_count,
            tree_depth: tree_stats.depth,
            avg_fanout: if tree_stats.relay_count > 0 {
                tree_stats.leaf_count as f64 / tree_stats.relay_count as f64
            } else {
                0.0
            },
            relay_count: tree_stats.relay_count,
            leaf_count: tree_stats.leaf_count,
        })
    }
    
    fn get_streams(&self) -> HashSet<StreamId> {
        self.trees.keys().cloned().collect()
    }
    
    fn publish(&mut self, stream_id: &StreamId, data: Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> {
        let topic_name = self.stream_topic_name(stream_id);
        
        // Rather than cloning the network, we create a copy of the needed data
        Box::pin(async {
            // In real implementation, we would publish to the topic
            // For now, just return success as a placeholder
            Ok(())
        })
    }
    
    fn subscribe(&mut self, _stream_id: &StreamId) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, OverlayError>> + Send>> {
        Box::pin(async {
            // This would be implemented to return the next chunk of stream data
            // For now, return an error as a placeholder
            Err(OverlayError::ProtocolError("Not implemented yet".to_string()))
        })
    }
} 