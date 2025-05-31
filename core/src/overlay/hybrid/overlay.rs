//! Hybrid overlay implementation
//!
//! This module implements the hybrid tree-mesh overlay.

use crate::overlay::interface::{Overlay, OverlayEvent, OverlayError, OverlayStats, StreamId};
use crate::overlay::peer::{Peer, LocalPeerId, PeerInfo, PeerRole};
use crate::overlay::tree::{StreamTree, TreeNode, TreeStats};
use crate::overlay::mesh::{StreamMesh, MeshStats};
use crate::overlay::topology::{TopologyManager, TopologyConfig, RelayScoreWeights};
use crate::overlay::network::{Network, NetworkEvent, NetworkConfig};
use crate::overlay::hybrid::config::HybridOverlayConfig;
use crate::overlay::hybrid::types::{StreamMetadata, StreamQuality};

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use futures::future::BoxFuture;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{self, sleep};
use tracing::{debug, error, info, trace, warn};

/// A hybrid overlay implementation that combines tree and mesh structures
pub struct HybridOverlay {
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
    pub fn new(config: HybridOverlayConfig) -> Result<Self, OverlayError> {
        // Create event channels
        let (event_tx, event_rx) = mpsc::channel(config.channel_buffer_size);
        
        // Create topology manager
        let topology = TopologyManager::new(config.topology_config.clone());
        
        // Create network layer
        let network = Network::new(config.network_config.clone())?;
        
        Ok(Self {
            config,
            local_peer_id: None,
            network,
            streams: Arc::new(RwLock::new(HashMap::new())),
            peers: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            event_rx: Some(event_rx),
            is_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            topology,
            trees: Arc::new(RwLock::new(HashMap::new())),
            meshes: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Handle network events
    pub fn handle_network_event(&mut self, event: NetworkEvent) {
        match event {
            NetworkEvent::PeerConnected { peer_id } => {
                // Update peer connection status
                if let Some(info) = self.update_peer_connection_status(&peer_id, true) {
                    debug!("Peer connected: {:?} with role {:?}", peer_id, info.role);
                    
                    // In a real implementation, we'd send a welcome message and exchange stream info
                }
            }
            NetworkEvent::PeerDisconnected { peer_id } => {
                // Update peer connection status
                if let Some(info) = self.update_peer_connection_status(&peer_id, false) {
                    debug!("Peer disconnected: {:?} with role {:?}", peer_id, info.role);
                    
                    // Handle the disconnection in trees and meshes
                    self.handle_peer_disconnection(&peer_id);
                }
            }
            NetworkEvent::MessageReceived { source, topic, data } => {
                // Process the message
                self.process_stream_message(topic, source, data);
            }
            NetworkEvent::Error { error } => {
                error!("Network error: {:?}", error);
            }
            // Other event types
            _ => {}
        }
    }

    /// Update a peer's connection status
    pub fn update_peer_connection_status(&self, peer_id: &LocalPeerId, connected: bool) -> Option<PeerInfo> {
        // Update the peer's connection status
        let mut peers = self.peers.blocking_write();
        
        if let Some(peer) = peers.get_mut(peer_id) {
            // Update connection status
            if connected {
                peer.info.connection_status = crate::overlay::peer::ConnectionStatus::Connected;
            } else {
                peer.info.connection_status = crate::overlay::peer::ConnectionStatus::Disconnected;
            }
            
            return Some(peer.info.clone());
        }
        
        // Peer not found
        None
    }

    /// Handle peer disconnection in trees and meshes
    pub fn handle_peer_disconnection(&mut self, peer_id: &LocalPeerId) {
        // Get all active streams
        let streams = self.streams.blocking_read().keys().cloned().collect::<Vec<_>>();
        
        for stream_id in streams {
            // Update tree
            if let Some(tree) = self.trees.blocking_write().get_mut(&stream_id) {
                // Remove the peer from the tree
                if let Err(e) = tree.remove_peer(peer_id) {
                    warn!("Failed to remove peer from tree: {:?}", e);
                }
            }
            
            // Update mesh
            if let Some(mesh) = self.meshes.blocking_write().get_mut(&stream_id) {
                // Remove the peer from the mesh
                if let Err(e) = mesh.remove_peer(peer_id) {
                    warn!("Failed to remove peer from mesh: {:?}", e);
                }
            }
        }
    }

    /// Process a stream message
    pub fn process_stream_message(&mut self, topic: String, source_peer: LocalPeerId, data: Vec<u8>) {
        // Parse the topic to get the stream ID and message type
        let parts: Vec<&str> = topic.split('/').collect();
        
        if parts.len() >= 3 && parts[0] == "stream" {
            // Try to parse the stream ID
            if let Ok(bytes) = hex::decode(parts[1]) {
                let stream_id = StreamId::from_bytes(bytes);
                
                // Check the message type
                match parts[2] {
                    "data" => {
                        // Handle data packet
                        self.handle_stream_data(stream_id, source_peer, data);
                    }
                    "control" => {
                        // Handle control message
                        self.handle_control_message(stream_id, source_peer, data);
                    }
                    _ => {
                        warn!("Unknown message type: {}", parts[2]);
                    }
                }
            }
        }
    }

    /// Handle stream data packet
    pub fn handle_stream_data(
        &mut self,
        stream_id: StreamId,
        source_peer: LocalPeerId,
        data: Vec<u8>,
    ) {
        // Check if we're subscribed to this stream
        let streams = self.streams.blocking_read();
        if let Some(stream) = streams.get(&stream_id) {
            if !stream.is_subscribed {
                // We're not subscribed, ignore the data
                return;
            }
            
            // In a real implementation, we'd process the data (e.g., decode it, play it, etc.)
            debug!("Received {} bytes of data for stream {:?} from {:?}", data.len(), stream_id, source_peer);
            
            // Forward the data to our tree children
            if let Some(tree) = self.trees.blocking_read().get(&stream_id) {
                for child in tree.get_children() {
                    // In a real implementation, we'd send the data to the child
                    debug!("Forwarding data to child: {:?}", child);
                }
            }
            
            // Forward the data to our mesh peers
            if let Some(mesh) = self.meshes.blocking_read().get(&stream_id) {
                for peer in mesh.get_peers() {
                    if peer != source_peer {
                        // In a real implementation, we'd send the data to the peer
                        debug!("Forwarding data to mesh peer: {:?}", peer);
                    }
                }
            }
        }
    }

    /// Handle control message
    pub fn handle_control_message(
        &mut self,
        stream_id: StreamId,
        source_peer: LocalPeerId,
        data: Vec<u8>,
    ) {
        // Parse control message
        // In a real implementation, we'd deserialize the message and handle various control commands
        let control_msg = String::from_utf8_lossy(&data);
        debug!("Received control message for stream {:?} from {:?}: {}", stream_id, source_peer, control_msg);
        
        // Example control message handling
        if control_msg.contains("rebalance") {
            debug!("Rebalancing tree for stream {:?}", stream_id);
            if let Err(e) = self.rebalance_tree(&stream_id) {
                error!("Failed to rebalance tree: {:?}", e);
            }
        }
    }

    /// Rebalance a tree
    pub fn rebalance_tree(&mut self, stream_id: &StreamId) -> Result<bool, OverlayError> {
        let mut trees = self.trees.blocking_write();
        
        if let Some(tree) = trees.get_mut(stream_id) {
            // Get all peers for this stream
            let peers = self.peers.blocking_read();
            let mut available_peers = Vec::new();
            
            for (peer_id, peer) in peers.iter() {
                if peer.is_connected() {
                    available_peers.push(peer_id.clone());
                }
            }
            
            // Rebalance the tree
            tree.rebalance(&available_peers, self.config.max_fan_out)?;
            
            // In a real implementation, we'd notify peers about the new tree structure
            
            Ok(true)
        } else {
            // Stream not found
            Err(OverlayError::StreamNotFound)
        }
    }

    /// Rebalance all overlays
    pub fn rebalance_overlays(&mut self) {
        // Get all active streams
        let streams = self.streams.blocking_read().keys().cloned().collect::<Vec<_>>();
        
        for stream_id in streams {
            // Rebalance tree
            if let Err(e) = self.rebalance_tree(&stream_id) {
                warn!("Failed to rebalance tree for stream {:?}: {:?}", stream_id, e);
            }
            
            // In a real implementation, we'd also rebalance meshes
        }
    }

    /// Main event loop
    pub fn run_event_loop(self) -> Result<(), OverlayError> {
        // Create a new runtime for the event loop
        let runtime = tokio::runtime::Runtime::new().map_err(|e| OverlayError::Other(format!("Failed to create runtime: {}", e)))?;
        
        // Spawn the event loop task
        runtime.block_on(async {
            // Set running flag
            self.is_running.store(true, std::sync::atomic::Ordering::SeqCst);
            
            // Initialize timers
            let rebalance_interval = Duration::from_secs(self.config.rebalance_interval);
            let heartbeat_interval = Duration::from_secs(self.config.heartbeat_interval);
            
            let mut last_rebalance = Instant::now();
            let mut last_heartbeat = Instant::now();
            
            // Run until stopped
            while self.is_running.load(std::sync::atomic::Ordering::SeqCst) {
                // Check if it's time to rebalance
                if last_rebalance.elapsed() >= rebalance_interval {
                    debug!("Rebalancing overlays");
                    self.rebalance_overlays();
                    last_rebalance = Instant::now();
                }
                
                // Check if it's time to send heartbeats
                if last_heartbeat.elapsed() >= heartbeat_interval {
                    debug!("Sending heartbeats");
                    // In a real implementation, we'd send heartbeats to all peers
                    last_heartbeat = Instant::now();
                }
                
                // Sleep for a short time to avoid busy-waiting
                time::sleep(Duration::from_millis(100)).await;
            }
            
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl Overlay for HybridOverlay {
    async fn start(&self) -> Result<(), OverlayError> {
        // Check if already running
        if self.is_running.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(OverlayError::AlreadyRunning);
        }
        
        // Set local peer ID
        // In a real implementation, we'd get this from the network layer
        // self.local_peer_id = Some(self.network.local_peer_id());
        
        // Start the network layer
        self.network.start().await?;
        
        // Start the event loop
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = this.run_event_loop() {
                error!("Event loop error: {:?}", e);
            }
        });
        
        // Set running flag
        self.is_running.store(true, std::sync::atomic::Ordering::SeqCst);
        
        Ok(())
    }

    async fn stop(&self) -> Result<(), OverlayError> {
        // Set running flag to false
        self.is_running.store(false, std::sync::atomic::Ordering::SeqCst);
        
        // Stop the network layer
        self.network.stop().await?;
        
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.is_running.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn local_peer_id(&self) -> LocalPeerId {
        self.local_peer_id.clone().unwrap_or_else(LocalPeerId::random)
    }

    async fn connect_peer(&self, addr: &str) -> Result<PeerInfo, OverlayError> {
        // Connect to the peer using the network layer
        let peer_id = self.network.connect(addr).await?;
        
        // Get peer info
        let peers = self.peers.read().await;
        if let Some(peer) = peers.get(&peer_id) {
            Ok(peer.info.clone())
        } else {
            // Create a new peer info
            let mut info = PeerInfo::new(peer_id);
            info.connection_status = crate::overlay::peer::ConnectionStatus::Connected;
            Ok(info)
        }
    }

    async fn disconnect_peer(&self, peer_id: &LocalPeerId) -> Result<(), OverlayError> {
        // Disconnect from the peer using the network layer
        self.network.disconnect(peer_id).await?;
        
        // Update peer connection status
        let mut peers = self.peers.write().await;
        if let Some(peer) = peers.get_mut(peer_id) {
            peer.info.connection_status = crate::overlay::peer::ConnectionStatus::Disconnected;
        }
        
        Ok(())
    }

    async fn start_stream(&self, stream_id: StreamId) -> Result<(), OverlayError> {
        // Check if the stream already exists
        let mut streams = self.streams.write().await;
        if streams.contains_key(&stream_id) {
            return Err(OverlayError::StreamAlreadyExists);
        }
        
        // Create stream metadata
        let metadata = StreamMetadata {
            stream_id: stream_id.clone(),
            publisher: self.local_peer_id(),
            metadata: HashMap::new(),
            timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
            relay_peers: Vec::new(),
            quality: StreamQuality::default(),
            is_active: true,
            is_subscribed: true,
        };
        
        // Add the stream
        streams.insert(stream_id.clone(), metadata);
        
        // Create tree and mesh for the stream
        let mut trees = self.trees.write().await;
        let mut meshes = self.meshes.write().await;
        
        trees.insert(stream_id.clone(), StreamTree::new(stream_id.clone(), self.local_peer_id()));
        meshes.insert(stream_id.clone(), StreamMesh::new(stream_id.clone(), self.local_peer_id()));
        
        // Emit event
        let _ = self.event_tx.send(OverlayEvent::StreamStarted {
            stream_id: stream_id.clone(),
        }).await;
        
        Ok(())
    }

    async fn stop_stream(&self, stream_id: StreamId) -> Result<(), OverlayError> {
        // Check if the stream exists
        let mut streams = self.streams.write().await;
        if !streams.contains_key(&stream_id) {
            return Err(OverlayError::StreamNotFound);
        }
        
        // Remove the stream
        streams.remove(&stream_id);
        
        // Remove tree and mesh for the stream
        let mut trees = self.trees.write().await;
        let mut meshes = self.meshes.write().await;
        
        trees.remove(&stream_id);
        meshes.remove(&stream_id);
        
        // Emit event
        let _ = self.event_tx.send(OverlayEvent::StreamStopped {
            stream_id: stream_id.clone(),
            reason: "Stopped by user".to_string(),
        }).await;
        
        Ok(())
    }

    async fn publish_stream_data(&self, stream_id: StreamId, data: Vec<u8>) -> Result<(), OverlayError> {
        // Check if the stream exists
        let streams = self.streams.read().await;
        if !streams.contains_key(&stream_id) {
            return Err(OverlayError::StreamNotFound);
        }
        
        // In a real implementation, we'd send the data to all subscribers
        // For now, we just log it
        debug!("Publishing {} bytes of data for stream {:?}", data.len(), stream_id);
        
        // Forward the data to our tree children
        if let Some(tree) = self.trees.read().await.get(&stream_id) {
            for child in tree.get_children() {
                // In a real implementation, we'd send the data to the child
                debug!("Forwarding data to child: {:?}", child);
            }
        }
        
        // Forward the data to our mesh peers
        if let Some(mesh) = self.meshes.read().await.get(&stream_id) {
            for peer in mesh.get_peers() {
                // In a real implementation, we'd send the data to the peer
                debug!("Forwarding data to mesh peer: {:?}", peer);
            }
        }
        
        Ok(())
    }

    async fn publish_stream_control(&self, stream_id: StreamId, command: String) -> Result<(), OverlayError> {
        // Check if the stream exists
        let streams = self.streams.read().await;
        if !streams.contains_key(&stream_id) {
            return Err(OverlayError::StreamNotFound);
        }
        
        // In a real implementation, we'd send the control message to all subscribers
        // For now, we just log it
        debug!("Publishing control message for stream {:?}: {}", stream_id, command);
        
        Ok(())
    }

    async fn next_event(&self) -> Option<OverlayEvent> {
        let mut rx = self.event_rx.as_ref()?.lock().await;
        rx.recv().await
    }

    async fn connected_peers(&self) -> Result<Vec<PeerInfo>, OverlayError> {
        let peers = self.peers.read().await;
        let mut result = Vec::new();
        
        for (_, peer) in peers.iter() {
            if peer.is_connected() {
                result.push(peer.info.clone());
            }
        }
        
        Ok(result)
    }

    async fn active_streams(&self) -> Result<Vec<StreamId>, OverlayError> {
        let streams = self.streams.read().await;
        Ok(streams.keys().cloned().collect())
    }

    async fn stats(&self) -> Result<OverlayStats, OverlayError> {
        let mut stats = OverlayStats::default();
        
        // Get network stats
        let network_stats = self.network.stats().await?;
        
        // Fill in the stats
        stats.connected_peers = network_stats.connected_peers_count;
        stats.active_streams = self.streams.read().await.len();
        
        // In a real implementation, we'd add more stats
        
        Ok(stats)
    }

    async fn rebalance_topology(&self) -> Result<(), OverlayError> {
        // Rebalance all overlays
        let streams = self.streams.read().await.keys().cloned().collect::<Vec<_>>();
        
        for stream_id in streams {
            // In a real implementation, we'd rebalance the tree and mesh for this stream
        }
        
        Ok(())
    }
}

// We need to implement Clone for HybridOverlay to make the code compile
// This is a simplified implementation for demonstration purposes
impl Clone for HybridOverlay {
    fn clone(&self) -> Self {
        // Create new channels
        let (event_tx, event_rx) = mpsc::channel(self.config.channel_buffer_size);
        
        Self {
            config: self.config.clone(),
            local_peer_id: self.local_peer_id.clone(),
            network: self.network.clone(),
            streams: self.streams.clone(),
            peers: self.peers.clone(),
            event_tx,
            event_rx: Some(event_rx),
            is_running: self.is_running.clone(),
            topology: self.topology.clone(),
            trees: self.trees.clone(),
            meshes: self.meshes.clone(),
        }
    }
}
