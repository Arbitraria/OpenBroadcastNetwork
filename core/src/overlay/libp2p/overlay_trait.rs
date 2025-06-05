//! Overlay trait implementation for the Libp2p overlay
//!
//! This module contains the implementation of the Overlay trait for Libp2pOverlay.

use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use anyhow::Result;
use futures::StreamExt;

// Local imports
use crate::overlay::interface::{Overlay, OverlayError, OverlayEvent, StreamId, OverlayStats};
use crate::overlay::relay::StreamChunk;
use crate::overlay::peer::{LocalPeerId, ConnectionStatus, PeerInfo, PeerRole};
use crate::overlay::libp2p::impl_core::Libp2pOverlay;
use crate::overlay::libp2p::topics;
use crate::overlay::libp2p::overlay_utils;
use crate::overlay::libp2p::types::to_libp2p_peer_id;

#[async_trait::async_trait]
impl Overlay for Libp2pOverlay {
    /// Check if the overlay is running
    fn is_running(&self) -> bool {
        // Use try_read to avoid blocking in an async runtime
        self.running.try_read().map_or(false, |r| *r)
    }
    
    /// Get the local peer ID
    fn local_peer_id(&self) -> LocalPeerId {
        self.local_peer_id.clone()
    }
    
    /// Connect to a peer
    async fn connect_peer(&self, addr: &str) -> Result<PeerInfo, OverlayError> {
        use crate::overlay::libp2p::overlay_utils::connect_peer_impl;
        connect_peer_impl(self, addr).await
    }
    
    /// Disconnect from a peer
    async fn disconnect_peer(&self, peer_id: &LocalPeerId) -> Result<(), OverlayError> {
        use crate::overlay::libp2p::overlay_utils::disconnect_peer_impl;
        disconnect_peer_impl(self, peer_id).await
    }
    
    /// Publish a stream
    async fn publish_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        debug!("Publishing stream {}", stream_id);
        
        // Add the stream to the relay manager's node
        let relay_node = self.relay.relay_node();
        relay_node.add_stream(stream_id.clone(), self.libp2p_peer_id.clone()).await?;
        
        // Add to local streams set
        let mut streams = self.streams.write().await;
        streams.insert(stream_id.clone());
        
        Ok(())
    }
    
    /// Stop a stream
    async fn stop_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        debug!("Stopping stream {}", stream_id);
        
        // Remove the stream from the relay manager's node
        let relay_node = self.relay.relay_node();
        relay_node.remove_stream(stream_id).await?;
        
        // Remove from local streams set
        let mut streams = self.streams.write().await;
        streams.remove(stream_id);
        
        Ok(())
    }
    
    /// Get list of connected peers with their info
    async fn connected_peers(&self) -> Result<Vec<PeerInfo>, OverlayError> {
        let peers = self.peers.read().await;
        
        // Filter for connected peers only and get their info
        let connected_peer_info: Vec<PeerInfo> = peers.iter()
            .filter(|(_, peer)| peer.info.status == ConnectionStatus::Connected)
            .map(|(_, peer)| peer.info.clone())
            .collect();
            
        Ok(connected_peer_info)
    }
    /// Start the overlay network
    async fn start(&self) -> Result<(), OverlayError> {
        info!("Starting libp2p overlay");
        
        // Initialize the overlay components
        self.init_topology().await?;
        self.init_relay().await?;
        self.init_mesh().await?;
        
        // Start the discovery manager
        let mut discovery = self.discovery.lock().await;
        discovery.start().await
            .map_err(|e| OverlayError::DiscoveryError(format!("Failed to start discovery: {}", e)))?;
        drop(discovery);
        
        // Start the swarm processing loop
        self.run_swarm().await?;
        
        // Mark the overlay as running
        *self.running.write().await = true;
        
        // The worker task will be managed separately
        // For now, we'll start the overlay without the background task
        info!("Libp2p overlay started successfully");
        Ok(())
    }
    
    /// Stop the overlay network
    async fn stop(&self) -> Result<(), OverlayError> {
        info!("Stopping libp2p overlay");
        
        // Mark the overlay as not running
        *self.running.write().await = false;
        
        // Abort the worker task
        if let Some(worker) = self.worker_task.lock().await.take() {
            worker.abort();
        }
        
        // Stop the discovery manager
        let mut discovery = self.discovery.lock().await;
        discovery.stop().await
            .map_err(|e| OverlayError::DiscoveryError(format!("Failed to stop discovery: {}", e)))?;
        drop(discovery);
        
        // Stop the managers
        self.stop_topology().await?;
        self.stop_relay().await?;
        self.stop_mesh().await?;
        
        info!("Libp2p overlay stopped");
        Ok(())
    }
    
    /// Relay a stream chunk to subscribers
    async fn relay_stream(&self, stream_id: &StreamId, target: &LocalPeerId) -> Result<(), OverlayError> {
        debug!("Relaying stream data for {} to {}", stream_id, target);
        
        // Convert local peer ID to libp2p peer ID for relay operations
        let target_libp2p = to_libp2p_peer_id(target)?;
        
        // For now, this is a stub implementation
        // Actual implementation should relay from stream_id to target
        
        Ok(())
    }

    async fn next_event(&self) -> Option<OverlayEvent> {
        // Get the event from the channel
        let mut rx = self.event_rx.lock().await;
        rx.next().await
    }

    async fn active_streams(&self) -> Result<Vec<StreamId>, OverlayError> {
        let streams = self.streams.read().await;
        let result = streams.iter().cloned().collect();
        Ok(result)
    }

    async fn stats(&self) -> Result<OverlayStats, OverlayError> {
        let peers = self.peers.read().await;
        let streams = self.streams.read().await;
        
        let mut stats = OverlayStats {
            connected_peers: peers.len(),
            discovered_peers: peers.len(),
            active_streams: streams.len(),
            relay_nodes: 0, // We'll need to count actual relay nodes
            incoming_bandwidth: 0,
            outgoing_bandwidth: 0,
            average_latency_ms: 0,
        };
        
        // Count relay nodes
        let relay_nodes = peers.iter()
            .filter(|(_, peer)| peer.info.role == PeerRole::Relay || peer.info.role == PeerRole::HybridRelay)
            .count();
        stats.relay_nodes = relay_nodes;
        
        // Calculate average latency if available
        let mut total_latency = 0;
        let mut latency_count = 0;
        
        for (_, peer) in peers.iter() {
            if let Some(latency) = peer.info.latency_ms {
                total_latency += latency;
                latency_count += 1;
            }
        }
        
        if latency_count > 0 {
            stats.average_latency_ms = total_latency / latency_count;
        }
        
        Ok(stats)
    }

    async fn rebalance_topology(&self) -> Result<(), OverlayError> {
        let streams = self.streams.read().await;
        
        for stream_id in streams.iter() {
            self.topology.rebalance_stream(stream_id).await?;
        }
        
        Ok(())
    }
}
