//! P2P Bridge for connecting overlay network to local subscribers.
//!
//! This module provides a bridge between the P2P overlay network and local
//! consumers (e.g., WebSocket clients). It receives `WireSegment` data from
//! the overlay network, deserializes it to `StreamSegment`, and forwards it
//! to local subscribers via a broadcast channel.
//!
//! # Architecture
//!
//! ```text
//! P2P Network (GossipSub)
//!         │
//!         ▼
//!    ┌─────────────┐
//!    │  P2PBridge  │  ◄── Receives WireSegment
//!    │             │      Converts to StreamSegment
//!    └─────────────┘      Forwards to channel
//!         │
//!         ▼
//!  broadcast::channel
//!         │
//!    ┌────┴────┐
//!    ▼         ▼
//! WebSocket  WebSocket
//! Client 1   Client 2
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};

use crate::media::segment::{StreamId, StreamSegment};
use crate::media::wire_format::WireSegment;
use crate::overlay::interface::{Overlay, OverlayError, OverlayEvent, StreamId as OverlayStreamId};

/// Configuration for the P2P bridge
#[derive(Debug, Clone)]
pub struct P2PBridgeConfig {
    /// Channel buffer size for forwarding segments
    pub channel_buffer_size: usize,
    /// Maximum number of streams to track
    pub max_streams: usize,
}

impl Default for P2PBridgeConfig {
    fn default() -> Self {
        Self {
            channel_buffer_size: 1000,
            max_streams: 100,
        }
    }
}

/// P2P Bridge that connects overlay network to local subscribers
pub struct P2PBridge<O: Overlay + Send + Sync + 'static> {
    /// Reference to the overlay network
    overlay: Arc<O>,
    /// Broadcast sender for forwarding segments to local consumers
    segment_sender: broadcast::Sender<StreamSegment>,
    /// Set of subscribed stream IDs
    subscribed_streams: Arc<RwLock<HashSet<StreamId>>>,
    /// Configuration
    config: P2PBridgeConfig,
    /// Flag to control the event loop
    is_running: Arc<RwLock<bool>>,
}

impl<O: Overlay + Send + Sync + 'static> P2PBridge<O> {
    /// Create a new P2P bridge
    pub fn new(overlay: Arc<O>, config: P2PBridgeConfig) -> Self {
        let (segment_sender, _) = broadcast::channel(config.channel_buffer_size);

        Self {
            overlay,
            segment_sender,
            subscribed_streams: Arc::new(RwLock::new(HashSet::new())),
            config,
            is_running: Arc::new(RwLock::new(false)),
        }
    }

    /// Create a new P2P bridge with default configuration
    pub fn with_overlay(overlay: Arc<O>) -> Self {
        Self::new(overlay, P2PBridgeConfig::default())
    }

    /// Subscribe to a stream and start receiving its segments
    pub async fn subscribe(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        // Check if already subscribed
        {
            let streams = self.subscribed_streams.read().await;
            if streams.contains(stream_id) {
                debug!("Already subscribed to stream: {}", stream_id);
                return Ok(());
            }
        }

        // Check max streams limit
        {
            let streams = self.subscribed_streams.read().await;
            if streams.len() >= self.config.max_streams {
                return Err(OverlayError::Other(format!(
                    "Maximum number of streams ({}) reached",
                    self.config.max_streams
                )));
            }
        }

        // Subscribe via overlay
        let overlay_stream_id = OverlayStreamId::from_bytes(stream_id.to_vec());
        self.overlay.subscribe_stream(&overlay_stream_id).await?;

        // Add to tracked streams
        {
            let mut streams = self.subscribed_streams.write().await;
            streams.insert(stream_id.clone());
        }

        info!("P2PBridge subscribed to stream: {}", stream_id);
        Ok(())
    }

    /// Unsubscribe from a stream
    pub async fn unsubscribe(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        // Remove from tracked streams
        {
            let mut streams = self.subscribed_streams.write().await;
            if !streams.remove(stream_id) {
                debug!("Not subscribed to stream: {}", stream_id);
                return Ok(());
            }
        }

        // Note: libp2p gossipsub doesn't have a direct unsubscribe-from-topic-by-id method
        // The stream will stop being tracked but the topic subscription remains
        // until the overlay is reconfigured

        info!("P2PBridge unsubscribed from stream: {}", stream_id);
        Ok(())
    }

    /// Get a receiver for segments
    ///
    /// Consumers can use this to receive segments from subscribed streams.
    pub fn subscribe_segments(&self) -> broadcast::Receiver<StreamSegment> {
        self.segment_sender.subscribe()
    }

    /// Get the number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.segment_sender.receiver_count()
    }

    /// Check if the bridge is running
    pub async fn is_running(&self) -> bool {
        *self.is_running.read().await
    }

    /// Start the bridge event loop
    ///
    /// This runs in the background, receiving overlay events and forwarding
    /// segments to local subscribers. Call `stop()` to terminate.
    pub async fn run(&self) -> Result<(), OverlayError> {
        // Mark as running
        {
            let mut running = self.is_running.write().await;
            if *running {
                return Err(OverlayError::AlreadyRunning);
            }
            *running = true;
        }

        info!("P2PBridge event loop started");

        // Event loop
        while *self.is_running.read().await {
            // Poll for overlay events
            if let Some(event) = self.overlay.next_event().await {
                if let Err(e) = self.handle_event(event).await {
                    warn!("Error handling overlay event: {}", e);
                }
            } else {
                // No event, yield briefly
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }

        info!("P2PBridge event loop stopped");
        Ok(())
    }

    /// Stop the bridge event loop
    pub async fn stop(&self) {
        let mut running = self.is_running.write().await;
        *running = false;
        info!("P2PBridge stopping");
    }

    /// Spawn the bridge event loop as a background task
    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<Result<(), OverlayError>> {
        tokio::spawn(async move { self.run().await })
    }

    /// Handle an overlay event
    async fn handle_event(&self, event: OverlayEvent) -> Result<(), OverlayError> {
        match event {
            OverlayEvent::StreamData {
                stream_id,
                source: _,
                data,
            } => {
                // Check if we're subscribed to this stream
                let unified_stream_id = StreamId::from_vec(stream_id.as_bytes().to_vec());
                {
                    let streams = self.subscribed_streams.read().await;
                    if !streams.contains(&unified_stream_id) {
                        // Not subscribed to this stream, ignore
                        return Ok(());
                    }
                }

                // Try to deserialize as WireSegment
                match WireSegment::from_bytes(&data) {
                    Ok(wire_segment) => {
                        // Convert to StreamSegment
                        let segment: StreamSegment = wire_segment.into();

                        debug!(
                            "P2PBridge received {} segment {} ({} bytes) for stream {}",
                            segment.media_type,
                            segment.sequence,
                            segment.data.len(),
                            unified_stream_id
                        );

                        // Forward to local subscribers
                        if self.segment_sender.receiver_count() > 0 {
                            if let Err(e) = self.segment_sender.send(segment) {
                                debug!("No active receivers for segment: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Failed to deserialize WireSegment: {}", e);
                    }
                }
            }
            OverlayEvent::StreamStopped { stream_id, reason } => {
                let unified_stream_id = StreamId::from_vec(stream_id.as_bytes().to_vec());
                info!("Stream {} stopped: {}", unified_stream_id, reason);
                // Auto-unsubscribe from stopped streams
                let _ = self.unsubscribe(&unified_stream_id).await;
            }
            _ => {
                // Ignore other events
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock overlay for testing
    struct MockOverlay {
        subscribed: Arc<RwLock<HashSet<String>>>,
    }

    impl MockOverlay {
        fn new() -> Self {
            Self {
                subscribed: Arc::new(RwLock::new(HashSet::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl Overlay for MockOverlay {
        async fn start(&self) -> Result<(), OverlayError> {
            Ok(())
        }

        async fn stop(&self) -> Result<(), OverlayError> {
            Ok(())
        }

        fn is_running(&self) -> bool {
            true
        }

        fn local_peer_id(&self) -> crate::overlay::peer::LocalPeerId {
            crate::overlay::peer::LocalPeerId::new_random()
        }

        async fn connect_peer(
            &self,
            addr: &str,
        ) -> Result<crate::overlay::peer::PeerInfo, OverlayError> {
            Ok(crate::overlay::peer::PeerInfo {
                id: crate::overlay::peer::LocalPeerId::new_random(),
                addresses: vec![addr.to_string()],
                role: crate::overlay::peer::PeerRole::Unknown,
                status: crate::overlay::peer::ConnectionStatus::Connected,
                ..Default::default()
            })
        }

        async fn disconnect_peer(
            &self,
            _peer_id: &crate::overlay::peer::LocalPeerId,
        ) -> Result<(), OverlayError> {
            Ok(())
        }

        async fn publish_stream(&self, _stream_id: &OverlayStreamId) -> Result<(), OverlayError> {
            Ok(())
        }

        async fn relay_stream(
            &self,
            _stream_id: &OverlayStreamId,
            _target: &crate::overlay::peer::LocalPeerId,
        ) -> Result<(), OverlayError> {
            Ok(())
        }

        async fn stop_stream(&self, _stream_id: &OverlayStreamId) -> Result<(), OverlayError> {
            Ok(())
        }

        async fn next_event(&self) -> Option<OverlayEvent> {
            None
        }

        async fn connected_peers(
            &self,
        ) -> Result<Vec<crate::overlay::peer::PeerInfo>, OverlayError> {
            Ok(vec![])
        }

        async fn active_streams(&self) -> Result<Vec<OverlayStreamId>, OverlayError> {
            Ok(vec![])
        }

        async fn stats(&self) -> Result<crate::overlay::interface::OverlayStats, OverlayError> {
            Ok(crate::overlay::interface::OverlayStats::default())
        }

        async fn rebalance_topology(&self) -> Result<(), OverlayError> {
            Ok(())
        }

        async fn subscribe_stream(&self, stream_id: &OverlayStreamId) -> Result<(), OverlayError> {
            let mut subscribed = self.subscribed.write().await;
            subscribed.insert(format!("{}", stream_id));
            Ok(())
        }

        async fn publish_stream_data(
            &self,
            _stream_id: &OverlayStreamId,
            _data: Vec<u8>,
        ) -> Result<(), OverlayError> {
            Ok(())
        }

        async fn publish_to_topic(&self, _topic: &str, _data: Vec<u8>) -> Result<(), OverlayError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_p2p_bridge_subscribe() {
        let overlay = Arc::new(MockOverlay::new());
        let bridge = P2PBridge::new(overlay.clone(), P2PBridgeConfig::default());

        let stream_id = StreamId::new("test_stream");
        bridge.subscribe(&stream_id).await.unwrap();

        let streams = bridge.subscribed_streams.read().await;
        assert!(streams.contains(&stream_id));
    }

    #[tokio::test]
    async fn test_p2p_bridge_unsubscribe() {
        let overlay = Arc::new(MockOverlay::new());
        let bridge = P2PBridge::new(overlay, P2PBridgeConfig::default());

        let stream_id = StreamId::new("test_stream");
        bridge.subscribe(&stream_id).await.unwrap();
        bridge.unsubscribe(&stream_id).await.unwrap();

        let streams = bridge.subscribed_streams.read().await;
        assert!(!streams.contains(&stream_id));
    }

    #[tokio::test]
    async fn test_p2p_bridge_segment_forwarding() {
        let overlay = Arc::new(MockOverlay::new());
        let bridge = P2PBridge::new(overlay, P2PBridgeConfig::default());

        // Get a receiver
        let mut receiver = bridge.subscribe_segments();

        // Create a test segment
        let segment = StreamSegment::video(
            StreamId::new("test"),
            0,
            0,
            33333,
            vec![1, 2, 3, 4, 5],
            true,
        );

        // Send directly via the sender (simulating what handle_event does)
        bridge.segment_sender.send(segment.clone()).unwrap();

        // Receive
        let received = receiver.recv().await.unwrap();
        assert_eq!(received.sequence, 0);
        assert!(received.is_keyframe);
    }

    #[tokio::test]
    async fn test_p2p_bridge_max_streams() {
        let overlay = Arc::new(MockOverlay::new());
        let config = P2PBridgeConfig {
            max_streams: 2,
            ..Default::default()
        };
        let bridge = P2PBridge::new(overlay, config);

        // Subscribe to max streams
        bridge.subscribe(&StreamId::new("stream1")).await.unwrap();
        bridge.subscribe(&StreamId::new("stream2")).await.unwrap();

        // Third should fail
        let result = bridge.subscribe(&StreamId::new("stream3")).await;
        assert!(result.is_err());
    }
}
