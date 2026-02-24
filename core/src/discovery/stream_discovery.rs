//! Stream discovery for finding available streams in the network.
//!
//! This module provides functionality for announcing and discovering live streams
//! in the decentralized P2P network. Publishers announce their streams, and viewers
//! can discover available streams to join.
//!
//! # Protocol
//!
//! Stream announcements are broadcast via GossipSub on the `obn/streams/announce` topic.
//! Peers cache announcements and can query their local cache to list available streams.

use crate::overlay::interface::{Overlay, OverlayError, StreamId as OverlayStreamId};
use crate::overlay::peer::LocalPeerId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Topic for stream announcements
pub const STREAM_ANNOUNCE_TOPIC: &str = "obn/streams/announce";

/// Default TTL for stream announcements (5 minutes)
pub const DEFAULT_ANNOUNCEMENT_TTL_SECS: u64 = 300;

/// A stream announcement broadcast to the network
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamAnnouncement {
    /// Unique stream identifier
    pub stream_id: OverlayStreamId,
    /// Human-readable stream title
    pub title: String,
    /// Publisher's peer ID
    pub publisher_id: Vec<u8>,
    /// Video codec (e.g., "h264", "vp9")
    pub video_codec: Option<String>,
    /// Audio codec (e.g., "aac", "opus")
    pub audio_codec: Option<String>,
    /// Video width in pixels
    pub video_width: Option<u32>,
    /// Video height in pixels
    pub video_height: Option<u32>,
    /// Unix timestamp when stream started
    pub started_at: u64,
    /// Unix timestamp when announcement was created
    pub announced_at: u64,
    /// Time-to-live in seconds
    pub ttl_secs: u64,
    /// Custom metadata
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl StreamAnnouncement {
    /// Create a new stream announcement
    pub fn new(stream_id: OverlayStreamId, title: String, publisher_id: LocalPeerId) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            stream_id,
            title,
            publisher_id: publisher_id.as_bytes().to_vec(),
            video_codec: None,
            audio_codec: None,
            video_width: None,
            video_height: None,
            started_at: now,
            announced_at: now,
            ttl_secs: DEFAULT_ANNOUNCEMENT_TTL_SECS,
            metadata: HashMap::new(),
        }
    }

    /// Create announcement with video info
    pub fn with_video(mut self, codec: impl Into<String>, width: u32, height: u32) -> Self {
        self.video_codec = Some(codec.into());
        self.video_width = Some(width);
        self.video_height = Some(height);
        self
    }

    /// Create announcement with audio info
    pub fn with_audio(mut self, codec: impl Into<String>) -> Self {
        self.audio_codec = Some(codec.into());
        self
    }

    /// Set custom metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Check if the announcement has expired
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now > self.announced_at + self.ttl_secs
    }

    /// Serialize to bytes for network transmission
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }

    /// Get a display-friendly summary
    pub fn summary(&self) -> String {
        let codec_info = match (&self.video_codec, &self.audio_codec) {
            (Some(v), Some(a)) => format!("{}/{}", v, a),
            (Some(v), None) => format!("{}/none", v),
            (None, Some(a)) => format!("none/{}", a),
            (None, None) => "unknown".to_string(),
        };

        let resolution = match (self.video_width, self.video_height) {
            (Some(w), Some(h)) => format!("{}x{}", w, h),
            _ => "unknown".to_string(),
        };

        format!("{} [{}] ({})", self.title, resolution, codec_info)
    }
}

/// Configuration for stream discovery
#[derive(Debug, Clone)]
pub struct StreamDiscoveryConfig {
    /// How often to re-announce active streams (seconds)
    pub announce_interval_secs: u64,
    /// Maximum number of streams to cache
    pub max_cached_streams: usize,
    /// How often to clean up expired announcements (seconds)
    pub cleanup_interval_secs: u64,
}

impl Default for StreamDiscoveryConfig {
    fn default() -> Self {
        Self {
            announce_interval_secs: 60, // Re-announce every minute
            max_cached_streams: 1000,
            cleanup_interval_secs: 30,
        }
    }
}

/// Stream discovery service
pub struct StreamDiscovery<O: Overlay + Send + Sync + 'static> {
    /// Reference to the overlay network
    overlay: Arc<O>,
    /// Cached stream announcements
    streams: Arc<RwLock<HashMap<OverlayStreamId, StreamAnnouncement>>>,
    /// Configuration
    config: StreamDiscoveryConfig,
    /// Is the service running
    is_running: Arc<RwLock<bool>>,
    /// Our own active streams (for re-announcement)
    own_streams: Arc<RwLock<Vec<StreamAnnouncement>>>,
}

impl<O: Overlay + Send + Sync + 'static> StreamDiscovery<O> {
    /// Create a new stream discovery service
    pub fn new(overlay: Arc<O>, config: StreamDiscoveryConfig) -> Self {
        Self {
            overlay,
            streams: Arc::new(RwLock::new(HashMap::new())),
            config,
            is_running: Arc::new(RwLock::new(false)),
            own_streams: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create with default configuration
    pub fn with_overlay(overlay: Arc<O>) -> Self {
        Self::new(overlay, StreamDiscoveryConfig::default())
    }

    /// Announce a stream to the network
    pub async fn announce_stream(
        &self,
        announcement: StreamAnnouncement,
    ) -> Result<(), OverlayError> {
        // Serialize the announcement
        let data = announcement
            .to_bytes()
            .map_err(|e| OverlayError::Other(format!("Failed to serialize announcement: {}", e)))?;

        // Create announce topic stream ID
        let announce_topic = OverlayStreamId::from_string(STREAM_ANNOUNCE_TOPIC);

        // Publish to the announce topic
        self.overlay.publish_stream(&announce_topic).await?;
        self.overlay
            .publish_stream_data(&announce_topic, data)
            .await?;

        // Add to our own streams for re-announcement
        {
            let mut own = self.own_streams.write().await;
            own.retain(|s| s.stream_id != announcement.stream_id);
            own.push(announcement.clone());
        }

        // Also cache locally
        {
            let mut streams = self.streams.write().await;
            streams.insert(announcement.stream_id.clone(), announcement.clone());
        }

        info!("Announced stream: {}", announcement.summary());
        Ok(())
    }

    /// Remove a stream announcement (stream ended)
    pub async fn remove_stream(&self, stream_id: &OverlayStreamId) -> Result<(), OverlayError> {
        // Remove from our own streams
        {
            let mut own = self.own_streams.write().await;
            own.retain(|s| &s.stream_id != stream_id);
        }

        // Remove from cache
        {
            let mut streams = self.streams.write().await;
            streams.remove(stream_id);
        }

        info!("Removed stream: {}", stream_id);
        Ok(())
    }

    /// Process a received announcement
    pub async fn process_announcement(
        &self,
        data: &[u8],
    ) -> Result<Option<StreamAnnouncement>, OverlayError> {
        let announcement = StreamAnnouncement::from_bytes(data).map_err(|e| {
            OverlayError::Other(format!("Failed to deserialize announcement: {}", e))
        })?;

        // Check if expired
        if announcement.is_expired() {
            debug!(
                "Ignoring expired announcement for: {}",
                announcement.stream_id
            );
            return Ok(None);
        }

        // Check cache limit
        let cache_full = {
            let streams = self.streams.read().await;
            streams.len() >= self.config.max_cached_streams
        };

        if cache_full {
            // Clean up expired entries first
            self.cleanup_expired().await;
        }

        // Cache the announcement
        {
            let mut streams = self.streams.write().await;
            if streams.len() < self.config.max_cached_streams {
                streams.insert(announcement.stream_id.clone(), announcement.clone());
                debug!("Cached stream announcement: {}", announcement.summary());
            } else {
                warn!("Stream cache full, ignoring announcement");
            }
        }

        Ok(Some(announcement))
    }

    /// Get all known streams
    pub async fn list_streams(&self) -> Vec<StreamAnnouncement> {
        let streams = self.streams.read().await;
        streams
            .values()
            .filter(|s| !s.is_expired())
            .cloned()
            .collect()
    }

    /// Get a specific stream by ID
    pub async fn get_stream(&self, stream_id: &OverlayStreamId) -> Option<StreamAnnouncement> {
        let streams = self.streams.read().await;
        streams.get(stream_id).filter(|s| !s.is_expired()).cloned()
    }

    /// Get streams by publisher
    pub async fn get_streams_by_publisher(&self, publisher_id: &[u8]) -> Vec<StreamAnnouncement> {
        let streams = self.streams.read().await;
        streams
            .values()
            .filter(|s| !s.is_expired() && s.publisher_id == publisher_id)
            .cloned()
            .collect()
    }

    /// Search streams by title (case-insensitive substring match)
    pub async fn search_streams(&self, query: &str) -> Vec<StreamAnnouncement> {
        let query_lower = query.to_lowercase();
        let streams = self.streams.read().await;
        streams
            .values()
            .filter(|s| !s.is_expired() && s.title.to_lowercase().contains(&query_lower))
            .cloned()
            .collect()
    }

    /// Clean up expired announcements
    pub async fn cleanup_expired(&self) {
        let mut streams = self.streams.write().await;
        let before = streams.len();
        streams.retain(|_, v| !v.is_expired());
        let after = streams.len();
        if before > after {
            debug!("Cleaned up {} expired stream announcements", before - after);
        }
    }

    /// Get stream count
    pub async fn stream_count(&self) -> usize {
        let streams = self.streams.read().await;
        streams.values().filter(|s| !s.is_expired()).count()
    }

    /// Start the discovery service (background tasks for cleanup and re-announcement)
    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let discovery = self;

        tokio::spawn(async move {
            {
                let mut running = discovery.is_running.write().await;
                *running = true;
            }

            let cleanup_interval = Duration::from_secs(discovery.config.cleanup_interval_secs);
            let announce_interval = Duration::from_secs(discovery.config.announce_interval_secs);

            let mut cleanup_timer = tokio::time::interval(cleanup_interval);
            let mut announce_timer = tokio::time::interval(announce_interval);

            loop {
                if !*discovery.is_running.read().await {
                    break;
                }

                tokio::select! {
                    _ = cleanup_timer.tick() => {
                        discovery.cleanup_expired().await;
                    }
                    _ = announce_timer.tick() => {
                        // Re-announce our streams
                        let own_streams = discovery.own_streams.read().await.clone();
                        for announcement in own_streams {
                            // Create updated announcement with new timestamp
                            let updated = StreamAnnouncement {
                                announced_at: SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                                ..announcement
                            };
                            if let Err(e) = discovery.announce_stream(updated).await {
                                warn!("Failed to re-announce stream: {}", e);
                            }
                        }
                    }
                }
            }

            info!("Stream discovery service stopped");
        })
    }

    /// Stop the discovery service
    pub async fn stop(&self) {
        let mut running = self.is_running.write().await;
        *running = false;
    }

    /// Check if running
    pub async fn is_running(&self) -> bool {
        *self.is_running.read().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_announcement_creation() {
        let stream_id = OverlayStreamId::from_string("test_stream");
        let publisher_id = LocalPeerId::new_random();

        let announcement =
            StreamAnnouncement::new(stream_id.clone(), "Test Stream".to_string(), publisher_id)
                .with_video("h264", 1920, 1080)
                .with_audio("aac")
                .with_metadata("category", "gaming");

        assert_eq!(announcement.stream_id, stream_id);
        assert_eq!(announcement.title, "Test Stream");
        assert_eq!(announcement.video_codec, Some("h264".to_string()));
        assert_eq!(announcement.audio_codec, Some("aac".to_string()));
        assert_eq!(announcement.video_width, Some(1920));
        assert_eq!(announcement.video_height, Some(1080));
        assert_eq!(
            announcement.metadata.get("category"),
            Some(&"gaming".to_string())
        );
    }

    #[test]
    fn test_stream_announcement_serialization() {
        let stream_id = OverlayStreamId::from_string("test_stream");
        let publisher_id = LocalPeerId::new_random();

        let announcement = StreamAnnouncement::new(stream_id, "Test".to_string(), publisher_id);

        let bytes = announcement.to_bytes().unwrap();
        let restored = StreamAnnouncement::from_bytes(&bytes).unwrap();

        assert_eq!(announcement.title, restored.title);
        assert_eq!(announcement.started_at, restored.started_at);
    }

    #[test]
    fn test_stream_announcement_expiry() {
        let stream_id = OverlayStreamId::from_string("test_stream");
        let publisher_id = LocalPeerId::new_random();

        let mut announcement = StreamAnnouncement::new(stream_id, "Test".to_string(), publisher_id);

        // Should not be expired initially
        assert!(!announcement.is_expired());

        // Set announced_at to past
        announcement.announced_at = 0;
        announcement.ttl_secs = 1;

        // Should be expired now
        assert!(announcement.is_expired());
    }

    #[test]
    fn test_stream_announcement_summary() {
        let stream_id = OverlayStreamId::from_string("test");
        let publisher_id = LocalPeerId::new_random();

        let announcement =
            StreamAnnouncement::new(stream_id, "My Stream".to_string(), publisher_id)
                .with_video("h264", 1280, 720)
                .with_audio("opus");

        let summary = announcement.summary();
        assert!(summary.contains("My Stream"));
        assert!(summary.contains("1280x720"));
        assert!(summary.contains("h264"));
        assert!(summary.contains("opus"));
    }
}
