//! Stream publisher for P2P media distribution
//!
//! This module provides the `StreamPublisher` which reads media from a source
//! and publishes it to the P2P overlay network.
//!
//! Uses the unified `StreamSegment` type with efficient bincode wire format
//! via `WireSegment` for P2P transmission.

use crate::media::segment::{SegmentBuilder, StreamId, StreamSegment};
use crate::media::wire_format::ToWireFormat;
use crate::media::Mp4Parser;
use crate::media::StreamMetadata;
use crate::overlay::interface::{Overlay, OverlayError, StreamId as OverlayStreamId};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::Instant;
use tracing::{debug, error, info};

/// Errors that can occur during stream publishing
#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    /// Media parsing or encoding error
    #[error("Media error: {0}")]
    Media(String),
    /// Overlay network communication error
    #[error("Overlay error: {0}")]
    Overlay(#[from] OverlayError),
    /// File I/O error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization error for metadata
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Publishes media streams to the P2P overlay network
pub struct StreamPublisher<O: Overlay + Send + Sync> {
    overlay: Arc<O>,
    stream_id: StreamId,
    overlay_stream_id: OverlayStreamId,
    segment_builder: SegmentBuilder,
    stop_signal: broadcast::Sender<()>,
}

impl<O: Overlay + Send + Sync> StreamPublisher<O> {
    /// Create a new publisher that will stream to the given overlay network
    pub fn new(overlay: Arc<O>, _title: String) -> Self {
        let stream_id = StreamId::generate();
        let overlay_stream_id =
            OverlayStreamId::from_string(stream_id.as_str().unwrap_or_default());
        let segment_builder = SegmentBuilder::new(stream_id.clone());
        let (stop_signal, _) = broadcast::channel(1);

        Self {
            overlay,
            stream_id,
            overlay_stream_id,
            segment_builder,
            stop_signal,
        }
    }

    /// Get the stream identifier for this publisher
    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    /// Parse an MP4 file and publish its segments to the overlay in real time
    pub async fn publish_from_file(&mut self, path: &Path) -> Result<(), PublisherError> {
        info!("Starting stream {} from file: {:?}", self.stream_id, path);

        self.overlay.publish_stream(&self.overlay_stream_id).await?;
        self.overlay
            .subscribe_stream(&self.overlay_stream_id)
            .await?;

        let mut parser = Mp4Parser::new();

        let file_data = std::fs::read(path)?;
        parser
            .parse(&file_data)
            .map_err(|e| PublisherError::Media(format!("Failed to parse MP4: {:?}", e)))?;

        // Get track info from parsed MP4
        let tracks = parser.get_tracks();
        let video_info = tracks.iter().find(|t| t.media_type == "vide");
        let audio_info = tracks.iter().find(|t| t.media_type == "soun");

        let metadata = StreamMetadata {
            title: path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "Unknown".to_string()),
            video_width: video_info.map(|_| 1920).unwrap_or(0),
            video_height: video_info.map(|_| 1080).unwrap_or(0),
            video_fps: 30,
            audio_sample_rate: audio_info.map(|a| a.timescale).unwrap_or(48000),
            audio_channels: 2,
            video_codec: video_info
                .map(|v| v.codec.clone())
                .unwrap_or_else(|| "h264".to_string()),
            audio_codec: audio_info
                .map(|a| a.codec.clone())
                .unwrap_or_else(|| "aac".to_string()),
        };

        let metadata_segment = self
            .segment_builder
            .metadata(serde_json::to_vec(&metadata)?);
        self.publish_segment(&metadata_segment).await?;
        info!("Published metadata segment");

        // Generate all StreamSegments (init segment is first, media segments follow)
        let segments = parser
            .generate_stream_segments(self.stream_id.clone())
            .map_err(|e| PublisherError::Media(format!("Failed to generate segments: {:?}", e)))?;

        if segments.is_empty() {
            return Err(PublisherError::Media("No segments generated".to_string()));
        }

        // Publish initialization segment(s) first
        let init_count = segments.iter().filter(|s| s.is_init()).count();
        for segment in segments.iter().filter(|s| s.is_init()) {
            self.publish_segment(segment).await?;
            info!(
                "Published initialization segment ({} bytes)",
                segment.size()
            );
        }

        // Remaining segments are media segments
        let media_segments: Vec<_> = segments.iter().filter(|s| !s.is_init()).collect();
        info!(
            "Publishing {} media segments ({} init segments sent)",
            media_segments.len(),
            init_count
        );

        let start_time = Instant::now();
        let mut stop_rx = self.stop_signal.subscribe();

        for (i, segment) in media_segments.iter().enumerate() {
            tokio::select! {
                _ = stop_rx.recv() => {
                    info!("Stop signal received, ending stream");
                    break;
                }
                _ = async {
                    if let Err(e) = self.publish_segment(segment).await {
                        error!("Failed to publish segment {}: {}", i, e);
                    } else if i % 30 == 0 {
                        debug!(
                            "Published segment {}/{} ({} bytes)",
                            i + 1, media_segments.len(), segment.size()
                        );
                    }

                    // Real-time pacing based on timestamp
                    let target_time = Duration::from_micros(segment.pts_us);
                    let elapsed = start_time.elapsed();
                    if target_time > elapsed {
                        tokio::time::sleep(target_time - elapsed).await;
                    }
                } => {}
            }
        }

        info!("Stream {} finished", self.stream_id);
        self.overlay.stop_stream(&self.overlay_stream_id).await?;

        Ok(())
    }

    /// Publish a unified StreamSegment using the efficient bincode wire format
    pub async fn publish_segment(&self, segment: &StreamSegment) -> Result<(), PublisherError> {
        let wire_bytes = segment
            .to_wire_bytes()
            .map_err(|e| PublisherError::Media(format!("Failed to serialize segment: {:?}", e)))?;
        self.overlay
            .publish_stream_data(&self.overlay_stream_id, wire_bytes)
            .await?;
        Ok(())
    }

    /// Signal the publisher to stop streaming
    pub fn stop(&self) {
        let _ = self.stop_signal.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id_generation() {
        let id = StreamId::generate();
        assert!(id.as_str().unwrap_or("").starts_with("stream_"));
    }
}
