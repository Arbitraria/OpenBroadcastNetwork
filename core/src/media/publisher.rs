//! Stream publisher for P2P media distribution
//!
//! This module provides the `StreamPublisher` which reads media from a source
//! and publishes it to the P2P overlay network.
//!
//! The publisher supports two serialization formats:
//! - Legacy JSON format via `MediaChunk` (for backward compatibility)
//! - New bincode format via `WireSegment` (more efficient, preferred)

use crate::media::segment::{
    MediaType, SegmentBuilder, StreamId as UnifiedStreamId, StreamSegment,
};
use crate::media::wire_format::{ToWireFormat, WireSegment};
use crate::media::{ChunkBuilder, MediaChunk, Mp4Parser, StreamId, StreamMetadata};
use crate::overlay::interface::{Overlay, OverlayError, StreamId as OverlayStreamId};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::Instant;
use tracing::{debug, error, info};

#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    #[error("Media error: {0}")]
    Media(String),
    #[error("Overlay error: {0}")]
    Overlay(#[from] OverlayError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub struct StreamPublisher<O: Overlay + Send + Sync> {
    overlay: Arc<O>,
    stream_id: StreamId,
    overlay_stream_id: OverlayStreamId,
    chunk_builder: ChunkBuilder,
    stop_signal: broadcast::Sender<()>,
}

impl<O: Overlay + Send + Sync> StreamPublisher<O> {
    pub fn new(overlay: Arc<O>, _title: String) -> Self {
        let stream_id = StreamId::generate();
        // Use from_string directly for cleaner conversion
        let overlay_stream_id = OverlayStreamId::from_string(stream_id.as_str());
        let chunk_builder = ChunkBuilder::new(stream_id.clone());
        let (stop_signal, _) = broadcast::channel(1);

        Self {
            overlay,
            stream_id,
            overlay_stream_id,
            chunk_builder,
            stop_signal,
        }
    }

    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

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

        let metadata_chunk = self.chunk_builder.metadata_chunk(metadata)?;
        self.publish_chunk(&metadata_chunk).await?;
        info!("Published metadata chunk");

        // Generate all MSE segments (init segment is first, media segments follow)
        let segments = parser
            .generate_mse_segments()
            .map_err(|e| PublisherError::Media(format!("Failed to generate segments: {:?}", e)))?;

        if segments.is_empty() {
            return Err(PublisherError::Media("No segments generated".to_string()));
        }

        // First segment is the initialization segment
        let init_segment = &segments[0];
        let init_chunk = MediaChunk::new_video(
            self.stream_id.clone(),
            0,
            init_segment.data.clone(),
            0,
            true,
        );
        self.publish_chunk(&init_chunk).await?;
        info!(
            "Published initialization segment ({} bytes)",
            init_chunk.size()
        );

        // Remaining segments are media segments
        let media_segments = &segments[1..];
        info!("Publishing {} media segments", media_segments.len());

        let start_time = Instant::now();
        let mut stop_rx = self.stop_signal.subscribe();

        for (i, segment) in media_segments.iter().enumerate() {
            tokio::select! {
                _ = stop_rx.recv() => {
                    info!("Stop signal received, ending stream");
                    break;
                }
                _ = async {
                    let timestamp_ms = segment.timestamp.unwrap_or(0);
                    let duration_ms = segment.duration.unwrap_or(1000);

                    let chunk = self.chunk_builder.next_video_chunk(
                        segment.data.clone(),
                        duration_ms,
                        segment.is_keyframe,
                    );

                    if let Err(e) = self.publish_chunk(&chunk).await {
                        error!("Failed to publish chunk {}: {}", i, e);
                    } else if i % 30 == 0 {
                        debug!("Published segment {}/{} ({} bytes)", i + 1, media_segments.len(), chunk.size());
                    }

                    // Real-time pacing based on timestamp
                    let target_time = Duration::from_millis(timestamp_ms);
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

    async fn publish_chunk(&self, chunk: &MediaChunk) -> Result<(), PublisherError> {
        let data = chunk.to_bytes()?;
        self.overlay
            .publish_stream_data(&self.overlay_stream_id, data)
            .await?;
        Ok(())
    }

    /// Publish a unified StreamSegment using the efficient bincode wire format
    ///
    /// This is the preferred method for new code. It uses the `WireSegment`
    /// format which is 30-40% smaller than JSON.
    pub async fn publish_segment(&self, segment: &StreamSegment) -> Result<(), PublisherError> {
        let wire_bytes = segment
            .to_wire_bytes()
            .map_err(|e| PublisherError::Media(format!("Failed to serialize segment: {:?}", e)))?;
        self.overlay
            .publish_stream_data(&self.overlay_stream_id, wire_bytes)
            .await?;
        Ok(())
    }

    /// Get a unified stream ID for use with the new segment types
    pub fn unified_stream_id(&self) -> UnifiedStreamId {
        UnifiedStreamId::new(self.stream_id.as_str())
    }

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
        assert!(id.as_str().starts_with("stream_"));
    }
}
