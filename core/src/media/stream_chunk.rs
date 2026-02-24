//! Streaming chunk format for decentralized media distribution
//!
//! **DEPRECATED**: This module contains legacy types for backward compatibility.
//! New code should use the unified types from [`crate::media::segment`]:
//! - [`crate::media::segment::StreamSegment`] instead of [`MediaChunk`]
//! - [`crate::media::segment::StreamId`] instead of [`StreamId`]
//! - [`crate::media::segment::MediaType`] instead of [`ChunkType`]
//!
//! For P2P transmission, use [`crate::media::wire_format::WireSegment`] which
//! provides more efficient bincode serialization (30-40% smaller than JSON).
//!
//! This module will be removed in a future version.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique identifier for a stream
///
/// **DEPRECATED**: Use [`crate::media::segment::StreamId`] instead, which provides
/// zero-copy cloning via `bytes::Bytes` and better performance.
#[deprecated(
    since = "0.2.0",
    note = "Use media::segment::StreamId instead for better performance"
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamId(pub String);

impl StreamId {
    /// Create a new stream ID
    pub fn new(id: String) -> Self {
        Self(id)
    }

    /// Generate a random stream ID
    pub fn generate() -> Self {
        use rand::Rng;
        let id: u64 = rand::thread_rng().gen();
        Self(format!("stream_{}", id))
    }

    /// Get the inner string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Type conversions between media::StreamId and overlay::StreamId
impl From<&StreamId> for crate::overlay::interface::StreamId {
    fn from(media_id: &StreamId) -> Self {
        crate::overlay::interface::StreamId::from_bytes(media_id.as_str().as_bytes().to_vec())
    }
}

impl From<StreamId> for crate::overlay::interface::StreamId {
    fn from(media_id: StreamId) -> Self {
        crate::overlay::interface::StreamId::from_bytes(media_id.as_str().as_bytes().to_vec())
    }
}

impl From<&crate::overlay::interface::StreamId> for StreamId {
    fn from(overlay_id: &crate::overlay::interface::StreamId) -> Self {
        StreamId::new(String::from_utf8_lossy(overlay_id.as_bytes()).to_string())
    }
}

impl From<crate::overlay::interface::StreamId> for StreamId {
    fn from(overlay_id: crate::overlay::interface::StreamId) -> Self {
        StreamId::new(String::from_utf8_lossy(overlay_id.as_bytes()).to_string())
    }
}

/// Type of media chunk
///
/// **DEPRECATED**: Use [`crate::media::segment::MediaType`] instead, which also includes
/// an `Initialization` variant for fMP4 init segments.
#[deprecated(since = "0.2.0", note = "Use media::segment::MediaType instead")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkType {
    /// Video data (H.264)
    Video,
    /// Audio data (Opus)
    Audio,
    /// Metadata chunk
    Metadata,
}

/// Media chunk for streaming
///
/// **DEPRECATED**: Use [`crate::media::segment::StreamSegment`] instead, which provides:
/// - Microsecond-precision timestamps
/// - Zero-copy data via `bytes::Bytes`
/// - Track identification for multi-track streams
/// - Efficient bincode serialization via [`crate::media::wire_format::WireSegment`]
///
/// Convert to/from `StreamSegment` using the `From` trait implementations.
#[deprecated(since = "0.2.0", note = "Use media::segment::StreamSegment instead")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaChunk {
    /// Stream identifier
    pub stream_id: StreamId,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Timestamp in milliseconds since epoch
    pub timestamp: u64,
    /// Type of media data
    pub chunk_type: ChunkType,
    /// Encoded media data
    pub data: Vec<u8>,
    /// Duration of this chunk in milliseconds
    pub duration_ms: u64,
    /// Whether this is a keyframe (for video)
    pub is_keyframe: bool,
}

impl MediaChunk {
    /// Create a new video chunk
    pub fn new_video(
        stream_id: StreamId,
        sequence: u64,
        data: Vec<u8>,
        duration_ms: u64,
        is_keyframe: bool,
    ) -> Self {
        Self {
            stream_id,
            sequence,
            timestamp: current_timestamp_ms(),
            chunk_type: ChunkType::Video,
            data,
            duration_ms,
            is_keyframe,
        }
    }

    /// Create a new audio chunk
    pub fn new_audio(stream_id: StreamId, sequence: u64, data: Vec<u8>, duration_ms: u64) -> Self {
        Self {
            stream_id,
            sequence,
            timestamp: current_timestamp_ms(),
            chunk_type: ChunkType::Audio,
            data,
            duration_ms,
            is_keyframe: false, // Audio doesn't have keyframes
        }
    }

    /// Create a metadata chunk
    pub fn new_metadata(
        stream_id: StreamId,
        sequence: u64,
        metadata: StreamMetadata,
    ) -> Result<Self, serde_json::Error> {
        let data = serde_json::to_vec(&metadata)?;
        Ok(Self {
            stream_id,
            sequence,
            timestamp: current_timestamp_ms(),
            chunk_type: ChunkType::Metadata,
            data,
            duration_ms: 0,
            is_keyframe: false,
        })
    }

    /// Get the size of this chunk in bytes
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Serialize chunk to bytes for transmission
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize chunk from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

/// Stream metadata for initialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetadata {
    /// Stream title/name
    pub title: String,
    /// Video width in pixels
    pub video_width: u32,
    /// Video height in pixels
    pub video_height: u32,
    /// Video framerate (fps)
    pub video_fps: u32,
    /// Audio sample rate (Hz)
    pub audio_sample_rate: u32,
    /// Number of audio channels
    pub audio_channels: u16,
    /// Video codec used
    pub video_codec: String,
    /// Audio codec used
    pub audio_codec: String,
}

impl StreamMetadata {
    /// Create metadata for H.264 + Opus stream
    pub fn new_h264_opus(
        title: String,
        video_width: u32,
        video_height: u32,
        video_fps: u32,
        audio_sample_rate: u32,
        audio_channels: u16,
    ) -> Self {
        Self {
            title,
            video_width,
            video_height,
            video_fps,
            audio_sample_rate,
            audio_channels,
            video_codec: "openh264".to_string(),
            audio_codec: "opus".to_string(),
        }
    }
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Stream chunk builder for easier construction
///
/// **DEPRECATED**: Use [`crate::media::segment::SegmentBuilder`] instead.
#[deprecated(since = "0.2.0", note = "Use media::segment::SegmentBuilder instead")]
pub struct ChunkBuilder {
    #[allow(deprecated)]
    stream_id: StreamId,
    video_sequence: u64,
    audio_sequence: u64,
}

impl ChunkBuilder {
    /// Create a new chunk builder
    pub fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            video_sequence: 0,
            audio_sequence: 0,
        }
    }

    /// Create next video chunk
    pub fn next_video_chunk(
        &mut self,
        data: Vec<u8>,
        duration_ms: u64,
        is_keyframe: bool,
    ) -> MediaChunk {
        let chunk = MediaChunk::new_video(
            self.stream_id.clone(),
            self.video_sequence,
            data,
            duration_ms,
            is_keyframe,
        );
        self.video_sequence += 1;
        chunk
    }

    /// Create next audio chunk
    pub fn next_audio_chunk(&mut self, data: Vec<u8>, duration_ms: u64) -> MediaChunk {
        let chunk = MediaChunk::new_audio(
            self.stream_id.clone(),
            self.audio_sequence,
            data,
            duration_ms,
        );
        self.audio_sequence += 1;
        chunk
    }

    /// Create metadata chunk
    pub fn metadata_chunk(
        &self,
        metadata: StreamMetadata,
    ) -> Result<MediaChunk, serde_json::Error> {
        MediaChunk::new_metadata(self.stream_id.clone(), 0, metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id_generation() {
        let id1 = StreamId::generate();
        let id2 = StreamId::generate();
        assert_ne!(id1, id2);
        assert!(id1.as_str().starts_with("stream_"));
    }

    #[test]
    fn test_media_chunk_serialization() {
        let stream_id = StreamId::new("test_stream".to_string());
        let chunk = MediaChunk::new_video(stream_id, 1, vec![1, 2, 3, 4], 33, true);

        let bytes = chunk.to_bytes().unwrap();
        let deserialized = MediaChunk::from_bytes(&bytes).unwrap();

        assert_eq!(chunk.stream_id, deserialized.stream_id);
        assert_eq!(chunk.sequence, deserialized.sequence);
        assert_eq!(chunk.data, deserialized.data);
        assert_eq!(chunk.is_keyframe, deserialized.is_keyframe);
    }

    #[test]
    fn test_chunk_builder() {
        let stream_id = StreamId::new("test".to_string());
        let mut builder = ChunkBuilder::new(stream_id);

        let video1 = builder.next_video_chunk(vec![1, 2, 3], 33, true);
        let video2 = builder.next_video_chunk(vec![4, 5, 6], 33, false);
        let audio1 = builder.next_audio_chunk(vec![7, 8, 9], 20);

        assert_eq!(video1.sequence, 0);
        assert_eq!(video2.sequence, 1);
        assert_eq!(audio1.sequence, 0);
        assert_eq!(video1.chunk_type, ChunkType::Video);
        assert_eq!(audio1.chunk_type, ChunkType::Audio);
    }
}
