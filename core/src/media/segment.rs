//! Unified streaming segment types for the decentralized streaming CDN.
//!
//! This module defines the core types for media segments used throughout the system:
//! - `StreamSegment` - The internal representation used for processing
//! - `StreamId` - Unified stream identifier using bytes::Bytes for zero-copy
//! - `MediaType` - Type of media content (video, audio, metadata, initialization)
//!
//! These types replace the fragmented type system that previously existed across:
//! - `MediaChunk` (stream_chunk.rs)
//! - `MseSegment` (mp4_parser.rs)
//! - `Sample` (fmp4_converter.rs)
//! - `MediaSample` (video_reader.rs)
//! - `StreamChunk` (relay/types.rs)

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// Type of media content in a segment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MediaType {
    /// Video data (e.g., H.264)
    Video,
    /// Audio data (e.g., AAC, Opus)
    Audio,
    /// Metadata (e.g., stream info, codec parameters)
    Metadata,
    /// Initialization segment (e.g., fMP4 init segment)
    Initialization,
}

impl MediaType {
    /// Convert to u8 for compact wire format
    pub fn to_u8(self) -> u8 {
        match self {
            MediaType::Video => 0,
            MediaType::Audio => 1,
            MediaType::Metadata => 2,
            MediaType::Initialization => 3,
        }
    }

    /// Convert from u8 for wire format deserialization
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(MediaType::Video),
            1 => Some(MediaType::Audio),
            2 => Some(MediaType::Metadata),
            3 => Some(MediaType::Initialization),
            _ => None,
        }
    }
}

impl fmt::Display for MediaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MediaType::Video => write!(f, "video"),
            MediaType::Audio => write!(f, "audio"),
            MediaType::Metadata => write!(f, "metadata"),
            MediaType::Initialization => write!(f, "init"),
        }
    }
}

/// Unified stream identifier using bytes::Bytes for zero-copy sharing.
///
/// This replaces both `String`-based and `Vec<u8>`-based stream IDs
/// used in different parts of the codebase, providing:
/// - Zero-copy cloning via `Bytes`
/// - Efficient hashing and equality comparison
/// - Display trait for debugging
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamId(Bytes);

impl StreamId {
    /// Create a new stream ID from a string
    pub fn new(id: impl Into<String>) -> Self {
        Self(Bytes::from(id.into()))
    }

    /// Create a stream ID from raw bytes
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self {
        Self(bytes.into())
    }

    /// Create a stream ID from a Vec<u8>
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self(Bytes::from(bytes))
    }

    /// Generate a random stream ID
    pub fn generate() -> Self {
        use rand::Rng;
        let id: u64 = rand::thread_rng().gen();
        Self::new(format!("stream_{}", id))
    }

    /// Get the inner bytes as a slice
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Get the inner Bytes object (zero-copy clone)
    pub fn bytes(&self) -> Bytes {
        self.0.clone()
    }

    /// Convert to Vec<u8> (copies the data)
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// Try to interpret as a UTF-8 string
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }

    /// Convert to String, using lossy UTF-8 conversion
    pub fn to_string_lossy(&self) -> String {
        String::from_utf8_lossy(&self.0).into_owned()
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Try UTF-8 first, fall back to hex
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "{}", s),
            Err(_) => {
                for byte in self.0.iter() {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
        }
    }
}

impl From<String> for StreamId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for StreamId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<Vec<u8>> for StreamId {
    fn from(v: Vec<u8>) -> Self {
        Self::from_vec(v)
    }
}

impl From<Bytes> for StreamId {
    fn from(b: Bytes) -> Self {
        Self::from_bytes(b)
    }
}

// Serialization support - serialize as bytes
impl Serialize for StreamId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for StreamId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
        Ok(StreamId::from_vec(bytes))
    }
}

/// Unified media segment type used throughout the system.
///
/// This is the internal representation for media segments, providing:
/// - Microsecond-precision timestamps
/// - Zero-copy data sharing via `Bytes`
/// - Track identification for multi-track streams
/// - Keyframe markers for video seeking
///
/// For P2P transmission, convert to `WireSegment` using `.into()`.
#[derive(Debug, Clone)]
pub struct StreamSegment {
    /// Stream this segment belongs to
    pub stream_id: StreamId,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Presentation timestamp in microseconds
    pub pts_us: u64,
    /// Duration in microseconds
    pub duration_us: u64,
    /// Type of media content
    pub media_type: MediaType,
    /// Segment data (zero-copy via Bytes)
    pub data: Bytes,
    /// Whether this is a keyframe (video) or sync point
    pub is_keyframe: bool,
    /// Track identifier: 0=combined, 1=video, 2=audio, etc.
    pub track_id: u8,
}

impl StreamSegment {
    /// Create a new stream segment
    pub fn new(
        stream_id: StreamId,
        sequence: u64,
        pts_us: u64,
        duration_us: u64,
        media_type: MediaType,
        data: impl Into<Bytes>,
        is_keyframe: bool,
        track_id: u8,
    ) -> Self {
        Self {
            stream_id,
            sequence,
            pts_us,
            duration_us,
            media_type,
            data: data.into(),
            is_keyframe,
            track_id,
        }
    }

    /// Create a video segment
    pub fn video(
        stream_id: StreamId,
        sequence: u64,
        pts_us: u64,
        duration_us: u64,
        data: impl Into<Bytes>,
        is_keyframe: bool,
    ) -> Self {
        Self::new(
            stream_id,
            sequence,
            pts_us,
            duration_us,
            MediaType::Video,
            data,
            is_keyframe,
            1, // video track
        )
    }

    /// Create an audio segment
    pub fn audio(
        stream_id: StreamId,
        sequence: u64,
        pts_us: u64,
        duration_us: u64,
        data: impl Into<Bytes>,
    ) -> Self {
        Self::new(
            stream_id,
            sequence,
            pts_us,
            duration_us,
            MediaType::Audio,
            data,
            false, // audio doesn't have keyframes in typical use
            2,     // audio track
        )
    }

    /// Create a metadata segment
    pub fn metadata(stream_id: StreamId, sequence: u64, data: impl Into<Bytes>) -> Self {
        Self::new(
            stream_id,
            sequence,
            current_timestamp_us(),
            0,
            MediaType::Metadata,
            data,
            false,
            0, // combined/meta track
        )
    }

    /// Create an initialization segment
    pub fn initialization(stream_id: StreamId, data: impl Into<Bytes>) -> Self {
        Self::new(
            stream_id,
            0, // init segments are always sequence 0
            0,
            0,
            MediaType::Initialization,
            data,
            true, // init segments are always "keyframes" for seeking purposes
            0,    // combined track
        )
    }

    /// Get the size of the data in bytes
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Get the duration in milliseconds
    pub fn duration_ms(&self) -> u64 {
        self.duration_us / 1000
    }

    /// Get the presentation timestamp in milliseconds
    pub fn pts_ms(&self) -> u64 {
        self.pts_us / 1000
    }

    /// Check if this is a video segment
    pub fn is_video(&self) -> bool {
        matches!(self.media_type, MediaType::Video)
    }

    /// Check if this is an audio segment
    pub fn is_audio(&self) -> bool {
        matches!(self.media_type, MediaType::Audio)
    }

    /// Check if this is an initialization segment
    pub fn is_init(&self) -> bool {
        matches!(self.media_type, MediaType::Initialization)
    }
}

/// Get current timestamp in microseconds since UNIX epoch
fn current_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Builder for creating sequential segments
pub struct SegmentBuilder {
    stream_id: StreamId,
    video_sequence: u64,
    audio_sequence: u64,
    meta_sequence: u64,
}

impl SegmentBuilder {
    /// Create a new segment builder for a stream
    pub fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            video_sequence: 0,
            audio_sequence: 0,
            meta_sequence: 0,
        }
    }

    /// Create the next video segment
    pub fn next_video(
        &mut self,
        pts_us: u64,
        duration_us: u64,
        data: impl Into<Bytes>,
        is_keyframe: bool,
    ) -> StreamSegment {
        let seq = self.video_sequence;
        self.video_sequence += 1;
        StreamSegment::video(
            self.stream_id.clone(),
            seq,
            pts_us,
            duration_us,
            data,
            is_keyframe,
        )
    }

    /// Create the next audio segment
    pub fn next_audio(
        &mut self,
        pts_us: u64,
        duration_us: u64,
        data: impl Into<Bytes>,
    ) -> StreamSegment {
        let seq = self.audio_sequence;
        self.audio_sequence += 1;
        StreamSegment::audio(self.stream_id.clone(), seq, pts_us, duration_us, data)
    }

    /// Create a metadata segment
    pub fn metadata(&mut self, data: impl Into<Bytes>) -> StreamSegment {
        let seq = self.meta_sequence;
        self.meta_sequence += 1;
        StreamSegment::metadata(self.stream_id.clone(), seq, data)
    }

    /// Create an initialization segment
    pub fn initialization(&self, data: impl Into<Bytes>) -> StreamSegment {
        StreamSegment::initialization(self.stream_id.clone(), data)
    }

    /// Get the current video sequence number
    pub fn video_sequence(&self) -> u64 {
        self.video_sequence
    }

    /// Get the current audio sequence number
    pub fn audio_sequence(&self) -> u64 {
        self.audio_sequence
    }
}

// =============================================================================
// Conversion traits from legacy types
// =============================================================================
//
// These conversions allow gradual migration from the old type system.
// They are implemented as From traits for ergonomic use during the transition.

/// Conversion context providing stream_id and sequence for types that don't have them
pub struct ConversionContext {
    /// Stream ID to use when converting types that lack one
    pub stream_id: StreamId,
    /// Sequence number to use (typically auto-incremented)
    pub sequence: u64,
}

impl ConversionContext {
    /// Create a new conversion context
    pub fn new(stream_id: StreamId, sequence: u64) -> Self {
        Self {
            stream_id,
            sequence,
        }
    }
}

// Conversion from MediaChunk (stream_chunk.rs)
impl From<&crate::media::stream_chunk::MediaChunk> for StreamSegment {
    fn from(chunk: &crate::media::stream_chunk::MediaChunk) -> Self {
        let media_type = match chunk.chunk_type {
            crate::media::stream_chunk::ChunkType::Video => MediaType::Video,
            crate::media::stream_chunk::ChunkType::Audio => MediaType::Audio,
            crate::media::stream_chunk::ChunkType::Metadata => MediaType::Metadata,
        };

        let track_id = match chunk.chunk_type {
            crate::media::stream_chunk::ChunkType::Video => 1,
            crate::media::stream_chunk::ChunkType::Audio => 2,
            crate::media::stream_chunk::ChunkType::Metadata => 0,
        };

        Self {
            stream_id: StreamId::new(chunk.stream_id.as_str()),
            sequence: chunk.sequence,
            pts_us: chunk.timestamp * 1000, // ms to us
            duration_us: chunk.duration_ms * 1000,
            media_type,
            data: Bytes::from(chunk.data.clone()),
            is_keyframe: chunk.is_keyframe,
            track_id,
        }
    }
}

impl From<crate::media::stream_chunk::MediaChunk> for StreamSegment {
    fn from(chunk: crate::media::stream_chunk::MediaChunk) -> Self {
        StreamSegment::from(&chunk)
    }
}

// Conversion from MseSegment (mp4_parser.rs)
// Note: MseSegment doesn't have stream_id or sequence, so we provide a conversion
// that takes additional context
impl StreamSegment {
    /// Create a StreamSegment from an MseSegment with additional context
    pub fn from_mse_segment(
        mse: &crate::media::mp4_parser::MseSegment,
        stream_id: StreamId,
        sequence: u64,
    ) -> Self {
        let media_type = if mse.segment_type == "initialization" {
            MediaType::Initialization
        } else if mse.track_id == 2 {
            MediaType::Audio
        } else {
            MediaType::Video
        };

        Self {
            stream_id,
            sequence,
            pts_us: mse.timestamp.unwrap_or(0) * 1000, // ms to us
            duration_us: mse.duration.unwrap_or(0) * 1000,
            media_type,
            data: Bytes::from(mse.data.clone()),
            is_keyframe: mse.is_keyframe,
            track_id: mse.track_id as u8,
        }
    }
}

// Conversion from Sample (fmp4_converter.rs)
// Sample doesn't have timing info in microseconds, so we need context
impl StreamSegment {
    /// Create a StreamSegment from a Sample with additional context
    ///
    /// The `timescale` parameter converts duration to microseconds.
    /// Typical values: 90000 for video (90kHz), 48000 for audio.
    pub fn from_sample(
        sample: &crate::media::fmp4_converter::Sample,
        stream_id: StreamId,
        sequence: u64,
        pts_us: u64,
        timescale: u32,
        media_type: MediaType,
    ) -> Self {
        // Convert duration from timescale units to microseconds
        let duration_us = if timescale > 0 {
            (sample.duration as u64 * 1_000_000) / timescale as u64
        } else {
            sample.duration as u64 * 1000 // assume ms if no timescale
        };

        Self {
            stream_id,
            sequence,
            pts_us,
            duration_us,
            media_type,
            data: Bytes::from(sample.data.clone()),
            is_keyframe: sample.is_keyframe,
            track_id: match media_type {
                MediaType::Video => 1,
                MediaType::Audio => 2,
                _ => 0,
            },
        }
    }
}

// Conversion from MediaSample (video_reader.rs)
impl StreamSegment {
    /// Create a StreamSegment from a MediaSample with additional context
    pub fn from_media_sample(
        sample: &crate::media::video_reader::MediaSample,
        stream_id: StreamId,
        sequence: u64,
    ) -> Self {
        // Determine media type from track_id (track 1 = video, track 2 = audio typically)
        let media_type = if sample.track_id == 2 {
            MediaType::Audio
        } else {
            MediaType::Video
        };

        Self {
            stream_id,
            sequence,
            pts_us: sample.timestamp.as_micros() as u64,
            duration_us: sample.duration.as_micros() as u64,
            media_type,
            data: Bytes::from(sample.data.clone()),
            is_keyframe: sample.is_sync,
            track_id: sample.track_id as u8,
        }
    }
}

// Conversion from FFmpegSample (ffmpeg_reader.rs) - requires `ffmpeg` feature
#[cfg(feature = "ffmpeg")]
impl StreamSegment {
    /// Create a StreamSegment from an FFmpegSample with additional context
    pub fn from_ffmpeg_sample(
        sample: &crate::media::ffmpeg_reader::FFmpegSample,
        stream_id: StreamId,
        sequence: u64,
        media_type: MediaType,
    ) -> Self {
        Self {
            stream_id,
            sequence,
            pts_us: sample.timestamp.as_micros() as u64,
            duration_us: sample.duration.as_micros() as u64,
            media_type,
            data: Bytes::from(sample.data.clone()),
            is_keyframe: sample.is_sync,
            track_id: match media_type {
                MediaType::Video => 1,
                MediaType::Audio => 2,
                _ => sample.stream_index as u8,
            },
        }
    }
}

// Conversion from overlay StreamChunk (relay/types.rs)
impl From<&crate::overlay::relay::StreamChunk> for StreamSegment {
    fn from(chunk: &crate::overlay::relay::StreamChunk) -> Self {
        // Determine media type from content_type string
        let media_type = match chunk.content_type.as_str() {
            "video" => MediaType::Video,
            "audio" => MediaType::Audio,
            "metadata" | "meta" => MediaType::Metadata,
            "init" | "initialization" => MediaType::Initialization,
            _ => MediaType::Video, // default to video
        };

        let track_id = match media_type {
            MediaType::Video => 1,
            MediaType::Audio => 2,
            _ => 0,
        };

        Self {
            stream_id: StreamId::from_vec(chunk.stream_id.as_bytes().to_vec()),
            sequence: chunk.sequence,
            pts_us: chunk.timestamp * 1000, // assuming ms, convert to us
            duration_us: 0,                 // StreamChunk doesn't have duration
            media_type,
            data: chunk.data.clone(), // zero-copy clone via Bytes
            is_keyframe: chunk.is_keyframe,
            track_id,
        }
    }
}

impl From<crate::overlay::relay::StreamChunk> for StreamSegment {
    fn from(chunk: crate::overlay::relay::StreamChunk) -> Self {
        // When we own the chunk, we can take the data directly
        let media_type = match chunk.content_type.as_str() {
            "video" => MediaType::Video,
            "audio" => MediaType::Audio,
            "metadata" | "meta" => MediaType::Metadata,
            "init" | "initialization" => MediaType::Initialization,
            _ => MediaType::Video, // default to video
        };

        let track_id = match media_type {
            MediaType::Video => 1,
            MediaType::Audio => 2,
            _ => 0,
        };

        Self {
            stream_id: StreamId::from_vec(chunk.stream_id.as_bytes().to_vec()),
            sequence: chunk.sequence,
            pts_us: chunk.timestamp * 1000,
            duration_us: 0,
            media_type,
            data: chunk.data, // take ownership directly
            is_keyframe: chunk.is_keyframe,
            track_id,
        }
    }
}

// =============================================================================
// Conversions TO legacy types (for backwards compatibility during migration)
// =============================================================================

impl From<&StreamSegment> for crate::media::stream_chunk::MediaChunk {
    fn from(segment: &StreamSegment) -> Self {
        let chunk_type = match segment.media_type {
            MediaType::Video => crate::media::stream_chunk::ChunkType::Video,
            MediaType::Audio => crate::media::stream_chunk::ChunkType::Audio,
            MediaType::Metadata | MediaType::Initialization => {
                crate::media::stream_chunk::ChunkType::Metadata
            }
        };

        Self {
            stream_id: crate::media::stream_chunk::StreamId::new(
                segment.stream_id.to_string_lossy(),
            ),
            sequence: segment.sequence,
            timestamp: segment.pts_us / 1000, // us to ms
            chunk_type,
            data: segment.data.to_vec(),
            duration_ms: segment.duration_us / 1000,
            is_keyframe: segment.is_keyframe,
        }
    }
}

impl From<StreamSegment> for crate::media::stream_chunk::MediaChunk {
    fn from(segment: StreamSegment) -> Self {
        crate::media::stream_chunk::MediaChunk::from(&segment)
    }
}

// Conversion to overlay StreamChunk
impl StreamSegment {
    /// Convert to overlay StreamChunk with the given chunk ID
    pub fn to_overlay_chunk(&self, id: u64) -> crate::overlay::relay::StreamChunk {
        let content_type = match self.media_type {
            MediaType::Video => "video",
            MediaType::Audio => "audio",
            MediaType::Metadata => "metadata",
            MediaType::Initialization => "init",
        };

        crate::overlay::relay::StreamChunk {
            id,
            stream_id: crate::overlay::interface::StreamId::from_bytes(self.stream_id.to_vec()),
            data: self.data.clone(), // zero-copy clone via Bytes
            timestamp: self.pts_us / 1000, // us to ms
            sequence: self.sequence,
            content_type: content_type.to_string(),
            is_keyframe: self.is_keyframe,
            source: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id_from_string() {
        let id = StreamId::new("test_stream");
        assert_eq!(id.to_string(), "test_stream");
        assert_eq!(id.as_str(), Some("test_stream"));
    }

    #[test]
    fn test_stream_id_from_bytes() {
        // Use bytes that are definitely not valid UTF-8
        let bytes = vec![0x80, 0x81, 0x82, 0x83];
        let id = StreamId::from_vec(bytes.clone());
        assert_eq!(id.to_vec(), bytes);
        // Non-UTF8 bytes should display as hex
        assert_eq!(id.to_string(), "80818283");
    }

    #[test]
    fn test_stream_id_generate() {
        let id1 = StreamId::generate();
        let id2 = StreamId::generate();
        assert_ne!(id1, id2);
        assert!(id1.to_string().starts_with("stream_"));
    }

    #[test]
    fn test_stream_id_zero_copy() {
        let id = StreamId::new("test");
        let cloned = id.clone();
        // Both should point to same underlying data (Bytes is reference-counted)
        assert_eq!(id.as_bytes().as_ptr(), cloned.as_bytes().as_ptr());
    }

    #[test]
    fn test_media_type_conversion() {
        for media_type in [
            MediaType::Video,
            MediaType::Audio,
            MediaType::Metadata,
            MediaType::Initialization,
        ] {
            let byte = media_type.to_u8();
            let converted = MediaType::from_u8(byte);
            assert_eq!(converted, Some(media_type));
        }
        assert_eq!(MediaType::from_u8(255), None);
    }

    #[test]
    fn test_stream_segment_video() {
        let stream_id = StreamId::new("test");
        let segment =
            StreamSegment::video(stream_id.clone(), 1, 1000000, 33333, vec![1, 2, 3], true);

        assert_eq!(segment.stream_id, stream_id);
        assert_eq!(segment.sequence, 1);
        assert_eq!(segment.pts_us, 1000000);
        assert_eq!(segment.pts_ms(), 1000);
        assert_eq!(segment.duration_us, 33333);
        assert_eq!(segment.duration_ms(), 33);
        assert_eq!(segment.media_type, MediaType::Video);
        assert!(segment.is_keyframe);
        assert!(segment.is_video());
        assert!(!segment.is_audio());
        assert_eq!(segment.track_id, 1);
    }

    #[test]
    fn test_stream_segment_audio() {
        let stream_id = StreamId::new("test");
        let segment = StreamSegment::audio(stream_id.clone(), 5, 2000000, 20000, vec![4, 5, 6]);

        assert_eq!(segment.sequence, 5);
        assert_eq!(segment.media_type, MediaType::Audio);
        assert!(!segment.is_keyframe);
        assert!(segment.is_audio());
        assert_eq!(segment.track_id, 2);
    }

    #[test]
    fn test_stream_segment_initialization() {
        let stream_id = StreamId::new("test");
        let segment =
            StreamSegment::initialization(stream_id, vec![0, 0, 0, 28, 102, 116, 121, 112]);

        assert_eq!(segment.sequence, 0);
        assert_eq!(segment.media_type, MediaType::Initialization);
        assert!(segment.is_keyframe);
        assert!(segment.is_init());
    }

    #[test]
    fn test_segment_builder() {
        let mut builder = SegmentBuilder::new(StreamId::new("test"));

        let v1 = builder.next_video(0, 33333, vec![1], true);
        let v2 = builder.next_video(33333, 33333, vec![2], false);
        let a1 = builder.next_audio(0, 20000, vec![3]);
        let a2 = builder.next_audio(20000, 20000, vec![4]);

        assert_eq!(v1.sequence, 0);
        assert_eq!(v2.sequence, 1);
        assert_eq!(a1.sequence, 0);
        assert_eq!(a2.sequence, 1);

        assert_eq!(builder.video_sequence(), 2);
        assert_eq!(builder.audio_sequence(), 2);
    }

    #[test]
    fn test_stream_id_serialization() {
        let id = StreamId::new("test_stream");
        let serialized = serde_json::to_string(&id).unwrap();
        let deserialized: StreamId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(id, deserialized);
    }

    // Conversion tests

    #[test]
    fn test_from_media_chunk() {
        use crate::media::stream_chunk::{ChunkType, MediaChunk, StreamId as LegacyStreamId};

        let chunk = MediaChunk {
            stream_id: LegacyStreamId::new("test_stream".to_string()),
            sequence: 42,
            timestamp: 1000, // ms
            chunk_type: ChunkType::Video,
            data: vec![1, 2, 3, 4, 5],
            duration_ms: 33,
            is_keyframe: true,
        };

        let segment: StreamSegment = chunk.into();

        assert_eq!(segment.stream_id.to_string(), "test_stream");
        assert_eq!(segment.sequence, 42);
        assert_eq!(segment.pts_us, 1_000_000); // converted from ms
        assert_eq!(segment.duration_us, 33_000);
        assert_eq!(segment.media_type, MediaType::Video);
        assert!(segment.is_keyframe);
        assert_eq!(segment.track_id, 1);
    }

    #[test]
    fn test_to_media_chunk() {
        use crate::media::stream_chunk::{ChunkType, MediaChunk};

        let segment = StreamSegment::video(
            StreamId::new("test_stream"),
            42,
            1_000_000, // us
            33_000,    // us
            vec![1, 2, 3, 4, 5],
            true,
        );

        let chunk: MediaChunk = segment.into();

        assert_eq!(chunk.stream_id.as_str(), "test_stream");
        assert_eq!(chunk.sequence, 42);
        assert_eq!(chunk.timestamp, 1000); // converted to ms
        assert_eq!(chunk.duration_ms, 33);
        assert_eq!(chunk.chunk_type, ChunkType::Video);
        assert!(chunk.is_keyframe);
    }

    #[test]
    fn test_from_overlay_stream_chunk() {
        use crate::overlay::interface::StreamId as OverlayStreamId;
        use crate::overlay::relay::StreamChunk;

        let chunk = StreamChunk {
            id: 1,
            stream_id: OverlayStreamId::from_string("test_stream"),
            data: Bytes::from(vec![1, 2, 3, 4, 5]),
            timestamp: 1000, // ms
            sequence: 42,
            content_type: "video".to_string(),
            is_keyframe: true,
            source: None,
        };

        let segment: StreamSegment = chunk.into();

        assert_eq!(segment.stream_id.to_string(), "test_stream");
        assert_eq!(segment.sequence, 42);
        assert_eq!(segment.pts_us, 1_000_000);
        assert_eq!(segment.media_type, MediaType::Video);
        assert!(segment.is_keyframe);
    }

    #[test]
    fn test_to_overlay_stream_chunk() {
        let segment = StreamSegment::video(
            StreamId::new("test_stream"),
            42,
            1_000_000,
            33_000,
            vec![1, 2, 3, 4, 5],
            true,
        );

        let chunk = segment.to_overlay_chunk(1);

        assert_eq!(
            String::from_utf8_lossy(chunk.stream_id.as_bytes()),
            "test_stream"
        );
        assert_eq!(chunk.sequence, 42);
        assert_eq!(chunk.timestamp, 1000); // converted to ms
        assert_eq!(chunk.content_type, "video");
        assert!(chunk.is_keyframe);
    }

    #[test]
    fn test_from_mse_segment() {
        use crate::media::mp4_parser::MseSegment;

        let mse = MseSegment {
            segment_type: "media".to_string(),
            data: vec![1, 2, 3, 4, 5],
            timestamp: Some(1000), // ms
            duration: Some(33),
            is_keyframe: true,
            track_id: 1,
        };

        let stream_id = StreamId::new("test_stream");
        let segment = StreamSegment::from_mse_segment(&mse, stream_id, 42);

        assert_eq!(segment.stream_id.to_string(), "test_stream");
        assert_eq!(segment.sequence, 42);
        assert_eq!(segment.pts_us, 1_000_000);
        assert_eq!(segment.duration_us, 33_000);
        assert_eq!(segment.media_type, MediaType::Video);
        assert!(segment.is_keyframe);
    }

    #[test]
    fn test_from_mse_segment_init() {
        use crate::media::mp4_parser::MseSegment;

        let mse = MseSegment {
            segment_type: "initialization".to_string(),
            data: vec![0, 0, 0, 28, 102, 116, 121, 112],
            timestamp: None,
            duration: None,
            is_keyframe: true,
            track_id: 0,
        };

        let stream_id = StreamId::new("test_stream");
        let segment = StreamSegment::from_mse_segment(&mse, stream_id, 0);

        assert_eq!(segment.media_type, MediaType::Initialization);
        assert_eq!(segment.track_id, 0);
    }

    #[test]
    fn test_from_sample() {
        use crate::media::fmp4_converter::Sample;

        let sample = Sample::new(vec![1, 2, 3, 4, 5], 3000, true); // 3000 units at 90kHz = 33.33ms

        let stream_id = StreamId::new("test_stream");
        let segment =
            StreamSegment::from_sample(&sample, stream_id, 42, 1_000_000, 90000, MediaType::Video);

        assert_eq!(segment.stream_id.to_string(), "test_stream");
        assert_eq!(segment.sequence, 42);
        assert_eq!(segment.pts_us, 1_000_000);
        // 3000 / 90000 * 1_000_000 = 33333us
        assert_eq!(segment.duration_us, 33333);
        assert_eq!(segment.media_type, MediaType::Video);
        assert!(segment.is_keyframe);
    }
}
