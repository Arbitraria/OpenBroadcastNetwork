//! Wire format for P2P media segment transmission.
//!
//! This module provides compact binary serialization for media segments
//! transmitted over the P2P network. It uses bincode for efficient encoding,
//! which is 30-40% smaller than JSON and faster to serialize/deserialize.
//!
//! The wire format is versioned to allow future evolution of the protocol.

use serde::{Deserialize, Serialize};

use super::segment::{MediaType, StreamId, StreamSegment};

/// Current wire format version
pub const WIRE_FORMAT_VERSION: u8 = 2;

/// Compact wire format for P2P segment transmission.
///
/// This struct is optimized for network transmission:
/// - Uses primitive types for minimal overhead
/// - Versioned for protocol evolution
/// - Serialized with bincode for compact binary format
///
/// Convert from `StreamSegment` using `.into()` and back using `.into()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireSegment {
    /// Wire format version (for forward compatibility)
    pub version: u8,
    /// Stream identifier (as raw bytes)
    pub stream_id: Vec<u8>,
    /// Sequence number
    pub sequence: u64,
    /// Presentation timestamp in microseconds
    pub pts_us: u64,
    /// Duration in microseconds
    pub duration_us: u64,
    /// Media type as u8: 0=video, 1=audio, 2=metadata, 3=init
    pub media_type: u8,
    /// Track identifier
    pub track_id: u8,
    /// Whether this is a keyframe
    pub is_keyframe: bool,
    /// Truncated blake3 content hash (16 bytes) for integrity verification
    pub content_hash: Vec<u8>,
    /// Media data
    pub data: Vec<u8>,
}

/// Compute a 16-byte truncated blake3 hash for content integrity
fn compute_content_hash(data: &[u8]) -> Vec<u8> {
    blake3::hash(data).as_bytes()[..16].to_vec()
}

impl WireSegment {
    /// Create a new wire segment with current version
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: Vec<u8>,
        sequence: u64,
        pts_us: u64,
        duration_us: u64,
        media_type: u8,
        track_id: u8,
        is_keyframe: bool,
        data: Vec<u8>,
    ) -> Self {
        let content_hash = compute_content_hash(&data);
        Self {
            version: WIRE_FORMAT_VERSION,
            stream_id,
            sequence,
            pts_us,
            duration_us,
            media_type,
            track_id,
            is_keyframe,
            content_hash,
            data,
        }
    }

    /// Verify that the content hash matches the data
    pub fn verify_integrity(&self) -> bool {
        let expected = compute_content_hash(&self.data);
        self.content_hash == expected
    }

    /// Serialize to bytes using bincode
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Deserialize from bytes using bincode
    pub fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }

    /// Get the size of serialized bytes
    pub fn serialized_size(&self) -> Result<u64, bincode::Error> {
        bincode::serialized_size(self)
    }

    /// Check if this wire segment is compatible with our version
    pub fn is_compatible(&self) -> bool {
        self.version <= WIRE_FORMAT_VERSION
    }

    /// Get the media type as enum
    pub fn media_type_enum(&self) -> Option<MediaType> {
        MediaType::from_u8(self.media_type)
    }
}

impl From<StreamSegment> for WireSegment {
    fn from(segment: StreamSegment) -> Self {
        let data = segment.data.to_vec();
        let content_hash = compute_content_hash(&data);
        Self {
            version: WIRE_FORMAT_VERSION,
            stream_id: segment.stream_id.to_vec(),
            sequence: segment.sequence,
            pts_us: segment.pts_us,
            duration_us: segment.duration_us,
            media_type: segment.media_type.to_u8(),
            track_id: segment.track_id,
            is_keyframe: segment.is_keyframe,
            content_hash,
            data,
        }
    }
}

impl From<&StreamSegment> for WireSegment {
    fn from(segment: &StreamSegment) -> Self {
        let data = segment.data.to_vec();
        let content_hash = compute_content_hash(&data);
        Self {
            version: WIRE_FORMAT_VERSION,
            stream_id: segment.stream_id.to_vec(),
            sequence: segment.sequence,
            pts_us: segment.pts_us,
            duration_us: segment.duration_us,
            media_type: segment.media_type.to_u8(),
            track_id: segment.track_id,
            is_keyframe: segment.is_keyframe,
            content_hash,
            data,
        }
    }
}

impl From<WireSegment> for StreamSegment {
    fn from(wire: WireSegment) -> Self {
        let media_type = MediaType::from_u8(wire.media_type).unwrap_or(MediaType::Video);
        Self {
            stream_id: StreamId::from_vec(wire.stream_id),
            sequence: wire.sequence,
            pts_us: wire.pts_us,
            duration_us: wire.duration_us,
            media_type,
            data: wire.data.into(),
            is_keyframe: wire.is_keyframe,
            track_id: wire.track_id,
        }
    }
}

impl TryFrom<&[u8]> for WireSegment {
    type Error = bincode::Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(data)
    }
}

/// Extension trait for converting iterators of segments to wire format
pub trait ToWireFormat {
    /// Convert to wire format bytes
    fn to_wire_bytes(&self) -> Result<Vec<u8>, bincode::Error>;
}

impl ToWireFormat for StreamSegment {
    fn to_wire_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        let wire: WireSegment = self.into();
        wire.to_bytes()
    }
}

/// Batch multiple segments into a single wire message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireSegmentBatch {
    /// Version of the batch format
    pub version: u8,
    /// Segments in this batch
    pub segments: Vec<WireSegment>,
}

impl WireSegmentBatch {
    /// Create a new batch from segments
    pub fn new(segments: Vec<WireSegment>) -> Self {
        Self {
            version: WIRE_FORMAT_VERSION,
            segments,
        }
    }

    /// Create batch from StreamSegments
    pub fn from_segments(segments: impl IntoIterator<Item = StreamSegment>) -> Self {
        Self::new(segments.into_iter().map(WireSegment::from).collect())
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }

    /// Convert back to StreamSegments
    pub fn into_segments(self) -> Vec<StreamSegment> {
        self.segments.into_iter().map(StreamSegment::from).collect()
    }

    /// Number of segments in batch
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_segment_roundtrip() {
        let wire = WireSegment::new(
            b"test_stream".to_vec(),
            42,
            1000000,
            33333,
            0, // video
            1,
            true,
            vec![1, 2, 3, 4, 5],
        );

        let bytes = wire.to_bytes().unwrap();
        let decoded = WireSegment::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.version, wire.version);
        assert_eq!(decoded.stream_id, wire.stream_id);
        assert_eq!(decoded.sequence, wire.sequence);
        assert_eq!(decoded.pts_us, wire.pts_us);
        assert_eq!(decoded.duration_us, wire.duration_us);
        assert_eq!(decoded.media_type, wire.media_type);
        assert_eq!(decoded.track_id, wire.track_id);
        assert_eq!(decoded.is_keyframe, wire.is_keyframe);
        assert_eq!(decoded.content_hash, wire.content_hash);
        assert_eq!(decoded.data, wire.data);
        assert!(decoded.verify_integrity());
    }

    #[test]
    fn test_stream_segment_to_wire_roundtrip() {
        let stream_id = StreamId::new("test_stream");
        let segment = StreamSegment::video(
            stream_id.clone(),
            1,
            1000000,
            33333,
            vec![1, 2, 3, 4, 5],
            true,
        );

        let wire: WireSegment = segment.clone().into();
        let restored: StreamSegment = wire.into();

        assert_eq!(restored.stream_id, segment.stream_id);
        assert_eq!(restored.sequence, segment.sequence);
        assert_eq!(restored.pts_us, segment.pts_us);
        assert_eq!(restored.duration_us, segment.duration_us);
        assert_eq!(restored.media_type, segment.media_type);
        assert_eq!(restored.track_id, segment.track_id);
        assert_eq!(restored.is_keyframe, segment.is_keyframe);
        assert_eq!(restored.data.as_ref(), segment.data.as_ref());
    }

    #[test]
    fn test_wire_format_smaller_than_json() {
        let segment = StreamSegment::video(
            StreamId::new("test_stream_with_longer_name"),
            12345,
            98765432100,
            33333,
            vec![0u8; 1000], // 1KB of data
            true,
        );

        let wire: WireSegment = segment.clone().into();
        let bincode_bytes = wire.to_bytes().unwrap();
        let json_bytes = serde_json::to_vec(&wire).unwrap();

        // Bincode should be significantly smaller
        assert!(
            bincode_bytes.len() < json_bytes.len(),
            "bincode ({}) should be smaller than json ({})",
            bincode_bytes.len(),
            json_bytes.len()
        );

        // For typical segments, bincode is 30-40% smaller
        let ratio = bincode_bytes.len() as f64 / json_bytes.len() as f64;
        assert!(
            ratio < 0.8,
            "bincode should be at least 20% smaller, ratio: {}",
            ratio
        );
    }

    #[test]
    fn test_wire_segment_batch() {
        let segments: Vec<StreamSegment> = (0..5)
            .map(|i| {
                StreamSegment::video(
                    StreamId::new("test"),
                    i,
                    i * 33333,
                    33333,
                    vec![i as u8; 100],
                    i == 0,
                )
            })
            .collect();

        let batch = WireSegmentBatch::from_segments(segments.clone());
        assert_eq!(batch.len(), 5);

        let bytes = batch.to_bytes().unwrap();
        let restored = WireSegmentBatch::from_bytes(&bytes).unwrap();
        assert_eq!(restored.len(), 5);

        let restored_segments = restored.into_segments();
        assert_eq!(restored_segments.len(), 5);
        assert_eq!(restored_segments[0].sequence, 0);
        assert_eq!(restored_segments[4].sequence, 4);
    }

    #[test]
    fn test_to_wire_bytes() {
        let segment = StreamSegment::audio(
            StreamId::new("audio_stream"),
            10,
            500000,
            20000,
            vec![42; 50],
        );

        let bytes = segment.to_wire_bytes().unwrap();
        let restored = WireSegment::from_bytes(&bytes).unwrap();

        assert_eq!(restored.media_type, MediaType::Audio.to_u8());
        assert_eq!(restored.sequence, 10);
    }

    #[test]
    fn test_media_type_enum_recovery() {
        for media_type in [
            MediaType::Video,
            MediaType::Audio,
            MediaType::Metadata,
            MediaType::Initialization,
        ] {
            let wire = WireSegment::new(
                b"test".to_vec(),
                0,
                0,
                0,
                media_type.to_u8(),
                0,
                false,
                vec![],
            );
            assert_eq!(wire.media_type_enum(), Some(media_type));
        }
    }
}
