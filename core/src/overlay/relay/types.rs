//! Core types for relay functionality
//!
//! This module defines the fundamental data types used in the relay system.
//!
//! During the type unification migration, `StreamChunk` can be converted to/from
//! the new unified `StreamSegment` type using the conversion traits in `media::segment`.

use crate::media::segment::StreamSegment;
use crate::overlay::interface::StreamId;
use crate::overlay::libp2p::utils::{deserialize_optional_peer_id, serialize_optional_peer_id};
use bytes::Bytes;
use libp2p::PeerId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::VecDeque;

/// Buffer for stream chunks
pub type StreamBuffer = VecDeque<StreamChunk>;

// Custom serialization for Bytes field
mod bytes_serde {
    use super::*;

    pub fn serialize<S>(data: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(data)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        Ok(Bytes::from(bytes))
    }
}

/// A chunk of stream data
///
/// This type is used internally by the relay system. For new code, consider using
/// `StreamSegment` from `media::segment` which provides better type safety and
/// more efficient wire serialization.
///
/// The `data` field uses `bytes::Bytes` for zero-copy cloning, which is critical
/// for performance in the hot relay path where chunks are frequently cloned.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamChunk {
    /// Chunk identifier
    pub id: u64,
    /// Stream ID this chunk belongs to
    pub stream_id: StreamId,
    /// The data (zero-copy via Bytes)
    #[serde(with = "bytes_serde")]
    pub data: Bytes,
    /// Chunk timestamp
    pub timestamp: u64,
    /// Sequence number
    pub sequence: u64,
    /// Content type
    pub content_type: String,
    /// Whether this is a keyframe
    pub is_keyframe: bool,
    /// Source peer ID - using custom serialization helpers
    #[serde(
        serialize_with = "serialize_optional_peer_id",
        deserialize_with = "deserialize_optional_peer_id"
    )]
    pub source: Option<PeerId>,
}

impl StreamChunk {
    /// Convert to a unified StreamSegment
    ///
    /// This is useful when integrating with code that uses the new unified type system.
    pub fn to_stream_segment(&self) -> StreamSegment {
        StreamSegment::from(self)
    }

    /// Create from a StreamSegment
    ///
    /// The `id` field is set to the sequence number since StreamSegment doesn't have
    /// a separate id field.
    pub fn from_stream_segment(segment: &StreamSegment) -> Self {
        segment.to_overlay_chunk(segment.sequence)
    }
}

/// A message for the relay manager
#[derive(Debug)]
pub enum RelayMessage {
    /// A new chunk to relay
    Chunk(StreamChunk),
    /// Add a stream
    AddStream(StreamId, PeerId),
    /// Remove a stream
    RemoveStream(StreamId),
    /// Add a subscriber to a stream
    AddSubscriber(StreamId, PeerId),
    /// Remove a subscriber from a stream
    RemoveSubscriber(StreamId, PeerId),
    /// Request chunks since a sequence number
    RequestChunks(StreamId, PeerId, u64),
    /// Stop the relay manager
    Stop,
}

/// Task handles for background tasks
#[derive(Debug)]
pub struct TaskHandles {
    /// Chunk processor task
    pub chunk_processor: Option<tokio::task::JoinHandle<()>>,
    /// Message processor task
    pub message_processor: Option<tokio::task::JoinHandle<()>>,
    /// Stats reporter task
    pub stats_reporter: Option<tokio::task::JoinHandle<()>>,
    /// Stream cleanup task
    pub stream_cleanup: Option<tokio::task::JoinHandle<()>>,
}
