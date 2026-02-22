//! Core types for relay functionality
//!
//! This module defines the fundamental data types used in the relay system.

use crate::overlay::interface::StreamId;
use crate::overlay::libp2p::utils::{deserialize_optional_peer_id, serialize_optional_peer_id};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Buffer for stream chunks
pub type StreamBuffer = VecDeque<StreamChunk>;

/// A chunk of stream data

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamChunk {
    /// Chunk identifier
    pub id: u64,
    /// Stream ID this chunk belongs to
    pub stream_id: StreamId,
    /// The data
    pub data: Vec<u8>,
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
