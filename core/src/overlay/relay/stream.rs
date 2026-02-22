//! Stream relay implementation
//!
//! This module handles the relaying of data for individual streams.

use super::types::{StreamBuffer, StreamChunk};
use crate::overlay::interface::StreamId;
use libp2p::PeerId;
use std::collections::HashSet;
use std::time::{Duration, Instant};

/// A relay for a specific stream
#[derive(Debug)]
pub struct StreamRelay {
    /// Stream ID
    pub stream_id: StreamId,
    /// Publisher/source peer ID
    pub publisher: PeerId,
    /// Subscribers (consumers)
    pub subscribers: HashSet<PeerId>,
    /// Buffer of recent chunks
    pub buffer: StreamBuffer,
    /// Last activity timestamp
    pub last_activity: Instant,
    /// Last sequence number
    pub last_sequence: u64,
}

impl StreamRelay {
    /// Create a new stream relay
    pub fn new(stream_id: StreamId, publisher: PeerId) -> Self {
        Self {
            stream_id,
            publisher,
            subscribers: HashSet::new(),
            buffer: StreamBuffer::new(),
            last_activity: Instant::now(),
            last_sequence: 0,
        }
    }

    /// Add a chunk to the buffer
    pub fn add_chunk(&mut self, chunk: StreamChunk, max_buffer_size: usize) {
        // Update last activity
        self.last_activity = Instant::now();

        // Update last sequence
        if chunk.sequence > self.last_sequence {
            self.last_sequence = chunk.sequence;
        }

        // Add to buffer
        self.buffer.push_back(chunk);

        // Enforce buffer size limit
        while self.buffer.len() > max_buffer_size {
            self.buffer.pop_front();
        }
    }

    /// Add a subscriber
    pub fn add_subscriber(&mut self, peer_id: PeerId) -> bool {
        self.subscribers.insert(peer_id)
    }

    /// Remove a subscriber
    pub fn remove_subscriber(&mut self, peer_id: &PeerId) -> bool {
        self.subscribers.remove(peer_id)
    }

    /// Check if the stream is inactive
    pub fn is_inactive(&self, timeout: Duration) -> bool {
        Instant::now().duration_since(self.last_activity) > timeout
    }

    /// Get chunks since a sequence number
    pub fn get_chunks_since(&self, sequence: u64) -> Vec<StreamChunk> {
        self.buffer
            .iter()
            .filter(|chunk| chunk.sequence > sequence)
            .cloned()
            .collect()
    }
}
