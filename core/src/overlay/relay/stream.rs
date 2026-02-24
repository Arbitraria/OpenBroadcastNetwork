//! Stream relay implementation
//!
//! This module handles the relaying of data for individual streams.
//!
//! # Late-Joiner Support
//!
//! The relay caches initialization segments and recent keyframes to allow
//! late-joining viewers to start playback immediately. When a new subscriber
//! joins, they can request catchup chunks via `get_catchup_chunks()`.

use super::types::{StreamBuffer, StreamChunk};
use crate::overlay::interface::StreamId;
use libp2p::PeerId;
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Default number of keyframes to buffer for late joiners
pub const DEFAULT_KEYFRAME_BUFFER_SIZE: usize = 3;

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
    /// Initialization segment (fMP4 init) for late joiners
    pub init_segment: Option<StreamChunk>,
    /// Buffer of recent keyframes for late joiners (video-only)
    pub keyframe_buffer: VecDeque<StreamChunk>,
    /// Maximum number of keyframes to buffer
    pub max_keyframes: usize,
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
            init_segment: None,
            keyframe_buffer: VecDeque::with_capacity(DEFAULT_KEYFRAME_BUFFER_SIZE),
            max_keyframes: DEFAULT_KEYFRAME_BUFFER_SIZE,
        }
    }

    /// Create a new stream relay with custom keyframe buffer size
    pub fn with_keyframe_buffer_size(
        stream_id: StreamId,
        publisher: PeerId,
        max_keyframes: usize,
    ) -> Self {
        Self {
            stream_id,
            publisher,
            subscribers: HashSet::new(),
            buffer: StreamBuffer::new(),
            last_activity: Instant::now(),
            last_sequence: 0,
            init_segment: None,
            keyframe_buffer: VecDeque::with_capacity(max_keyframes),
            max_keyframes,
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

        // Check if this is an initialization segment
        if chunk.content_type == "init" || chunk.content_type == "initialization" {
            self.init_segment = Some(chunk.clone());
        }
        // Check if this is a keyframe (for video chunks)
        else if chunk.is_keyframe
            && (chunk.content_type == "video" || chunk.content_type.starts_with("video/"))
        {
            // Add to keyframe buffer
            self.keyframe_buffer.push_back(chunk.clone());

            // Enforce keyframe buffer limit
            while self.keyframe_buffer.len() > self.max_keyframes {
                self.keyframe_buffer.pop_front();
            }
        }

        // Add to regular buffer
        self.buffer.push_back(chunk);

        // Enforce buffer size limit
        while self.buffer.len() > max_buffer_size {
            self.buffer.pop_front();
        }
    }

    /// Set the initialization segment explicitly
    pub fn set_init_segment(&mut self, chunk: StreamChunk) {
        self.init_segment = Some(chunk);
    }

    /// Get the initialization segment
    pub fn get_init_segment(&self) -> Option<&StreamChunk> {
        self.init_segment.as_ref()
    }

    /// Get catchup chunks for late-joining viewers
    ///
    /// Returns the initialization segment (if available) followed by the most
    /// recent keyframe and all chunks after it. This allows late joiners to
    /// start playback immediately from the most recent keyframe.
    pub fn get_catchup_chunks(&self) -> Vec<StreamChunk> {
        let mut chunks = Vec::new();

        // Add init segment first
        if let Some(init) = &self.init_segment {
            chunks.push(init.clone());
        }

        // Find the most recent keyframe
        if let Some(last_keyframe) = self.keyframe_buffer.back() {
            let keyframe_seq = last_keyframe.sequence;

            // Add the keyframe
            chunks.push(last_keyframe.clone());

            // Add all chunks after the keyframe
            for chunk in self.buffer.iter() {
                if chunk.sequence > keyframe_seq {
                    chunks.push(chunk.clone());
                }
            }
        }

        chunks
    }

    /// Get all buffered keyframes
    pub fn get_keyframes(&self) -> Vec<StreamChunk> {
        self.keyframe_buffer.iter().cloned().collect()
    }

    /// Get the number of buffered keyframes
    pub fn keyframe_count(&self) -> usize {
        self.keyframe_buffer.len()
    }

    /// Check if we have an initialization segment
    pub fn has_init_segment(&self) -> bool {
        self.init_segment.is_some()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay::interface::StreamId as OverlayStreamId;

    fn create_test_chunk(sequence: u64, content_type: &str, is_keyframe: bool) -> StreamChunk {
        StreamChunk {
            id: sequence,
            stream_id: OverlayStreamId::from_string("test_stream"),
            data: bytes::Bytes::from(vec![sequence as u8; 100]),
            timestamp: sequence * 33,
            sequence,
            content_type: content_type.to_string(),
            is_keyframe,
            source: None,
        }
    }

    #[test]
    fn test_init_segment_caching() {
        let stream_id = OverlayStreamId::from_string("test");
        let peer_id = libp2p::PeerId::random();
        let mut relay = StreamRelay::new(stream_id, peer_id);

        // Add init segment
        let init_chunk = create_test_chunk(0, "init", true);
        relay.add_chunk(init_chunk.clone(), 100);

        assert!(relay.has_init_segment());
        assert_eq!(relay.get_init_segment().unwrap().sequence, 0);
    }

    #[test]
    fn test_keyframe_buffering() {
        let stream_id = OverlayStreamId::from_string("test");
        let peer_id = libp2p::PeerId::random();
        let mut relay = StreamRelay::new(stream_id, peer_id);

        // Add some keyframes
        relay.add_chunk(create_test_chunk(0, "video", true), 100);
        relay.add_chunk(create_test_chunk(1, "video", false), 100);
        relay.add_chunk(create_test_chunk(2, "video", false), 100);
        relay.add_chunk(create_test_chunk(3, "video", true), 100);
        relay.add_chunk(create_test_chunk(4, "video", false), 100);

        assert_eq!(relay.keyframe_count(), 2);
    }

    #[test]
    fn test_keyframe_buffer_limit() {
        let stream_id = OverlayStreamId::from_string("test");
        let peer_id = libp2p::PeerId::random();
        let mut relay = StreamRelay::with_keyframe_buffer_size(stream_id, peer_id, 2);

        // Add more keyframes than the limit
        relay.add_chunk(create_test_chunk(0, "video", true), 100);
        relay.add_chunk(create_test_chunk(10, "video", true), 100);
        relay.add_chunk(create_test_chunk(20, "video", true), 100);
        relay.add_chunk(create_test_chunk(30, "video", true), 100);

        assert_eq!(relay.keyframe_count(), 2);
        // Should have the two most recent keyframes
        let keyframes = relay.get_keyframes();
        assert_eq!(keyframes[0].sequence, 20);
        assert_eq!(keyframes[1].sequence, 30);
    }

    #[test]
    fn test_catchup_chunks() {
        let stream_id = OverlayStreamId::from_string("test");
        let peer_id = libp2p::PeerId::random();
        let mut relay = StreamRelay::new(stream_id, peer_id);

        // Add init segment
        relay.add_chunk(create_test_chunk(0, "init", true), 100);

        // Add some video chunks
        relay.add_chunk(create_test_chunk(1, "video", true), 100); // keyframe
        relay.add_chunk(create_test_chunk(2, "video", false), 100);
        relay.add_chunk(create_test_chunk(3, "video", false), 100);
        relay.add_chunk(create_test_chunk(4, "video", true), 100); // keyframe
        relay.add_chunk(create_test_chunk(5, "video", false), 100);

        let catchup = relay.get_catchup_chunks();

        // Should have: init + last keyframe (4) + chunk 5
        assert_eq!(catchup.len(), 3);
        assert_eq!(catchup[0].content_type, "init");
        assert_eq!(catchup[1].sequence, 4);
        assert!(catchup[1].is_keyframe);
        assert_eq!(catchup[2].sequence, 5);
    }
}
