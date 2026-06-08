//! Statistics for relay performance monitoring
//!
//! This module provides utilities for tracking and analyzing relay performance.

use std::collections::HashMap;
use std::time::Instant;

/// Statistics for a relay
#[derive(Debug, Clone)]
pub struct RelayStats {
    /// Number of chunks relayed
    pub chunks_relayed: u64,
    /// Number of bytes relayed
    pub bytes_relayed: u64,
    /// Average chunk size
    pub avg_chunk_size: u64,
    /// Number of active streams
    pub active_streams: usize,
    /// Number of connected peers
    pub connected_peers: usize,
    /// Incoming bandwidth (bytes/second)
    pub incoming_bandwidth: u64,
    /// Outgoing bandwidth (bytes/second)
    pub outgoing_bandwidth: u64,
    /// Measurement period start
    pub period_start: Instant,
    /// Per-stream chunk counters
    pub per_stream_chunks: HashMap<String, u64>,
}

impl Default for RelayStats {
    fn default() -> Self {
        Self {
            chunks_relayed: 0,
            bytes_relayed: 0,
            avg_chunk_size: 0,
            active_streams: 0,
            connected_peers: 0,
            incoming_bandwidth: 0,
            outgoing_bandwidth: 0,
            period_start: Instant::now(),
            per_stream_chunks: HashMap::new(),
        }
    }
}

impl RelayStats {
    /// Create new relay stats
    pub fn new() -> Self {
        Self {
            period_start: Instant::now(),
            ..Default::default()
        }
    }

    /// Reset the measurement period
    pub fn reset_period(&mut self) {
        self.period_start = Instant::now();
        self.incoming_bandwidth = 0;
        self.outgoing_bandwidth = 0;
    }

    /// Record a relayed chunk with optional stream tracking
    pub fn record_chunk(&mut self, chunk_size: usize) {
        self.chunks_relayed += 1;
        self.bytes_relayed += chunk_size as u64;

        // Update average chunk size
        if self.chunks_relayed > 0 {
            self.avg_chunk_size = self.bytes_relayed / self.chunks_relayed;
        }
    }

    /// Record a relayed chunk with per-stream tracking
    pub fn record_chunk_for_stream(&mut self, chunk_size: usize, stream_id: &str) {
        self.record_chunk(chunk_size);
        *self
            .per_stream_chunks
            .entry(stream_id.to_string())
            .or_insert(0) += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_chunk() {
        let mut stats = RelayStats::new();
        stats.record_chunk(100);
        stats.record_chunk(200);
        stats.record_chunk(300);

        assert_eq!(stats.chunks_relayed, 3);
        assert_eq!(stats.bytes_relayed, 600);
        assert_eq!(stats.avg_chunk_size, 200); // 600 / 3
    }

    #[test]
    fn test_record_chunk_for_stream() {
        let mut stats = RelayStats::new();
        stats.record_chunk_for_stream(100, "stream-a");
        stats.record_chunk_for_stream(200, "stream-a");
        stats.record_chunk_for_stream(300, "stream-b");

        assert_eq!(stats.chunks_relayed, 3);
        assert_eq!(stats.bytes_relayed, 600);
        assert_eq!(*stats.per_stream_chunks.get("stream-a").unwrap(), 2);
        assert_eq!(*stats.per_stream_chunks.get("stream-b").unwrap(), 1);
    }

    #[test]
    fn test_reset_period() {
        let mut stats = RelayStats::new();
        stats.record_chunk(500);
        stats.incoming_bandwidth = 1000;
        stats.outgoing_bandwidth = 2000;

        stats.reset_period();

        // Bandwidth fields reset
        assert_eq!(stats.incoming_bandwidth, 0);
        assert_eq!(stats.outgoing_bandwidth, 0);
        // Cumulative counters preserved
        assert_eq!(stats.chunks_relayed, 1);
        assert_eq!(stats.bytes_relayed, 500);
    }
}
