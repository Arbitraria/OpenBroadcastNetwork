//! Configuration for relay functionality
//!
//! This module provides configuration options for the relay system.

use std::time::Duration;

/// Configuration for relay nodes
#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// Maximum buffer size per stream (in chunks)
    pub max_buffer_size: usize,
    /// Maximum chunk size
    pub max_chunk_size: usize,
    /// Statistics reporting interval
    pub stats_interval: Duration,
    /// Stream cleanup interval (remove inactive streams)
    pub cleanup_interval: Duration,
    /// Stream inactivity timeout (how long before a stream is considered inactive)
    pub inactivity_timeout: Duration,
    /// Maximum number of streams to relay
    pub max_streams: usize,
    /// Whether to enable bandwidth limiting
    pub enable_bandwidth_limit: bool,
    /// Maximum outgoing bandwidth (bytes/second)
    pub max_outgoing_bandwidth: u64,
    /// Maximum number of subscribers per stream
    pub max_subscribers: usize,
    /// Chunk size for stream data
    pub chunk_size: usize,
    /// Buffer size for relay queue
    pub relay_buffer_size: usize,
    /// Queue size for relay operations
    pub relay_queue_size: usize,
    /// Timeout for relay operations in milliseconds
    pub relay_timeout_ms: u64,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 100,
            max_chunk_size: 64 * 1024, // 64 KB
            stats_interval: Duration::from_secs(5),
            cleanup_interval: Duration::from_secs(30),
            inactivity_timeout: Duration::from_secs(60),
            max_streams: 10,
            enable_bandwidth_limit: false,
            max_outgoing_bandwidth: 5 * 1024 * 1024, // 5 MB/s
            max_subscribers: 50,
            chunk_size: 16 * 1024, // 16 KB
            relay_buffer_size: 100,
            relay_queue_size: 1000,
            relay_timeout_ms: 5000,
        }
    }
}
