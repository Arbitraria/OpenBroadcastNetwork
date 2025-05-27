//! Buffer management for media streaming.
//!
//! This module provides functionality for buffering media data to handle
//! network jitter and ensure smooth playback.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::media::interface::MediaError;

/// Configuration for media buffering
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Maximum size of the buffer in bytes
    pub max_size: usize,
    /// Maximum duration of buffered data
    pub max_duration: Duration,
    /// Initial buffering target before starting playback
    pub initial_buffer_target: Duration,
}

impl Default for BufferConfig {
    fn default() -> Self {
        BufferConfig {
            max_size: 10 * 1024 * 1024, // 10 MB
            max_duration: Duration::from_secs(30), // 30 seconds
            initial_buffer_target: Duration::from_secs(5), // 5 seconds
        }
    }
}

/// A buffer for media data
#[derive(Debug)]
pub struct MediaBuffer {
    /// The actual data buffer
    data: VecDeque<u8>,
    /// Configuration for the buffer
    config: BufferConfig,
    /// Timestamp of the first sample in the buffer
    start_time: Option<Instant>,
    /// Current position in the buffer
    position: usize,
    /// Total number of bytes added to the buffer
    total_bytes: usize,
}

/// Error type for buffer operations
#[derive(Debug)]
pub struct BufferError {
    message: String,
}

impl std::fmt::Display for BufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Buffer error: {}", self.message)
    }
}

impl std::error::Error for BufferError {}

impl From<&str> for BufferError {
    fn from(err: &str) -> Self {
        BufferError {
            message: err.to_string(),
        }
    }
}

impl MediaBuffer {
    /// Create a new buffer with the given configuration
    pub fn new(config: BufferConfig) -> Self {
        MediaBuffer {
            data: VecDeque::new(),
            config,
            start_time: None,
            position: 0,
            total_bytes: 0,
        }
    }
    
    /// Add data to the buffer
    pub fn push(&mut self, data: &[u8]) -> Result<(), BufferError> {
        if self.size() + data.len() > self.config.max_size {
            return Err(BufferError::from("Buffer full"));
        }
        
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }
        
        self.data.extend(data);
        self.total_bytes += data.len();
        
        Ok(())
    }
    
    /// Get data from the buffer
    pub fn pop(&mut self, size: usize) -> Result<Vec<u8>, BufferError> {
        if self.data.len() < size {
            return Err(BufferError::from("Not enough data in buffer"));
        }
        
        let mut result = Vec::with_capacity(size);
        for _ in 0..size {
            if let Some(byte) = self.data.pop_front() {
                result.push(byte);
            }
        }
        
        self.position += result.len();
        
        Ok(result)
    }
    
    /// Get the current size of the buffer in bytes
    pub fn size(&self) -> usize {
        self.data.len()
    }
    
    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    
    /// Clear the buffer
    pub fn clear(&mut self) {
        self.data.clear();
        self.position = 0;
        self.start_time = None;
    }
    
    /// Get the current playback position
    pub fn position(&self) -> usize {
        self.position
    }
    
    /// Get the total number of bytes added to the buffer
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }
    
    /// Check if the buffer has enough data to start playback
    pub fn has_enough_data(&self) -> bool {
        self.duration() >= self.config.initial_buffer_target
    }
    
    /// Get the current duration of buffered data
    pub fn duration(&self) -> Duration {
        // This is a simplified implementation
        // In a real implementation, you would calculate the actual duration based on the codec and bitrate
        Duration::from_millis((self.size() as f64 / 1024.0 * 8.0) as u64)
    }
}
