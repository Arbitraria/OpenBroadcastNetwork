//! Buffer management for media streams
//!
//! This module provides buffers for storing and managing media frames.

use crate::media::interface::{MediaFrame, MediaError, MediaParams};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};
use std::sync::Arc;

/// Configuration for media buffers
#[derive(Debug, Clone)]
pub struct MediaBufferConfig {
    /// Maximum number of frames to buffer
    pub max_frames: usize,
    /// Maximum memory usage (in bytes)
    pub max_memory: usize,
    /// Maximum age of frames to keep (older frames are dropped)
    pub max_age: Duration,
}

impl Default for MediaBufferConfig {
    fn default() -> Self {
        Self {
            max_frames: 120,           // 120 frames (about 4 seconds at 30fps)
            max_memory: 5 * 1024 * 1024, // 5 MB
            max_age: Duration::from_secs(10),
        }
    }
}

/// Statistics for a media buffer
#[derive(Debug, Clone, Default)]
pub struct MediaBufferStats {
    /// Current number of frames
    pub frame_count: usize,
    /// Current memory usage
    pub memory_usage: usize,
    /// Total number of frames added
    pub frames_added: u64,
    /// Number of frames dropped due to overflow
    pub frames_dropped_overflow: u64,
    /// Number of frames dropped due to age
    pub frames_dropped_age: u64,
    /// Total bytes added
    pub bytes_added: u64,
    /// Buffer fullness (0.0 - 1.0)
    pub fullness: f32,
}

/// A buffered media frame with timing information
struct BufferedFrame {
    /// The media frame
    frame: MediaFrame,
    /// When the frame was added to the buffer
    added_at: Instant,
}

/// A buffer for media frames
pub struct MediaBuffer {
    /// Buffer configuration
    config: MediaBufferConfig,
    /// Media parameters
    params: MediaParams,
    /// Buffer contents
    frames: Mutex<VecDeque<BufferedFrame>>,
    /// Notify when new frames are available
    notify: Arc<Notify>,
    /// Buffer statistics
    stats: Mutex<MediaBufferStats>,
}

impl MediaBuffer {
    /// Create a new media buffer
    pub fn new(params: MediaParams, config: MediaBufferConfig) -> Self {
        Self {
            config,
            params,
            frames: Mutex::new(VecDeque::new()),
            notify: Arc::new(Notify::new()),
            stats: Mutex::new(MediaBufferStats::default()),
        }
    }
    
    /// Get media parameters
    pub fn params(&self) -> &MediaParams {
        &self.params
    }
    
    /// Add a frame to the buffer
    pub async fn add_frame(&self, frame: MediaFrame) -> Result<(), MediaError> {
        let mut frames = self.frames.lock().await;
        let mut stats = self.stats.lock().await;
        
        // Update statistics
        stats.frames_added += 1;
        stats.bytes_added += frame.data.len() as u64;
        
        // Check if buffer is full
        if frames.len() >= self.config.max_frames {
            // Drop the oldest frame
            if let Some(old_frame) = frames.pop_front() {
                stats.frames_dropped_overflow += 1;
                stats.memory_usage -= old_frame.frame.data.len();
            }
        }
        
        // Add the new frame
        stats.memory_usage += frame.data.len();
        frames.push_back(BufferedFrame {
            frame,
            added_at: Instant::now(),
        });
        
        // Update fullness stat
        stats.frame_count = frames.len();
        stats.fullness = frames.len() as f32 / self.config.max_frames as f32;
        
        // Notify waiters
        self.notify.notify_one();
        
        Ok(())
    }
    
    /// Get a frame from the buffer
    pub async fn get_frame(&self) -> Result<MediaFrame, MediaError> {
        let mut frames = self.frames.lock().await;
        
        // Check if buffer is empty
        if frames.is_empty() {
            return Err(MediaError::BufferError("Buffer is empty".to_string()));
        }
        
        // Get the oldest frame
        if let Some(buffered_frame) = frames.pop_front() {
            let mut stats = self.stats.lock().await;
            stats.frame_count = frames.len();
            stats.memory_usage -= buffered_frame.frame.data.len();
            stats.fullness = frames.len() as f32 / self.config.max_frames as f32;
            
            Ok(buffered_frame.frame)
        } else {
            Err(MediaError::BufferError("Failed to get frame".to_string()))
        }
    }
    
    /// Peek at the next frame without removing it
    pub async fn peek_frame(&self) -> Result<MediaFrame, MediaError> {
        let frames = self.frames.lock().await;
        
        // Check if buffer is empty
        if frames.is_empty() {
            return Err(MediaError::BufferError("Buffer is empty".to_string()));
        }
        
        // Get the oldest frame
        if let Some(buffered_frame) = frames.front() {
            Ok(buffered_frame.frame.clone())
        } else {
            Err(MediaError::BufferError("Failed to peek frame".to_string()))
        }
    }
    
    /// Wait for a frame to be available
    pub async fn wait_for_frame(&self, timeout: Option<Duration>) -> Result<(), MediaError> {
        let is_empty = {
            let frames = self.frames.lock().await;
            frames.is_empty()
        };
        
        if is_empty {
            if let Some(timeout) = timeout {
                tokio::select! {
                    _ = self.notify.notified() => Ok(()),
                    _ = tokio::time::sleep(timeout) => {
                        Err(MediaError::BufferError("Timeout waiting for frame".to_string()))
                    }
                }
            } else {
                self.notify.notified().await;
                Ok(())
            }
        } else {
            Ok(())
        }
    }
    
    /// Purge old frames
    pub async fn purge_old_frames(&self) -> usize {
        let mut frames = self.frames.lock().await;
        let mut stats = self.stats.lock().await;
        let now = Instant::now();
        let mut purged = 0;
        
        // Remove frames older than max_age
        while let Some(frame) = frames.front() {
            if now.duration_since(frame.added_at) > self.config.max_age {
                if let Some(old_frame) = frames.pop_front() {
                    stats.frames_dropped_age += 1;
                    stats.memory_usage -= old_frame.frame.data.len();
                    purged += 1;
                }
            } else {
                break;
            }
        }
        
        // Update stats
        stats.frame_count = frames.len();
        stats.fullness = frames.len() as f32 / self.config.max_frames as f32;
        
        purged
    }
    
    /// Clear all frames
    pub async fn clear(&self) {
        let mut frames = self.frames.lock().await;
        let mut stats = self.stats.lock().await;
        
        frames.clear();
        stats.frame_count = 0;
        stats.memory_usage = 0;
        stats.fullness = 0.0;
    }
    
    /// Get buffer statistics
    pub async fn stats(&self) -> MediaBufferStats {
        self.stats.lock().await.clone()
    }
} 