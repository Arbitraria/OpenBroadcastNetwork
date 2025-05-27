//! Quality management for media streaming.
//!
//! This module provides functionality for managing the quality of media streams,
//! including bitrate adaptation and quality of service (QoS) monitoring.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Represents the quality level of a media stream
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QualityLevel {
    /// Lowest quality (e.g., 240p)
    Low,
    /// Medium quality (e.g., 480p)
    Medium,
    /// High quality (e.g., 720p)
    High,
    /// Highest quality (e.g., 1080p or 4K)
    Highest,
}

/// Configuration for quality adaptation
#[derive(Debug, Clone)]
pub struct QualityConfig {
    /// Minimum quality level
    pub min_quality: QualityLevel,
    /// Maximum quality level
    pub max_quality: QualityLevel,
    /// Target buffer duration for quality up-switching
    pub up_switch_buffer: Duration,
    /// Target buffer duration for quality down-switching
    pub down_switch_buffer: Duration,
    /// Window size for measuring bandwidth
    pub bandwidth_window_size: usize,
}

impl Default for QualityConfig {
    fn default() -> Self {
        QualityConfig {
            min_quality: QualityLevel::Low,
            max_quality: QualityLevel::Highest,
            up_switch_buffer: Duration::from_secs(10),
            down_switch_buffer: Duration::from_secs(5),
            bandwidth_window_size: 10,
        }
    }
}

/// Tracks bandwidth usage for quality adaptation
pub struct BandwidthMonitor {
    /// Configuration for quality adaptation
    config: QualityConfig,
    /// Timestamp and size of recent downloads
    samples: VecDeque<(Instant, u64)>,
    /// Current bandwidth in bits per second
    current_bandwidth: f64,
}

impl BandwidthMonitor {
    /// Create a new bandwidth monitor with the given configuration
    pub fn new(config: QualityConfig) -> Self {
        BandwidthMonitor {
            config,
            samples: VecDeque::new(),
            current_bandwidth: 0.0,
        }
    }
    
    /// Record a download of the given size at the current time
    pub fn record_download(&mut self, size: u64) {
        let now = Instant::now();
        self.samples.push_back((now, size));
        
        // Remove samples older than our window
        let window_start = now.checked_sub(Duration::from_secs(60)).unwrap_or(now);
        while let Some(&(time, _)) = self.samples.front() {
            if time >= window_start {
                break;
            }
            self.samples.pop_front();
        }
        
        // Calculate current bandwidth
        self.current_bandwidth = self.calculate_bandwidth();
    }
    
    /// Get the current bandwidth in bits per second
    pub fn current_bandwidth(&self) -> f64 {
        self.current_bandwidth
    }
    
    /// Calculate the current bandwidth based on recent samples
    fn calculate_bandwidth(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }
        
        let mut total_bytes = 0u64;
        let mut min_time = self.samples[0].0;
        let mut max_time = self.samples[0].0;
        
        for &(time, size) in &self.samples {
            total_bytes += size;
            min_time = min_time.min(time);
            max_time = max_time.max(time);
        }
        
        let duration = max_time.duration_since(min_time).as_secs_f64();
        if duration > 0.0 {
            (total_bytes as f64 * 8.0) / duration
        } else {
            0.0
        }
    }
    
    /// Get the recommended quality level based on current conditions
    pub fn recommended_quality(&self) -> QualityLevel {
        // This is a simplified implementation
        // In a real implementation, you would consider factors like:
        // - Current bandwidth
        // - Buffer levels
        // - Device capabilities
        // - User preferences
        
        // For now, just return a fixed quality level
        QualityLevel::Medium
    }
}

/// Manages quality adaptation for a media stream
pub struct QualityManager {
    /// Bandwidth monitor
    bandwidth_monitor: BandwidthMonitor,
    /// Current quality level
    current_quality: QualityLevel,
    /// Target quality level
    target_quality: QualityLevel,
    /// Configuration
    config: QualityConfig,
}

impl QualityManager {
    /// Create a new quality manager with the given configuration
    pub fn new(config: QualityConfig) -> Self {
        QualityManager {
            bandwidth_monitor: BandwidthMonitor::new(config.clone()),
            current_quality: config.min_quality,
            target_quality: config.min_quality,
            config,
        }
    }
    
    /// Update the quality based on current conditions
    pub fn update_quality(&mut self, buffer_level: Duration) -> Option<QualityLevel> {
        // Update target quality based on bandwidth
        self.target_quality = self.bandwidth_monitor.recommended_quality();
        
        // Adjust quality based on buffer levels
        if buffer_level > self.config.up_switch_buffer {
            // Increase quality if buffer is healthy
            let new_quality = self.current_quality.max(self.target_quality);
            if new_quality != self.current_quality {
                self.current_quality = new_quality;
                return Some(self.current_quality);
            }
        } else if buffer_level < self.config.down_switch_buffer {
            // Decrease quality if buffer is low
            let new_quality = self.current_quality.min(self.target_quality);
            if new_quality != self.current_quality {
                self.current_quality = new_quality;
                return Some(self.current_quality);
            }
        }
        
        None
    }
    
    /// Get the current quality level
    pub fn current_quality(&self) -> QualityLevel {
        self.current_quality
    }
    
    /// Get the target quality level
    pub fn target_quality(&self) -> QualityLevel {
        self.target_quality
    }
    
    /// Record a download for bandwidth monitoring
    pub fn record_download(&mut self, size: u64) {
        self.bandwidth_monitor.record_download(size);
    }
}
