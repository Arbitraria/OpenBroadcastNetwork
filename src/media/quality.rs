//! Quality selection for adaptive streaming
//!
//! This module provides mechanisms for selecting appropriate quality levels for media streams.

use crate::media::interface::{MediaParams, QualityLevel, MediaType};
use std::time::{Duration, Instant};

/// Bandwidth estimation data
#[derive(Debug, Clone)]
pub struct BandwidthEstimate {
    /// Estimated available bandwidth in bits per second
    pub bandwidth_bps: u32,
    /// Round-trip time in milliseconds
    pub rtt_ms: u32,
    /// Packet loss rate (0.0-1.0)
    pub packet_loss: f32,
    /// When this estimate was created
    pub timestamp: Instant,
}

impl Default for BandwidthEstimate {
    fn default() -> Self {
        Self {
            bandwidth_bps: 2_000_000, // 2 Mbps as default
            rtt_ms: 100,
            packet_loss: 0.0,
            timestamp: Instant::now(),
        }
    }
}

/// Quality level profile with its requirements
#[derive(Debug, Clone)]
pub struct QualityProfile {
    /// Quality level
    pub level: QualityLevel,
    /// Minimum bandwidth required (bps)
    pub min_bandwidth_bps: u32,
    /// Ideal bandwidth (bps)
    pub ideal_bandwidth_bps: u32,
    /// Maximum RTT allowed (ms)
    pub max_rtt_ms: u32,
    /// Maximum packet loss tolerated
    pub max_packet_loss: f32,
    /// Media parameters for this quality level
    pub params: MediaParams,
}

/// Configuration for quality selection
#[derive(Debug, Clone)]
pub struct QualitySelectorConfig {
    /// How often to re-evaluate quality (ms)
    pub evaluation_interval_ms: u64,
    /// Stability threshold - how much bandwidth change triggers a quality change
    pub stability_threshold: f32,
    /// How quickly to adapt to bandwidth changes (0.0-1.0)
    pub adaptation_speed: f32,
    /// Available quality profiles
    pub profiles: Vec<QualityProfile>,
}

impl Default for QualitySelectorConfig {
    fn default() -> Self {
        // These are just example defaults
        Self {
            evaluation_interval_ms: 2000,
            stability_threshold: 0.2,       // 20% change
            adaptation_speed: 0.3,         // Moderate adaptation
            profiles: Vec::new(),          // Will be populated based on stream
        }
    }
}

/// Statistics for a quality selector
#[derive(Debug, Clone, Default)]
pub struct QualitySelectorStats {
    /// Current selected quality level
    pub current_quality: QualityLevel,
    /// Number of quality switches
    pub quality_switches: u32,
    /// Time at current quality
    pub time_at_current_quality: Duration,
    /// Available bandwidth (bps)
    pub available_bandwidth_bps: u32,
    /// Buffer fullness (0.0-1.0)
    pub buffer_fullness: f32,
    /// Last switch reason
    pub last_switch_reason: String,
}

/// Quality selector for adaptive streaming
pub struct QualitySelector {
    /// Configuration
    config: QualitySelectorConfig,
    /// Current quality level
    current_quality: QualityLevel,
    /// Current bandwidth estimate
    bandwidth_estimate: BandwidthEstimate,
    /// Statistics
    stats: QualitySelectorStats,
    /// When the current quality was selected
    quality_selected_at: Instant,
    /// Last evaluation time
    last_evaluation: Instant,
}

impl QualitySelector {
    /// Create a new quality selector
    pub fn new(config: QualitySelectorConfig) -> Result<Self, String> {
        if config.profiles.is_empty() {
            return Err("No quality profiles provided".to_string());
        }
        
        // Start with the lowest quality
        let lowest_profile = config.profiles.iter()
            .min_by_key(|p| p.min_bandwidth_bps)
            .unwrap();
            
        let now = Instant::now();
            
        let mut stats = QualitySelectorStats::default();
        stats.current_quality = lowest_profile.level;
        
        Ok(Self {
            config,
            current_quality: lowest_profile.level,
            bandwidth_estimate: BandwidthEstimate::default(),
            stats,
            quality_selected_at: now,
            last_evaluation: now,
        })
    }
    
    /// Update bandwidth estimate
    pub fn update_bandwidth(&mut self, estimate: BandwidthEstimate) {
        self.bandwidth_estimate = estimate;
        self.stats.available_bandwidth_bps = estimate.bandwidth_bps;
    }
    
    /// Update buffer fullness
    pub fn update_buffer_fullness(&mut self, fullness: f32) {
        self.stats.buffer_fullness = fullness;
    }
    
    /// Evaluate if quality should change
    pub fn evaluate(&mut self) -> Option<QualityLevel> {
        let now = Instant::now();
        
        // Skip if not enough time has passed since last evaluation
        if now.duration_since(self.last_evaluation).as_millis() < self.config.evaluation_interval_ms as u128 {
            return None;
        }
        
        self.last_evaluation = now;
        self.stats.time_at_current_quality = now.duration_since(self.quality_selected_at);
        
        // Find suitable profiles based on bandwidth
        let available_bw = self.bandwidth_estimate.bandwidth_bps;
        let rtt = self.bandwidth_estimate.rtt_ms;
        let packet_loss = self.bandwidth_estimate.packet_loss;
        
        // Find all profiles that could work with current conditions
        let suitable_profiles: Vec<&QualityProfile> = self.config.profiles.iter()
            .filter(|p| {
                p.min_bandwidth_bps <= available_bw && 
                p.max_rtt_ms >= rtt &&
                p.max_packet_loss >= packet_loss
            })
            .collect();
            
        if suitable_profiles.is_empty() {
            // No suitable profiles, try to find the lowest quality
            if let Some(lowest) = self.config.profiles.iter()
                .min_by_key(|p| p.min_bandwidth_bps) {
                if lowest.level != self.current_quality {
                    self.quality_selected_at = now;
                    self.stats.quality_switches += 1;
                    self.stats.current_quality = lowest.level;
                    self.stats.last_switch_reason = "Fallback to lowest quality".to_string();
                    return Some(lowest.level);
                }
            }
            return None;
        }
        
        // Find the highest quality that can be sustained
        if let Some(best_profile) = suitable_profiles.iter()
            .max_by_key(|p| p.level as u8) {
            if best_profile.level != self.current_quality {
                // Check if the change is significant enough based on stability threshold
                let current_profile = self.config.profiles.iter()
                    .find(|p| p.level == self.current_quality)
                    .unwrap_or(best_profile);
                
                let bw_change_ratio = (best_profile.min_bandwidth_bps as f32 / current_profile.min_bandwidth_bps as f32) - 1.0;
                
                if bw_change_ratio.abs() >= self.config.stability_threshold {
                    self.quality_selected_at = now;
                    self.stats.quality_switches += 1;
                    self.stats.current_quality = best_profile.level;
                    
                    if best_profile.level > self.current_quality {
                        self.stats.last_switch_reason = "Increased quality due to good conditions".to_string();
                    } else {
                        self.stats.last_switch_reason = "Decreased quality due to limited bandwidth".to_string();
                    }
                    
                    return Some(best_profile.level);
                }
            }
        }
        
        // No quality change needed
        None
    }
    
    /// Get the current quality level
    pub fn current_quality(&self) -> QualityLevel {
        self.current_quality
    }
    
    /// Get the current media parameters based on quality
    pub fn current_params(&self) -> Option<&MediaParams> {
        self.config.profiles.iter()
            .find(|p| p.level == self.current_quality)
            .map(|p| &p.params)
    }
    
    /// Get statistics
    pub fn stats(&self) -> &QualitySelectorStats {
        &self.stats
    }
}

/// Helper to create quality profiles for common video resolutions
pub fn create_video_profiles() -> Vec<QualityProfile> {
    vec![
        // Low quality - 480p @ 500 kbps
        QualityProfile {
            level: QualityLevel::Low,
            min_bandwidth_bps: 400_000,
            ideal_bandwidth_bps: 800_000,
            max_rtt_ms: 500,
            max_packet_loss: 0.1,
            params: MediaParams {
                media_type: MediaType::Video,
                quality: QualityLevel::Low,
                bitrate: 500_000,
                framerate: Some(24.0),
                width: Some(640),
                height: Some(480),
                sample_rate: None,
                channels: None,
            },
        },
        // Medium quality - 720p @ 1.5 Mbps
        QualityProfile {
            level: QualityLevel::Medium,
            min_bandwidth_bps: 1_200_000,
            ideal_bandwidth_bps: 2_000_000,
            max_rtt_ms: 300,
            max_packet_loss: 0.05,
            params: MediaParams {
                media_type: MediaType::Video,
                quality: QualityLevel::Medium,
                bitrate: 1_500_000,
                framerate: Some(30.0),
                width: Some(1280),
                height: Some(720),
                sample_rate: None,
                channels: None,
            },
        },
        // High quality - 1080p @ 3 Mbps
        QualityProfile {
            level: QualityLevel::High,
            min_bandwidth_bps: 2_500_000,
            ideal_bandwidth_bps: 4_000_000,
            max_rtt_ms: 200,
            max_packet_loss: 0.02,
            params: MediaParams {
                media_type: MediaType::Video,
                quality: QualityLevel::High,
                bitrate: 3_000_000,
                framerate: Some(30.0),
                width: Some(1920),
                height: Some(1080),
                sample_rate: None,
                channels: None,
            },
        },
    ]
}

/// Helper to create quality profiles for audio
pub fn create_audio_profiles() -> Vec<QualityProfile> {
    vec![
        // Low quality - 32 kbps mono
        QualityProfile {
            level: QualityLevel::Low,
            min_bandwidth_bps: 24_000,
            ideal_bandwidth_bps: 40_000,
            max_rtt_ms: 500,
            max_packet_loss: 0.1,
            params: MediaParams {
                media_type: MediaType::Audio,
                quality: QualityLevel::Low,
                bitrate: 32_000,
                framerate: None,
                width: None,
                height: None,
                sample_rate: Some(16000), // 16 kHz
                channels: Some(1),       // Mono
            },
        },
        // Medium quality - 96 kbps stereo
        QualityProfile {
            level: QualityLevel::Medium,
            min_bandwidth_bps: 80_000,
            ideal_bandwidth_bps: 120_000,
            max_rtt_ms: 300,
            max_packet_loss: 0.05,
            params: MediaParams {
                media_type: MediaType::Audio,
                quality: QualityLevel::Medium,
                bitrate: 96_000,
                framerate: None,
                width: None,
                height: None,
                sample_rate: Some(44100), // 44.1 kHz
                channels: Some(2),       // Stereo
            },
        },
        // High quality - 192 kbps stereo
        QualityProfile {
            level: QualityLevel::High,
            min_bandwidth_bps: 160_000,
            ideal_bandwidth_bps: 240_000,
            max_rtt_ms: 200,
            max_packet_loss: 0.02,
            params: MediaParams {
                media_type: MediaType::Audio,
                quality: QualityLevel::High,
                bitrate: 192_000,
                framerate: None,
                width: None,
                height: None,
                sample_rate: Some(48000), // 48 kHz
                channels: Some(2),       // Stereo
            },
        },
    ]
} 