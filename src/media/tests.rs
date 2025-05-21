//! Tests for media components
//!
//! This module contains unit tests for the media pipeline components.

#[cfg(test)]
mod tests {
    use super::super::interface::{MediaParams, MediaType, QualityLevel, MediaFrameMetadata, MediaFrame};
    use super::super::buffer::{MediaBuffer, MediaBufferConfig};
    use super::super::quality::{BandwidthEstimate, QualitySelector, QualitySelectorConfig, QualityProfile};
    use super::super::stream::{StreamId, BufferedMediaSource, BufferedMediaSink};
    use std::time::{Duration, Instant};
    
    /// Helper to create a test media frame
    fn create_test_frame(sequence: u64, size: usize) -> MediaFrame {
        MediaFrame {
            data: vec![0; size],
            metadata: MediaFrameMetadata {
                timestamp: Duration::from_millis(sequence * 33), // 30fps
                is_keyframe: sequence % 30 == 0,                // Keyframe every 30 frames
                duration: Duration::from_millis(33),            // ~30fps
                sequence,
            },
        }
    }
    
    /// Helper to create test video parameters
    fn create_test_video_params() -> MediaParams {
        MediaParams {
            media_type: MediaType::Video,
            quality: QualityLevel::Medium,
            bitrate: 1_500_000,
            framerate: Some(30.0),
            width: Some(1280),
            height: Some(720),
            sample_rate: None,
            channels: None,
        }
    }
    
    /// Helper to create test audio parameters
    fn create_test_audio_params() -> MediaParams {
        MediaParams {
            media_type: MediaType::Audio,
            quality: QualityLevel::Medium,
            bitrate: 96_000,
            framerate: None,
            width: None,
            height: None,
            sample_rate: Some(44100),
            channels: Some(2),
        }
    }
    
    #[tokio::test]
    async fn test_media_buffer() {
        // Create a buffer
        let params = create_test_video_params();
        let config = MediaBufferConfig::default();
        let buffer = MediaBuffer::new(params, config);
        
        // Add some frames
        for i in 0..10 {
            let frame = create_test_frame(i, 1000);
            assert!(buffer.add_frame(frame).await.is_ok());
        }
        
        // Check buffer stats
        let stats = buffer.stats().await;
        assert_eq!(stats.frame_count, 10);
        assert_eq!(stats.frames_added, 10);
        assert_eq!(stats.bytes_added, 10 * 1000);
        
        // Get frames back
        for i in 0..10 {
            let frame = buffer.get_frame().await.unwrap();
            assert_eq!(frame.metadata.sequence, i);
        }
        
        // Buffer should be empty now
        assert!(buffer.get_frame().await.is_err());
    }
    
    #[tokio::test]
    async fn test_media_buffer_overflow() {
        // Create a small buffer
        let params = create_test_video_params();
        let mut config = MediaBufferConfig::default();
        config.max_frames = 5;
        let buffer = MediaBuffer::new(params, config);
        
        // Add more frames than the buffer can hold
        for i in 0..10 {
            let frame = create_test_frame(i, 1000);
            assert!(buffer.add_frame(frame).await.is_ok());
        }
        
        // Check buffer stats - should only have 5 frames
        let stats = buffer.stats().await;
        assert_eq!(stats.frame_count, 5);
        assert_eq!(stats.frames_added, 10);
        assert_eq!(stats.frames_dropped_overflow, 5);
        
        // The buffer should have frames 5-9
        for i in 5..10 {
            let frame = buffer.get_frame().await.unwrap();
            assert_eq!(frame.metadata.sequence, i);
        }
    }
    
    #[test]
    fn test_quality_selector() {
        // Create quality profiles
        let low_profile = QualityProfile {
            level: QualityLevel::Low,
            min_bandwidth_bps: 500_000,
            ideal_bandwidth_bps: 800_000,
            max_rtt_ms: 500,
            max_packet_loss: 0.1,
            params: create_test_video_params(),
        };
        
        let medium_profile = QualityProfile {
            level: QualityLevel::Medium,
            min_bandwidth_bps: 1_500_000,
            ideal_bandwidth_bps: 2_000_000,
            max_rtt_ms: 300,
            max_packet_loss: 0.05,
            params: create_test_video_params(),
        };
        
        let high_profile = QualityProfile {
            level: QualityLevel::High,
            min_bandwidth_bps: 3_000_000,
            ideal_bandwidth_bps: 5_000_000,
            max_rtt_ms: 200,
            max_packet_loss: 0.02,
            params: create_test_video_params(),
        };
        
        // Create a quality selector with the profiles
        let config = QualitySelectorConfig {
            evaluation_interval_ms: 100,
            stability_threshold: 0.2,
            adaptation_speed: 0.3,
            profiles: vec![low_profile, medium_profile, high_profile],
        };
        
        let mut selector = QualitySelector::new(config).unwrap();
        
        // Should start with the lowest quality
        assert_eq!(selector.current_quality(), QualityLevel::Low);
        
        // Update bandwidth to good conditions
        selector.update_bandwidth(BandwidthEstimate {
            bandwidth_bps: 4_000_000,
            rtt_ms: 100,
            packet_loss: 0.01,
            timestamp: Instant::now(),
        });
        
        // Should switch to high quality
        let new_quality = selector.evaluate();
        assert_eq!(new_quality, Some(QualityLevel::High));
        
        // Update bandwidth to poor conditions
        selector.update_bandwidth(BandwidthEstimate {
            bandwidth_bps: 300_000,
            rtt_ms: 600,
            packet_loss: 0.15,
            timestamp: Instant::now(),
        });
        
        // Should switch back to low quality
        let new_quality = selector.evaluate();
        assert_eq!(new_quality, Some(QualityLevel::Low));
    }
} 