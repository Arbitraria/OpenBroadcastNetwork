//! Streaming demo application showcasing H.264 + Opus encoding over P2P network
//!
//! This demo creates a simple video stream using our OpenH264 and Opus codecs,
//! then distributes the chunks over the libp2p network using GossipSub.

use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, sleep};
use tracing::{debug, error, info};

use OpenBroadcastNetwork_core::media::{
    ChunkBuilder, MediaChunk, OpenH264Codec, OpusCodec, StreamId, StreamMetadata,
};
use OpenBroadcastNetwork_core::overlay::interface::OverlayError;
use OpenBroadcastNetwork_core::overlay::libp2p::impl_core::Libp2pOverlay;
use OpenBroadcastNetwork_core::pubsub::Topic;

/// Streaming demo configuration
#[derive(Debug, Clone)]
pub struct StreamingDemoConfig {
    /// Stream title
    pub title: String,
    /// Video resolution width
    pub video_width: u32,
    /// Video resolution height  
    pub video_height: u32,
    /// Video framerate (fps)
    pub video_fps: u32,
    /// Audio sample rate (Hz)
    pub audio_sample_rate: u32,
    /// Audio channels
    pub audio_channels: u16,
    /// Duration to run demo (seconds)
    pub duration_seconds: u64,
}

impl Default for StreamingDemoConfig {
    fn default() -> Self {
        Self {
            title: "OpenBroadcastNetwork Demo Stream".to_string(),
            video_width: 640,
            video_height: 480,
            video_fps: 30,
            audio_sample_rate: 48000,
            audio_channels: 2,
            duration_seconds: 60, // 1 minute demo
        }
    }
}

/// Simple streaming demo that generates synthetic content
pub struct StreamingDemo {
    config: StreamingDemoConfig,
    stream_id: StreamId,
    video_codec: Arc<OpenH264Codec>,
    audio_codec: Arc<OpusCodec>,
    chunk_builder: ChunkBuilder,
    overlay: Arc<Libp2pOverlay>,
}

impl StreamingDemo {
    /// Create a new streaming demo
    pub async fn new(
        config: StreamingDemoConfig,
        overlay: Arc<Libp2pOverlay>,
    ) -> Result<Self, OverlayError> {
        let stream_id = StreamId::generate();

        // Create codecs with demo configuration
        let video_codec = Arc::new(OpenH264Codec::with_dimensions(
            config.video_width,
            config.video_height,
        ));

        let audio_codec = Arc::new(OpusCodec::with_params(
            config.audio_sample_rate,
            config.audio_channels,
        ));

        // Initialize codecs
        video_codec
            .init_encoder()
            .await
            .map_err(|e| OverlayError::General(format!("Video codec init failed: {}", e)))?;

        audio_codec
            .init_encoder()
            .await
            .map_err(|e| OverlayError::General(format!("Audio codec init failed: {}", e)))?;

        let chunk_builder = ChunkBuilder::new(stream_id.clone());

        info!("Created streaming demo for stream: {}", stream_id);

        Ok(Self {
            config,
            stream_id,
            video_codec,
            audio_codec,
            chunk_builder,
            overlay,
        })
    }

    /// Start the streaming demo
    pub async fn start(&mut self) -> Result<(), OverlayError> {
        info!("Starting streaming demo: '{}'", self.config.title);

        // Create stream metadata and send it first
        let metadata = StreamMetadata::new_h264_opus(
            self.config.title.clone(),
            self.config.video_width,
            self.config.video_height,
            self.config.video_fps,
            self.config.audio_sample_rate,
            self.config.audio_channels,
        );

        let metadata_chunk = self.chunk_builder.metadata_chunk(metadata).map_err(|e| {
            OverlayError::General(format!("Failed to create metadata chunk: {}", e))
        })?;

        self.send_chunk(metadata_chunk).await?;

        // Create streaming topic
        let stream_topic = Topic::stream_topic(&self.stream_id.as_str());

        // Start video and audio generation loops
        let _video_handle = self.start_video_generation(stream_topic.clone());
        let _audio_handle = self.start_audio_generation(stream_topic.clone());

        // Wait for demo duration
        sleep(Duration::from_secs(self.config.duration_seconds)).await;

        info!("Streaming demo completed");
        Ok(())
    }

    /// Start video frame generation and encoding
    fn start_video_generation(&mut self, topic: Topic) -> tokio::task::JoinHandle<()> {
        let video_codec = Arc::clone(&self.video_codec);
        let frame_duration_ms = 1000 / self.config.video_fps as u64;
        let mut frame_interval = interval(Duration::from_millis(frame_duration_ms));
        let stream_id = self.stream_id.clone();
        let overlay = Arc::clone(&self.overlay);
        let width = self.config.video_width;
        let height = self.config.video_height;

        tokio::spawn(async move {
            let mut frame_count = 0u64;

            loop {
                frame_interval.tick().await;

                // Generate a synthetic video frame (colored pattern)
                let frame_data = generate_test_video_frame(width, height, frame_count);

                // Encode the frame (this is a placeholder - real encoding would need proper setup)
                // For now, we'll create a dummy encoded frame
                let encoded_frame = format!("h264_frame_{}", frame_count).into_bytes();

                // Create video chunk
                let chunk = MediaChunk::new_video(
                    stream_id.clone(),
                    frame_count,
                    encoded_frame,
                    frame_duration_ms,
                    frame_count % 30 == 0, // Keyframe every second
                );

                // Send chunk over P2P network
                if let Err(e) = send_chunk_to_network(&overlay, &topic, chunk).await {
                    error!("Failed to send video chunk: {}", e);
                    break;
                }

                frame_count += 1;

                // Stop after demo duration
                if frame_count >= 30 * 60 {
                    // 1 minute at 30fps
                    break;
                }
            }

            info!("Video generation completed");
        })
    }

    /// Start audio generation and encoding
    fn start_audio_generation(&mut self, topic: Topic) -> tokio::task::JoinHandle<()> {
        let audio_codec = Arc::clone(&self.audio_codec);
        let sample_rate = self.config.audio_sample_rate;
        let channels = self.config.audio_channels;
        let stream_id = self.stream_id.clone();
        let overlay = Arc::clone(&self.overlay);

        // Opus typically uses 20ms frames
        let frame_duration_ms = 20;
        let mut audio_interval = interval(Duration::from_millis(frame_duration_ms));

        tokio::spawn(async move {
            let mut audio_sequence = 0u64;

            loop {
                audio_interval.tick().await;

                // Generate synthetic audio (sine wave)
                let audio_data = generate_test_audio_frame(
                    sample_rate,
                    channels,
                    frame_duration_ms,
                    audio_sequence,
                );

                // Encode the audio (placeholder - real encoding would need proper setup)
                let encoded_audio = format!("opus_frame_{}", audio_sequence).into_bytes();

                // Create audio chunk
                let chunk = MediaChunk::new_audio(
                    stream_id.clone(),
                    audio_sequence,
                    encoded_audio,
                    frame_duration_ms,
                );

                // Send chunk over P2P network
                if let Err(e) = send_chunk_to_network(&overlay, &topic, chunk).await {
                    error!("Failed to send audio chunk: {}", e);
                    break;
                }

                audio_sequence += 1;

                // Stop after demo duration (50 chunks per second * 60 seconds)
                if audio_sequence >= 50 * 60 {
                    break;
                }
            }

            info!("Audio generation completed");
        })
    }

    /// Send a media chunk over the P2P network
    async fn send_chunk(&self, chunk: MediaChunk) -> Result<(), OverlayError> {
        let topic = Topic::stream_topic(&self.stream_id.as_str());
        send_chunk_to_network(&self.overlay, &topic, chunk).await
    }
}

/// Send a chunk to the P2P network via GossipSub
async fn send_chunk_to_network(
    _overlay: &Libp2pOverlay,
    topic: &Topic,
    chunk: MediaChunk,
) -> Result<(), OverlayError> {
    // Serialize chunk for transmission
    let chunk_data = chunk
        .to_bytes()
        .map_err(|e| OverlayError::General(format!("Failed to serialize chunk: {}", e)))?;

    debug!(
        "Sending {} chunk {} ({} bytes)",
        match chunk.chunk_type {
            OpenBroadcastNetwork_core::media::ChunkType::Video => "video",
            OpenBroadcastNetwork_core::media::ChunkType::Audio => "audio",
            OpenBroadcastNetwork_core::media::ChunkType::Metadata => "metadata",
        },
        chunk.sequence,
        chunk_data.len()
    );

    // For now, just simulate publishing to the topic
    // TODO: Implement proper GossipSub integration when the overlay API is complete
    info!(
        "Would publish {} bytes to topic: {}",
        chunk_data.len(),
        topic.id()
    );

    // In a real implementation, this would:
    // 1. Subscribe to the topic if not already subscribed
    // 2. Publish the chunk data to all subscribers
    // 3. Handle any network errors

    Ok(())
}

/// Generate a test video frame with a colored pattern
fn generate_test_video_frame(width: u32, height: u32, frame_number: u64) -> Vec<u8> {
    // Generate YUV420P test pattern
    let y_size = (width * height) as usize;
    let uv_size = y_size / 4;
    let total_size = y_size + uv_size * 2;

    let mut frame = vec![0u8; total_size];

    // Create a simple animated pattern
    let phase = (frame_number % 255) as u8;

    // Y plane (luminance) - create moving diagonal stripes
    for y in 0..height {
        for x in 0..width {
            let idx = (y * width + x) as usize;
            frame[idx] = ((x + y + frame_number as u32) % 255) as u8;
        }
    }

    // U plane (blue chroma) - static pattern
    for i in 0..uv_size {
        frame[y_size + i] = 128 + (phase / 2);
    }

    // V plane (red chroma) - static pattern
    for i in 0..uv_size {
        frame[y_size + uv_size + i] = 128 - (phase / 2);
    }

    frame
}

/// Generate test audio data (sine wave)
fn generate_test_audio_frame(
    sample_rate: u32,
    channels: u16,
    duration_ms: u64,
    sequence: u64,
) -> Vec<u8> {
    let samples_per_frame = (sample_rate as u64 * duration_ms / 1000) as usize;
    let total_samples = samples_per_frame * channels as usize;

    let mut audio_data = Vec::with_capacity(total_samples * 2); // 16-bit samples

    // Generate sine wave at 440 Hz (A note)
    let frequency = 440.0;
    let phase_offset =
        (sequence as f64 * duration_ms as f64 / 1000.0) * frequency * 2.0 * std::f64::consts::PI;

    for i in 0..samples_per_frame {
        let t = i as f64 / sample_rate as f64;
        let sample_value = (0.3
            * ((t * frequency * 2.0 * std::f64::consts::PI) + phase_offset).sin()
            * i16::MAX as f64) as i16;

        // Add sample for each channel
        for _ in 0..channels {
            audio_data.extend_from_slice(&sample_value.to_le_bytes());
        }
    }

    audio_data
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_test_video_frame() {
        let frame = generate_test_video_frame(640, 480, 0);
        let expected_size = 640 * 480 * 3 / 2; // YUV420P format
        assert_eq!(frame.len(), expected_size);
    }

    #[test]
    fn test_generate_test_audio_frame() {
        let audio = generate_test_audio_frame(48000, 2, 20, 0); // 20ms stereo
        let expected_samples = 48000 * 20 / 1000 * 2; // samples * channels
        let expected_bytes = expected_samples * 2; // 16-bit samples
        assert_eq!(audio.len(), expected_bytes);
    }

    #[test]
    fn test_streaming_demo_config() {
        let config = StreamingDemoConfig::default();
        assert_eq!(config.video_width, 640);
        assert_eq!(config.video_height, 480);
        assert_eq!(config.video_fps, 30);
        assert_eq!(config.audio_sample_rate, 48000);
        assert_eq!(config.audio_channels, 2);
    }
}
