//! Media codec support for the decentralized streaming CDN.
//!
//! This module provides functionality for encoding and decoding media streams
//! using pure Rust codec implementations for better integration and simpler deployment.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use openh264::formats::{YUVBuffer, YUVSource};
use tokio::sync::Mutex;

// Will use MediaError when implementing actual codec functionality

/// Error type for codec operations
#[derive(Debug)]
pub enum CodecError {
    /// OpenH264-related error
    OpenH264Error(String),
    /// Opus-related error
    OpusError(String),
    /// General codec error
    General(String),
    /// Initialization failed
    InitializationFailed(String),
    /// Encoding failed
    EncodingFailed(String),
    /// Decoding failed  
    DecodingFailed(String),
    /// Unsupported format
    UnsupportedFormat(String),
    /// Invalid parameters
    InvalidParameters(String),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::OpenH264Error(err) => write!(f, "OpenH264 error: {}", err),
            CodecError::OpusError(err) => write!(f, "Opus error: {}", err),
            CodecError::General(msg) => write!(f, "Codec error: {}", msg),
            CodecError::InitializationFailed(msg) => write!(f, "Initialization failed: {}", msg),
            CodecError::EncodingFailed(msg) => write!(f, "Encoding failed: {}", msg),
            CodecError::DecodingFailed(msg) => write!(f, "Decoding failed: {}", msg),
            CodecError::UnsupportedFormat(msg) => write!(f, "Unsupported format: {}", msg),
            CodecError::InvalidParameters(msg) => write!(f, "Invalid parameters: {}", msg),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<&str> for CodecError {
    fn from(err: &str) -> Self {
        CodecError::General(err.to_string())
    }
}

impl From<String> for CodecError {
    fn from(err: String) -> Self {
        CodecError::General(err)
    }
}

/// Convert YUV420P to RGB24 format
/// This is needed because OpenH264 0.4 only provides with_rgb constructor
fn yuv420p_to_rgb24(
    y_plane: &[u8],
    u_plane: &[u8],
    v_plane: &[u8],
    width: usize,
    height: usize,
) -> Result<Vec<u8>, CodecError> {
    let mut rgb_data = Vec::with_capacity(width * height * 3);

    for row in 0..height {
        for col in 0..width {
            let y_idx = row * width + col;
            let uv_row = row / 2;
            let uv_col = col / 2;
            let uv_idx = uv_row * (width / 2) + uv_col;

            if y_idx >= y_plane.len() || uv_idx >= u_plane.len() || uv_idx >= v_plane.len() {
                return Err(CodecError::InvalidParameters(
                    "YUV plane index out of bounds".to_string(),
                ));
            }

            let y = y_plane[y_idx] as i32;
            let u = u_plane[uv_idx] as i32 - 128;
            let v = v_plane[uv_idx] as i32 - 128;

            // YUV to RGB conversion using ITU-R BT.601 coefficients
            let r = y + (1.402 * v as f32) as i32;
            let g = y - (0.344 * u as f32) as i32 - (0.714 * v as f32) as i32;
            let b = y + (1.772 * u as f32) as i32;

            // Clamp values to 0-255 range
            let r = r.clamp(0, 255) as u8;
            let g = g.clamp(0, 255) as u8;
            let b = b.clamp(0, 255) as u8;

            rgb_data.push(r);
            rgb_data.push(g);
            rgb_data.push(b);
        }
    }

    Ok(rgb_data)
}

/// Trait for media codecs
#[async_trait]
pub trait Codec: Send + Sync + std::fmt::Debug + 'static {
    /// Get the name of the codec
    fn name(&self) -> &'static str;

    /// Get the MIME type of the codec
    fn mime_type(&self) -> &'static str;

    /// Encode a frame of media data
    async fn encode(&mut self, frame: &[u8]) -> Result<Vec<u8>, CodecError>;

    /// Decode a frame of media data
    async fn decode(&mut self, data: &[u8]) -> Result<Vec<u8>, CodecError>;
}

/// Opus audio codec implementation
#[derive(Debug)]
pub struct OpusCodec {
    encoder: Arc<Mutex<Option<OpusEncoder>>>,
    decoder: Arc<Mutex<Option<OpusDecoder>>>,
    sample_rate: u32,
    channels: u16,
}

/// Opus encoder state
struct OpusEncoder {
    encoder: opus::Encoder,
    frame_size: usize,
    frame_count: u64,
}

impl std::fmt::Debug for OpusEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpusEncoder")
            .field("frame_size", &self.frame_size)
            .field("frame_count", &self.frame_count)
            .finish()
    }
}

/// Opus decoder state
struct OpusDecoder {
    decoder: opus::Decoder,
    frame_count: u64,
}

impl std::fmt::Debug for OpusDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpusDecoder")
            .field("frame_count", &self.frame_count)
            .finish()
    }
}

impl Default for OpusCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl OpusCodec {
    /// Create a new Opus codec with default settings (48kHz, stereo)
    pub fn new() -> Self {
        Self::with_params(48000, 2)
    }

    /// Create a new Opus codec with specific parameters
    pub fn with_params(sample_rate: u32, channels: u16) -> Self {
        Self {
            encoder: Arc::new(Mutex::new(None)),
            decoder: Arc::new(Mutex::new(None)),
            sample_rate,
            channels,
        }
    }

    /// Initialize Opus encoder
    pub async fn init_encoder(&self) -> Result<(), CodecError> {
        let mut encoder_guard = self.encoder.lock().await;

        // Create Opus encoder with optimized settings for real-time streaming
        let encoder = opus::Encoder::new(
            self.sample_rate,
            match self.channels {
                1 => opus::Channels::Mono,
                2 => opus::Channels::Stereo,
                _ => {
                    return Err(CodecError::InvalidParameters(
                        "Only mono and stereo supported".to_string(),
                    ))
                }
            },
            opus::Application::Audio, // Optimized for general audio
        )
        .map_err(|e| CodecError::OpusError(format!("Failed to create encoder: {:?}", e)))?;

        // Calculate frame size (20ms at sample rate)
        let frame_size = (self.sample_rate / 50) as usize; // 20ms frames

        *encoder_guard = Some(OpusEncoder {
            encoder,
            frame_size,
            frame_count: 0,
        });

        Ok(())
    }

    /// Initialize Opus decoder
    pub async fn init_decoder(&self) -> Result<(), CodecError> {
        let mut decoder_guard = self.decoder.lock().await;

        // Create Opus decoder
        let decoder = opus::Decoder::new(
            self.sample_rate,
            match self.channels {
                1 => opus::Channels::Mono,
                2 => opus::Channels::Stereo,
                _ => {
                    return Err(CodecError::InvalidParameters(
                        "Only mono and stereo supported".to_string(),
                    ))
                }
            },
        )
        .map_err(|e| CodecError::OpusError(format!("Failed to create decoder: {:?}", e)))?;

        *decoder_guard = Some(OpusDecoder {
            decoder,
            frame_count: 0,
        });

        Ok(())
    }

    /// Get sample rate
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Get number of channels
    pub fn channels(&self) -> u16 {
        self.channels
    }
}

#[async_trait]
impl Codec for OpusCodec {
    fn name(&self) -> &'static str {
        "opus"
    }

    fn mime_type(&self) -> &'static str {
        "audio/opus"
    }

    async fn encode(&mut self, frame: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut encoder_guard = self.encoder.lock().await;

        let encoder = encoder_guard
            .as_mut()
            .ok_or_else(|| CodecError::EncodingFailed("Encoder not initialized".to_string()))?;

        // Convert bytes to i16 samples (assuming 16-bit PCM input)
        if frame.len() % 2 != 0 {
            return Err(CodecError::InvalidParameters(
                "Frame length must be even for 16-bit samples".to_string(),
            ));
        }

        let mut samples = Vec::with_capacity(frame.len() / 2);
        for chunk in frame.chunks_exact(2) {
            let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
            samples.push(sample);
        }

        // Check if we have the right number of samples for a frame
        let expected_samples = encoder.frame_size * self.channels as usize;
        if samples.len() != expected_samples {
            return Err(CodecError::InvalidParameters(format!(
                "Expected {} samples, got {}",
                expected_samples,
                samples.len()
            )));
        }

        // Encode the audio frame
        let mut output = vec![0u8; 4000]; // Max Opus packet size
        let encoded_size = encoder
            .encoder
            .encode(&samples, &mut output)
            .map_err(|e| CodecError::OpusError(format!("Encoding failed: {:?}", e)))?;

        output.truncate(encoded_size);
        encoder.frame_count += 1;
        Ok(output)
    }

    async fn decode(&mut self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut decoder_guard = self.decoder.lock().await;

        let decoder = decoder_guard
            .as_mut()
            .ok_or_else(|| CodecError::DecodingFailed("Decoder not initialized".to_string()))?;

        // Decode the Opus data
        let frame_size = (self.sample_rate / 50) as usize; // 20ms frames
        let mut output = vec![0i16; frame_size * self.channels as usize];

        let decoded_samples = decoder
            .decoder
            .decode(data, &mut output, false)
            .map_err(|e| CodecError::OpusError(format!("Decoding failed: {:?}", e)))?;

        // Convert i16 samples back to bytes
        let mut result = Vec::with_capacity(decoded_samples * 2);
        for sample in &output[..decoded_samples] {
            result.extend_from_slice(&sample.to_le_bytes());
        }

        decoder.frame_count += 1;
        Ok(result)
    }
}

/// Legacy audio codec for compatibility
#[derive(Debug, Default)]
pub struct AudioCodec {
    // Legacy implementation - use OpusCodec instead
}

#[async_trait]
impl Codec for AudioCodec {
    fn name(&self) -> &'static str {
        "audio"
    }

    fn mime_type(&self) -> &'static str {
        "audio/raw"
    }

    async fn encode(&mut self, _frame: &[u8]) -> Result<Vec<u8>, CodecError> {
        Err(CodecError::General("Use OpusCodec instead".to_string()))
    }

    async fn decode(&mut self, _data: &[u8]) -> Result<Vec<u8>, CodecError> {
        Err(CodecError::General("Use OpusCodec instead".to_string()))
    }
}

/// H.264 video codec implementation using OpenH264
#[derive(Debug)]
pub struct OpenH264Codec {
    encoder: Arc<Mutex<Option<OpenH264Encoder>>>,
    decoder: Arc<Mutex<Option<OpenH264Decoder>>>,
    width: u32,
    height: u32,
}

/// H.264 encoder state using OpenH264
struct OpenH264Encoder {
    encoder: openh264::encoder::Encoder,
    frame_count: u64,
}

impl std::fmt::Debug for OpenH264Encoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenH264Encoder")
            .field("frame_count", &self.frame_count)
            .finish()
    }
}

/// H.264 decoder state using OpenH264
struct OpenH264Decoder {
    decoder: openh264::decoder::Decoder,
    frame_count: u64,
}

impl std::fmt::Debug for OpenH264Decoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenH264Decoder")
            .field("frame_count", &self.frame_count)
            .finish()
    }
}

impl Default for OpenH264Codec {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenH264Codec {
    /// Create a new OpenH264 codec with default dimensions
    pub fn new() -> Self {
        Self::with_dimensions(640, 480)
    }

    /// Create a new OpenH264 codec with specific dimensions
    pub fn with_dimensions(width: u32, height: u32) -> Self {
        Self {
            encoder: Arc::new(Mutex::new(None)),
            decoder: Arc::new(Mutex::new(None)),
            width,
            height,
        }
    }

    /// Initialize H.264 encoder with OpenH264
    pub async fn init_encoder(&self) -> Result<(), CodecError> {
        let mut encoder_guard = self.encoder.lock().await;

        // Create encoder configuration optimized for real-time streaming
        let config = openh264::encoder::EncoderConfig::new(self.width, self.height)
            .set_bitrate_bps(2_000_000) // 2 Mbps - good balance for streaming
            .max_frame_rate(30.0) // 30 fps
            .enable_skip_frame(true) // Allow frame skipping for rate control
            .rate_control_mode(openh264::encoder::RateControlMode::Bitrate); // CBR for consistent streaming

        let encoder = openh264::encoder::Encoder::with_config(config)
            .map_err(|e| CodecError::OpenH264Error(format!("Failed to create encoder: {:?}", e)))?;

        *encoder_guard = Some(OpenH264Encoder {
            encoder,
            frame_count: 0,
        });

        Ok(())
    }

    /// Initialize H.264 decoder with OpenH264
    pub async fn init_decoder(&self) -> Result<(), CodecError> {
        let mut decoder_guard = self.decoder.lock().await;

        // Create decoder with default configuration
        let decoder = openh264::decoder::Decoder::new()
            .map_err(|e| CodecError::OpenH264Error(format!("Failed to create decoder: {:?}", e)))?;

        *decoder_guard = Some(OpenH264Decoder {
            decoder,
            frame_count: 0,
        });

        Ok(())
    }

    /// Get the configured width
    pub fn width(&self) -> u32 {
        self.width
    }

    /// Get the configured height
    pub fn height(&self) -> u32 {
        self.height
    }
}

#[async_trait]
impl Codec for OpenH264Codec {
    fn name(&self) -> &'static str {
        "openh264"
    }

    fn mime_type(&self) -> &'static str {
        "video/h264"
    }

    async fn encode(&mut self, frame: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut encoder_guard = self.encoder.lock().await;

        let encoder = encoder_guard
            .as_mut()
            .ok_or_else(|| CodecError::EncodingFailed("Encoder not initialized".to_string()))?;

        // Expect frame data to be in YUV420P format
        // The input frame should be width * height * 3/2 bytes (Y + U/2 + V/2)
        let expected_size = (self.width * self.height * 3 / 2) as usize;
        if frame.len() != expected_size {
            return Err(CodecError::InvalidParameters(format!(
                "Expected frame size {} bytes, got {}",
                expected_size,
                frame.len()
            )));
        }

        // Create YUV buffer from raw YUV420P data
        let y_size = (self.width * self.height) as usize;
        let uv_size = y_size / 4;

        // Extract Y, U, V planes from the input frame
        let y_plane = &frame[0..y_size];
        let u_plane = &frame[y_size..y_size + uv_size];
        let v_plane = &frame[y_size + uv_size..y_size + uv_size * 2];

        // OpenH264 0.4 limitation: No direct YUV420P constructor available
        // We need to convert YUV420P to RGB first, then use with_rgb constructor

        // Convert YUV420P to RGB24
        let rgb_data = yuv420p_to_rgb24(
            y_plane,
            u_plane,
            v_plane,
            self.width as usize,
            self.height as usize,
        )?;

        // Create YUV buffer using RGB constructor
        let yuv_buffer = YUVBuffer::with_rgb(self.width as usize, self.height as usize, &rgb_data);

        // Encode the frame
        let bitstream = encoder
            .encoder
            .encode(&yuv_buffer)
            .map_err(|e| CodecError::OpenH264Error(format!("Encoding failed: {:?}", e)))?;

        encoder.frame_count += 1;
        // Convert EncodedBitStream to Vec<u8>
        Ok(bitstream.to_vec())
    }

    async fn decode(&mut self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut decoder_guard = self.decoder.lock().await;

        let decoder = decoder_guard
            .as_mut()
            .ok_or_else(|| CodecError::DecodingFailed("Decoder not initialized".to_string()))?;

        // Decode the H.264 data
        match decoder.decoder.decode(data) {
            Ok(Some(decoded_yuv)) => {
                // Convert decoded YUV back to raw YUV420P format
                let _width = decoded_yuv.width();
                let _height = decoded_yuv.height();

                // Get YUV plane data using YUVSource trait methods
                let y_plane = decoded_yuv.y();
                let u_plane = decoded_yuv.u();
                let v_plane = decoded_yuv.v();

                // For stride information, we need to determine if stride methods exist
                // OpenH264 0.4 may return (&[u8], usize) tuples for stride info
                // Let's try to get stride information if available

                // Reconstruct continuous YUV420P data
                let mut result = Vec::new();

                // Add Y plane (full resolution) - just copy the data directly
                result.extend_from_slice(y_plane);

                // Add U plane (quarter resolution)
                result.extend_from_slice(u_plane);

                // Add V plane (quarter resolution)
                result.extend_from_slice(v_plane);

                decoder.frame_count += 1;
                Ok(result)
            }
            Ok(None) => {
                // No frame ready yet (might need more data)
                Ok(Vec::new())
            }
            Err(e) => Err(CodecError::OpenH264Error(format!(
                "Decoding failed: {:?}",
                e
            ))),
        }
    }
}

/// Legacy video codec for compatibility
#[derive(Debug, Default)]
pub struct VideoCodec {
    // Legacy implementation - use H264Codec instead
}

#[async_trait]
impl Codec for VideoCodec {
    fn name(&self) -> &'static str {
        "video"
    }

    fn mime_type(&self) -> &'static str {
        "video/raw"
    }

    async fn encode(&mut self, _frame: &[u8]) -> Result<Vec<u8>, CodecError> {
        Err(CodecError::General("Use H264Codec instead".to_string()))
    }

    async fn decode(&mut self, _data: &[u8]) -> Result<Vec<u8>, CodecError> {
        Err(CodecError::General("Use H264Codec instead".to_string()))
    }
}

/// Registry for managing available codecs
#[derive(Debug)]
pub struct CodecRegistry {
    codecs: HashMap<String, Arc<dyn Codec>>,
}

impl Default for CodecRegistry {
    fn default() -> Self {
        let mut registry = CodecRegistry {
            codecs: HashMap::new(),
        };

        // Register pure Rust codecs (preferred)
        registry.register(Arc::new(OpenH264Codec::new()));
        registry.register(Arc::new(OpusCodec::new()));

        // Register legacy codecs for compatibility
        registry.register(Arc::new(AudioCodec {}));
        registry.register(Arc::new(VideoCodec {}));

        registry
    }
}

impl CodecRegistry {
    /// Create a new codec registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new codec
    pub fn register(&mut self, codec: Arc<dyn Codec>) {
        self.codecs.insert(codec.name().to_string(), codec);
    }

    /// Get a codec by name
    pub fn get(&self, name: &str) -> Option<Arc<dyn Codec>> {
        self.codecs.get(name).cloned()
    }

    /// List all registered codecs
    pub fn list(&self) -> Vec<String> {
        self.codecs.keys().cloned().collect()
    }
}
