//! Codec interfaces for media encoding and decoding
//!
//! This module defines interfaces for encoding and decoding media streams.

use crate::media::interface::{MediaFrame, MediaError, MediaParams};
use std::future::Future;
use std::pin::Pin;

/// Video codec types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoCodecType {
    /// H.264 codec
    H264,
    /// H.265 codec (HEVC)
    H265,
    /// VP8 codec
    VP8,
    /// VP9 codec
    VP9,
    /// AV1 codec
    AV1,
}

/// Audio codec types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioCodecType {
    /// Opus codec
    Opus,
    /// AAC codec
    AAC,
    /// MP3 codec
    MP3,
    /// PCM codec
    PCM,
}

/// Common interface for codecs
pub trait Codec {
    /// Get the codec name
    fn name(&self) -> &str;
    
    /// Get the media parameters
    fn params(&self) -> &MediaParams;
    
    /// Reset the codec state
    fn reset(&mut self) -> Result<(), MediaError>;
}

/// Interface for media encoders
pub trait Encoder: Codec {
    /// Encode a raw frame
    fn encode(&mut self, raw_frame: &[u8]) -> Pin<Box<dyn Future<Output = Result<MediaFrame, MediaError>> + Send>>;
    
    /// Flush the encoder
    fn flush(&mut self) -> Pin<Box<dyn Future<Output = Result<Vec<MediaFrame>, MediaError>> + Send>>;
    
    /// Get encoder statistics
    fn stats(&self) -> EncoderStats;
}

/// Interface for media decoders
pub trait Decoder: Codec {
    /// Decode a compressed frame
    fn decode(&mut self, frame: &MediaFrame) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, MediaError>> + Send>>;
    
    /// Get decoder statistics
    fn stats(&self) -> DecoderStats;
}

/// Statistics for an encoder
#[derive(Debug, Clone, Default)]
pub struct EncoderStats {
    /// Number of frames encoded
    pub frames_encoded: u64,
    /// Number of bytes input
    pub bytes_in: u64,
    /// Number of bytes output
    pub bytes_out: u64,
    /// Compression ratio (bytes_out / bytes_in)
    pub compression_ratio: f32,
    /// Average encoding time per frame
    pub avg_encoding_time_us: u64,
}

/// Statistics for a decoder
#[derive(Debug, Clone, Default)]
pub struct DecoderStats {
    /// Number of frames decoded
    pub frames_decoded: u64,
    /// Number of bytes input
    pub bytes_in: u64,
    /// Number of bytes output
    pub bytes_out: u64,
    /// Number of frames with errors
    pub error_frames: u64,
    /// Average decoding time per frame
    pub avg_decoding_time_us: u64,
}

/// Interface for video codecs
pub trait VideoCodec: Codec {
    /// Get the video codec type
    fn codec_type(&self) -> VideoCodecType;
    
    /// Create a keyframe at the next opportunity
    fn force_keyframe(&mut self);
}

/// Interface for audio codecs
pub trait AudioCodec: Codec {
    /// Get the audio codec type
    fn codec_type(&self) -> AudioCodecType;
    
    /// Get the sample rate
    fn sample_rate(&self) -> u32;
    
    /// Get the number of channels
    fn channels(&self) -> u8;
} 