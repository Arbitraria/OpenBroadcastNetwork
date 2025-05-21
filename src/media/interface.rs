//! Core interfaces for media processing
//!
//! This module defines the fundamental interfaces for working with media streams.

use std::fmt;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Metadata for media frames
#[derive(Debug, Clone)]
pub struct MediaFrameMetadata {
    /// Frame timestamp
    pub timestamp: Duration,
    /// Whether this is a keyframe (for video)
    pub is_keyframe: bool,
    /// Duration of the frame
    pub duration: Duration,
    /// Sequence number in the stream
    pub sequence: u64,
}

/// A single media frame
#[derive(Debug, Clone)]
pub struct MediaFrame {
    /// Raw frame data
    pub data: Vec<u8>,
    /// Metadata for the frame
    pub metadata: MediaFrameMetadata,
}

/// Media stream types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaType {
    /// Video stream
    Video,
    /// Audio stream
    Audio,
    /// Data stream (e.g., for subtitles)
    Data,
}

/// Media quality level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QualityLevel {
    /// Low quality
    Low,
    /// Medium quality
    Medium,
    /// High quality
    High,
}

/// Media stream parameters
#[derive(Debug, Clone)]
pub struct MediaParams {
    /// Type of media
    pub media_type: MediaType,
    /// Desired quality level
    pub quality: QualityLevel,
    /// Bitrate in bits per second
    pub bitrate: u32,
    /// Frame rate (for video)
    pub framerate: Option<f32>,
    /// Width in pixels (for video)
    pub width: Option<u32>,
    /// Height in pixels (for video)
    pub height: Option<u32>,
    /// Sample rate in Hz (for audio)
    pub sample_rate: Option<u32>,
    /// Number of channels (for audio)
    pub channels: Option<u8>,
}

/// Errors that can occur in the media pipeline
#[derive(Debug, thiserror::Error)]
pub enum MediaError {
    /// Invalid media format
    #[error("Invalid media format: {0}")]
    InvalidFormat(String),
    
    /// Encoding error
    #[error("Encoding error: {0}")]
    EncodingError(String),
    
    /// Decoding error
    #[error("Decoding error: {0}")]
    DecodingError(String),
    
    /// Buffer error
    #[error("Buffer error: {0}")]
    BufferError(String),
    
    /// Stream closed
    #[error("Stream closed")]
    StreamClosed,
    
    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Interface for a media stream
pub trait MediaStream {
    /// Get the media parameters
    fn params(&self) -> &MediaParams;
    
    /// Check if the stream is active
    fn is_active(&self) -> bool;
    
    /// Close the stream
    fn close(&mut self) -> Result<(), MediaError>;
}

/// Interface for a media source
pub trait MediaSource: MediaStream {
    /// Read the next frame from the source
    fn read_frame(&mut self) -> Pin<Box<dyn Future<Output = Result<MediaFrame, MediaError>> + Send>>;
    
    /// Get source statistics
    fn stats(&self) -> MediaSourceStats;
}

/// Interface for a media sink
pub trait MediaSink: MediaStream {
    /// Write a frame to the sink
    fn write_frame(&mut self, frame: MediaFrame) -> Pin<Box<dyn Future<Output = Result<(), MediaError>> + Send>>;
    
    /// Flush any buffered frames
    fn flush(&mut self) -> Pin<Box<dyn Future<Output = Result<(), MediaError>> + Send>>;
    
    /// Get sink statistics
    fn stats(&self) -> MediaSinkStats;
}

/// Statistics for a media source
#[derive(Debug, Clone, Default)]
pub struct MediaSourceStats {
    /// Number of frames read
    pub frames_read: u64,
    /// Number of bytes read
    pub bytes_read: u64,
    /// Average frame rate
    pub avg_framerate: f32,
    /// Average bitrate
    pub avg_bitrate: u32,
}

/// Statistics for a media sink
#[derive(Debug, Clone, Default)]
pub struct MediaSinkStats {
    /// Number of frames written
    pub frames_written: u64,
    /// Number of bytes written
    pub bytes_written: u64,
    /// Number of frames dropped
    pub frames_dropped: u64,
    /// Average processing time per frame
    pub avg_processing_time: Duration,
} 