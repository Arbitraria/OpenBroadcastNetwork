//! Media streaming interfaces and types.
//!
//! This module defines the core traits and types for media streaming in the
//! decentralized streaming CDN.

use std::fmt;
use std::time::Duration;

/// Error type for media operations
#[derive(Debug)]
pub struct MediaError {
    message: String,
}

impl fmt::Display for MediaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Media error: {}", self.message)
    }
}

impl std::error::Error for MediaError {}

impl From<&str> for MediaError {
    fn from(err: &str) -> Self {
        MediaError {
            message: err.to_string(),
        }
    }
}

impl From<String> for MediaError {
    fn from(err: String) -> Self {
        MediaError { message: err }
    }
}

/// Represents a media format (e.g., HLS, DASH, WebRTC)
#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Default)]
pub enum MediaFormat {
    /// HTTP Live Streaming
    Hls,
    /// Dynamic Adaptive Streaming over HTTP
    Dash,
    /// WebRTC
    WebRtc,
    /// Raw media data
    #[default]
    Raw,
    /// Custom format
    Custom(String),
}


/// Represents a media stream with metadata
#[derive(Debug, Clone)]
pub struct MediaStream {
    /// Unique identifier for the stream
    pub id: String,
    /// Format of the media stream
    pub format: MediaFormat,
    /// Duration of the stream, if known
    pub duration: Option<Duration>,
    /// Bitrate in bits per second, if known
    pub bitrate: Option<u64>,
    /// Codec information, if available
    pub codec: Option<String>,
}

/// Trait for media sources
#[async_trait::async_trait]
pub trait MediaSource: Send + Sync + std::fmt::Debug + 'static {
    /// Get the next chunk of media data
    async fn next_chunk(&mut self) -> Result<Vec<u8>, MediaError>;

    /// Get information about the media stream
    fn stream_info(&self) -> &dyn std::fmt::Debug;

    /// Seek to a specific position in the stream
    async fn seek(&mut self, position: Duration) -> Result<(), MediaError>;
}

/// Trait for media sinks
#[async_trait::async_trait]
pub trait MediaSink: Send + Sync + std::fmt::Debug + 'static {
    /// Write a chunk of media data to the sink
    async fn write_chunk(&mut self, data: &[u8]) -> Result<(), MediaError>;

    /// Flush any buffered data
    async fn flush(&mut self) -> Result<(), MediaError>;
}

/// Builder for creating media sources
#[derive(Debug)]
pub struct MediaSourceBuilder {
    stream: MediaStream,
}

impl MediaSourceBuilder {
    /// Create a new media source builder
    pub fn new(id: impl Into<String>, format: MediaFormat) -> Self {
        MediaSourceBuilder {
            stream: MediaStream {
                id: id.into(),
                format,
                duration: None,
                bitrate: None,
                codec: None,
            },
        }
    }

    /// Set the duration of the media stream
    pub fn duration(mut self, duration: Duration) -> Self {
        self.stream.duration = Some(duration);
        self
    }

    /// Set the bitrate of the media stream
    pub fn bitrate(mut self, bitrate: u64) -> Self {
        self.stream.bitrate = Some(bitrate);
        self
    }

    /// Set the codec of the media stream
    pub fn codec(mut self, codec: impl Into<String>) -> Self {
        self.stream.codec = Some(codec.into());
        self
    }

    /// Build the media source
    pub fn build(self) -> MediaStream {
        self.stream
    }
}
