//! Media streaming functionality for the decentralized streaming CDN.
//!
//! This module provides a comprehensive set of tools for handling media streaming,
//! including sources, sinks, codecs, and buffer management. It's designed to be
//! flexible and extensible, supporting various media formats and protocols.

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]

/// Media buffer implementation for caching and managing streaming data chunks
pub mod buffer;
/// Encoding and decoding functionality for various media formats
pub mod codec;
/// Core interfaces and traits for the media pipeline components
pub mod interface;
/// Media processing pipeline for transforming and forwarding stream data
pub mod pipeline;
/// Quality management and adaptive bitrate control
pub mod quality;
/// Output destinations for processed media streams
pub mod sink;
/// Input sources for media content
pub mod source;
/// Stream management and metadata handling
pub mod stream;

#[cfg(test)]
mod tests;

// Re-export main types for easy access
pub use buffer::{BufferConfig, BufferError, MediaBuffer};
pub use codec::{AudioCodec, Codec, CodecError, CodecRegistry, VideoCodec};
pub use interface::{
    MediaError, MediaFormat, MediaSink, MediaSource, MediaStream,
};
pub use pipeline::{MediaPipeline, PipelineStage, PassThroughStage};
pub use quality::{BandwidthMonitor, QualityConfig, QualityLevel, QualityManager};
pub use sink::{FileSink, MemorySink, NullSink};
pub use source::{FileSource, MemorySource, NetworkSource};
pub use stream::MediaStreamImpl;

/// Convenience type alias for Result<T, MediaError>
pub type Result<T> = std::result::Result<T, MediaError>;