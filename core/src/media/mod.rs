//! Media streaming functionality for the decentralized streaming CDN.
//!
//! This module provides tools for handling media streaming, including
//! sources, sinks, codecs, and buffer management.
//!
//! # Unified Types
//!
//! The core media types are defined in the `segment` module:
//! - [`StreamSegment`] - The unified internal representation for media segments
//! - [`segment::StreamId`] - Unified stream identifier with zero-copy cloning
//! - [`MediaType`] - Type of media content (video, audio, metadata, initialization)
//!
//! For P2P transmission, use the wire format:
//! - [`WireSegment`] - Compact binary format for network transmission
//! - [`WireSegmentBatch`] - Batch multiple segments for efficiency

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]

// ── Platform-independent modules ──

/// Media buffer implementation for caching and managing streaming data chunks
pub mod buffer;
/// Proper fragmented MP4 converter for MSE compatibility
pub mod fmp4_converter;
/// MP4 fragment parsing (read-only operations)
#[cfg(not(target_arch = "wasm32"))]
pub mod fragment_parser;
/// MP4 fragment writing and generation
#[cfg(not(target_arch = "wasm32"))]
pub mod fragment_writer;
/// Core interfaces and traits for the media pipeline components
pub mod interface;
/// Quality management and adaptive bitrate control
pub mod quality;
/// Unified media segment types (StreamSegment, StreamId, MediaType)
pub mod segment;
/// Output destinations for processed media streams
pub mod sink;
/// Input sources for media content
pub mod source;
/// Stream management and metadata handling
pub mod stream;
/// Compact wire format for P2P segment transmission
pub mod wire_format;

// ── Native-only modules (require native codecs / tokio / libp2p) ──

/// Encoding and decoding functionality for various media formats
#[cfg(not(target_arch = "wasm32"))]
pub mod codec;
/// FFmpeg-based video reader for proper fMP4 generation
#[cfg(feature = "ffmpeg")]
pub mod ffmpeg_reader;
/// MP4 parser and MSE segment generator for browser streaming
#[cfg(not(target_arch = "wasm32"))]
pub mod mp4_parser;
/// P2P bridge for connecting overlay network to local subscribers
#[cfg(not(target_arch = "wasm32"))]
pub mod p2p_bridge;
/// Media processing pipeline for transforming and forwarding stream data
#[cfg(not(target_arch = "wasm32"))]
pub mod pipeline;
/// Stream publisher for P2P media distribution
#[cfg(not(target_arch = "wasm32"))]
pub mod publisher;
/// Video file reading and MP4 parsing for real media content
#[cfg(not(target_arch = "wasm32"))]
pub mod video_reader;

#[cfg(test)]
mod tests;

// ── Platform-independent re-exports ──

pub use buffer::{BufferConfig, BufferError, MediaBuffer};
pub use fmp4_converter::{FragmentedMp4Converter, FrameData};
#[cfg(not(target_arch = "wasm32"))]
pub use fragment_parser::FragmentParser;
#[cfg(not(target_arch = "wasm32"))]
pub use fragment_writer::FragmentWriter;
pub use interface::{MediaError, MediaFormat, MediaSink, MediaSource, MediaStream};
pub use quality::{BandwidthMonitor, QualityConfig, QualityLevel, QualityManager};
pub use segment::{MediaType, SegmentBuilder, StreamId, StreamMetadata, StreamSegment};
pub use sink::NullSink;
#[cfg(not(target_arch = "wasm32"))]
pub use sink::{FileSink, MemorySink};
#[cfg(not(target_arch = "wasm32"))]
pub use source::{FileSource, MemorySource, NetworkSource};
#[cfg(not(target_arch = "wasm32"))]
pub use stream::MediaStreamImpl;
pub use wire_format::{ToWireFormat, WireSegment, WireSegmentBatch, WIRE_FORMAT_VERSION};

// ── Native-only re-exports ──

#[cfg(not(target_arch = "wasm32"))]
pub use codec::{
    AudioCodec, Codec, CodecError, CodecRegistry, OpenH264Codec, OpusCodec, VideoCodec,
};
#[cfg(feature = "ffmpeg")]
pub use ffmpeg_reader::{
    FFmpegAudioTrack, FFmpegReaderError, FFmpegSample, FFmpegVideoReader, FFmpegVideoTrack,
};
#[cfg(not(target_arch = "wasm32"))]
pub use mp4_parser::{BoxHeader, Mp4Parser, Mp4Track, SampleEntry, SampleTable};
#[cfg(not(target_arch = "wasm32"))]
pub use p2p_bridge::{P2PBridge, P2PBridgeConfig};
#[cfg(not(target_arch = "wasm32"))]
pub use pipeline::{MediaPipeline, PassThroughStage, PipelineStage};
#[cfg(not(target_arch = "wasm32"))]
pub use publisher::{PublisherError, StreamPublisher};
#[cfg(not(target_arch = "wasm32"))]
pub use video_reader::{AudioTrackInfo, VideoReader, VideoReaderError, VideoTrackInfo};

/// Convenience type alias for Result<T, MediaError>
pub type Result<T> = std::result::Result<T, MediaError>;
