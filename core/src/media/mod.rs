//! Media streaming functionality for the decentralized streaming CDN.
//!
//! This module provides a comprehensive set of tools for handling media streaming,
//! including sources, sinks, codecs, and buffer management. It's designed to be
//! flexible and extensible, supporting various media formats and protocols.
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

/// Media buffer implementation for caching and managing streaming data chunks
pub mod buffer;
/// Encoding and decoding functionality for various media formats
pub mod codec;
/// FFmpeg-based video reader for proper fMP4 generation (requires `ffmpeg` feature)
#[cfg(feature = "ffmpeg")]
pub mod ffmpeg_reader;
/// Proper fragmented MP4 converter for MSE compatibility
pub mod fmp4_converter;
/// MP4 fragment parsing (read-only operations)
pub mod fragment_parser;
/// MP4 fragment writing and generation
pub mod fragment_writer;
/// Core interfaces and traits for the media pipeline components
pub mod interface;
/// MP4 parser and MSE segment generator for browser streaming
pub mod mp4_parser;
/// P2P bridge for connecting overlay network to local subscribers
pub mod p2p_bridge;
/// Media processing pipeline for transforming and forwarding stream data
pub mod pipeline;
/// Stream publisher for P2P media distribution
pub mod publisher;
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
/// Video file reading and MP4 parsing for real media content
pub mod video_reader;
/// Compact wire format for P2P segment transmission
pub mod wire_format;

#[cfg(test)]
mod tests;

// Re-export main types for easy access
pub use buffer::{BufferConfig, BufferError, MediaBuffer};
pub use codec::{
    AudioCodec, Codec, CodecError, CodecRegistry, OpenH264Codec, OpusCodec, VideoCodec,
};
#[cfg(feature = "ffmpeg")]
pub use ffmpeg_reader::{
    FFmpegAudioTrack, FFmpegReaderError, FFmpegSample, FFmpegVideoReader, FFmpegVideoTrack,
};
pub use fmp4_converter::{FragmentedMp4Converter, FrameData};
pub use fragment_parser::FragmentParser;
pub use fragment_writer::FragmentWriter;
pub use interface::{MediaError, MediaFormat, MediaSink, MediaSource, MediaStream};
pub use mp4_parser::{BoxHeader, Mp4Parser, Mp4Track, SampleEntry, SampleTable};
pub use pipeline::{MediaPipeline, PassThroughStage, PipelineStage};
pub use publisher::{PublisherError, StreamPublisher};
pub use quality::{BandwidthMonitor, QualityConfig, QualityLevel, QualityManager};
// Unified segment types
pub use segment::{
    MediaType, SegmentBuilder, StreamId, StreamMetadata, StreamSegment,
};
pub use sink::{FileSink, MemorySink, NullSink};
pub use source::{FileSource, MemorySource, NetworkSource};
pub use stream::MediaStreamImpl;
pub use video_reader::{AudioTrackInfo, VideoReader, VideoReaderError, VideoTrackInfo};
// Wire format for P2P transmission
pub use wire_format::{ToWireFormat, WireSegment, WireSegmentBatch, WIRE_FORMAT_VERSION};
// P2P bridge
pub use p2p_bridge::{P2PBridge, P2PBridgeConfig};

/// Convenience type alias for Result<T, MediaError>
pub type Result<T> = std::result::Result<T, MediaError>;
