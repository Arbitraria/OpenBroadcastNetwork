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
/// FFmpeg-based video reader for proper fMP4 generation
pub mod ffmpeg_reader;
/// MP4 parser and MSE segment generator for browser streaming
pub mod mp4_parser;
/// MP4 fragment parsing (read-only operations)
pub mod fragment_parser;
/// MP4 fragment writing and generation
pub mod fragment_writer;
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
/// Streaming chunk format for P2P media distribution
pub mod stream_chunk;
/// Video file reading and MP4 parsing for real media content
pub mod video_reader;
/// Proper fragmented MP4 converter for MSE compatibility
pub mod fmp4_converter;

#[cfg(test)]
mod tests;

// Re-export main types for easy access
pub use buffer::{BufferConfig, BufferError, MediaBuffer};
pub use codec::{AudioCodec, Codec, CodecError, CodecRegistry, VideoCodec, OpenH264Codec, OpusCodec};
pub use interface::{
    MediaError, MediaFormat, MediaSink, MediaSource, MediaStream,
};
pub use pipeline::{MediaPipeline, PipelineStage, PassThroughStage};
pub use quality::{BandwidthMonitor, QualityConfig, QualityLevel, QualityManager};
pub use sink::{FileSink, MemorySink, NullSink};
pub use source::{FileSource, MemorySource, NetworkSource};
pub use stream::MediaStreamImpl;
pub use stream_chunk::{ChunkBuilder, ChunkType, MediaChunk, StreamId, StreamMetadata};
pub use video_reader::{VideoReader, VideoReaderError, MediaSample, VideoTrackInfo, AudioTrackInfo};
pub use ffmpeg_reader::{FFmpegVideoReader, FFmpegReaderError, FFmpegSample, FFmpegVideoTrack, FFmpegAudioTrack};
pub use mp4_parser::{Mp4Parser, MseSegment, Mp4Track, BoxHeader};
pub use fragment_parser::FragmentParser;
pub use fragment_writer::FragmentWriter;
pub use fmp4_converter::{FragmentedMp4Converter, Sample};

/// Convenience type alias for Result<T, MediaError>
pub type Result<T> = std::result::Result<T, MediaError>;