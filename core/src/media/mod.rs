// Media pipeline for decentralized streaming
//
// This module handles media streaming functionality including
// sources, sinks, codecs, and buffer management.

pub mod interface;
pub mod codec;
pub mod buffer;
pub mod stream;
pub mod pipeline;
pub mod quality;
pub mod source;
pub mod sink;

#[cfg(test)]
mod tests;

// Re-export main types
pub use interface::{MediaStream, MediaSource, MediaSink, MediaFormat, MediaError};
pub use codec::{Codec, CodecRegistry, AudioCodec, VideoCodec};
pub use buffer::{MediaBuffer, BufferConfig, BufferError};
pub use stream::{StreamHandler, StreamSegment, StreamInfo};
pub use pipeline::{MediaPipeline, PipelineConfig, PipelineEvent};
pub use quality::{QualityLevel, QualitySelector, AdaptiveQuality};
pub use source::{FileSource, NetworkSource, MemorySource};
pub use sink::{FileSink, NetworkSink, MemorySink}; 