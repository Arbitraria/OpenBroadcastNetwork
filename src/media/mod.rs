// Media processing module for the decentralized streaming system
//
// This module will handle encoding, decoding, and processing of media streams.

pub mod interface;
pub mod codec;
pub mod buffer;
pub mod quality;
pub mod stream;
pub mod pipeline;

#[cfg(test)]
mod tests;

// Re-export the main types
pub use interface::{MediaStream, MediaSource, MediaSink, MediaError};
pub use codec::{Codec, Encoder, Decoder, VideoCodec, AudioCodec};
pub use buffer::MediaBuffer;
pub use quality::QualitySelector;
pub use pipeline::MediaPipeline; 