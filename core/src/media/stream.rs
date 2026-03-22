//! Media stream handling for the decentralized streaming CDN.
//!
//! This module provides functionality for managing media streams, including
//! creating, reading, and writing media data.

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::Mutex;

#[cfg(not(target_arch = "wasm32"))]
use crate::media::buffer::{BufferConfig, MediaBuffer};
#[cfg(not(target_arch = "wasm32"))]
use crate::media::codec::{AudioCodec, Codec, CodecRegistry, VideoCodec};
#[cfg(not(target_arch = "wasm32"))]
use crate::media::interface::{MediaError, MediaSink, MediaSource, MediaStream};

/// Implementation of a media stream that can be read from or written to
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct MediaStreamImpl {
    /// The underlying media stream
    stream: Box<dyn MediaSource>,
    /// Buffer for the media data
    buffer: Arc<Mutex<MediaBuffer>>,
    /// Codec registry for encoding/decoding
    codec_registry: Arc<CodecRegistry>,
    /// Name of the current codec being used
    current_codec: Option<String>,
    /// Stream metadata
    pub metadata: crate::media::interface::MediaStream,
}

#[cfg(not(target_arch = "wasm32"))]
impl MediaStreamImpl {
    /// Create a new media stream
    pub fn new(
        stream: Box<dyn MediaSource>,
        buffer_config: Option<BufferConfig>,
        codec_registry: Option<Arc<CodecRegistry>>,
    ) -> Self {
        let buffer_config = buffer_config.unwrap_or_default();
        let codec_registry = codec_registry.unwrap_or_else(|| Arc::new(CodecRegistry::default()));

        // Get stream info from the underlying source
        let _stream_info = stream.stream_info();
        let metadata = MediaStream {
            id: "".to_string(),         // Will be set by the caller if needed
            format: Default::default(), // Will be set by the caller if needed
            duration: None,
            bitrate: None,
            codec: None,
        };

        MediaStreamImpl {
            stream,
            buffer: Arc::new(Mutex::new(MediaBuffer::new(buffer_config))),
            codec_registry,
            current_codec: None,
            metadata,
        }
    }

    /// Set the codec for the stream
    pub async fn set_codec(&mut self, codec_name: &str) -> Result<(), MediaError> {
        // Check if the codec exists in the registry
        if self.codec_registry.get(codec_name).is_some() {
            self.current_codec = Some(codec_name.to_string());
            Ok(())
        } else {
            Err(MediaError::from(format!("Codec not found: {}", codec_name)))
        }
    }

    /// Read data from the stream
    pub async fn read(&mut self, size: usize) -> Result<Vec<u8>, MediaError> {
        let mut buffer = self.buffer.lock().await;

        if buffer.is_empty() {
            // Read more data from the underlying stream
            let data = self.stream.next_chunk().await?;
            buffer
                .push(&data)
                .map_err(|e| MediaError::from(e.to_string()))?;
        }

        // Get data from the buffer
        let data = buffer
            .pop(size)
            .map_err(|e| MediaError::from(e.to_string()))?;

        // If we have a codec, decode the data
        if let Some(codec_name) = &self.current_codec {
            // Create a new instance of the codec for this operation
            let mut codec: Box<dyn Codec> = match codec_name.as_str() {
                "audio" => Box::new(AudioCodec::default()),
                "video" => Box::new(VideoCodec::default()),
                _ => return Err(MediaError::from("Unknown codec type")),
            };

            codec
                .decode(&data)
                .await
                .map_err(|e| MediaError::from(e.to_string()))
        } else {
            Ok(data)
        }
    }

    /// Write data to the stream
    pub async fn write(&mut self, data: &[u8]) -> Result<(), MediaError> {
        let encoded_data = if let Some(codec_name) = &self.current_codec {
            // Create a new instance of the codec for this operation
            let mut codec: Box<dyn Codec> = match codec_name.as_str() {
                "audio" => Box::new(AudioCodec::default()),
                "video" => Box::new(VideoCodec::default()),
                _ => return Err(MediaError::from("Unknown codec type")),
            };

            codec
                .encode(data)
                .await
                .map_err(|e| MediaError::from(e.to_string()))?
        } else {
            data.to_vec()
        };

        let mut buffer = self.buffer.lock().await;
        buffer
            .push(&encoded_data)
            .map_err(|e| MediaError::from(e.to_string()))
    }

    /// Get information about the media stream
    pub fn stream_info(&self) -> &dyn std::fmt::Debug {
        self.stream.stream_info()
    }

    /// Seek to a specific position in the stream
    pub async fn seek(&mut self, position: Duration) -> Result<(), MediaError> {
        self.stream.seek(position).await
    }

    /// Check if the stream is ready to be read from
    pub async fn is_ready(&self) -> bool {
        let buffer = self.buffer.lock().await;
        buffer.has_enough_data()
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl MediaSource for MediaStreamImpl {
    async fn next_chunk(&mut self) -> Result<Vec<u8>, MediaError> {
        self.read(4096).await // Default chunk size of 4KB
    }

    fn stream_info(&self) -> &dyn std::fmt::Debug {
        self.stream.stream_info()
    }

    async fn seek(&mut self, position: Duration) -> Result<(), MediaError> {
        self.seek(position).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl MediaSink for MediaStreamImpl {
    async fn write_chunk(&mut self, data: &[u8]) -> Result<(), MediaError> {
        self.write(data).await
    }

    async fn flush(&mut self) -> Result<(), MediaError> {
        // No-op for now
        Ok(())
    }
}
