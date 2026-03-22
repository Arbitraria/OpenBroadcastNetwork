//! Media source implementations for the decentralized streaming CDN.
//!
//! This module provides various implementations of the `MediaSource` trait
//! for different types of media sources.

#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use tokio::fs::File;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io::AsyncReadExt;

#[cfg(not(target_arch = "wasm32"))]
use crate::media::interface::{MediaError, MediaSource};
#[cfg(not(target_arch = "wasm32"))]
use crate::media::stream::MediaStreamImpl;

/// A dummy implementation of MediaSource for testing and initialization
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
struct DummySource;

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl MediaSource for DummySource {
    async fn next_chunk(&mut self) -> Result<Vec<u8>, MediaError> {
        Ok(Vec::new())
    }

    async fn seek(&mut self, _position: Duration) -> Result<(), MediaError> {
        Ok(())
    }

    fn stream_info(&self) -> &dyn std::fmt::Debug {
        static INFO: &str = "DummySource";
        &INFO as &dyn std::fmt::Debug
    }
}

/// A media source that reads from a file
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct FileSource {
    /// The underlying file
    file: File,
    /// Information about the media stream
    stream: MediaStreamImpl,
}

#[cfg(not(target_arch = "wasm32"))]
impl FileSource {
    /// Create a new file source
    pub async fn new<P: AsRef<Path>>(
        path: P,
        format: crate::media::interface::MediaFormat,
    ) -> Result<Self, MediaError> {
        let file = File::open(path.as_ref())
            .await
            .map_err(|e| MediaError::from(format!("Failed to open file: {}", e)))?;

        let mut stream = MediaStreamImpl::new(Box::new(DummySource), None, None);

        // Set the stream metadata
        stream.metadata = crate::media::interface::MediaStream {
            id: path.as_ref().to_string_lossy().to_string(),
            format,
            duration: None, // Will be filled in if the format supports it
            bitrate: None,
            codec: None,
        };

        Ok(FileSource { file, stream })
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl MediaSource for FileSource {
    async fn next_chunk(&mut self) -> Result<Vec<u8>, MediaError> {
        let mut buffer = vec![0u8; 4096]; // 4KB chunks
        let bytes_read = self
            .file
            .read(&mut buffer)
            .await
            .map_err(|e| MediaError::from(format!("Failed to read from file: {}", e)))?;

        if bytes_read == 0 {
            return Err(MediaError::from("End of file"));
        }

        buffer.truncate(bytes_read);
        Ok(buffer)
    }

    fn stream_info(&self) -> &dyn std::fmt::Debug {
        &self.stream as &dyn std::fmt::Debug
    }

    async fn seek(&mut self, _position: Duration) -> Result<(), MediaError> {
        // Implementation would depend on the file format
        // For simplicity, we'll just return an error for now
        Err(MediaError::from("Seek not supported"))
    }
}

/// A media source that reads from a network stream
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct NetworkSource {
    /// The URL of the stream
    url: String,
    /// Information about the media stream
    stream: MediaStreamImpl,
    /// HTTP client
    client: reqwest::Client,
}

#[cfg(not(target_arch = "wasm32"))]
impl NetworkSource {
    /// Create a new network source
    pub fn new(url: &str, format: crate::media::interface::MediaFormat) -> Self {
        let mut stream = MediaStreamImpl::new(Box::new(DummySource), None, None);

        // Set the stream metadata
        stream.metadata = crate::media::interface::MediaStream {
            id: url.to_string(),
            format,
            duration: None,
            bitrate: None,
            codec: None,
        };

        NetworkSource {
            url: url.to_string(),
            stream,
            client: reqwest::Client::new(),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl MediaSource for NetworkSource {
    async fn next_chunk(&mut self) -> Result<Vec<u8>, MediaError> {
        let response = self
            .client
            .get(&self.url)
            .send()
            .await
            .map_err(|e| MediaError::from(format!("Failed to send request: {}", e)))?;

        if !response.status().is_success() {
            return Err(MediaError::from(format!(
                "Request failed with status: {}",
                response.status()
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| MediaError::from(format!("Failed to read response: {}", e)))?;

        Ok(bytes.to_vec())
    }

    fn stream_info(&self) -> &dyn std::fmt::Debug {
        &self.stream as &dyn std::fmt::Debug
    }

    async fn seek(&mut self, _position: Duration) -> Result<(), MediaError> {
        // Implementation would depend on the protocol
        // For simplicity, we'll just return an error for now
        Err(MediaError::from("Seek not supported"))
    }
}

/// A media source that reads from memory
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct MemorySource {
    /// The data to read from
    data: Arc<Vec<u8>>,
    /// Current position in the data
    position: usize,
    /// Information about the media stream
    stream: MediaStreamImpl,
}

#[cfg(not(target_arch = "wasm32"))]
impl MemorySource {
    /// Create a new memory source
    pub fn new(data: Vec<u8>, format: crate::media::interface::MediaFormat) -> Self {
        let mut stream = MediaStreamImpl::new(Box::new(DummySource), None, None);

        // Set the stream metadata
        stream.metadata = crate::media::interface::MediaStream {
            id: "memory".to_string(),
            format,
            duration: None,
            bitrate: None,
            codec: None,
        };

        MemorySource {
            data: Arc::new(data),
            position: 0,
            stream,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl MediaSource for MemorySource {
    async fn next_chunk(&mut self) -> Result<Vec<u8>, MediaError> {
        if self.position >= self.data.len() {
            return Err(MediaError::from("End of data"));
        }

        // Read up to 4KB at a time
        let end = std::cmp::min(self.position + 4096, self.data.len());
        let chunk = self.data[self.position..end].to_vec();
        self.position = end;

        Ok(chunk)
    }

    fn stream_info(&self) -> &dyn std::fmt::Debug {
        &self.stream as &dyn std::fmt::Debug
    }

    async fn seek(&mut self, position: Duration) -> Result<(), MediaError> {
        // For simplicity, we'll just convert the duration to bytes
        // In a real implementation, this would depend on the codec and format
        let pos_bytes = position.as_millis() as usize; // 1ms = 1 byte for simplicity
        if pos_bytes > self.data.len() {
            return Err(MediaError::from("Seek position out of range"));
        }

        self.position = pos_bytes;
        Ok(())
    }
}
