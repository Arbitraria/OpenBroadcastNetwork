//! Media sink implementations for the decentralized streaming CDN.
//!
//! This module provides various implementations of the `MediaSink` trait
//! for different types of media destinations.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::media::interface::{MediaError, MediaSink};

/// A media sink that writes to a file
#[derive(Debug)]
pub struct FileSink {
    /// The underlying file
    file: File,
}

impl FileSink {
    /// Create a new file sink
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, MediaError> {
        let file = File::create(path.as_ref())
            .await
            .map_err(|e| MediaError::from(format!("Failed to create file: {}", e)))?;

        Ok(FileSink { file })
    }
}

#[async_trait]
impl MediaSink for FileSink {
    async fn write_chunk(&mut self, data: &[u8]) -> Result<(), MediaError> {
        self.file
            .write_all(data)
            .await
            .map_err(|e| MediaError::from(format!("Failed to write to file: {}", e)))?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), MediaError> {
        self.file
            .flush()
            .await
            .map_err(|e| MediaError::from(format!("Failed to flush file: {}", e)))
    }
}

/// A media sink that writes to memory
#[derive(Debug)]
pub struct MemorySink {
    /// The data being written to
    data: Arc<tokio::sync::Mutex<Vec<u8>>>,
}

impl MemorySink {
    /// Create a new memory sink
    pub fn new() -> Self {
        MemorySink {
            data: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    /// Get the data that was written to the sink
    pub async fn into_inner(self) -> Vec<u8> {
        Arc::try_unwrap(self.data)
            .unwrap_or_else(|_| panic!("Failed to unwrap Arc"))
            .into_inner()
    }
}

#[async_trait]
impl MediaSink for MemorySink {
    async fn write_chunk(&mut self, data: &[u8]) -> Result<(), MediaError> {
        let mut buffer = self.data.lock().await;
        buffer.extend_from_slice(data);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), MediaError> {
        // No-op for memory sink
        Ok(())
    }
}

impl Default for MemorySink {
    fn default() -> Self {
        Self::new()
    }
}

/// A media sink that discards all data
#[derive(Debug)]
pub struct NullSink;

#[async_trait]
impl MediaSink for NullSink {
    async fn write_chunk(&mut self, _data: &[u8]) -> Result<(), MediaError> {
        // Discard the data
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), MediaError> {
        // No-op
        Ok(())
    }
}

impl Default for NullSink {
    fn default() -> Self {
        NullSink
    }
}
