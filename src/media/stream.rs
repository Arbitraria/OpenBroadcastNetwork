//! Media stream implementations
//!
//! This module provides concrete implementations of media sources and sinks.

use crate::media::interface::{
    MediaStream, MediaSource, MediaSink, MediaFrame, MediaError,
    MediaParams, MediaSourceStats, MediaSinkStats, QualityLevel,
};
use crate::media::buffer::{MediaBuffer, MediaBufferConfig};
use crate::media::codec::{Encoder, Decoder};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time;

/// Stream identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamId(pub Vec<u8>);

/// Stream direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamDirection {
    /// Outgoing stream (local to remote)
    Outgoing,
    /// Incoming stream (remote to local)
    Incoming,
}

/// Base implementation for media streams
pub struct BaseMediaStream {
    /// Stream ID
    id: StreamId,
    /// Media parameters
    params: MediaParams,
    /// Stream direction
    direction: StreamDirection,
    /// Is the stream active
    active: bool,
    /// When the stream was created
    created_at: Instant,
}

impl BaseMediaStream {
    /// Create a new base media stream
    pub fn new(id: StreamId, params: MediaParams, direction: StreamDirection) -> Self {
        Self {
            id,
            params,
            direction,
            active: true,
            created_at: Instant::now(),
        }
    }
    
    /// Get the stream ID
    pub fn id(&self) -> &StreamId {
        &self.id
    }
    
    /// Get the stream direction
    pub fn direction(&self) -> StreamDirection {
        self.direction
    }
    
    /// Get the stream creation time
    pub fn created_at(&self) -> Instant {
        self.created_at
    }
}

impl MediaStream for BaseMediaStream {
    fn params(&self) -> &MediaParams {
        &self.params
    }
    
    fn is_active(&self) -> bool {
        self.active
    }
    
    fn close(&mut self) -> Result<(), MediaError> {
        self.active = false;
        Ok(())
    }
}

/// A buffered media source
pub struct BufferedMediaSource {
    /// Base media stream
    base: BaseMediaStream,
    /// Buffer for media frames
    buffer: Arc<MediaBuffer>,
    /// Source statistics
    stats: MediaSourceStats,
    /// Last frame sequence number
    last_sequence: u64,
}

impl BufferedMediaSource {
    /// Create a new buffered media source
    pub fn new(id: StreamId, params: MediaParams, buffer_config: MediaBufferConfig) -> Self {
        let base = BaseMediaStream::new(id, params.clone(), StreamDirection::Incoming);
        let buffer = Arc::new(MediaBuffer::new(params, buffer_config));
        
        Self {
            base,
            buffer,
            stats: MediaSourceStats::default(),
            last_sequence: 0,
        }
    }
    
    /// Get the buffer
    pub fn buffer(&self) -> Arc<MediaBuffer> {
        self.buffer.clone()
    }
}

impl MediaStream for BufferedMediaSource {
    fn params(&self) -> &MediaParams {
        self.base.params()
    }
    
    fn is_active(&self) -> bool {
        self.base.is_active()
    }
    
    fn close(&mut self) -> Result<(), MediaError> {
        self.base.close()
    }
}

impl MediaSource for BufferedMediaSource {
    fn read_frame(&mut self) -> Pin<Box<dyn Future<Output = Result<MediaFrame, MediaError>> + Send>> {
        let buffer = self.buffer.clone();
        
        Box::pin(async move {
            // First check if there's a frame available
            match buffer.get_frame().await {
                Ok(frame) => Ok(frame),
                Err(_) => {
                    // Wait for a frame with timeout
                    buffer.wait_for_frame(Some(Duration::from_secs(5))).await?;
                    buffer.get_frame().await
                }
            }
        })
    }
    
    fn stats(&self) -> MediaSourceStats {
        self.stats.clone()
    }
}

/// A buffered media sink
pub struct BufferedMediaSink {
    /// Base media stream
    base: BaseMediaStream,
    /// Buffer for media frames
    buffer: Arc<MediaBuffer>,
    /// Sink statistics
    stats: Mutex<MediaSinkStats>,
}

impl BufferedMediaSink {
    /// Create a new buffered media sink
    pub fn new(id: StreamId, params: MediaParams, buffer_config: MediaBufferConfig) -> Self {
        let base = BaseMediaStream::new(id, params.clone(), StreamDirection::Outgoing);
        let buffer = Arc::new(MediaBuffer::new(params, buffer_config));
        
        Self {
            base,
            buffer,
            stats: Mutex::new(MediaSinkStats::default()),
        }
    }
    
    /// Get the buffer
    pub fn buffer(&self) -> Arc<MediaBuffer> {
        self.buffer.clone()
    }
}

impl MediaStream for BufferedMediaSink {
    fn params(&self) -> &MediaParams {
        self.base.params()
    }
    
    fn is_active(&self) -> bool {
        self.base.is_active()
    }
    
    fn close(&mut self) -> Result<(), MediaError> {
        self.base.close()
    }
}

impl MediaSink for BufferedMediaSink {
    fn write_frame(&mut self, frame: MediaFrame) -> Pin<Box<dyn Future<Output = Result<(), MediaError>> + Send>> {
        let buffer = self.buffer.clone();
        let stats = self.stats.clone();
        
        Box::pin(async move {
            let start = Instant::now();
            let result = buffer.add_frame(frame.clone()).await;
            let elapsed = start.elapsed();
            
            // Update stats
            let mut stats_lock = stats.lock().await;
            stats_lock.frames_written += 1;
            stats_lock.bytes_written += frame.data.len() as u64;
            
            // Update average processing time using exponential moving average
            if stats_lock.avg_processing_time.as_nanos() == 0 {
                stats_lock.avg_processing_time = elapsed;
            } else {
                let old_avg = stats_lock.avg_processing_time.as_nanos() as f64;
                let new_avg = (old_avg * 0.9) + (elapsed.as_nanos() as f64 * 0.1);
                stats_lock.avg_processing_time = Duration::from_nanos(new_avg as u64);
            }
            
            result
        })
    }
    
    fn flush(&mut self) -> Pin<Box<dyn Future<Output = Result<(), MediaError>> + Send>> {
        // Nothing to do for a buffered sink
        Box::pin(async { Ok(()) })
    }
    
    fn stats(&self) -> MediaSinkStats {
        futures::executor::block_on(async {
            self.stats.lock().await.clone()
        })
    }
}

/// An encoded media source that applies encoding to raw frames
pub struct EncodedMediaSource<S: MediaSource> {
    /// Inner media source
    inner: S,
    /// Encoder for raw frames
    encoder: Arc<Mutex<Box<dyn Encoder + Send>>>,
}

impl<S: MediaSource> EncodedMediaSource<S> {
    /// Create a new encoded media source
    pub fn new(inner: S, encoder: Box<dyn Encoder + Send>) -> Self {
        Self {
            inner,
            encoder: Arc::new(Mutex::new(encoder)),
        }
    }
}

impl<S: MediaSource> MediaStream for EncodedMediaSource<S> {
    fn params(&self) -> &MediaParams {
        self.inner.params()
    }
    
    fn is_active(&self) -> bool {
        self.inner.is_active()
    }
    
    fn close(&mut self) -> Result<(), MediaError> {
        self.inner.close()
    }
}

impl<S: MediaSource> MediaSource for EncodedMediaSource<S> {
    fn read_frame(&mut self) -> Pin<Box<dyn Future<Output = Result<MediaFrame, MediaError>> + Send>> {
        let inner = &mut self.inner;
        let encoder = self.encoder.clone();
        
        Box::pin(async move {
            // Read raw frame from inner source
            let raw_frame = inner.read_frame().await?;
            
            // Encode the frame
            let mut encoder_lock = encoder.lock().await;
            encoder_lock.encode(&raw_frame.data).await
        })
    }
    
    fn stats(&self) -> MediaSourceStats {
        self.inner.stats()
    }
}

/// A decoded media sink that applies decoding to compressed frames
pub struct DecodedMediaSink<T: MediaSink> {
    /// Inner media sink
    inner: T,
    /// Decoder for compressed frames
    decoder: Arc<Mutex<Box<dyn Decoder + Send>>>,
}

impl<T: MediaSink> DecodedMediaSink<T> {
    /// Create a new decoded media sink
    pub fn new(inner: T, decoder: Box<dyn Decoder + Send>) -> Self {
        Self {
            inner,
            decoder: Arc::new(Mutex::new(decoder)),
        }
    }
}

impl<T: MediaSink> MediaStream for DecodedMediaSink<T> {
    fn params(&self) -> &MediaParams {
        self.inner.params()
    }
    
    fn is_active(&self) -> bool {
        self.inner.is_active()
    }
    
    fn close(&mut self) -> Result<(), MediaError> {
        self.inner.close()
    }
}

impl<T: MediaSink> MediaSink for DecodedMediaSink<T> {
    fn write_frame(&mut self, frame: MediaFrame) -> Pin<Box<dyn Future<Output = Result<(), MediaError>> + Send>> {
        let inner = &mut self.inner;
        let decoder = self.decoder.clone();
        
        Box::pin(async move {
            // Decode the frame
            let mut decoder_lock = decoder.lock().await;
            let raw_data = decoder_lock.decode(&frame).await?;
            
            // Create a new frame with the decoded data
            let decoded_frame = MediaFrame {
                data: raw_data,
                metadata: frame.metadata,
            };
            
            // Write the decoded frame to the inner sink
            inner.write_frame(decoded_frame).await
        })
    }
    
    fn flush(&mut self) -> Pin<Box<dyn Future<Output = Result<(), MediaError>> + Send>> {
        self.inner.flush()
    }
    
    fn stats(&self) -> MediaSinkStats {
        self.inner.stats()
    }
} 