//! Media pipeline for processing streams
//!
//! This module provides the central media pipeline that connects sources, sinks, and processing.

use crate::media::interface::{
    MediaStream, MediaSource, MediaSink, MediaFrame, MediaError,
    MediaParams, MediaType, QualityLevel,
};
use crate::media::buffer::{MediaBuffer, MediaBufferConfig};
use crate::media::codec::{Encoder, Decoder, VideoCodec, AudioCodec};
use crate::media::quality::{QualitySelector, QualitySelectorConfig, BandwidthEstimate};
use crate::media::stream::{StreamId, BufferedMediaSource, BufferedMediaSink};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Media pipeline configuration
#[derive(Debug, Clone)]
pub struct MediaPipelineConfig {
    /// Input buffer configuration
    pub input_buffer: MediaBufferConfig,
    /// Output buffer configuration
    pub output_buffer: MediaBufferConfig,
    /// Quality selector configuration
    pub quality_selector: QualitySelectorConfig,
    /// Maximum concurrent streams
    pub max_streams: usize,
    /// Processing interval in milliseconds
    pub processing_interval_ms: u64,
    /// Stats reporting interval in milliseconds
    pub stats_interval_ms: u64,
}

impl Default for MediaPipelineConfig {
    fn default() -> Self {
        Self {
            input_buffer: MediaBufferConfig::default(),
            output_buffer: MediaBufferConfig::default(),
            quality_selector: QualitySelectorConfig::default(),
            max_streams: 10,
            processing_interval_ms: 10,  // Process frames every 10ms
            stats_interval_ms: 1000,    // Report stats every second
        }
    }
}

/// Media pipeline statistics
#[derive(Debug, Clone, Default)]
pub struct MediaPipelineStats {
    /// Number of active streams
    pub active_streams: usize,
    /// Total frames processed
    pub frames_processed: u64,
    /// Total bytes processed
    pub bytes_processed: u64,
    /// Average processing time per frame
    pub avg_processing_time: Duration,
    /// Frames processed per second
    pub frames_per_second: f32,
    /// Current bandwidth utilization in bits per second
    pub bandwidth_bps: u32,
    /// Time since last stats reset
    pub time_since_reset: Duration,
}

/// Stream processing state
struct StreamProcessor {
    /// Stream ID
    id: StreamId,
    /// Media parameters
    params: MediaParams,
    /// Input buffer
    input: Arc<MediaBuffer>,
    /// Output buffer
    output: Arc<MediaBuffer>,
    /// Quality selector
    quality_selector: Option<QualitySelector>,
    /// Processing task handle
    task_handle: Option<JoinHandle<()>>,
    /// Is the processor running
    running: bool,
    /// Last frame processed time
    last_processed: Instant,
    /// Statistics
    stats: MediaPipelineStats,
}

impl StreamProcessor {
    /// Create a new stream processor
    fn new(
        id: StreamId,
        params: MediaParams,
        input: Arc<MediaBuffer>,
        output: Arc<MediaBuffer>,
        quality_selector: Option<QualitySelector>,
    ) -> Self {
        Self {
            id,
            params,
            input,
            output,
            quality_selector,
            task_handle: None,
            running: false,
            last_processed: Instant::now(),
            stats: MediaPipelineStats::default(),
        }
    }
    
    /// Start processing
    fn start(&mut self, interval_ms: u64) {
        if self.running {
            return;
        }
        
        self.running = true;
        let input = self.input.clone();
        let output = self.output.clone();
        let mut quality_selector = self.quality_selector.take();
        let media_type = self.params.media_type;
        
        let task = tokio::spawn(async move {
            let interval_duration = Duration::from_millis(interval_ms);
            let mut interval = tokio::time::interval(interval_duration);
            let mut frames_processed = 0;
            let mut bytes_processed = 0;
            let mut total_processing_time = Duration::from_nanos(0);
            let start_time = Instant::now();
            
            while let Ok(()) = input.wait_for_frame(Some(Duration::from_secs(1))).await {
                interval.tick().await;
                
                // Check if we should switch quality
                if let Some(quality_selector) = &mut quality_selector {
                    // Update buffer fullness
                    let input_stats = input.stats().await;
                    let output_stats = output.stats().await;
                    quality_selector.update_buffer_fullness(
                        (input_stats.fullness + output_stats.fullness) / 2.0
                    );
                    
                    // Evaluate quality change
                    if let Some(new_quality) = quality_selector.evaluate() {
                        info!("Quality changed for stream {:?} to {:?}", media_type, new_quality);
                        // In a real implementation, we would apply the quality change
                        // For now, we just log it
                    }
                }
                
                // Process available frames
                let processing_start = Instant::now();
                
                // Get a frame from the input buffer
                if let Ok(frame) = input.peek_frame().await {
                    // In a real implementation, we would process the frame here
                    // For example, apply filters, resize, etc.
                    
                    // Add the frame to the output buffer
                    if let Err(e) = output.add_frame(frame.clone()).await {
                        error!("Failed to add frame to output buffer: {}", e);
                    } else {
                        // Remove the processed frame from the input buffer
                        let _ = input.get_frame().await;
                        
                        // Update stats
                        frames_processed += 1;
                        bytes_processed += frame.data.len() as u64;
                        total_processing_time += processing_start.elapsed();
                    }
                }
            }
        });
        
        self.task_handle = Some(task);
    }
    
    /// Stop processing
    fn stop(&mut self) {
        self.running = false;
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }
    
    /// Update statistics
    fn update_stats(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_processed);
        
        // Update the stats (in a real implementation, we would gather real metrics)
        self.stats.time_since_reset = elapsed;
        self.stats.frames_per_second = self.stats.frames_processed as f32 / elapsed.as_secs_f32();
        self.stats.bandwidth_bps = (self.stats.bytes_processed * 8 / elapsed.as_secs() as u64) as u32;
        
        self.last_processed = now;
    }
}

/// Central media pipeline manager
pub struct MediaPipeline {
    /// Pipeline configuration
    config: MediaPipelineConfig,
    /// Stream processors
    processors: RwLock<HashMap<StreamId, Arc<Mutex<StreamProcessor>>>>,
    /// Sources by stream ID
    sources: RwLock<HashMap<StreamId, Arc<Mutex<BufferedMediaSource>>>>,
    /// Sinks by stream ID
    sinks: RwLock<HashMap<StreamId, Arc<Mutex<BufferedMediaSink>>>>,
    /// Overall pipeline statistics
    stats: RwLock<MediaPipelineStats>,
    /// Is the pipeline running
    running: RwLock<bool>,
    /// Stats task handle
    stats_task: Mutex<Option<JoinHandle<()>>>,
}

impl MediaPipeline {
    /// Create a new media pipeline
    pub fn new(config: MediaPipelineConfig) -> Self {
        Self {
            config,
            processors: RwLock::new(HashMap::new()),
            sources: RwLock::new(HashMap::new()),
            sinks: RwLock::new(HashMap::new()),
            stats: RwLock::new(MediaPipelineStats::default()),
            running: RwLock::new(false),
            stats_task: Mutex::new(None),
        }
    }
    
    /// Start the pipeline
    pub async fn start(&self) -> Result<(), MediaError> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        
        *running = true;
        
        // Start all processors
        let processors = self.processors.read().await;
        for processor in processors.values() {
            let mut processor_lock = processor.lock().await;
            processor_lock.start(self.config.processing_interval_ms);
        }
        
        // Start the stats collection task
        let stats_interval = self.config.stats_interval_ms;
        let processors_ref = self.processors.clone();
        let stats_ref = self.stats.clone();
        
        let stats_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(stats_interval));
            
            loop {
                interval.tick().await;
                
                // Update stats for each processor
                let processors = processors_ref.read().await;
                let mut overall_stats = MediaPipelineStats::default();
                overall_stats.active_streams = processors.len();
                
                for processor in processors.values() {
                    let mut processor_lock = processor.lock().await;
                    processor_lock.update_stats();
                    
                    // Aggregate stats
                    overall_stats.frames_processed += processor_lock.stats.frames_processed;
                    overall_stats.bytes_processed += processor_lock.stats.bytes_processed;
                    
                    // Use weighted average for timing stats
                    if processor_lock.stats.frames_processed > 0 {
                        let weight = processor_lock.stats.frames_processed as f64 / 
                                    overall_stats.frames_processed.max(1) as f64;
                        
                        let current_avg = overall_stats.avg_processing_time.as_nanos() as f64;
                        let processor_avg = processor_lock.stats.avg_processing_time.as_nanos() as f64;
                        
                        let new_avg = current_avg + (processor_avg - current_avg) * weight;
                        overall_stats.avg_processing_time = Duration::from_nanos(new_avg as u64);
                    }
                }
                
                // Calculate frames per second
                if overall_stats.time_since_reset.as_secs() > 0 {
                    overall_stats.frames_per_second = overall_stats.frames_processed as f32 / 
                                                    overall_stats.time_since_reset.as_secs_f32();
                    overall_stats.bandwidth_bps = (overall_stats.bytes_processed * 8 / 
                                                overall_stats.time_since_reset.as_secs() as u64) as u32;
                }
                
                // Update the overall stats
                let mut stats = stats_ref.write().await;
                *stats = overall_stats;
            }
        });
        
        // Store the task handle
        let mut stats_task_guard = self.stats_task.lock().await;
        *stats_task_guard = Some(stats_task);
        
        Ok(())
    }
    
    /// Stop the pipeline
    pub async fn stop(&self) -> Result<(), MediaError> {
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }
        
        *running = false;
        
        // Stop all processors
        let processors = self.processors.read().await;
        for processor in processors.values() {
            let mut processor_lock = processor.lock().await;
            processor_lock.stop();
        }
        
        // Stop the stats collection task
        let mut stats_task_guard = self.stats_task.lock().await;
        if let Some(handle) = stats_task_guard.take() {
            handle.abort();
        }
        
        Ok(())
    }
    
    /// Create a new media source
    pub async fn create_source(&self, id: StreamId, params: MediaParams) -> Result<Arc<Mutex<BufferedMediaSource>>, MediaError> {
        let mut sources = self.sources.write().await;
        
        if sources.contains_key(&id) {
            return Err(MediaError::InvalidFormat(format!("Source with ID {:?} already exists", id)));
        }
        
        // Create a new source with buffer
        let source = Arc::new(Mutex::new(BufferedMediaSource::new(
            id.clone(),
            params.clone(),
            self.config.input_buffer.clone(),
        )));
        
        sources.insert(id.clone(), source.clone());
        
        // Set up the corresponding processor if a sink exists
        self.setup_processor(id, params).await?;
        
        Ok(source)
    }
    
    /// Create a new media sink
    pub async fn create_sink(&self, id: StreamId, params: MediaParams) -> Result<Arc<Mutex<BufferedMediaSink>>, MediaError> {
        let mut sinks = self.sinks.write().await;
        
        if sinks.contains_key(&id) {
            return Err(MediaError::InvalidFormat(format!("Sink with ID {:?} already exists", id)));
        }
        
        // Create a new sink with buffer
        let sink = Arc::new(Mutex::new(BufferedMediaSink::new(
            id.clone(),
            params.clone(),
            self.config.output_buffer.clone(),
        )));
        
        sinks.insert(id.clone(), sink.clone());
        
        // Set up the corresponding processor if a source exists
        self.setup_processor(id, params).await?;
        
        Ok(sink)
    }
    
    /// Set up a processor for a stream if both source and sink exist
    async fn setup_processor(&self, id: StreamId, params: MediaParams) -> Result<(), MediaError> {
        let sources = self.sources.read().await;
        let sinks = self.sinks.read().await;
        
        // Check if both source and sink exist
        if let (Some(source), Some(sink)) = (sources.get(&id), sinks.get(&id)) {
            let mut processors = self.processors.write().await;
            
            // If processor already exists, return
            if processors.contains_key(&id) {
                return Ok(());
            }
            
            // Create quality selector if needed
            let quality_selector = match params.media_type {
                MediaType::Video | MediaType::Audio => {
                    // Create a quality selector with appropriate profiles
                    let mut config = self.config.quality_selector.clone();
                    
                    // In a real implementation, we would populate config.profiles
                    // based on the media type and available codecs
                    
                    Some(QualitySelector::new(config).unwrap_or_else(|_| {
                        warn!("Failed to create quality selector, proceeding without one");
                        None
                    }).unwrap_or_else(|| {
                        warn!("No quality selector created");
                        None
                    }))
                },
                _ => None,
            };
            
            // Get buffer references
            let source_lock = source.lock().await;
            let input_buffer = source_lock.buffer();
            drop(source_lock);
            
            let sink_lock = sink.lock().await;
            let output_buffer = sink_lock.buffer();
            drop(sink_lock);
            
            // Create a new processor
            let processor = Arc::new(Mutex::new(StreamProcessor::new(
                id.clone(),
                params,
                input_buffer,
                output_buffer,
                quality_selector,
            )));
            
            // If the pipeline is running, start the processor
            if *self.running.read().await {
                let mut processor_lock = processor.lock().await;
                processor_lock.start(self.config.processing_interval_ms);
            }
            
            processors.insert(id, processor);
        }
        
        Ok(())
    }
    
    /// Remove a stream (source, sink, and processor)
    pub async fn remove_stream(&self, id: &StreamId) -> Result<(), MediaError> {
        // Stop the processor if it exists
        let mut processors = self.processors.write().await;
        if let Some(processor) = processors.remove(id) {
            let mut processor_lock = processor.lock().await;
            processor_lock.stop();
        }
        
        // Remove source and sink
        let mut sources = self.sources.write().await;
        sources.remove(id);
        
        let mut sinks = self.sinks.write().await;
        sinks.remove(id);
        
        Ok(())
    }
    
    /// Update the bandwidth estimate for a stream
    pub async fn update_bandwidth(&self, id: &StreamId, estimate: BandwidthEstimate) -> Result<(), MediaError> {
        let processors = self.processors.read().await;
        
        if let Some(processor) = processors.get(id) {
            let mut processor_lock = processor.lock().await;
            
            // Update the quality selector if it exists
            if let Some(quality_selector) = &mut processor_lock.quality_selector {
                quality_selector.update_bandwidth(estimate);
            }
            
            Ok(())
        } else {
            Err(MediaError::InvalidFormat(format!("Stream with ID {:?} not found", id)))
        }
    }
    
    /// Get the pipeline statistics
    pub async fn stats(&self) -> MediaPipelineStats {
        self.stats.read().await.clone()
    }
    
    /// Get the active stream count
    pub async fn active_stream_count(&self) -> usize {
        self.processors.read().await.len()
    }
} 