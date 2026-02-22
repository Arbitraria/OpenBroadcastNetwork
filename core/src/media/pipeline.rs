//! Media processing pipeline for the decentralized streaming CDN.
//!
//! This module provides functionality for building and managing media processing
//! pipelines that can transform media data as it flows through the system. The pipeline
//! architecture is based on a series of modular stages that each perform a specific
//! transformation on the data, allowing for flexible and extensible media processing.
//!
//! The pipeline follows these key design principles:
//! - **Modularity**: Each processing stage is independent and can be combined freely
//! - **Flexibility**: Pipelines can be dynamically reconfigured during runtime
//! - **Efficiency**: Minimizes data copying and optimizes for streaming performance
//! - **Error resilience**: Handles errors gracefully to maintain stream continuity

use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
// Will use Stream when implementing actual pipeline functionality
use tokio::sync::Mutex;

use crate::media::interface::{MediaError, MediaSink, MediaSource};

/// A stage in the media processing pipeline
///
/// Represents a single processing unit in the media pipeline that transforms
/// input data in some way. Each stage receives a chunk of media data, processes it,
/// and produces output data that flows to the next stage in the pipeline.
///
/// Examples of pipeline stages include encoders, decoders, filters, scalers,
/// mixers, and other media transformations. Stages can be stateful or stateless
/// depending on their specific requirements.
#[async_trait]
pub trait PipelineStage: Send + Sync + std::fmt::Debug + 'static {
    /// Process a chunk of media data
    ///
    /// Takes a chunk of binary media data as input, applies the stage's transformation,
    /// and returns the processed output data. This is the core method that implements
    /// the stage's specific media processing functionality.
    ///
    /// # Arguments
    /// * `data` - The input media data chunk to process
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - The processed output data
    /// * `Err(MediaError)` - If processing fails for any reason
    async fn process(&mut self, data: Vec<u8>) -> Result<Vec<u8>, MediaError>;

    /// Get the name of the stage
    ///
    /// Returns a human-readable identifier for this pipeline stage, used for logging,
    /// debugging, and telemetry purposes. The name should be unique within a pipeline.
    ///
    /// # Returns
    /// A static string representing the stage's name
    fn name(&self) -> &'static str;
}

/// A simple pass-through pipeline stage
///
/// This is a utility stage that passes data through unchanged. It's useful for testing,
/// as a placeholder in pipeline development, or for creating monitoring points in a
/// pipeline without affecting the data flow.
#[derive(Debug)]
pub struct PassThroughStage {
    name: &'static str,
}

impl PassThroughStage {
    /// Create a new pass-through stage
    pub fn new(name: &'static str) -> Self {
        PassThroughStage { name }
    }
}

#[async_trait]
impl PipelineStage for PassThroughStage {
    async fn process(&mut self, data: Vec<u8>) -> Result<Vec<u8>, MediaError> {
        Ok(data)
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

/// A media processing pipeline
///
/// The central component that manages the flow of media data through a series of
/// processing stages. The pipeline allows media data to be transformed through multiple
/// stages in sequence, with each stage applying its specific processing logic.
///
/// The pipeline handles the orchestration of data flow, error management, and the
/// composition of different processing stages. It can be connected to media sources
/// and sinks to form a complete media processing chain.
#[derive(Debug)]
pub struct MediaPipeline {
    stages: Vec<Box<dyn PipelineStage>>,
    buffer: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

impl MediaPipeline {
    /// Create a new empty media pipeline
    ///
    /// Initializes a pipeline with no processing stages. Stages can be added later
    /// using the `add_stage` method to build the desired processing chain.
    ///
    /// # Returns
    /// A new MediaPipeline instance with an empty processing chain
    pub fn new() -> Self {
        MediaPipeline {
            stages: Vec::new(),
            buffer: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Add a processing stage to the pipeline
    ///
    /// Appends a new processing stage to the end of the pipeline. Stages are executed
    /// in the order they are added, with data flowing sequentially through each stage.
    ///
    /// # Arguments
    /// * `stage` - The processing stage to add to the pipeline
    pub fn add_stage(&mut self, stage: impl PipelineStage + 'static) {
        self.stages.push(Box::new(stage));
    }

    /// Process a chunk of media data through the entire pipeline
    ///
    /// Takes input data and passes it through each stage of the pipeline in sequence.
    /// Each stage transforms the data before passing it to the next stage. If any stage
    /// returns an error, processing stops and the error is returned.
    ///
    /// # Arguments
    /// * `data` - The input media data to process
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - The fully processed output data after passing through all stages
    /// * `Err(MediaError)` - If processing fails at any stage
    pub async fn process(&mut self, data: Vec<u8>) -> Result<Vec<u8>, MediaError> {
        let mut result = data;

        for stage in &mut self.stages {
            result = stage.process(result).await?;
        }

        Ok(result)
    }

    /// Create a source that reads from this pipeline
    pub fn into_source<S>(self, source: S) -> PipelineSource<S>
    where
        S: MediaSource + Send + std::fmt::Debug + 'static,
    {
        PipelineSource {
            source,
            pipeline: self,
        }
    }

    /// Create a sink that writes to this pipeline
    pub fn into_sink<S>(self, sink: S) -> PipelineSink<S>
    where
        S: MediaSink + Send + std::fmt::Debug + 'static,
    {
        PipelineSink {
            sink,
            pipeline: self,
        }
    }
}

/// A media source that processes data through a pipeline
#[derive(Debug)]
pub struct PipelineSource<S>
where
    S: std::fmt::Debug,
{
    source: S,
    pipeline: MediaPipeline,
}

#[async_trait]
impl<S> MediaSource for PipelineSource<S>
where
    S: MediaSource + Send + std::fmt::Debug + 'static,
{
    async fn next_chunk(&mut self) -> Result<Vec<u8>, MediaError> {
        let data = self.source.next_chunk().await?;
        self.pipeline.process(data).await
    }

    fn stream_info(&self) -> &dyn std::fmt::Debug {
        self.source.stream_info()
    }

    async fn seek(&mut self, position: std::time::Duration) -> Result<(), MediaError> {
        self.source.seek(position).await
    }
}

/// A media sink that processes data through a pipeline
#[derive(Debug)]
pub struct PipelineSink<S>
where
    S: std::fmt::Debug,
{
    sink: S,
    pipeline: MediaPipeline,
}

#[async_trait]
impl<S> MediaSink for PipelineSink<S>
where
    S: MediaSink + Send + std::fmt::Debug + 'static,
{
    async fn write_chunk(&mut self, data: &[u8]) -> Result<(), MediaError> {
        let processed = self.pipeline.process(data.to_vec()).await?;
        self.sink.write_chunk(&processed).await
    }

    async fn flush(&mut self) -> Result<(), MediaError> {
        self.sink.flush().await
    }
}
