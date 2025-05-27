//! Media processing pipeline for the decentralized streaming CDN.
//!
//! This module provides functionality for building and managing media processing
//! pipelines that can transform media data as it flows through the system.

use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::Stream;
use tokio::sync::Mutex;

use crate::media::interface::{MediaError, MediaSink, MediaSource};

/// A stage in the media processing pipeline
#[async_trait]
pub trait PipelineStage: Send + Sync + std::fmt::Debug + 'static {
    /// Process a chunk of data
    async fn process(&mut self, data: Vec<u8>) -> Result<Vec<u8>, MediaError>;
    
    /// Get the name of the stage
    fn name(&self) -> &'static str;
}

/// A simple pass-through pipeline stage
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
#[derive(Debug)]
pub struct MediaPipeline {
    stages: Vec<Box<dyn PipelineStage>>,
    buffer: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

impl MediaPipeline {
    /// Create a new media pipeline
    pub fn new() -> Self {
        MediaPipeline {
            stages: Vec::new(),
            buffer: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
    
    /// Add a stage to the pipeline
    pub fn add_stage(&mut self, stage: impl PipelineStage + 'static) {
        self.stages.push(Box::new(stage));
    }
    
    /// Process data through the pipeline
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
pub struct PipelineSource<S> where S: std::fmt::Debug {
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
pub struct PipelineSink<S> where S: std::fmt::Debug {
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
