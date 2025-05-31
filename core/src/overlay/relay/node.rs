//! Relay node implementation
//!
//! This module implements the core relay node functionality for the overlay network.

use libp2p::PeerId;
use crate::overlay::interface::{StreamId, OverlayError};
use crate::overlay::topology::TopologyManager;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Mutex};
use tracing::{debug, info, warn, error};

use super::config::RelayConfig;
use super::stats::RelayStats;
use super::stream::StreamRelay;
use super::types::{StreamChunk, RelayMessage, TaskHandles};

/// A relay node that handles stream data distribution
pub struct RelayNode {
    /// Local peer ID
    pub local_peer_id: PeerId,
    /// Configuration
    pub config: RelayConfig,
    /// Topology manager
    pub topology: Arc<TopologyManager>,
    /// Active streams
    pub streams: Arc<RwLock<HashMap<StreamId, StreamRelay>>>,
    /// Statistics
    pub stats: Arc<RwLock<RelayStats>>,
    /// Task handles
    tasks: Arc<RwLock<TaskHandles>>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Chunk handler function - excluded from Debug
    chunk_handler: Arc<RwLock<Option<Box<dyn Fn(PeerId, StreamChunk) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> + Send + Sync>>>>,
    /// Channel for sending chunks
    chunk_tx: Mutex<Option<mpsc::Sender<StreamChunk>>>,
    /// Channel for receiving chunks
    chunk_rx: Mutex<Option<mpsc::Receiver<StreamChunk>>>,
    /// Channel for sending messages
    message_tx: Mutex<Option<mpsc::Sender<RelayMessage>>>,
    /// Channel for receiving messages
    message_rx: Mutex<Option<mpsc::Receiver<RelayMessage>>>,
}

// Manual implementation of Debug for RelayNode
impl fmt::Debug for RelayNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RelayNode")
            .field("local_peer_id", &self.local_peer_id)
            .field("config", &self.config)
            .field("running", &self.running.load(Ordering::SeqCst))
            // Skip fields that don't implement Debug
            .finish()
    }
}

impl RelayNode {
    /// Create a new relay node
    pub fn new(
        local_peer_id: PeerId,
        config: RelayConfig,
        topology: Arc<TopologyManager>,
    ) -> Self {
        // Create channels
        let (chunk_tx, chunk_rx) = mpsc::channel(100);
        let (message_tx, message_rx) = mpsc::channel(100);
        
        Self {
            local_peer_id,
            config,
            topology,
            streams: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(RelayStats::new())),
            tasks: Arc::new(RwLock::new(TaskHandles {
                chunk_processor: None,
                message_processor: None,
                stats_reporter: None,
                stream_cleanup: None,
            })),
            running: Arc::new(AtomicBool::new(false)),
            chunk_handler: Arc::new(RwLock::new(None)),
            chunk_tx: Mutex::new(Some(chunk_tx)),
            chunk_rx: Mutex::new(Some(chunk_rx)),
            message_tx: Mutex::new(Some(message_tx)),
            message_rx: Mutex::new(Some(message_rx)),
        }
    }
    
    /// Set the chunk handler function
    pub async fn set_chunk_handler<F>(&self, handler: F) -> Result<(), OverlayError>
    where
        F: Fn(PeerId, StreamChunk) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> + Send + Sync + 'static,
    {
        if self.running.load(Ordering::SeqCst) {
            return Err(OverlayError::AlreadyRunning);
        }
        
        let mut chunk_handler = self.chunk_handler.write().await;
        *chunk_handler = Some(Box::new(handler));
        Ok(())
    }
    
    /// Start the relay node
    pub async fn start(&self) -> Result<(), OverlayError> {
        if self.running.load(Ordering::SeqCst) {
            return Err(OverlayError::AlreadyRunning);
        }
        
        // Start processor tasks
        self.start_processors().await?;
        
        // Start stats reporter task
        self.start_stats_reporter().await?;
        
        // Start stream cleanup task
        self.start_stream_cleanup().await?;
        
        // Set running flag
        self.running.store(true, Ordering::SeqCst);
        
        info!("Relay node started with peer ID: {}", self.local_peer_id);
        Ok(())
    }
    
    /// Stop the relay node
    pub async fn stop(&self) -> Result<(), OverlayError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(OverlayError::NotRunning);
        }
        
        // Set running flag
        self.running.store(false, Ordering::SeqCst);
        
        // Stop all tasks
        let mut tasks = self.tasks.write().await;
        
        // Abort all tasks
        if let Some(task) = tasks.chunk_processor.take() {
            task.abort();
        }
        
        if let Some(task) = tasks.message_processor.take() {
            task.abort();
        }
        
        if let Some(task) = tasks.stats_reporter.take() {
            task.abort();
        }
        
        if let Some(task) = tasks.stream_cleanup.take() {
            task.abort();
        }
        
        // Send stop message
        if let Some(tx) = self.message_tx.lock().await.as_ref() {
            let _ = tx.send(RelayMessage::Stop).await;
        }
        
        info!("Relay node stopped");
        Ok(())
    }
    
    /// Start background processor tasks
    async fn start_processors(&self) -> Result<(), OverlayError> {
        let mut tasks = self.tasks.write().await;
        
        // Start chunk processor
        let chunk_rx = self.chunk_rx.lock().await.take();
        if let Some(mut rx) = chunk_rx {
            let streams = self.streams.clone();
            let stats = self.stats.clone();
            let chunk_handler = self.chunk_handler.clone();
            let max_buffer_size = self.config.max_buffer_size;
            let running = self.running.clone();
            
            let task = tokio::spawn(async move {
                while let Some(chunk) = rx.recv().await {
                    if !running.load(Ordering::SeqCst) {
                        break;
                    }
                    
                    // Process the chunk
                    let result = Self::process_chunk_internal(
                        &streams,
                        chunk,
                        max_buffer_size,
                        &chunk_handler,
                        &stats,
                    ).await;
                    
                    if let Err(e) = result {
                        error!("Failed to process chunk: {}", e);
                    }
                }
            });
            
            tasks.chunk_processor = Some(task);
        }
        
        // Start message processor
        let message_rx = self.message_rx.lock().await.take();
        if let Some(mut rx) = message_rx {
            let streams = self.streams.clone();
            let stats = self.stats.clone();
            let chunk_handler = self.chunk_handler.clone();
            let running = self.running.clone();
            let max_buffer_size = self.config.max_buffer_size;
            
            let task = tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    if !running.load(Ordering::SeqCst) {
                        break;
                    }
                    
                    match msg {
                        RelayMessage::AddStream(stream_id, publisher) => {
                            let _ = Self::add_stream_internal(&streams, stream_id, publisher, &stats).await;
                        },
                        RelayMessage::RemoveStream(stream_id) => {
                            let _ = Self::remove_stream_internal(&streams, &stream_id, &stats).await;
                        },
                        RelayMessage::AddSubscriber(stream_id, peer_id) => {
                            let _ = Self::add_subscriber_internal(&streams, stream_id, peer_id, &stats).await;
                        },
                        RelayMessage::RemoveSubscriber(stream_id, peer_id) => {
                            let _ = Self::remove_subscriber_internal(&streams, stream_id, peer_id, &stats).await;
                        },
                        RelayMessage::RequestChunks(stream_id, peer_id, sequence) => {
                            let _ = Self::handle_request_chunks_internal(
                                &streams,
                                stream_id,
                                peer_id,
                                sequence,
                                &chunk_handler,
                            ).await;
                        },
                        RelayMessage::Chunk(chunk) => {
                            let _ = Self::process_chunk_internal(
                                &streams,
                                chunk,
                                max_buffer_size,
                                &chunk_handler,
                                &stats,
                            ).await;
                        },
                        RelayMessage::Stop => {
                            break;
                        },
                    }
                }
            });
            
            tasks.message_processor = Some(task);
        }
        
        Ok(())
    }
    
    /// Start statistics reporter task
    async fn start_stats_reporter(&self) -> Result<(), OverlayError> {
        let mut tasks = self.tasks.write().await;
        
        let stats = self.stats.clone();
        let streams = self.streams.clone();
        let interval = self.config.stats_interval;
        let running = self.running.clone();
        
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            
            while running.load(Ordering::SeqCst) {
                ticker.tick().await;
                
                // Update stats
                let mut stats_guard = stats.write().await;
                let streams_guard = streams.read().await;
                
                stats_guard.active_streams = streams_guard.len();
                
                // Count total connected peers
                let mut unique_peers = HashSet::new();
                for stream in streams_guard.values() {
                    unique_peers.insert(stream.publisher);
                    for subscriber in &stream.subscribers {
                        unique_peers.insert(*subscriber);
                    }
                }
                
                stats_guard.connected_peers = unique_peers.len();
                
                // Calculate bandwidth over the period
                let elapsed = stats_guard.period_start.elapsed();
                if elapsed.as_secs() > 0 {
                    stats_guard.outgoing_bandwidth = stats_guard.bytes_relayed / elapsed.as_secs();
                }
                
                // Reset period
                stats_guard.reset_period();
                
                debug!("Relay stats: {} active streams, {} connected peers, {} chunks relayed, {} bytes/sec",
                    stats_guard.active_streams,
                    stats_guard.connected_peers,
                    stats_guard.chunks_relayed,
                    stats_guard.outgoing_bandwidth);
            }
        });
        
        tasks.stats_reporter = Some(task);
        Ok(())
    }
    
    /// Start stream cleanup task
    async fn start_stream_cleanup(&self) -> Result<(), OverlayError> {
        let mut tasks = self.tasks.write().await;
        
        let streams = self.streams.clone();
        let stats = self.stats.clone();
        let interval = self.config.cleanup_interval;
        let timeout = self.config.inactivity_timeout;
        let running = self.running.clone();
        
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            
            while running.load(Ordering::SeqCst) {
                ticker.tick().await;
                
                // Find inactive streams
                let mut to_remove = Vec::new();
                
                {
                    let streams_guard = streams.read().await;
                    
                    for (stream_id, stream) in streams_guard.iter() {
                        if stream.is_inactive(timeout) {
                            to_remove.push(stream_id.clone());
                        }
                    }
                }
                
                // Remove inactive streams
                for stream_id in to_remove {
                    if let Err(e) = Self::remove_stream_internal(&streams, &stream_id, &stats).await {
                        error!("Failed to remove inactive stream {}: {}", stream_id, e);
                    } else {
                        info!("Removed inactive stream: {}", stream_id);
                    }
                }
            }
        });
        
        tasks.stream_cleanup = Some(task);
        Ok(())
    }
    
    /// Process a chunk (internal implementation)
    async fn process_chunk_internal(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        chunk: StreamChunk,
        max_buffer_size: usize,
        chunk_handler: &Arc<RwLock<Option<Box<dyn Fn(PeerId, StreamChunk) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> + Send + Sync>>>>,
        stats: &Arc<RwLock<RelayStats>>,
    ) -> Result<(), OverlayError> {
        let stream_id = chunk.stream_id.clone();
        
        // Record stats
        {
            let mut stats_guard = stats.write().await;
            stats_guard.record_chunk(chunk.data.len());
        }
        
        // Check if we have this stream
        let subscribers = {
            let mut streams_guard = streams.write().await;
            
            if let Some(stream) = streams_guard.get_mut(&stream_id) {
                // Add chunk to stream buffer
                stream.add_chunk(chunk.clone(), max_buffer_size);
                
                // Get subscribers for this stream
                stream.subscribers.clone()
            } else {
                // Stream not found, just ignore the chunk
                return Err(OverlayError::StreamNotFound(stream_id));
            }
        };
        
        // Send chunk to all subscribers
        if let Some(handler) = &*chunk_handler.read().await {
            for subscriber in subscribers {
                if let Err(e) = handler(subscriber, chunk.clone()).await {
                    error!("Failed to send chunk to {}: {}", subscriber, e);
                }
            }
        } else {
            return Err(OverlayError::NoChunkHandler);
        }
        
        Ok(())
    }
    
    /// Add a stream (internal implementation)
    async fn add_stream_internal(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
        publisher: PeerId,
        stats: &Arc<RwLock<RelayStats>>,
    ) -> Result<(), OverlayError> {
        let mut streams_guard = streams.write().await;
        
        if streams_guard.contains_key(&stream_id) {
            return Err(OverlayError::StreamAlreadyExists(stream_id));
        }
        
        // Create new stream relay
        let stream = StreamRelay::new(stream_id.clone(), publisher);
        
        // Add to streams
        streams_guard.insert(stream_id.clone(), stream);
        
        // Update stats
        {
            let mut stats_guard = stats.write().await;
            stats_guard.active_streams = streams_guard.len();
        }
        
        info!("Added stream: {}", stream_id);
        Ok(())
    }
    
    /// Remove a stream (internal implementation)
    async fn remove_stream_internal(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: &StreamId,
        stats: &Arc<RwLock<RelayStats>>,
    ) -> Result<(), OverlayError> {
        let mut streams_guard = streams.write().await;
        
        if !streams_guard.contains_key(stream_id) {
            return Err(OverlayError::StreamNotFound(stream_id.clone()));
        }
        
        // Remove stream
        streams_guard.remove(stream_id);
        
        // Update stats
        {
            let mut stats_guard = stats.write().await;
            stats_guard.active_streams = streams_guard.len();
        }
        
        info!("Removed stream: {}", stream_id);
        Ok(())
    }
    
    /// Add a subscriber to a stream (internal implementation)
    async fn add_subscriber_internal(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
        peer_id: PeerId,
        stats: &Arc<RwLock<RelayStats>>,
    ) -> Result<(), OverlayError> {
        let mut streams_guard = streams.write().await;
        
        if let Some(stream) = streams_guard.get_mut(&stream_id) {
            // Add subscriber
            stream.add_subscriber(peer_id);
            
            // Update stats
            {
                let mut stats_guard = stats.write().await;
                stats_guard.connected_peers += 1;
            }
            
            info!("Added subscriber {} to stream {}", peer_id, stream_id);
            Ok(())
        } else {
            Err(OverlayError::StreamNotFound(stream_id))
        }
    }
    
    /// Remove a subscriber from a stream (internal implementation)
    async fn remove_subscriber_internal(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
        peer_id: PeerId,
        stats: &Arc<RwLock<RelayStats>>,
    ) -> Result<(), OverlayError> {
        let mut streams_guard = streams.write().await;
        
        if let Some(stream) = streams_guard.get_mut(&stream_id) {
            // Remove subscriber
            if stream.remove_subscriber(&peer_id) {
                // Update stats
                {
                    let mut stats_guard = stats.write().await;
                    if stats_guard.connected_peers > 0 {
                        stats_guard.connected_peers -= 1;
                    }
                }
                
                info!("Removed subscriber {} from stream {}", peer_id, stream_id);
                Ok(())
            } else {
                Err(OverlayError::PeerNotFound(peer_id))
            }
        } else {
            Err(OverlayError::StreamNotFound(stream_id))
        }
    }
    
    /// Handle a request for chunks since a sequence number (internal implementation)
    async fn handle_request_chunks_internal(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
        peer_id: PeerId,
        sequence: u64,
        chunk_handler: &Arc<RwLock<Option<Box<dyn Fn(PeerId, StreamChunk) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> + Send + Sync>>>>,
    ) -> Result<(), OverlayError> {
        let chunks = {
            let streams_guard = streams.read().await;
            
            if let Some(stream) = streams_guard.get(&stream_id) {
                stream.get_chunks_since(sequence)
            } else {
                // Stream not found
                return Err(OverlayError::StreamNotFound(stream_id));
            }
        };
        
        // Send chunks to the peer
        if let Some(handler) = &*chunk_handler.read().await {
            for chunk in chunks {
                if let Err(e) = handler(peer_id.clone(), chunk).await {
                    error!("Failed to send requested chunk to {}: {}", peer_id, e);
                    return Err(e);
                }
            }
        } else {
            return Err(OverlayError::NoChunkHandler);
        }
        
        Ok(())
    }
    
    /// Publish a chunk to a stream
    pub async fn publish_chunk(&self, chunk: StreamChunk) -> Result<(), OverlayError> {
        // Validate chunk size
        if chunk.data.len() > self.config.max_chunk_size {
            return Err(OverlayError::RelayError(
                format!("Chunk too large: {} bytes (max {})",
                    chunk.data.len(), self.config.max_chunk_size)
            ));
        }
        
        if let Some(tx) = self.chunk_tx.lock().await.as_ref() {
            if let Err(e) = tx.send(chunk).await {
                error!("Failed to send chunk: {}", e);
                return Err(OverlayError::RelayError(format!("Failed to send chunk: {}", e)));
            }
            Ok(())
        } else {
            Err(OverlayError::NotRunning)
        }
    }
    
    /// Add a stream
    pub async fn add_stream(&self, stream_id: StreamId, publisher: PeerId) -> Result<(), OverlayError> {
        if let Some(tx) = self.message_tx.lock().await.as_ref() {
            if let Err(e) = tx.send(RelayMessage::AddStream(stream_id.clone(), publisher)).await {
                error!("Failed to send add stream message: {}", e);
                return Err(OverlayError::RelayError(format!("Failed to send add stream message: {}", e)));
            }
            Ok(())
        } else {
            Err(OverlayError::NotRunning)
        }
    }
    
    /// Remove a stream
    pub async fn remove_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        if let Some(tx) = self.message_tx.lock().await.as_ref() {
            if let Err(e) = tx.send(RelayMessage::RemoveStream(stream_id.clone())).await {
                error!("Failed to send remove stream message: {}", e);
                return Err(OverlayError::RelayError(format!("Failed to send remove stream message: {}", e)));
            }
            Ok(())
        } else {
            Err(OverlayError::NotRunning)
        }
    }
    
    /// Add a subscriber to a stream
    pub async fn add_subscriber(&self, stream_id: &StreamId, peer_id: PeerId) -> Result<(), OverlayError> {
        if let Some(tx) = self.message_tx.lock().await.as_ref() {
            if let Err(e) = tx.send(RelayMessage::AddSubscriber(stream_id.clone(), peer_id)).await {
                error!("Failed to send add subscriber message: {}", e);
                return Err(OverlayError::RelayError(format!("Failed to send add subscriber message: {}", e)));
            }
            Ok(())
        } else {
            Err(OverlayError::NotRunning)
        }
    }
    
    /// Remove a subscriber from a stream
    pub async fn remove_subscriber(&self, stream_id: &StreamId, peer_id: &PeerId) -> Result<(), OverlayError> {
        if let Some(tx) = self.message_tx.lock().await.as_ref() {
            if let Err(e) = tx.send(RelayMessage::RemoveSubscriber(stream_id.clone(), peer_id.clone())).await {
                error!("Failed to send remove subscriber message: {}", e);
                return Err(OverlayError::RelayError(format!("Failed to send remove subscriber message: {}", e)));
            }
            Ok(())
        } else {
            Err(OverlayError::NotRunning)
        }
    }
    
    /// Request chunks since a sequence number
    pub async fn request_chunks(&self, stream_id: &StreamId, peer_id: &PeerId, sequence: u64) -> Result<(), OverlayError> {
        if let Some(tx) = self.message_tx.lock().await.as_ref() {
            if let Err(e) = tx.send(RelayMessage::RequestChunks(stream_id.clone(), peer_id.clone(), sequence)).await {
                error!("Failed to send request chunks message: {}", e);
                return Err(OverlayError::RelayError(format!("Failed to send request chunks message: {}", e)));
            }
            Ok(())
        } else {
            Err(OverlayError::NotRunning)
        }
    }
    
    /// Get statistics
    pub async fn get_stats(&self) -> RelayStats {
        let stats = self.stats.read().await;
        stats.clone()
    }
    
    /// Get active streams
    pub async fn get_active_streams(&self) -> Vec<StreamId> {
        let streams = self.streams.read().await;
        streams.keys().cloned().collect()
    }
    
    /// Get stream info
    pub async fn get_stream_info(&self, stream_id: &StreamId) -> Option<(PeerId, HashSet<PeerId>)> {
        let streams = self.streams.read().await;
        
        streams.get(stream_id).map(|stream| 
            (stream.publisher.clone(), stream.subscribers.clone())
        )
    }
}
