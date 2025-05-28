//! Stream relay implementation
//!
//! This module handles the relaying of stream data between peers.

use libp2p::PeerId;
use crate::overlay::interface::{StreamId, OverlayError};
use crate::overlay::topology::TopologyManager;

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Mutex};
use tracing::{debug, info, warn, error};

/// Buffer for stream chunks
type StreamBuffer = VecDeque<StreamChunk>;

/// A chunk of stream data
#[derive(Debug, Clone)]
pub struct StreamChunk {
    /// Chunk identifier
    pub id: u64,
    /// Stream ID this chunk belongs to
    pub stream_id: StreamId,
    /// The data
    pub data: Vec<u8>,
    /// Chunk timestamp
    pub timestamp: u64,
    /// Sequence number
    pub sequence: u64,
    /// Content type
    pub content_type: String,
    /// Whether this is a keyframe
    pub is_keyframe: bool,
    /// Source peer ID
    pub source: Option<PeerId>,
}

/// Statistics for a relay
#[derive(Debug, Clone)]
pub struct RelayStats {
    /// Number of chunks relayed
    pub chunks_relayed: u64,
    /// Number of bytes relayed
    pub bytes_relayed: u64,
    /// Average chunk size
    pub avg_chunk_size: u64,
    /// Number of active streams
    pub active_streams: usize,
    /// Number of connected peers
    pub connected_peers: usize,
    /// Incoming bandwidth (bytes/second)
    pub incoming_bandwidth: u64,
    /// Outgoing bandwidth (bytes/second)
    pub outgoing_bandwidth: u64,
    /// Measurement period start
    pub period_start: Instant,
}

impl Default for RelayStats {
    fn default() -> Self {
        Self {
            chunks_relayed: 0,
            bytes_relayed: 0,
            avg_chunk_size: 0,
            active_streams: 0,
            connected_peers: 0,
            incoming_bandwidth: 0,
            outgoing_bandwidth: 0,
            period_start: Instant::now(),
        }
    }
}

impl RelayStats {
    /// Create new relay stats
    pub fn new() -> Self {
        Self {
            period_start: Instant::now(),
            ..Default::default()
        }
    }
    
    /// Reset the measurement period
    pub fn reset_period(&mut self) {
        self.period_start = Instant::now();
        self.incoming_bandwidth = 0;
        self.outgoing_bandwidth = 0;
    }
    
    /// Record a relayed chunk
    pub fn record_chunk(&mut self, chunk_size: usize) {
        self.chunks_relayed += 1;
        self.bytes_relayed += chunk_size as u64;
        
        // Update average chunk size
        if self.chunks_relayed > 0 {
            self.avg_chunk_size = self.bytes_relayed / self.chunks_relayed;
        }
    }
}

/// Configuration for relay nodes
#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// Maximum buffer size per stream (in chunks)
    pub max_buffer_size: usize,
    /// Maximum chunk size
    pub max_chunk_size: usize,
    /// Statistics reporting interval
    pub stats_interval: Duration,
    /// Stream cleanup interval (remove inactive streams)
    pub cleanup_interval: Duration,
    /// Stream inactivity timeout (how long before a stream is considered inactive)
    pub inactivity_timeout: Duration,
    /// Maximum number of streams to relay
    pub max_streams: usize,
    /// Whether to enable bandwidth limiting
    pub enable_bandwidth_limit: bool,
    /// Maximum outgoing bandwidth (bytes/second)
    pub max_outgoing_bandwidth: u64,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 100,
            max_chunk_size: 64 * 1024, // 64 KB
            stats_interval: Duration::from_secs(5),
            cleanup_interval: Duration::from_secs(30),
            inactivity_timeout: Duration::from_secs(60),
            max_streams: 10,
            enable_bandwidth_limit: false,
            max_outgoing_bandwidth: 5 * 1024 * 1024, // 5 MB/s
        }
    }
}

/// A relay for a specific stream
#[derive(Debug)]
pub struct StreamRelay {
    /// Stream ID
    pub stream_id: StreamId,
    /// Publisher peer ID
    pub publisher: PeerId,
    /// Subscribers receiving this stream
    pub subscribers: HashSet<PeerId>,
    /// Buffer of recent chunks for late-joining peers
    pub buffer: StreamBuffer,
    /// Last activity timestamp
    pub last_activity: Instant,
    /// Current sequence number
    pub current_sequence: u64,
    /// Whether this relay is active
    pub active: bool,
    /// Stream metadata
    pub metadata: HashMap<String, String>,
}

impl StreamRelay {
    /// Create a new stream relay
    pub fn new(stream_id: StreamId, publisher: PeerId) -> Self {
        Self {
            stream_id,
            publisher,
            subscribers: HashSet::new(),
            buffer: VecDeque::with_capacity(100),
            last_activity: Instant::now(),
            current_sequence: 0,
            active: true,
            metadata: HashMap::new(),
        }
    }
    
    /// Add a chunk to the buffer
    pub fn add_chunk(&mut self, chunk: StreamChunk, max_buffer_size: usize) {
        self.last_activity = Instant::now();
        
        // Update sequence number
        if chunk.sequence > self.current_sequence {
            self.current_sequence = chunk.sequence;
        }
        
        // Add to buffer
        self.buffer.push_back(chunk);
        
        // Trim buffer if needed
        while self.buffer.len() > max_buffer_size {
            self.buffer.pop_front();
        }
    }
    
    /// Add a subscriber
    pub fn add_subscriber(&mut self, peer_id: PeerId) -> bool {
        self.subscribers.insert(peer_id)
    }
    
    /// Remove a subscriber
    pub fn remove_subscriber(&mut self, peer_id: &PeerId) -> bool {
        self.subscribers.remove(peer_id)
    }
    
    /// Check if the stream is inactive
    pub fn is_inactive(&self, timeout: Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }
    
    /// Get chunks since a sequence number
    pub fn get_chunks_since(&self, sequence: u64) -> Vec<StreamChunk> {
        self.buffer.iter()
            .filter(|chunk| chunk.sequence > sequence)
            .cloned()
            .collect()
    }
}

/// A message for the relay manager
#[derive(Debug)]
enum RelayMessage {
    /// A new chunk to relay
    Chunk(StreamChunk),
    /// Add a stream
    AddStream(StreamId, PeerId),
    /// Remove a stream
    RemoveStream(StreamId),
    /// Add a subscriber to a stream
    AddSubscriber(StreamId, PeerId),
    /// Remove a subscriber from a stream
    RemoveSubscriber(StreamId, PeerId),
    /// Request chunks since a sequence number
    RequestChunks(StreamId, PeerId, u64),
    /// Stop the relay manager
    Stop,
}

/// A relay node that handles stream data distribution
pub struct RelayNode {
    /// Local peer ID
    local_peer_id: PeerId,
    /// Configuration
    config: RelayConfig,
    /// Topology manager
    topology: Arc<TopologyManager>,
    /// Active streams
    streams: Arc<RwLock<HashMap<StreamId, StreamRelay>>>,
    /// Stream chunk sender
    chunk_tx: Mutex<Option<mpsc::Sender<StreamChunk>>>,
    /// Stream chunk receiver
    chunk_rx: Mutex<Option<mpsc::Receiver<StreamChunk>>>,
    /// Message sender
    message_tx: Mutex<Option<mpsc::Sender<RelayMessage>>>,
    /// Message receiver
    message_rx: Mutex<Option<mpsc::Receiver<RelayMessage>>>,
    /// Handler for relayed chunks
    chunk_handler: Arc<RwLock<Option<Box<dyn Fn(PeerId, StreamChunk) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> + Send + Sync>>>>,
    /// Statistics
    stats: Arc<RwLock<RelayStats>>,
    /// Task handles
    task_handles: Mutex<TaskHandles>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Stats sender
    stats_tx: Option<mpsc::Sender<RelayStats>>,
}

struct TaskHandles {
    worker_task: Option<tokio::task::JoinHandle<()>>,
    stats_task: Option<tokio::task::JoinHandle<()>>,
    cleanup_task: Option<tokio::task::JoinHandle<()>>,
}

impl RelayNode {
    /// Create a new relay node
    pub fn new(
        local_peer_id: PeerId,
        config: RelayConfig,
        topology: Arc<TopologyManager>,
    ) -> Self {
        Self {
            local_peer_id,
            config,
            topology,
            streams: Arc::new(RwLock::new(HashMap::new())),
            chunk_tx: Mutex::new(None),
            chunk_rx: Mutex::new(None),
            message_tx: Mutex::new(None),
            message_rx: Mutex::new(None),
            running: Arc::new(AtomicBool::new(false)),
            stats_tx: None,
            chunk_handler: Arc::new(RwLock::new(None)),
            stats: Arc::new(RwLock::new(RelayStats::new())),
            task_handles: Mutex::new(TaskHandles {
                worker_task: None,
                stats_task: None,
                cleanup_task: None,
            }),
        }
    }
    
    /// Set the chunk handler function
    pub async fn set_chunk_handler<F, Fut>(&self, handler: F) -> Result<(), OverlayError>
    where
        F: Fn(PeerId, StreamChunk) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), OverlayError>> + Send + 'static,
    {
        let mut chunk_handler = self.chunk_handler.write().await;
        *chunk_handler = Some(Box::new(move |peer_id, chunk| {
            let fut = handler(peer_id, chunk);
            Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
        }));
        Ok(())
    }
    
    /// Start the relay node
    pub async fn start(&self) -> Result<(), OverlayError> {
        // Set running flag to true
        self.running.store(true, Ordering::SeqCst);
        
        // Start the worker task
        self.start_worker().await?;
        
        // Start the stats task
        self.start_stats_task().await?;
        
        // Start the cleanup task
        self.start_cleanup_task().await?;
        
        info!("Relay node started with peer ID: {}", self.local_peer_id);
        Ok(())
    }
    
    /// Start the worker task that processes messages and chunks
    async fn start_worker(&self) -> Result<(), OverlayError> {
        // Create channels for communication with a default capacity of 1000
        let (chunk_tx, mut chunk_rx) = mpsc::channel(1000);
        let (message_tx, mut message_rx) = mpsc::channel(1000);
        
        // Store the senders
        *self.chunk_tx.lock().await = Some(chunk_tx);
        *self.message_tx.lock().await = Some(message_tx);
        
        // Clone the necessary fields
        let running = Arc::clone(&self.running);
        let config = self.config.clone();
        let streams = Arc::clone(&self.streams);
        let topology = Arc::clone(&self.topology);
        let stats = Arc::clone(&self.stats);
        let chunk_handler = self.chunk_handler.clone();
        let local_peer_id = self.local_peer_id.clone();
        
        // Create a new task that will process messages and chunks
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(100));
            
            'worker_loop: while running.load(Ordering::SeqCst) {
                let streams_clone = Arc::clone(&streams);
                let topology_clone = Arc::clone(&topology);
                let chunk_handler_clone = chunk_handler.clone();
                let stats_clone = Arc::clone(&stats);
                let local_peer_id_clone = local_peer_id.clone();
                
                tokio::select! {
                    // Handle incoming chunks
                    Some(chunk) = chunk_rx.recv() => {
                        if let Err(e) = Self::handle_chunk(
                            &streams_clone,
                            &topology_clone,
                            &chunk,
                            &chunk_handler_clone,
                            &config,
                            &stats_clone,
                            &local_peer_id_clone,
                        ).await {
                            error!("Error handling chunk: {}", e);
                        }
                    },
                    
                    // Handle incoming messages
                    Some(message) = message_rx.recv() => {
                        let streams = Arc::clone(&streams);
                        let _topology = Arc::clone(&topology);
                        let chunk_handler = chunk_handler.clone();
                        
                        if let Err(e) = match message {
                            RelayMessage::Chunk(chunk) => {
                                // Handle chunk directly
                                if let Some(handler) = &*chunk_handler.read().await {
                                    let _ = handler(local_peer_id.clone(), chunk).await;
                                }
                                Ok(())
                            },
                            RelayMessage::AddStream(stream_id, publisher) => {
                                Self::handle_add_stream(&streams, stream_id, publisher).await
                            },
                            RelayMessage::RemoveStream(stream_id) => {
                                Self::handle_remove_stream(&streams, stream_id).await
                            },
                            RelayMessage::AddSubscriber(stream_id, peer_id) => {
                                Self::handle_add_subscriber(&streams, stream_id, peer_id).await
                            },
                            RelayMessage::RemoveSubscriber(stream_id, peer_id) => {
                                Self::handle_remove_subscriber(&streams, stream_id, peer_id).await
                            },
                            RelayMessage::RequestChunks(stream_id, peer_id, sequence) => {
                                Self::handle_request_chunks(
                                    &streams,
                                    stream_id,
                                    peer_id,
                                    sequence,
                                    &chunk_handler,
                                ).await
                            },
                            RelayMessage::Stop => {
                                // Stop the worker
                                break 'worker_loop;
                            }
                        } {
                            error!("Error handling message: {}", e);
                        }
                    },
                    
                    // Check if we should stop
                    _ = ticker.tick() => {
                        if !running.load(Ordering::SeqCst) {
                            break 'worker_loop;
                        }
                    },
                    
                    // Handle shutdown
                    else => break 'worker_loop,
                }
            }
            
            debug!("Relay worker task shutting down");
        });
        
        // Store the task handle
        let mut task_handles = self.task_handles.lock().await;
        task_handles.worker_task = Some(handle);
        
        Ok(())
    }
    async fn start_stats_task(&self) -> Result<(), OverlayError> {
        // Clone required references
        let stats_tx = self.stats_tx.clone();
        let stats = self.stats.clone();
        let streams = self.streams.clone();
        let running = self.running.clone();
        let stats_interval = self.config.stats_interval;
        
        // Create a new task that will report stats periodically
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(stats_interval);
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                // Get current stats
                let mut stats_guard = stats.write().await;
                let streams_guard = streams.read().await;
                
                // Count unique peers across all streams
                let mut unique_peers = std::collections::HashSet::new();
                for stream in streams_guard.values() {
                    unique_peers.insert(stream.publisher.clone());
                    for subscriber in &stream.subscribers {
                        unique_peers.insert(subscriber.clone());
                    }
                }
                
                // Update stats
                stats_guard.active_streams = streams_guard.len();
                stats_guard.connected_peers = unique_peers.len();
                
                // Calculate bandwidth if enough time has passed
                let elapsed = stats_guard.period_start.elapsed().as_secs();
                if elapsed > 0 {
                    stats_guard.incoming_bandwidth = stats_guard.bytes_relayed.saturating_div(elapsed as u64);
                    // For now, outgoing = incoming (could be refined)
                    stats_guard.outgoing_bandwidth = stats_guard.incoming_bandwidth;
                }
                
                // Clone stats for sending while we still have the lock
                let stats_data = stats_guard.clone();
                
                // Release the locks before sending
                drop(stats_guard);
                drop(streams_guard);
                
                // Send stats if there's a subscriber
                if let Some(tx) = &stats_tx {
                    if let Err(e) = tx.send(stats_data).await {
                        error!("Failed to send stats: {}", e);
                        // Don't break, keep trying to send stats
                    }
                }
            }
            
            debug!("Stats task completed");
        });
        
        // Store the stats task handle
        let mut task_handles = self.task_handles.lock().await;
        task_handles.stats_task = Some(handle);
        
        Ok(())
    }
    
    /// Start the cleanup task
    async fn start_cleanup_task(&self) -> Result<(), OverlayError> {
        // Clone required references
        let streams = self.streams.clone();
        let config = self.config.clone();
        let running = self.running.clone();
        
        // Create a new task that will clean up inactive streams
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.cleanup_interval);
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                // Find and remove inactive streams
                let mut streams_guard = streams.write().await;
                
                let mut to_remove = Vec::new();
                
                // Check each stream for inactivity
                for (stream_id, stream) in streams_guard.iter() {
                    if stream.is_inactive(config.inactivity_timeout) {
                        to_remove.push(stream_id.clone());
                    }
                }
                
                // Remove inactive streams
                for stream_id in to_remove {
                    if let Some(stream) = streams_guard.remove(&stream_id) {
                        debug!("Removed inactive stream: {} (last active: {:?})", 
                            stream_id, stream.last_activity);
                    }
                }
                
                // Explicitly drop the guard to release the lock
                drop(streams_guard);
            }
            
            debug!("Cleanup task completed");
        });
        
        // Store the cleanup task handle
        let mut task_handles = self.task_handles.lock().await;
        task_handles.cleanup_task = Some(handle);
        
        Ok(())
    }
    
    /// Handle an incoming chunk of stream data
    async fn handle_chunk(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        _topology: &Arc<TopologyManager>,
        chunk: &StreamChunk,
        chunk_handler: &Arc<RwLock<Option<Box<dyn Fn(PeerId, StreamChunk) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> + Send + Sync>>>>,
        _config: &RelayConfig,
        stats: &RwLock<RelayStats>,
        local_peer_id: &PeerId,
    ) -> Result<(), OverlayError> {
        // Get a read lock on the streams
        let streams_guard = streams.read().await;
        
        // Look up the stream
        let stream = streams_guard.get(&chunk.stream_id)
            .ok_or_else(|| OverlayError::StreamNotFound(chunk.stream_id.clone()))?;
        
        // Clone the chunk for each subscriber
        let subscribers: Vec<PeerId> = stream.subscribers
            .iter()
            .filter(|peer_id| {
                // Skip the source peer and local peer
                if let Some(source) = &chunk.source {
                    *peer_id != source && *peer_id != local_peer_id
                } else {
                    *peer_id != local_peer_id
                }
            })
            .cloned()
            .collect();
            
        // Release the read lock before awaiting
        drop(streams_guard);
        
        // Update stats
        {
            let mut stats_guard = stats.write().await;
            stats_guard.record_chunk(chunk.data.len());
        }
        
        // Forward the chunk to each subscriber
        if let Some(handler) = &*chunk_handler.read().await {
            for subscriber in subscribers {
                let mut subscriber_chunk = chunk.clone();
                subscriber_chunk.source = Some(local_peer_id.clone());
                
                if let Err(e) = handler(subscriber.clone(), subscriber_chunk).await {
                    error!("Failed to relay chunk to {}: {}", subscriber, e);
                }
            }
        }
        
        Ok(())
    }
    
    /// Stop the relay node and all associated tasks
    pub async fn stop(&self) -> Result<(), OverlayError> {
        debug!("Stopping relay node...");
        
        // Set running flag to false
        self.running.store(false, Ordering::SeqCst);
        
        // Send stop message to worker
        if let Some(tx) = self.message_tx.lock().await.as_ref() {
            if let Err(e) = tx.send(RelayMessage::Stop).await {
                error!("Failed to send stop message to worker: {}", e);
            }
        } else {
            error!("Message sender not initialized");
        }
        
        // Wait for all tasks to complete
        let mut handles = Vec::new();
        
        // Get all task handles
        let mut task_handles = self.task_handles.lock().await;
        if let Some(handle) = task_handles.worker_task.take() {
            handles.push(handle);
        }
        if let Some(handle) = task_handles.stats_task.take() {
            handles.push(handle);
        }
        if let Some(handle) = task_handles.cleanup_task.take() {
            handles.push(handle);
        }
        
        // Drop the lock before awaiting
        drop(task_handles);
        
        // Wait for all tasks to complete with a timeout
        let timeout = tokio::time::sleep(std::time::Duration::from_secs(5));
        tokio::pin!(timeout);
        
        tokio::select! {
            _ = async {
                for handle in handles {
                    let _ = handle.await;
                }
            } => {
                debug!("All tasks completed gracefully");
            }
            _ = &mut timeout => {
                warn!("Timed out waiting for tasks to complete, forcing shutdown");
                // Abort any remaining tasks
                let mut task_handles = self.task_handles.lock().await;
                if let Some(handle) = task_handles.worker_task.take() {
                    handle.abort();
                }
                if let Some(handle) = task_handles.stats_task.take() {
                    handle.abort();
                }
                if let Some(handle) = task_handles.cleanup_task.take() {
                    handle.abort();
                }
            }
        }
        
        Ok(())
    }
    
    /// Handle adding a new stream
    async fn handle_add_stream(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
        publisher: PeerId,
    ) -> Result<(), OverlayError> {
        let mut streams_guard = streams.write().await;
        
        if !streams_guard.contains_key(&stream_id) {
            let relay = StreamRelay::new(stream_id.clone(), publisher);
            streams_guard.insert(stream_id.clone(), relay);
            debug!("Added new stream: {:?}", stream_id);
        }
        
        Ok(())
    }
    
    /// Handle removing a stream
    async fn handle_remove_stream(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
    ) -> Result<(), OverlayError> {
        let mut streams_guard = streams.write().await;
        
        if streams_guard.remove(&stream_id).is_some() {
            debug!("Removed stream: {:?}", stream_id);
        }
        
        Ok(())
    }
    
    /// Handle adding a subscriber to a stream
    async fn handle_add_subscriber(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
        peer_id: PeerId,
    ) -> Result<(), OverlayError> {
        let mut streams_guard = streams.write().await;
        
        if let Some(stream) = streams_guard.get_mut(&stream_id) {
            stream.add_subscriber(peer_id.clone());
            debug!("Added subscriber {} to stream {:?}", peer_id, stream_id);
        } else {
            return Err(OverlayError::StreamNotFound(stream_id));
        }
        
        Ok(())
    }
    
    /// Handle removing a subscriber from a stream
    async fn handle_remove_subscriber(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
        peer_id: PeerId,
    ) -> Result<(), OverlayError> {
        let mut streams_guard = streams.write().await;
        
        if let Some(stream) = streams_guard.get_mut(&stream_id) {
            stream.remove_subscriber(&peer_id);
            debug!("Removed subscriber {} from stream {:?}", peer_id, stream_id);
        } else {
            return Err(OverlayError::StreamNotFound(stream_id));
        }
        
        Ok(())
    }
    
    /// Handle a request for chunks since a sequence number
    async fn handle_request_chunks(
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

/// A manager for handling multiple relay nodes
pub struct RelayManager {
    /// Local peer ID
    local_peer_id: PeerId,
    /// Configuration
    config: RelayConfig,
    /// Topology manager
    topology: Arc<TopologyManager>,
    /// Local relay node
    relay_node: Arc<RelayNode>,
}

impl RelayManager {
    /// Create a new relay manager
    pub fn new(
        local_peer_id: PeerId,
        config: RelayConfig,
        topology: Arc<TopologyManager>,
    ) -> Self {
        let relay_node = Arc::new(RelayNode::new(
            local_peer_id.clone(),
            config.clone(),
            topology.clone(),
        ));
        
        Self {
            local_peer_id,
            config,
            topology,
            relay_node,
        }
    }
    
    /// Get the relay node
    pub fn relay_node(&self) -> Arc<RelayNode> {
        self.relay_node.clone()
    }
    
    /// Start the relay manager
    pub async fn start(&self) -> Result<(), OverlayError> {
        // Start the relay node
        self.relay_node.start().await?;
        
        Ok(())
    }
    
    /// Stop the relay manager
    pub async fn stop(&self) -> Result<(), OverlayError> {
        // Stop the relay node
        self.relay_node.stop().await?;
        
        Ok(())
    }
} 