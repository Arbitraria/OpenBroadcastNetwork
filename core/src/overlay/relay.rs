//! Stream relay implementation
//!
//! This module handles the relaying of stream data between peers.

use crate::overlay::peer::{Peer, PeerId, PeerInfo, PeerRole};
use crate::overlay::interface::{StreamId, OverlayError};
use crate::overlay::topology::{TopologyManager, RelayTree};

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Mutex};
use tokio::time;
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
#[derive(Debug, Clone, Default)]
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
    /// Active stream relays
    streams: RwLock<HashMap<StreamId, StreamRelay>>,
    /// Stream chunk sender
    chunk_tx: mpsc::Sender<StreamChunk>,
    /// Stream chunk receiver
    chunk_rx: Mutex<mpsc::Receiver<StreamChunk>>,
    /// Message sender
    message_tx: mpsc::Sender<RelayMessage>,
    /// Message receiver
    message_rx: Mutex<mpsc::Receiver<RelayMessage>>,
    /// Handler for relayed chunks
    chunk_handler: Option<Box<dyn Fn(PeerId, StreamChunk) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> + Send + Sync>>,
    /// Statistics
    stats: RwLock<RelayStats>,
    /// Worker task handle
    worker_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Stats task handle
    stats_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Cleanup task handle
    cleanup_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RelayNode {
    /// Create a new relay node
    pub fn new(
        local_peer_id: PeerId,
        config: RelayConfig,
        topology: Arc<TopologyManager>,
    ) -> Self {
        let (chunk_tx, chunk_rx) = mpsc::channel(100);
        let (message_tx, message_rx) = mpsc::channel(100);
        
        Self {
            local_peer_id,
            config,
            topology,
            streams: RwLock::new(HashMap::new()),
            chunk_tx,
            chunk_rx: Mutex::new(chunk_rx),
            message_tx,
            message_rx: Mutex::new(message_rx),
            chunk_handler: None,
            stats: RwLock::new(RelayStats::new()),
            worker_task: Mutex::new(None),
            stats_task: Mutex::new(None),
            cleanup_task: Mutex::new(None),
        }
    }
    
    /// Set the chunk handler function
    pub fn set_chunk_handler<F, Fut>(&mut self, handler: F)
    where
        F: Fn(PeerId, StreamChunk) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), OverlayError>> + Send + 'static,
    {
        self.chunk_handler = Some(Box::new(move |peer_id, chunk| {
            Box::pin(handler(peer_id, chunk))
        }));
    }
    
    /// Start the relay node
    pub async fn start(&self) -> Result<(), OverlayError> {
        // Start the worker task
        self.start_worker().await?;
        
        // Start the stats task
        self.start_stats_task().await?;
        
        // Start the cleanup task
        self.start_cleanup_task().await?;
        
        info!("Relay node started with peer ID: {}", self.local_peer_id);
        
        Ok(())
    }
    
    /// Stop the relay node
    pub async fn stop(&self) -> Result<(), OverlayError> {
        // Send stop message
        if let Err(e) = self.message_tx.send(RelayMessage::Stop).await {
            warn!("Failed to send stop message: {}", e);
        }
        
        // Abort worker task
        let mut worker_task = self.worker_task.lock().await;
        if let Some(task) = worker_task.take() {
            task.abort();
        }
        
        // Abort stats task
        let mut stats_task = self.stats_task.lock().await;
        if let Some(task) = stats_task.take() {
            task.abort();
        }
        
        // Abort cleanup task
        let mut cleanup_task = self.cleanup_task.lock().await;
        if let Some(task) = cleanup_task.take() {
            task.abort();
        }
        
        info!("Relay node stopped");
        
        Ok(())
    }
    
    /// Start the worker task that processes messages and chunks
    async fn start_worker(&self) -> Result<(), OverlayError> {
        let mut worker_task = self.worker_task.lock().await;
        
        // Don't start if already running
        if worker_task.is_some() {
            return Ok(());
        }
        
        // Clone required references
        let message_rx = self.message_rx.clone();
        let chunk_rx = self.chunk_rx.clone();
        let streams = self.streams.clone();
        let config = self.config.clone();
        let stats = self.stats.clone();
        let topology = self.topology.clone();
        let local_peer_id = self.local_peer_id.clone();
        let chunk_handler = self.chunk_handler.clone();
        
        // Start worker task
        let task = tokio::spawn(async move {
            let mut message_receiver = message_rx.lock().await;
            let mut chunk_receiver = chunk_rx.lock().await;
            
            loop {
                tokio::select! {
                    Some(message) = message_receiver.recv() => {
                        match message {
                            RelayMessage::Chunk(chunk) => {
                                Self::handle_chunk(
                                    &streams,
                                    &topology,
                                    &chunk,
                                    &chunk_handler,
                                    &config,
                                    &stats,
                                    &local_peer_id,
                                ).await;
                            },
                            RelayMessage::AddStream(stream_id, publisher) => {
                                Self::handle_add_stream(&streams, stream_id, publisher).await;
                            },
                            RelayMessage::RemoveStream(stream_id) => {
                                Self::handle_remove_stream(&streams, stream_id).await;
                            },
                            RelayMessage::AddSubscriber(stream_id, peer_id) => {
                                Self::handle_add_subscriber(&streams, stream_id, peer_id).await;
                            },
                            RelayMessage::RemoveSubscriber(stream_id, peer_id) => {
                                Self::handle_remove_subscriber(&streams, stream_id, peer_id).await;
                            },
                            RelayMessage::RequestChunks(stream_id, peer_id, sequence) => {
                                Self::handle_request_chunks(
                                    &streams,
                                    stream_id,
                                    peer_id,
                                    sequence,
                                    &chunk_handler,
                                ).await;
                            },
                            RelayMessage::Stop => {
                                debug!("Stopping relay worker");
                                break;
                            },
                        }
                    },
                    Some(chunk) = chunk_receiver.recv() => {
                        // Handle incoming chunk (same as RelayMessage::Chunk)
                        Self::handle_chunk(
                            &streams,
                            &topology,
                            &chunk,
                            &chunk_handler,
                            &config,
                            &stats,
                            &local_peer_id,
                        ).await;
                    },
                    else => break,
                }
            }
        });
        
        *worker_task = Some(task);
        
        Ok(())
    }
    
    /// Start the stats reporting task
    async fn start_stats_task(&self) -> Result<(), OverlayError> {
        let mut stats_task = self.stats_task.lock().await;
        
        // Don't start if already running
        if stats_task.is_some() {
            return Ok(());
        }
        
        // Clone required references
        let stats = self.stats.clone();
        let streams = self.streams.clone();
        let interval = self.config.stats_interval;
        
        // Start stats task
        let task = tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                // Update stats
                let mut stats_guard = stats.write().await;
                let streams_guard = streams.read().await;
                
                stats_guard.active_streams = streams_guard.len();
                
                let mut connected_peers = HashSet::new();
                for stream in streams_guard.values() {
                    connected_peers.insert(stream.publisher.clone());
                    connected_peers.extend(stream.subscribers.iter().cloned());
                }
                
                stats_guard.connected_peers = connected_peers.len();
                
                // Calculate bandwidth
                let elapsed = stats_guard.period_start.elapsed().as_secs();
                if elapsed > 0 {
                    stats_guard.incoming_bandwidth = stats_guard.bytes_relayed / elapsed;
                    // For now, outgoing = incoming (could be refined)
                    stats_guard.outgoing_bandwidth = stats_guard.incoming_bandwidth;
                }
                
                // Reset for next period
                stats_guard.reset_period();
                
                debug!("Relay stats: {} streams, {} peers, {:?} B/s in, {:?} B/s out",
                    stats_guard.active_streams,
                    stats_guard.connected_peers,
                    stats_guard.incoming_bandwidth,
                    stats_guard.outgoing_bandwidth);
            }
        });
        
        *stats_task = Some(task);
        
        Ok(())
    }
    
    /// Start the cleanup task for inactive streams
    async fn start_cleanup_task(&self) -> Result<(), OverlayError> {
        let mut cleanup_task = self.cleanup_task.lock().await;
        
        // Don't start if already running
        if cleanup_task.is_some() {
            return Ok(());
        }
        
        // Clone required references
        let streams = self.streams.clone();
        let interval = self.config.cleanup_interval;
        let timeout = self.config.inactivity_timeout;
        
        // Start cleanup task
        let task = tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                // Find inactive streams
                let mut streams_guard = streams.write().await;
                let inactive_streams: Vec<StreamId> = streams_guard
                    .iter()
                    .filter(|(_, stream)| stream.is_inactive(timeout))
                    .map(|(id, _)| id.clone())
                    .collect();
                
                // Remove inactive streams
                for stream_id in inactive_streams {
                    debug!("Removing inactive stream: {:?}", stream_id);
                    streams_guard.remove(&stream_id);
                }
            }
        });
        
        *cleanup_task = Some(task);
        
        Ok(())
    }
    
    /// Handle an incoming chunk
    async fn handle_chunk(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        topology: &Arc<TopologyManager>,
        chunk: &StreamChunk,
        chunk_handler: &Option<Box<dyn Fn(PeerId, StreamChunk) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> + Send + Sync>>,
        config: &RelayConfig,
        stats: &RwLock<RelayStats>,
        local_peer_id: &PeerId,
    ) {
        // Update stream buffer
        {
            let mut streams_guard = streams.write().await;
            
            if let Some(stream) = streams_guard.get_mut(&chunk.stream_id) {
                stream.add_chunk(chunk.clone(), config.max_buffer_size);
            } else {
                // Stream not found, can't relay
                return;
            }
        }
        
        // Update stats
        {
            let mut stats_guard = stats.write().await;
            stats_guard.record_chunk(chunk.data.len());
        }
        
        // Get subscribers to relay to
        let subscribers = {
            let streams_guard = streams.read().await;
            
            if let Some(stream) = streams_guard.get(&chunk.stream_id) {
                stream.subscribers.clone()
            } else {
                // Stream not found, can't relay
                return;
            }
        };
        
        // Relay to subscribers
        if let Some(handler) = chunk_handler {
            for subscriber in subscribers {
                // Skip if this is the source of the chunk
                if let Some(ref source) = chunk.source {
                    if source == &subscriber {
                        continue;
                    }
                }
                
                // Create a copy for this subscriber
                let mut subscriber_chunk = chunk.clone();
                subscriber_chunk.source = Some(local_peer_id.clone());
                
                // Relay the chunk
                if let Err(e) = handler(subscriber.clone(), subscriber_chunk).await {
                    error!("Failed to relay chunk to {}: {}", subscriber, e);
                }
            }
        }
    }
    
    /// Handle adding a new stream
    async fn handle_add_stream(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
        publisher: PeerId,
    ) {
        let mut streams_guard = streams.write().await;
        
        if !streams_guard.contains_key(&stream_id) {
            let relay = StreamRelay::new(stream_id.clone(), publisher);
            streams_guard.insert(stream_id, relay);
            debug!("Added new stream: {:?}", stream_id);
        }
    }
    
    /// Handle removing a stream
    async fn handle_remove_stream(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
    ) {
        let mut streams_guard = streams.write().await;
        
        if streams_guard.remove(&stream_id).is_some() {
            debug!("Removed stream: {:?}", stream_id);
        }
    }
    
    /// Handle adding a subscriber to a stream
    async fn handle_add_subscriber(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
        peer_id: PeerId,
    ) {
        let mut streams_guard = streams.write().await;
        
        if let Some(stream) = streams_guard.get_mut(&stream_id) {
            stream.add_subscriber(peer_id.clone());
            debug!("Added subscriber {} to stream {:?}", peer_id, stream_id);
        }
    }
    
    /// Handle removing a subscriber from a stream
    async fn handle_remove_subscriber(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
        peer_id: PeerId,
    ) {
        let mut streams_guard = streams.write().await;
        
        if let Some(stream) = streams_guard.get_mut(&stream_id) {
            stream.remove_subscriber(&peer_id);
            debug!("Removed subscriber {} from stream {:?}", peer_id, stream_id);
        }
    }
    
    /// Handle a request for chunks since a sequence number
    async fn handle_request_chunks(
        streams: &RwLock<HashMap<StreamId, StreamRelay>>,
        stream_id: StreamId,
        peer_id: PeerId,
        sequence: u64,
        chunk_handler: &Option<Box<dyn Fn(PeerId, StreamChunk) -> Pin<Box<dyn Future<Output = Result<(), OverlayError>> + Send>> + Send + Sync>>,
    ) {
        let chunks = {
            let streams_guard = streams.read().await;
            
            if let Some(stream) = streams_guard.get(&stream_id) {
                stream.get_chunks_since(sequence)
            } else {
                // Stream not found
                Vec::new()
            }
        };
        
        // Send chunks to the peer
        if let Some(handler) = chunk_handler {
            for chunk in chunks {
                if let Err(e) = handler(peer_id.clone(), chunk).await {
                    error!("Failed to send requested chunk to {}: {}", peer_id, e);
                }
            }
        }
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
        
        // Send to chunk channel
        if let Err(e) = self.chunk_tx.send(chunk).await {
            return Err(OverlayError::RelayError(
                format!("Failed to send chunk: {}", e)
            ));
        }
        
        Ok(())
    }
    
    /// Create a new stream
    pub async fn create_stream(&self, stream_id: StreamId, publisher: PeerId) -> Result<(), OverlayError> {
        // Check if we've reached the max streams limit
        let stream_count = {
            let streams = self.streams.read().await;
            streams.len()
        };
        
        if stream_count >= self.config.max_streams {
            return Err(OverlayError::RelayError(
                format!("Maximum number of streams reached: {}", self.config.max_streams)
            ));
        }
        
        // Send message
        if let Err(e) = self.message_tx.send(RelayMessage::AddStream(stream_id, publisher)).await {
            return Err(OverlayError::RelayError(
                format!("Failed to create stream: {}", e)
            ));
        }
        
        Ok(())
    }
    
    /// Remove a stream
    pub async fn remove_stream(&self, stream_id: StreamId) -> Result<(), OverlayError> {
        // Send message
        if let Err(e) = self.message_tx.send(RelayMessage::RemoveStream(stream_id)).await {
            return Err(OverlayError::RelayError(
                format!("Failed to remove stream: {}", e)
            ));
        }
        
        Ok(())
    }
    
    /// Subscribe a peer to a stream
    pub async fn subscribe_peer(&self, stream_id: StreamId, peer_id: PeerId) -> Result<(), OverlayError> {
        // Send message
        if let Err(e) = self.message_tx.send(RelayMessage::AddSubscriber(stream_id, peer_id)).await {
            return Err(OverlayError::RelayError(
                format!("Failed to add subscriber: {}", e)
            ));
        }
        
        Ok(())
    }
    
    /// Unsubscribe a peer from a stream
    pub async fn unsubscribe_peer(&self, stream_id: StreamId, peer_id: PeerId) -> Result<(), OverlayError> {
        // Send message
        if let Err(e) = self.message_tx.send(RelayMessage::RemoveSubscriber(stream_id, peer_id)).await {
            return Err(OverlayError::RelayError(
                format!("Failed to remove subscriber: {}", e)
            ));
        }
        
        Ok(())
    }
    
    /// Request chunks since a sequence number
    pub async fn request_chunks(&self, stream_id: StreamId, peer_id: PeerId, sequence: u64) -> Result<(), OverlayError> {
        // Send message
        if let Err(e) = self.message_tx.send(RelayMessage::RequestChunks(stream_id, peer_id, sequence)).await {
            return Err(OverlayError::RelayError(
                format!("Failed to request chunks: {}", e)
            ));
        }
        
        Ok(())
    }
    
    /// Get current relay stats
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