//! Web Server for OpenBroadcastNetwork Streaming
//!
//! This module implements a WebSocket-based streaming server that delivers
//! video content to web browsers using Media Source Extensions (MSE).
//!
//! # Architecture
//!
//! The server consists of:
//! 1. **HTTP Server**: Serves static files (HTML viewers) and handles WebSocket upgrades
//! 2. **WebSocket Handler**: Manages client connections and streams video segments
//! 3. **Stream Manager**: Coordinates video file parsing and segment distribution
//!
//! # Protocol
//!
//! The WebSocket protocol uses a mix of JSON messages and binary data:
//! - **JSON Messages**: Control messages (stream_info, chunk_info)
//! - **Binary Data**: Video/audio segments in MP4 format
//!
//! # Message Sequence
//!
//! 1. Client connects via WebSocket
//! 2. Server sends `stream_info` with codec information
//! 3. Server sends `chunk_info` before each segment
//! 4. Server sends binary segment data
//! 5. Client assembles and plays video
//!
//! # Browser Compatibility
//!
//! - **Chrome**: Requires specific AAC encoding parameters
//! - **Firefox**: More flexible codec support
//! - **Safari**: Good support for Apple-standard codecs

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Router,
    Json,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex, RwLock};
use tower::ServiceBuilder;
use tower_http::{
    cors::CorsLayer,
    services::ServeDir,
};
use tracing::{debug, error, info, warn};

use OpenBroadcastNetwork_core::media::codec::{OpenH264Codec, OpusCodec};
use OpenBroadcastNetwork_core::media::video_reader::{VideoReader, MediaSample};
use OpenBroadcastNetwork_core::media::ffmpeg_reader::FFmpegVideoReader;
use OpenBroadcastNetwork_core::media::mp4_parser::{Mp4Parser, MseSegment};
use OpenBroadcastNetwork_core::overlay::interface::Overlay;
use OpenBroadcastNetwork_core::overlay::libp2p::impl_core::Libp2pOverlay;
use OpenBroadcastNetwork_core::pubsub::Topic;
use OpenBroadcastNetwork_core::media::{MediaChunk, StreamId};
use std::time::Duration;

/// Configuration for the web server
#[derive(Debug, Clone)]
pub struct WebServerConfig {
    pub host: String,
    pub port: u16,
    pub web_root: PathBuf,
    pub enable_cors: bool,
    pub video_file: Option<PathBuf>,
}

impl Default for WebServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            web_root: PathBuf::from("web_viewer"),
            enable_cors: true,
            video_file: None,
        }
    }
}

/// Shared state for the web server
#[derive(Clone)]
pub struct AppState {
    pub config: WebServerConfig,
    pub stream_manager: Arc<StreamManager>,
    pub clients: Arc<RwLock<HashMap<String, ClientConnection>>>,
}

/// Manages streaming connections and codec operations
pub struct StreamManager {
    pub h264_codec: Arc<Mutex<OpenH264Codec>>,
    pub opus_codec: Arc<Mutex<OpusCodec>>,
    pub chunk_sender: broadcast::Sender<StreamChunk>,
    pub is_streaming: Arc<Mutex<bool>>,
    pub video_samples: Arc<Mutex<Option<Vec<MediaSample>>>>,
    pub overlay: Option<Arc<Libp2pOverlay>>,
    pub stream_id: Option<StreamId>,
    pub initialization_segment: Arc<Mutex<Option<Vec<u8>>>>,
    pub video_codec_info: Arc<Mutex<Option<(String, String)>>>,
    pub audio_codec_info: Arc<Mutex<Option<(String, String)>>>,
}

/// Represents a client WebSocket connection
#[derive(Debug)]
pub struct ClientConnection {
    pub id: String,
    pub addr: SocketAddr,
    pub connected_at: chrono::DateTime<chrono::Utc>,
}

/// Different types of stream chunks
#[derive(Debug, Clone)]
pub enum StreamChunk {
    Video {
        data: Vec<u8>,
        timestamp: u64,
        is_keyframe: bool,
    },
    Audio {
        data: Vec<u8>,
        timestamp: u64,
    },
    Metadata {
        video_width: u32,
        video_height: u32,
        video_fps: f32,
        audio_sample_rate: u32,
        audio_channels: u16,
    },
}

/// WebSocket message types for client communication
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "stream_info")]
    StreamInfo {
        data: StreamInfo,
    },
    #[serde(rename = "chunk_info")]
    ChunkInfo {
        data: ChunkInfo,
    },
    #[serde(rename = "error")]
    Error {
        message: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamInfo {
    pub video: Option<VideoInfo>,
    pub audio: Option<AudioInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VideoInfo {
    pub width: u32,
    pub height: u32,
    pub fps: f32,
    pub codec: String,
    pub mime_type: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AudioInfo {
    pub sample_rate: u32,
    pub channels: u16,
    pub codec: String,
    pub mime_type: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub chunk_type: String,
    pub size: usize,
    pub timestamp: u64,
}

impl StreamManager {
    pub fn new() -> Self {
        let (chunk_sender, _) = broadcast::channel(1000);
        
        Self {
            h264_codec: Arc::new(Mutex::new(OpenH264Codec::with_dimensions(640, 480))),
            opus_codec: Arc::new(Mutex::new(OpusCodec::with_params(48000, 2))),
            chunk_sender,
            is_streaming: Arc::new(Mutex::new(false)),
            video_samples: Arc::new(Mutex::new(None)),
            overlay: None,
            stream_id: None,
            initialization_segment: Arc::new(Mutex::new(None)),
            video_codec_info: Arc::new(Mutex::new(None)),
            audio_codec_info: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_p2p(overlay: Arc<Libp2pOverlay>, stream_id: StreamId) -> Self {
        let (chunk_sender, _) = broadcast::channel(1000);
        
        Self {
            h264_codec: Arc::new(Mutex::new(OpenH264Codec::with_dimensions(640, 480))),
            opus_codec: Arc::new(Mutex::new(OpusCodec::with_params(48000, 2))),
            chunk_sender,
            is_streaming: Arc::new(Mutex::new(false)),
            video_samples: Arc::new(Mutex::new(None)),
            overlay: Some(overlay),
            stream_id: Some(stream_id),
            initialization_segment: Arc::new(Mutex::new(None)),
            video_codec_info: Arc::new(Mutex::new(None)),
            audio_codec_info: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn start_streaming(&self) -> Result<(), String> {
        info!("Starting streaming session");
        
        // Initialize codecs
        {
            let h264_codec = self.h264_codec.lock().await;
            h264_codec.init_encoder().await.map_err(|e| e.to_string())?;
            h264_codec.init_decoder().await.map_err(|e| e.to_string())?;
        }
        
        {
            let opus_codec = self.opus_codec.lock().await;
            opus_codec.init_encoder().await.map_err(|e| e.to_string())?;
            opus_codec.init_decoder().await.map_err(|e| e.to_string())?;
        }

        // Send initial metadata only if there are receivers
        if self.chunk_sender.receiver_count() > 0 {
            let metadata = StreamChunk::Metadata {
                video_width: 640,
                video_height: 480,
                video_fps: 30.0,
                audio_sample_rate: 48000,
                audio_channels: 2,
            };
            
            let _ = self.chunk_sender.send(metadata);
        }

        *self.is_streaming.lock().await = true;
        
        // If P2P overlay is available, start listening for chunks
        if let Some(overlay) = &self.overlay {
            if let Some(stream_id) = &self.stream_id {
                self.start_p2p_chunk_listener(overlay.clone(), stream_id.clone()).await?;
            }
        }
        
        // Start streaming loaded video segments if available
        let video_samples = Arc::clone(&self.video_samples);
        let chunk_sender = self.chunk_sender.clone();
        let is_streaming = Arc::clone(&self.is_streaming);
        
        tokio::spawn(async move {
            // Wait a bit for clients to connect
            tokio::time::sleep(Duration::from_millis(1000)).await;
            
            let samples = video_samples.lock().await;
            if let Some(ref video_samples) = *samples {
                info!("Starting to stream {} loaded segments", video_samples.len());
                
                for (index, sample) in video_samples.iter().enumerate() {
                    // Skip initialization segment (track_id 99) as it's sent separately
                    if sample.track_id == 99 {
                        continue;
                    }
                    
                    // Only send if there are active receivers
                    if chunk_sender.receiver_count() == 0 {
                        info!("No clients connected, stopping segment streaming");
                        break;
                    }
                    
                    let timestamp = sample.timestamp.as_millis() as u64;
                    let chunk = StreamChunk::Video {
                        data: sample.data.clone(),
                        timestamp,
                        is_keyframe: sample.is_sync,
                    };
                    
                    match chunk_sender.send(chunk) {
                        Ok(_) => {
                            info!("Sent video segment {} ({} bytes, ts={}ms, keyframe={})", 
                                  index, sample.data.len(), timestamp, sample.is_sync);
                            
                            // Add delay between segments to simulate streaming
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        },
                        Err(_) => {
                            info!("All receivers dropped, stopping segment streaming");
                            break;
                        }
                    }
                }
                
                info!("Finished streaming all segments");
                *is_streaming.lock().await = false;
            } else {
                warn!("No video samples loaded");
            }
        });
        
        Ok(())
    }

    /// Start listening for chunks from the P2P network and relay them to web clients
    #[cfg(feature = "libp2p")]
    async fn start_p2p_chunk_listener(&self, overlay: Arc<Libp2pOverlay>, stream_id: StreamId) -> Result<(), String> {
        info!("Starting P2P chunk listener for stream: {}", stream_id);
        
        let stream_topic = Topic::stream_topic(&stream_id.as_str());
        
        // Subscribe to the stream topic via overlay
        match overlay.subscribe(&stream_topic.id()).await {
            Ok(_) => info!("Successfully subscribed to P2P stream topic: {}", stream_topic.id()),
            Err(e) => {
                warn!("Failed to subscribe to P2P topic: {} - continuing without P2P", e);
                return Ok(());
            }
        }
        
        // Clone components for the background task
        let chunk_sender = self.chunk_sender.clone();
        let is_streaming = Arc::clone(&self.is_streaming);
        
        // Start background task to listen for P2P messages
        tokio::spawn(async move {
            info!("P2P chunk listener task started");
            
            while *is_streaming.lock().await {
                // Poll for overlay events with a timeout
                if let Some(event) = overlay.next_event().await {
                    match event {
                        OverlayEvent::DataReceived { stream_id: recv_stream_id, data, .. } => {
                            if recv_stream_id == stream_id {
                                // Convert received P2P data to StreamChunk and forward
                                if let Ok(chunk) = serde_json::from_slice::<StreamChunk>(&data) {
                                    if chunk_sender.receiver_count() > 0 {
                                        let _ = chunk_sender.send(chunk);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                } else {
                    // No events, wait briefly
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            
            info!("P2P chunk listener task stopped");
        });
        
        Ok(())
    }
    
    /// Start listening for chunks from the P2P network (disabled when libp2p feature is off)
    #[cfg(not(feature = "libp2p"))]
    async fn start_p2p_chunk_listener(&self, _overlay: Arc<Libp2pOverlay>, _stream_id: StreamId) -> Result<(), String> {
        info!("P2P chunk listener disabled - libp2p feature not enabled");
        
        // Log periodically to indicate P2P is disabled
        let is_streaming = Arc::clone(&self.is_streaming);
        tokio::spawn(async move {
            let mut log_interval = tokio::time::interval(Duration::from_secs(5));
            while *is_streaming.lock().await {
                log_interval.tick().await;
                debug!("P2P streaming disabled - running in local-only mode");
            }
        });
        
        Ok(())
    }

    pub async fn stop_streaming(&self) {
        info!("Stopping streaming session");
        *self.is_streaming.lock().await = false;
    }

    pub async fn load_video_file(&self, video_path: &std::path::Path) -> Result<(), String> {
        info!("Loading video file: {:?}", video_path);
        
        // FFmpeg support is disabled for now due to thread safety issues
        // TODO: Fix FFmpeg thread safety and re-enable
        // if let Ok(mut ffmpeg_reader) = FFmpegVideoReader::open(video_path) {
        //     info!("Using FFmpeg for video file processing");
        //     return self.load_video_with_ffmpeg(ffmpeg_reader).await;
        // }
        
        // Fallback to manual approach
        info!("Using manual video reader approach");
        let mut video_reader = VideoReader::open(video_path)
            .map_err(|e| format!("Failed to open video file: {}", e))?;
        
        // Log video track info
        if let Some(video_track) = video_reader.video_track() {
            info!("Video track: {}x{} @ {:.2}fps, codec: {}", 
                  video_track.width, video_track.height, video_track.fps, video_track.codec);
        }
        
        // Log audio track info
        if let Some(audio_track) = video_reader.audio_track() {
            info!("Audio track: {} Hz, {} channels, codec: {}", 
                  audio_track.sample_rate, audio_track.channels, audio_track.codec);
        }
        
        // Use the new MP4 parser to generate proper MSE segments
        info!("Using MP4 parser to generate MSE-compatible segments");
        
        let file_data = std::fs::read(video_path)
            .map_err(|e| format!("Failed to read video file: {}", e))?;
        
        // Parse the MP4 file
        let mut mp4_parser = Mp4Parser::new();
        mp4_parser.parse(&file_data)
            .map_err(|e| format!("Failed to parse MP4 file: {}", e))?;
        
        info!("MP4 parsing complete: {}", mp4_parser.get_summary());
        
        // Store detected codec information
        if let Some((video_codec, video_mime)) = mp4_parser.get_video_codec_info() {
            info!("Detected video codec: {} -> {}", video_codec, video_mime);
            *self.video_codec_info.lock().await = Some((video_codec.to_string(), video_mime.to_string()));
        }
        if let Some((audio_codec, audio_mime)) = mp4_parser.get_audio_codec_info() {
            info!("Detected audio codec: {} -> {}", audio_codec, audio_mime);
            *self.audio_codec_info.lock().await = Some((audio_codec.to_string(), audio_mime.to_string()));
        }
        
        // Generate MSE segments
        let mse_segments = mp4_parser.generate_mse_segments()
            .map_err(|e| format!("Failed to generate MSE segments: {}", e))?;
        
        info!("Generated {} MSE segments", mse_segments.len());
        
        // No codec correction needed - we're keeping original ESDS data
        // Browser validation requires codec string to match binary data exactly
        
        // Convert MSE segments to MediaSample format for compatibility
        let mut prepared_samples = Vec::new();
        for (index, segment) in mse_segments.iter().enumerate() {
            // Store the initialization segment for new clients
            if segment.segment_type == "initialization" {
                *self.initialization_segment.lock().await = Some(segment.data.clone());
                info!("Stored initialization segment ({} bytes) for new clients", segment.data.len());
            }
            
            let sample = MediaSample {
                data: segment.data.clone(),
                timestamp: segment.timestamp.map(|t| Duration::from_millis(t)).unwrap_or(Duration::ZERO),
                duration: segment.duration.map(|d| Duration::from_millis(d)).unwrap_or(Duration::from_secs(1)),
                is_sync: segment.is_keyframe,
                track_id: if segment.segment_type == "initialization" { 99 } else { 0 }, // Special track ID for init
            };
            prepared_samples.push(sample);
            
            debug!("MSE Segment {}: type={}, size={} bytes, keyframe={}", 
                   index, segment.segment_type, segment.data.len(), segment.is_keyframe);
        }
        
        info!("Converted {} MSE segments to MediaSample format", prepared_samples.len());
        
        // Store prepared samples for streaming
        *self.video_samples.lock().await = Some(prepared_samples);
        
        Ok(())
    }

    #[cfg(feature = "ffmpeg")]
    async fn _load_video_with_ffmpeg(&self, _ffmpeg_reader: FFmpegVideoReader) -> Result<(), String> {
        // TODO: Re-implement when FFmpeg thread safety issues are resolved
        Err("FFmpeg support temporarily disabled".to_string())
    }
    
    #[cfg(not(feature = "ffmpeg"))]
    async fn _load_video_with_ffmpeg(&self, _ffmpeg_reader: FFmpegVideoReader) -> Result<(), String> {
        Err("FFmpeg feature disabled at compile time".to_string())
    }

    pub async fn send_video_chunk(&self, data: Vec<u8>, is_keyframe: bool) -> Result<(), String> {
        // Only send if there are active receivers
        if self.chunk_sender.receiver_count() == 0 {
            return Ok(()); // No clients connected, skip silently
        }

        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        let chunk = StreamChunk::Video {
            data,
            timestamp,
            is_keyframe,
        };

        match self.chunk_sender.send(chunk) {
            Ok(_) => Ok(()),
            Err(broadcast::error::SendError(_)) => {
                // All receivers dropped, which is fine
                Ok(())
            }
        }
    }

    pub async fn send_audio_chunk(&self, data: Vec<u8>) -> Result<(), String> {
        // Only send if there are active receivers
        if self.chunk_sender.receiver_count() == 0 {
            return Ok(()); // No clients connected, skip silently
        }

        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        let chunk = StreamChunk::Audio {
            data,
            timestamp,
        };

        match self.chunk_sender.send(chunk) {
            Ok(_) => Ok(()),
            Err(broadcast::error::SendError(_)) => {
                // All receivers dropped, which is fine
                Ok(())
            }
        }
    }
}

/// Main web server implementation
pub struct WebServer {
    config: WebServerConfig,
    app_state: AppState,
}

impl WebServer {
    pub fn new(config: WebServerConfig) -> Self {
        let stream_manager = Arc::new(StreamManager::new());
        let clients = Arc::new(RwLock::new(HashMap::new()));

        let app_state = AppState {
            config: config.clone(),
            stream_manager,
            clients,
        };

        Self {
            config,
            app_state,
        }
    }

    pub fn new_with_state(config: WebServerConfig, app_state: AppState) -> Self {
        Self {
            config,
            app_state,
        }
    }

    pub async fn start(&self) -> Result<(), anyhow::Error> {
        let app = self.create_app().await;
        
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let socket_addr: SocketAddr = addr.parse()?;

        info!("Starting web server on http://{}", addr);
        info!("Web viewer available at http://{}/", addr);
        info!("WebSocket streaming endpoint: ws://{}/stream", addr);

        axum::Server::bind(&socket_addr)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await?;

        Ok(())
    }

    async fn create_app(&self) -> Router {
        let mut router = Router::new()
            .route("/stream", get(websocket_handler))
            .route("/api/stream/start", post(start_stream_handler))
            .route("/api/stream/stop", post(stop_stream_handler))
            .route("/api/stream/status", get(stream_status_handler))
            .with_state(self.app_state.clone());

        // Add CORS if enabled
        if self.config.enable_cors {
            router = router.layer(
                ServiceBuilder::new()
                    .layer(CorsLayer::permissive())
            );
        }

        // Serve static files (web viewer)
        if self.config.web_root.exists() {
            info!("Serving static files from: {:?}", self.config.web_root);
            router = router.fallback_service(ServeDir::new(&self.config.web_root));
        } else {
            warn!("Web root directory not found: {:?}", self.config.web_root);
            router = router.fallback(|| async { Html(DEFAULT_HTML) });
        }

        router
    }

    pub fn get_stream_manager(&self) -> Arc<StreamManager> {
        self.app_state.stream_manager.clone()
    }
}

/// WebSocket handler for streaming connections
///
/// This function upgrades HTTP connections to WebSocket for real-time streaming.
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> Response {
    ws.on_upgrade(|socket| handle_websocket(socket, state))
}

/// Handle individual WebSocket connections
///
/// This is the main streaming loop that:
/// 1. Sends codec information to the client
/// 2. Sends the initialization segment
/// 3. Streams media segments in real-time
///
/// # Protocol
///
/// Messages are sent in a specific order:
/// 1. `stream_info`: Contains video/audio codec information
/// 2. `chunk_info`: Metadata about the next chunk
/// 3. Binary data: The actual MP4 segment
///
/// # WebSocket Frame Size Limitation
///
/// Firefox has a 1MB WebSocket frame limit, so large segments are
/// automatically chunked into smaller pieces (see lines 615-700).
async fn handle_websocket(socket: WebSocket, state: AppState) {
    let client_id = uuid::Uuid::new_v4().to_string();
    info!("New WebSocket client connected: {}", client_id);

    // Split the socket into sender and receiver
    let (mut sender, mut receiver) = socket.split();

    // Subscribe to stream chunks
    let mut chunk_receiver = state.stream_manager.chunk_sender.subscribe();

    // Send initial stream info with detected codec information
    let (video_codec, video_mime) = if let Some((codec, mime)) = &*state.stream_manager.video_codec_info.lock().await {
        (codec.clone(), Some(mime.clone()))
    } else {
        ("H.264".to_string(), Some("video/mp4; codecs=\"avc1.42E01E\"".to_string()))
    };
    
    // Check if we actually have an audio track
    let audio_info = if let Some((codec, mime)) = &*state.stream_manager.audio_codec_info.lock().await {
        info!("🔍 WEBSOCKET DEBUG: Using cached audio codec info: {} -> {}", codec, mime);
        Some(AudioInfo {
            sample_rate: 48000,
            channels: 2,
            codec: codec.clone(),
            mime_type: Some(mime.clone()),
        })
    } else {
        info!("🔍 WEBSOCKET DEBUG: No audio track detected - sending video-only stream info");
        None
    };
    
    info!("🔍 WEBSOCKET DEBUG: Final stream configuration - Video: {:?}, Audio: {:?}", video_mime, audio_info.as_ref().map(|a| &a.mime_type));
    
    let stream_info = ClientMessage::StreamInfo {
        data: StreamInfo {
            video: Some(VideoInfo {
                width: 640,
                height: 480,
                fps: 30.0,
                codec: video_codec,
                mime_type: video_mime,
            }),
            audio: audio_info,
        },
    };

    if let Ok(message) = serde_json::to_string(&stream_info) {
        info!("🔍 WEBSOCKET DEBUG: Serialized stream_info message: {}", message);
        if let Err(e) = sender.send(Message::Text(message)).await {
            error!("Failed to send initial stream info: {}", e);
            return;
        }
        info!("🔍 WEBSOCKET DEBUG: Successfully sent stream_info message to client");
    }

    // Send the initialization segment if available
    if let Some(init_segment) = &*state.stream_manager.initialization_segment.lock().await {
        info!("Sending initialization segment to new client {} ({} bytes)", client_id, init_segment.len());
        
        // Send chunk info first
        let chunk_info = ClientMessage::ChunkInfo {
            data: ChunkInfo {
                chunk_type: "initialization".to_string(),
                size: init_segment.len(),
                timestamp: 0,
            },
        };

        if let Ok(info_message) = serde_json::to_string(&chunk_info) {
            if let Err(e) = sender.send(Message::Text(info_message)).await {
                error!("Failed to send initialization chunk info: {}", e);
                return;
            }
        }

        // Then send the actual initialization segment
        info!("Sending binary initialization segment: {} bytes", init_segment.len());
        
        // Log first few bytes for debugging
        if init_segment.len() >= 16 {
            let first_bytes: Vec<String> = init_segment[0..16].iter()
                .map(|b| format!("{:02x}", b))
                .collect();
            info!("First 16 bytes of init segment: {}", first_bytes.join(" "));
            
            // Check if this looks like valid MP4 data
            if init_segment.len() >= 8 {
                let box_size = u32::from_be_bytes([init_segment[0], init_segment[1], init_segment[2], init_segment[3]]);
                let box_type = String::from_utf8_lossy(&init_segment[4..8]);
                info!("First box in init segment: type='{}', size={}", box_type, box_size);
            }
        }
        
        // WebSocket frame size limit handling
        // Firefox has a default frame size limit of 1MB (1048576 bytes)
        // Large initialization segments (e.g., 1.6MB for Stargate video) will cause
        // WebSocket disconnection with error code 1009 ("message too big")
        // Solution: Chunk large segments into smaller pieces
        const MAX_CHUNK_SIZE: usize = 512 * 1024; // 512KB chunks to be safe
        
        if init_segment.len() > MAX_CHUNK_SIZE {
            info!("Initialization segment exceeds WebSocket frame limit, chunking into {} chunks", 
                  (init_segment.len() + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE);
            
            let mut offset = 0;
            let mut chunk_num = 0;
            
            while offset < init_segment.len() {
                let chunk_end = std::cmp::min(offset + MAX_CHUNK_SIZE, init_segment.len());
                let chunk = &init_segment[offset..chunk_end];
                chunk_num += 1;
                
                // Send chunk info
                let chunk_info = ClientMessage::ChunkInfo {
                    data: ChunkInfo {
                        chunk_type: format!("initialization_chunk_{}", chunk_num),
                        size: chunk.len(),
                        timestamp: offset as u64, // Use offset as timestamp for ordering
                    },
                };
                
                if let Ok(info_message) = serde_json::to_string(&chunk_info) {
                    if let Err(e) = sender.send(Message::Text(info_message)).await {
                        error!("Failed to send chunk {} info: {}", chunk_num, e);
                        return;
                    }
                }
                
                // Send the chunk
                if let Err(e) = sender.send(Message::Binary(chunk.to_vec())).await {
                    error!("Failed to send initialization chunk {}: {}", chunk_num, e);
                    return;
                }
                
                info!("Sent initialization chunk {} ({} bytes)", chunk_num, chunk.len());
                offset = chunk_end;
            }
            
            // Send completion marker
            let completion_info = ClientMessage::ChunkInfo {
                data: ChunkInfo {
                    chunk_type: "initialization_complete".to_string(),
                    size: init_segment.len(),
                    timestamp: 0,
                },
            };
            
            if let Ok(info_message) = serde_json::to_string(&completion_info) {
                if let Err(e) = sender.send(Message::Text(info_message)).await {
                    error!("Failed to send completion marker: {}", e);
                    return;
                }
            }
            
            info!("Successfully sent chunked initialization segment to client {}", client_id);
        } else {
            // Small enough to send in one frame
            if let Err(e) = sender.send(Message::Binary(init_segment.clone())).await {
                error!("Failed to send initialization segment: {}", e);
                return;
            }
            
            info!("Successfully sent initialization segment to client {}", client_id);
        }
    } else {
        warn!("No initialization segment available for client {}", client_id);
    }

    // Spawn task to handle incoming messages from client
    let client_id_clone = client_id.clone();
    tokio::spawn(async move {
        while let Some(message) = receiver.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    debug!("Received text from client {}: {}", client_id_clone, text);
                    // Handle client control messages here
                }
                Ok(Message::Binary(_)) => {
                    debug!("Received binary data from client {}", client_id_clone);
                    // Handle binary data if needed
                }
                Ok(Message::Close(_)) => {
                    info!("Client {} disconnected", client_id_clone);
                    break;
                }
                Err(e) => {
                    error!("WebSocket error for client {}: {}", client_id_clone, e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Handle outgoing stream chunks to client
    loop {
        tokio::select! {
            chunk_result = chunk_receiver.recv() => {
                match chunk_result {
                    Ok(chunk) => {
                        let message = match chunk {
                            StreamChunk::Video { data, timestamp, is_keyframe } => {
                                // Send chunk info first
                                let chunk_info = ClientMessage::ChunkInfo {
                                    data: ChunkInfo {
                                        chunk_type: if is_keyframe { "video-keyframe" } else { "video" }.to_string(),
                                        size: data.len(),
                                        timestamp,
                                    },
                                };

                                if let Ok(info_message) = serde_json::to_string(&chunk_info) {
                                    if let Err(_) = sender.send(Message::Text(info_message)).await {
                                        // Client disconnected, exit gracefully
                                        break;
                                    }
                                }

                                // Add a small delay to prevent overwhelming the client's SourceBuffer
                                // Chrome's SourceBuffer needs time to process each segment
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                                // Then send the actual data
                                Message::Binary(data)
                            }
                            StreamChunk::Audio { data, timestamp } => {
                                let chunk_info = ClientMessage::ChunkInfo {
                                    data: ChunkInfo {
                                        chunk_type: "audio".to_string(),
                                        size: data.len(),
                                        timestamp,
                                    },
                                };

                                if let Ok(info_message) = serde_json::to_string(&chunk_info) {
                                    if let Err(_) = sender.send(Message::Text(info_message)).await {
                                        // Client disconnected, exit gracefully
                                        break;
                                    }
                                }

                                Message::Binary(data)
                            }
                            StreamChunk::Metadata { video_width, video_height, video_fps, audio_sample_rate, audio_channels } => {
                                let stream_info = ClientMessage::StreamInfo {
                                    data: StreamInfo {
                                        video: Some(VideoInfo {
                                            width: video_width,
                                            height: video_height,
                                            fps: video_fps,
                                            codec: "H.264".to_string(),
                                            mime_type: Some("video/mp4; codecs=\"avc1.42E01E\"".to_string()),
                                        }),
                                        audio: Some(AudioInfo {
                                            sample_rate: audio_sample_rate,
                                            channels: audio_channels,
                                            codec: "AAC".to_string(),
                                            mime_type: Some("audio/mp4; codecs=\"mp4a.02.2\"".to_string()),
                                        }),
                                    },
                                };

                                if let Ok(message) = serde_json::to_string(&stream_info) {
                                    Message::Text(message)
                                } else {
                                    continue;
                                }
                            }
                        };

                        if let Err(_) = sender.send(message).await {
                            // Client disconnected, exit gracefully
                            break;
                        }
                    }
                    Err(_) => {
                        // Channel closed, exit gracefully
                        break;
                    }
                }
            }
            // Check if we should signal end of stream after a timeout
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {
                // If no more data is being streamed, send end of stream signal
                if !*state.stream_manager.is_streaming.lock().await {
                    let end_signal = ClientMessage::ChunkInfo {
                        data: ChunkInfo {
                            chunk_type: "end_of_stream".to_string(),
                            size: 0,
                            timestamp: 0,
                        },
                    };
                    
                    if let Ok(end_message) = serde_json::to_string(&end_signal) {
                        if sender.send(Message::Text(end_message)).await.is_ok() {
                            info!("Sent end_of_stream signal to client {}", client_id);
                        }
                    }
                    break;
                }
            }
        }
    }

    info!("WebSocket handler finished for client: {}", client_id);
}

/// API endpoint to start streaming
async fn start_stream_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.stream_manager.start_streaming().await {
        Ok(_) => Json(serde_json::json!({
            "status": "success",
            "message": "Streaming started"
        })),
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "message": format!("Failed to start streaming: {}", e)
        })),
    }
}

/// API endpoint to stop streaming
async fn stop_stream_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.stream_manager.stop_streaming().await;
    Json(serde_json::json!({
        "status": "success",
        "message": "Streaming stopped"
    }))
}

/// API endpoint to get streaming status
async fn stream_status_handler(State(state): State<AppState>) -> impl IntoResponse {
    let is_streaming = *state.stream_manager.is_streaming.lock().await;
    let client_count = state.clients.read().await.len();

    Json(serde_json::json!({
        "streaming": is_streaming,
        "clients": client_count,
        "codecs": {
            "video": "H.264",
            "audio": "AAC"
        }
    }))
}

/// Default HTML page when web_root is not available
const DEFAULT_HTML: &str = r#"
<!DOCTYPE html>
<html>
<head>
    <title>OpenBroadcastNetwork</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #1e3c72; color: white; }
        .container { max-width: 800px; margin: 0 auto; text-align: center; }
        h1 { color: #A8E6CF; }
        .status { background: rgba(255,255,255,0.1); padding: 20px; border-radius: 10px; margin: 20px 0; }
    </style>
</head>
<body>
    <div class="container">
        <h1>OpenBroadcastNetwork Streaming Server</h1>
        <div class="status">
            <h2>Server Running</h2>
            <p>The OpenBroadcastNetwork streaming server is running.</p>
            <p>Web viewer files should be placed in the <code>web_viewer</code> directory.</p>
            <p>WebSocket streaming endpoint: <code>ws://localhost:8080/stream</code></p>
        </div>
        
        <div class="status">
            <h3>API Endpoints</h3>
            <ul style="text-align: left; max-width: 400px; margin: 0 auto;">
                <li><strong>GET /api/stream/status</strong> - Get streaming status</li>
                <li><strong>POST /api/stream/start</strong> - Start streaming</li>
                <li><strong>POST /api/stream/stop</strong> - Stop streaming</li>
                <li><strong>WS /stream</strong> - WebSocket for chunk streaming</li>
            </ul>
        </div>
    </div>
</body>
</html>
"#;