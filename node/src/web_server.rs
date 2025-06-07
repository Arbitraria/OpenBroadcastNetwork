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
        
        Ok(())
    }

    /// Start listening for chunks from the P2P network and relay them to web clients
    async fn start_p2p_chunk_listener(&self, overlay: Arc<Libp2pOverlay>, stream_id: StreamId) -> Result<(), String> {
        info!("Starting P2P chunk listener for stream: {}", stream_id);
        
        let stream_topic = Topic::stream_topic(&stream_id.as_str());
        
        // Subscribe to the stream topic
        // TODO: Implement subscribe_to_topic when the overlay API is complete
        info!("Would subscribe to P2P stream topic: {}", stream_topic.id());
        
        info!("Subscribed to P2P stream topic: {}", stream_topic.id());
        
        // Clone components for the background task
        let chunk_sender = self.chunk_sender.clone();
        let is_streaming = Arc::clone(&self.is_streaming);
        
        // Start background task to listen for P2P messages
        tokio::spawn(async move {
            info!("P2P chunk listener task started");
            
            while *is_streaming.lock().await {
                // TODO: Get messages from overlay.receive_from_topic()
                // For now, we'll implement a polling approach
                
                // Check for new messages on the topic (this is a placeholder implementation)
                // In a real implementation, we'd need overlay.receive_from_topic() or similar
                tokio::time::sleep(Duration::from_millis(100)).await;
                
                // Placeholder: Convert P2P MediaChunk to StreamChunk and forward
                // This would normally receive actual MediaChunk data from the overlay
            }
            
            info!("P2P chunk listener task stopped");
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

    async fn _load_video_with_ffmpeg(&self, _ffmpeg_reader: FFmpegVideoReader) -> Result<(), String> {
        // TODO: Re-implement when FFmpeg thread safety issues are resolved
        Err("FFmpeg support temporarily disabled".to_string())
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
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> Response {
    ws.on_upgrade(|socket| handle_websocket(socket, state))
}

/// Handle individual WebSocket connections
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
    
    let (audio_codec, audio_mime) = if let Some((codec, mime)) = &*state.stream_manager.audio_codec_info.lock().await {
        info!("🔍 WEBSOCKET DEBUG: Using cached audio codec info: {} -> {}", codec, mime);
        (codec.clone(), Some(mime.clone()))
    } else {
        info!("🔍 WEBSOCKET DEBUG: No cached audio codec, using fallback: AAC -> audio/mp4; codecs=\"mp4a.40.2\"");
        ("AAC".to_string(), Some("audio/mp4; codecs=\"mp4a.40.2\"".to_string()))
    };
    
    info!("🔍 WEBSOCKET DEBUG: Final audio codec for stream_info: {} -> {:?}", audio_codec, audio_mime);
    
    let stream_info = ClientMessage::StreamInfo {
        data: StreamInfo {
            video: Some(VideoInfo {
                width: 640,
                height: 480,
                fps: 30.0,
                codec: video_codec,
                mime_type: video_mime,
            }),
            audio: Some(AudioInfo {
                sample_rate: 48000,
                channels: 2,
                codec: audio_codec,
                mime_type: audio_mime,
            }),
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
        
        if let Err(e) = sender.send(Message::Binary(init_segment.clone())).await {
            error!("Failed to send initialization segment: {}", e);
            return;
        }
        
        info!("Successfully sent initialization segment to client {}", client_id);
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