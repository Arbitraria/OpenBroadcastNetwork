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
        Path, Query, State,
    },
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, services::ServeDir};
use tracing::{debug, error, info, warn};

// Import WebRTC signaling types
use OpenBroadcastNetwork_core::transport::{SignalingMessage, PeerInfo};

use std::time::Duration;
use OpenBroadcastNetwork_core::media::codec::{OpenH264Codec, OpusCodec};
#[cfg(feature = "ffmpeg")]
use OpenBroadcastNetwork_core::media::ffmpeg_reader::FFmpegVideoReader;
use OpenBroadcastNetwork_core::media::mp4_parser::Mp4Parser;
// Unified segment types - StreamSegment is the internal type, WireSegment for P2P
use OpenBroadcastNetwork_core::media::segment::{MediaType, StreamId, StreamSegment};
use OpenBroadcastNetwork_core::media::video_reader::VideoReader;
use OpenBroadcastNetwork_core::media::wire_format::{ToWireFormat, WireSegment};
use OpenBroadcastNetwork_core::media::StreamPublisher;
use OpenBroadcastNetwork_core::overlay::interface::{
    Overlay, OverlayEvent, StreamId as OverlayStreamId,
};
use OpenBroadcastNetwork_core::overlay::libp2p::impl_core::Libp2pOverlay;
use OpenBroadcastNetwork_core::discovery::stream_discovery::{
    StreamAnnouncement, StreamDiscovery,
};

/// Configuration for the web server
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WebServerConfig {
    pub host: String,
    pub port: u16,
    pub web_root: PathBuf,
    pub enable_cors: bool,
    pub video_file: Option<PathBuf>,
    /// Enable P2P streaming via overlay network
    pub enable_p2p: bool,
    /// Fall back to direct WebSocket when P2P is unavailable
    pub p2p_fallback: bool,
}

impl Default for WebServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            web_root: PathBuf::from("web_viewer"),
            enable_cors: true,
            video_file: None,
            enable_p2p: true,
            p2p_fallback: true,
        }
    }
}

/// Shared state for the web server
#[derive(Clone)]
pub struct AppState {
    #[allow(dead_code)]
    pub config: WebServerConfig,
    pub stream_manager: Arc<StreamManager>,
    pub clients: Arc<RwLock<HashMap<String, ClientConnection>>>,
    /// WebRTC signaling state for P2P connections
    pub signaling_state: Arc<SignalingState>,
    /// Optional moderation manager for peer blocking and stream flagging
    pub moderation: Option<Arc<OpenBroadcastNetwork_core::moderation::ModerationManager>>,
    /// Optional relay stats shared with RelayNode
    pub relay_stats: Option<Arc<RwLock<OpenBroadcastNetwork_core::overlay::RelayStats>>>,
}

/// State for WebRTC signaling coordination
pub struct SignalingState {
    /// Map of stream_id -> list of connected peers
    pub streams: RwLock<HashMap<String, Vec<SignalingPeer>>>,
    /// Map of peer_id -> sender for signaling messages
    pub peer_senders: RwLock<HashMap<String, mpsc::UnboundedSender<SignalingMessage>>>,
}

/// Represents a connected signaling peer
#[derive(Debug, Clone)]
pub struct SignalingPeer {
    pub peer_id: String,
    pub is_publisher: bool,
    pub can_relay: bool,
}

impl Default for SignalingState {
    fn default() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
            peer_senders: RwLock::new(HashMap::new()),
        }
    }
}

/// Manages streaming connections and codec operations
pub struct StreamManager {
    pub h264_codec: Arc<Mutex<OpenH264Codec>>,
    pub opus_codec: Arc<Mutex<OpusCodec>>,
    /// Unified segment broadcast channel - StreamSegment is the internal type
    pub segment_sender: broadcast::Sender<StreamSegment>,
    pub is_streaming: Arc<Mutex<bool>>,
    pub video_samples: Arc<Mutex<Option<Vec<StreamSegment>>>>,
    pub overlay: Option<Arc<Libp2pOverlay>>,
    pub stream_id: Option<StreamId>,
    /// Unified stream ID for segments
    pub segment_stream_id: StreamId,
    /// Legacy combined initialization segment (kept for backward compatibility)
    pub initialization_segment: Arc<Mutex<Option<Vec<u8>>>>,
    /// Video-only initialization segment for Chrome MSE buffer separation
    pub video_init_segment: Arc<Mutex<Option<Vec<u8>>>>,
    /// Audio-only initialization segment for Chrome MSE buffer separation
    pub audio_init_segment: Arc<Mutex<Option<Vec<u8>>>>,
    pub video_codec_info: Arc<Mutex<Option<(String, String)>>>,
    pub audio_codec_info: Arc<Mutex<Option<(String, String)>>>,
    /// P2P publisher for overlay network distribution
    #[allow(dead_code)]
    pub p2p_publisher: Option<Arc<StreamPublisher<Libp2pOverlay>>>,
    /// Whether P2P is enabled
    pub enable_p2p: bool,
    /// Sequence counter for segments
    pub segment_sequence: Arc<Mutex<u64>>,
    /// Stream discovery service for finding streams on the network
    pub stream_discovery: Option<Arc<StreamDiscovery<Libp2pOverlay>>>,
}

/// Represents a client WebSocket connection
#[derive(Debug)]
#[allow(dead_code)]
pub struct ClientConnection {
    pub id: String,
    pub addr: SocketAddr,
    pub connected_at: chrono::DateTime<chrono::Utc>,
}

/// WebSocket message types for client communication
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "stream_info")]
    StreamInfo { data: StreamInfo },
    #[serde(rename = "chunk_info")]
    ChunkInfo { data: ChunkInfo },
    #[serde(rename = "error")]
    Error { message: String },
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
        let (segment_sender, _) = broadcast::channel(1000);
        let segment_stream_id = StreamId::generate();

        Self {
            h264_codec: Arc::new(Mutex::new(OpenH264Codec::with_dimensions(640, 480))),
            opus_codec: Arc::new(Mutex::new(OpusCodec::with_params(48000, 2))),
            segment_sender,
            is_streaming: Arc::new(Mutex::new(false)),
            video_samples: Arc::new(Mutex::new(None)),
            overlay: None,
            stream_id: None,
            segment_stream_id,
            initialization_segment: Arc::new(Mutex::new(None)),
            video_init_segment: Arc::new(Mutex::new(None)),
            audio_init_segment: Arc::new(Mutex::new(None)),
            video_codec_info: Arc::new(Mutex::new(None)),
            audio_codec_info: Arc::new(Mutex::new(None)),
            p2p_publisher: None,
            enable_p2p: false,
            segment_sequence: Arc::new(Mutex::new(0)),
            stream_discovery: None,
        }
    }

    pub fn with_p2p(overlay: Arc<Libp2pOverlay>, stream_id: StreamId) -> Self {
        let (segment_sender, _) = broadcast::channel(1000);
        let segment_stream_id = stream_id.clone();

        // Create P2P publisher
        let publisher =
            StreamPublisher::new(overlay.clone(), format!("Stream {}", stream_id));

        // Create stream discovery service and start background tasks
        let discovery = Arc::new(StreamDiscovery::with_overlay(overlay.clone()));
        discovery.clone().spawn();

        Self {
            h264_codec: Arc::new(Mutex::new(OpenH264Codec::with_dimensions(640, 480))),
            opus_codec: Arc::new(Mutex::new(OpusCodec::with_params(48000, 2))),
            segment_sender,
            is_streaming: Arc::new(Mutex::new(false)),
            video_samples: Arc::new(Mutex::new(None)),
            overlay: Some(overlay),
            stream_id: Some(stream_id),
            segment_stream_id,
            initialization_segment: Arc::new(Mutex::new(None)),
            video_init_segment: Arc::new(Mutex::new(None)),
            audio_init_segment: Arc::new(Mutex::new(None)),
            video_codec_info: Arc::new(Mutex::new(None)),
            audio_codec_info: Arc::new(Mutex::new(None)),
            p2p_publisher: Some(Arc::new(publisher)),
            enable_p2p: true,
            segment_sequence: Arc::new(Mutex::new(0)),
            stream_discovery: Some(discovery),
        }
    }

    /// Create a StreamManager with P2P overlay for discovery/browsing only.
    /// No pre-selected stream or publisher — the user picks a stream later.
    pub fn with_discovery(overlay: Arc<Libp2pOverlay>) -> Self {
        let (segment_sender, _) = broadcast::channel(1000);
        let segment_stream_id = StreamId::generate();

        // Create stream discovery service and start background tasks
        let discovery = Arc::new(StreamDiscovery::with_overlay(overlay.clone()));
        discovery.clone().spawn();

        Self {
            h264_codec: Arc::new(Mutex::new(OpenH264Codec::with_dimensions(640, 480))),
            opus_codec: Arc::new(Mutex::new(OpusCodec::with_params(48000, 2))),
            segment_sender,
            is_streaming: Arc::new(Mutex::new(false)),
            video_samples: Arc::new(Mutex::new(None)),
            overlay: Some(overlay),
            stream_id: None,
            segment_stream_id,
            initialization_segment: Arc::new(Mutex::new(None)),
            video_init_segment: Arc::new(Mutex::new(None)),
            audio_init_segment: Arc::new(Mutex::new(None)),
            video_codec_info: Arc::new(Mutex::new(None)),
            audio_codec_info: Arc::new(Mutex::new(None)),
            p2p_publisher: None,
            enable_p2p: true,
            segment_sequence: Arc::new(Mutex::new(0)),
            stream_discovery: Some(discovery),
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

        // Send initial metadata as a StreamSegment
        if self.segment_sender.receiver_count() > 0 {
            let metadata_json = serde_json::json!({
                "video_width": 640,
                "video_height": 480,
                "video_fps": 30.0,
                "audio_sample_rate": 48000,
                "audio_channels": 2,
            });
            let metadata_segment = StreamSegment::metadata(
                self.segment_stream_id.clone(),
                0,
                metadata_json.to_string().into_bytes(),
            );
            let _ = self.segment_sender.send(metadata_segment);
        }

        *self.is_streaming.lock().await = true;

        // If P2P overlay is available, start listening for segments
        if let Some(overlay) = &self.overlay {
            if let Some(stream_id) = &self.stream_id {
                self.start_p2p_segment_listener(overlay.clone(), stream_id.clone())
                    .await?;
            }
        }

        // Announce stream to the P2P network for discovery (publisher only)
        let has_video = self.video_samples.lock().await.is_some();
        if has_video {
        if let (Some(discovery), Some(overlay), Some(stream_id)) =
            (&self.stream_discovery, &self.overlay, &self.stream_id)
        {
            let overlay_stream_id =
                OverlayStreamId::from_bytes(stream_id.as_str().unwrap_or_default().as_bytes().to_vec());
            let peer_id = overlay.local_peer_id();

            let video_info = self.video_codec_info.lock().await;
            let audio_info = self.audio_codec_info.lock().await;

            let mut announcement = StreamAnnouncement::new(
                overlay_stream_id,
                format!("Stream {}", stream_id.as_str().unwrap_or_default()),
                peer_id,
            );

            if let Some((codec, _mime)) = video_info.as_ref() {
                announcement = announcement.with_video(codec.clone(), 0, 0);
            }
            if let Some((codec, _mime)) = audio_info.as_ref() {
                announcement = announcement.with_audio(codec.clone());
            }

            if let Err(e) = discovery.announce_stream(announcement).await {
                warn!("Failed to announce stream: {}", e);
            } else {
                info!("Stream announced to P2P network");
            }
        }
        } // end publisher-only announcement

        // Browse mode: no stream_id but overlay exists — listen for announcements only
        if self.overlay.is_some() && self.stream_id.is_none() {
            let overlay = self.overlay.as_ref().unwrap().clone();
            let discovery = self.stream_discovery.clone();
            let is_streaming = Arc::clone(&self.is_streaming);

            tokio::spawn(async move {
                info!("Browse-mode event listener started");
                while *is_streaming.lock().await {
                    if let Some(event) = overlay.next_event().await {
                        if let OverlayEvent::StreamAnnounced { data, .. } = event {
                            if let Some(ref disc) = discovery {
                                match disc.process_announcement(&data).await {
                                    Ok(Some(ann)) => {
                                        info!(
                                            "Discovered stream: {}",
                                            ann.summary()
                                        );
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        warn!(
                                            "Failed to process announcement: {}",
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
                info!("Browse-mode event listener stopped");
            });
        }

        // Start streaming loaded video segments if available
        let video_samples = Arc::clone(&self.video_samples);
        let segment_sender = self.segment_sender.clone();
        let is_streaming = Arc::clone(&self.is_streaming);

        // Clone fields for the streaming task
        let enable_p2p = self.enable_p2p;
        let overlay = self.overlay.clone();
        let stream_id = self.stream_id.clone();
        let init_segment = Arc::clone(&self.initialization_segment);

        tokio::spawn(async move {
            // Wait a bit for clients to connect
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // If P2P enabled, subscribe to the GossipSub topic before publishing
            if enable_p2p {
                if let (Some(overlay), Some(stream_id)) = (&overlay, &stream_id) {
                    let overlay_stream_id =
                        OverlayStreamId::from_bytes(
                            stream_id.as_str().unwrap_or_default().as_bytes().to_vec(),
                        );
                    if let Err(e) = overlay.publish_stream(&overlay_stream_id).await
                    {
                        warn!(
                            "Failed to publish_stream (topic subscribe): {}",
                            e
                        );
                    } else {
                        info!("Subscribed to GossipSub topic for publishing");
                    }
                }
            }

            // If P2P enabled, publish init segment first
            if enable_p2p {
                if let (Some(overlay), Some(stream_id)) = (&overlay, &stream_id) {
                    if let Some(init_data) = &*init_segment.lock().await {
                        let init_stream_id = StreamId::new(stream_id.as_str().unwrap_or_default());
                        let segment =
                            StreamSegment::initialization(init_stream_id, init_data.clone());
                        if let Ok(wire_bytes) = segment.to_wire_bytes() {
                            let overlay_stream_id =
                                OverlayStreamId::from_bytes(stream_id.as_str().unwrap_or_default().as_bytes().to_vec());
                            if let Err(e) = overlay
                                .publish_stream_data(&overlay_stream_id, wire_bytes)
                                .await
                            {
                                warn!("Failed to publish init segment to P2P: {}", e);
                            } else {
                                info!("Published init segment to P2P ({} bytes)", init_data.len());
                            }
                        }
                    }
                }
            }

            let samples = video_samples.lock().await;
            if let Some(ref video_segments) = *samples {
                info!("Starting to stream {} loaded segments", video_segments.len());

                for (index, segment) in video_segments.iter().enumerate() {
                    // Skip initialization segments as they're sent separately
                    if segment.is_init() {
                        continue;
                    }

                    // Only send if there are active receivers OR P2P is enabled
                    if segment_sender.receiver_count() == 0 && !enable_p2p {
                        info!("No clients connected and P2P disabled, stopping segment streaming");
                        break;
                    }

                    // Send to local WebSocket clients
                    if segment_sender.receiver_count() > 0 && segment_sender.send(segment.clone()).is_err() {
                        info!("All receivers dropped");
                    }

                    // Publish to P2P overlay network
                    if enable_p2p {
                        if let (Some(overlay), Some(stream_id)) = (&overlay, &stream_id) {
                            if let Ok(wire_bytes) = segment.to_wire_bytes() {
                                let overlay_stream_id = OverlayStreamId::from_bytes(
                                    stream_id.as_str().unwrap_or_default().as_bytes().to_vec(),
                                );
                                if let Err(e) = overlay
                                    .publish_stream_data(&overlay_stream_id, wire_bytes)
                                    .await
                                {
                                    warn!("Failed to publish segment {} to P2P: {}", index, e);
                                } else {
                                    debug!(
                                        "Published segment {} to P2P ({} bytes, keyframe={})",
                                        index,
                                        segment.data.len(),
                                        segment.is_keyframe
                                    );
                                }
                            }
                        }
                    }

                    info!(
                        "Sent segment {} ({} bytes, ts={}us, keyframe={})",
                        index,
                        segment.data.len(),
                        segment.pts_us,
                        segment.is_keyframe
                    );

                    // Add delay between segments to simulate streaming
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }

                info!("Finished streaming all segments");
                *is_streaming.lock().await = false;
            } else if enable_p2p {
                info!(
                    "No video samples loaded — subscriber mode, \
                     P2P listener stays active"
                );
            } else {
                warn!("No video samples loaded and P2P disabled");
                *is_streaming.lock().await = false;
            }
        });

        Ok(())
    }

    /// Start listening for P2P segments and forward them to local WebSocket clients
    async fn start_p2p_segment_listener(
        &self,
        overlay: Arc<Libp2pOverlay>,
        stream_id: StreamId,
    ) -> Result<(), String> {
        info!("Starting P2P segment listener for stream: {}", stream_id);

        let overlay_stream_id =
            OverlayStreamId::from_bytes(stream_id.as_str().unwrap_or_default().as_bytes().to_vec());

        overlay
            .subscribe_stream(&overlay_stream_id)
            .await
            .map_err(|e| format!("Failed to subscribe to P2P stream: {}", e))?;

        info!("Successfully subscribed to P2P stream: {}", stream_id);

        let segment_sender = self.segment_sender.clone();
        let is_streaming = Arc::clone(&self.is_streaming);
        let init_segment = Arc::clone(&self.initialization_segment);
        let stream_discovery = self.stream_discovery.clone();

        tokio::spawn(async move {
            info!("P2P segment listener task started");
            run_p2p_receive_loop(
                overlay,
                stream_id,
                segment_sender,
                init_segment,
                stream_discovery,
                Some(is_streaming),
            )
            .await;
            info!("P2P segment listener task stopped");
        });

        Ok(())
    }

    pub async fn stop_streaming(&self) {
        info!("Stopping streaming session");
        *self.is_streaming.lock().await = false;

        // Remove stream announcement from discovery
        if let (Some(discovery), Some(stream_id)) =
            (&self.stream_discovery, &self.stream_id)
        {
            let overlay_stream_id =
                OverlayStreamId::from_bytes(stream_id.as_str().unwrap_or_default().as_bytes().to_vec());
            if let Err(e) = discovery.remove_stream(&overlay_stream_id).await {
                warn!("Failed to remove stream announcement: {}", e);
            }
        }
    }

    /// Publish a segment to the P2P overlay network
    ///
    /// Serializes the StreamSegment to WireFormat for efficient P2P distribution.
    #[allow(dead_code)]
    pub async fn publish_segment_to_p2p(&self, segment: &StreamSegment) -> Result<(), String> {
        if !self.enable_p2p {
            return Ok(()); // P2P disabled, skip silently
        }

        let overlay = match &self.overlay {
            Some(o) => o,
            None => return Ok(()), // No overlay available
        };

        let stream_id = match &self.stream_id {
            Some(id) => id,
            None => return Err("No stream ID configured".to_string()),
        };

        // Serialize to wire format (bincode, ~30-40% smaller than JSON)
        let wire_bytes = segment
            .to_wire_bytes()
            .map_err(|e| format!("Failed to serialize segment to wire format: {:?}", e))?;

        // Convert to overlay stream ID
        let overlay_stream_id = OverlayStreamId::from_bytes(stream_id.as_str().unwrap_or_default().as_bytes().to_vec());

        // Publish to overlay network
        overlay
            .publish_stream_data(&overlay_stream_id, wire_bytes)
            .await
            .map_err(|e| format!("Failed to publish to P2P: {}", e))?;

        debug!(
            "Published {} segment {} to P2P ({} bytes)",
            segment.media_type,
            segment.sequence,
            segment.data.len()
        );

        Ok(())
    }

    /// Publish initialization segment to P2P network
    #[allow(dead_code)]
    pub async fn publish_init_to_p2p(&self, init_data: &[u8]) -> Result<(), String> {
        if !self.enable_p2p {
            return Ok(());
        }

        // Create initialization segment using the stored segment_stream_id
        let segment = StreamSegment::initialization(self.segment_stream_id.clone(), init_data.to_vec());
        self.publish_segment_to_p2p(&segment).await?;

        info!(
            "Published initialization segment to P2P ({} bytes)",
            init_data.len()
        );
        Ok(())
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
        let video_reader = VideoReader::open(video_path)
            .map_err(|e| format!("Failed to open video file: {}", e))?;

        // Log video track info
        if let Some(video_track) = video_reader.video_track() {
            info!(
                "Video track: {}x{} @ {:.2}fps, codec: {}",
                video_track.width, video_track.height, video_track.fps, video_track.codec
            );
        }

        // Log audio track info
        if let Some(audio_track) = video_reader.audio_track() {
            info!(
                "Audio track: {} Hz, {} channels, codec: {}",
                audio_track.sample_rate, audio_track.channels, audio_track.codec
            );
        }

        // Use the new MP4 parser to generate proper MSE segments
        info!("Using MP4 parser to generate MSE-compatible segments");

        let file_data =
            std::fs::read(video_path).map_err(|e| format!("Failed to read video file: {}", e))?;

        // Parse the MP4 file
        let mut mp4_parser = Mp4Parser::new();
        mp4_parser
            .parse(&file_data)
            .map_err(|e| format!("Failed to parse MP4 file: {}", e))?;

        info!("MP4 parsing complete: {}", mp4_parser.get_summary());

        // Store detected codec information
        if let Some((video_codec, video_mime)) = mp4_parser.get_video_codec_info() {
            info!("Detected video codec: {} -> {}", video_codec, video_mime);
            *self.video_codec_info.lock().await =
                Some((video_codec.to_string(), video_mime.to_string()));
        }
        if let Some((audio_codec, audio_mime)) = mp4_parser.get_audio_codec_info() {
            info!("Detected audio codec: {} -> {}", audio_codec, audio_mime);
            *self.audio_codec_info.lock().await =
                Some((audio_codec.to_string(), audio_mime.to_string()));
        }

        // Generate separate video/audio init segments for Chrome MSE buffer separation
        match mp4_parser.generate_separate_init_segments() {
            Ok((video_init, audio_init)) => {
                info!(
                    "Generated separate init segments for Chrome MSE: video={} bytes, audio={:?} bytes",
                    video_init.len(),
                    audio_init.as_ref().map(|a| a.len())
                );
                *self.video_init_segment.lock().await = Some(video_init);
                *self.audio_init_segment.lock().await = audio_init;
            }
            Err(e) => {
                warn!(
                    "Failed to generate separate init segments: {}. Will use combined init.",
                    e
                );
            }
        }

        // Generate unified StreamSegments directly
        let stream_segments = mp4_parser
            .generate_stream_segments(self.segment_stream_id.clone())
            .map_err(|e| format!("Failed to generate segments: {}", e))?;

        info!("Generated {} stream segments", stream_segments.len());

        // Store initialization segment and verify ESDS
        for (index, segment) in stream_segments.iter().enumerate() {
            if segment.is_init() {
                *self.initialization_segment.lock().await =
                    Some(segment.data.to_vec());
                info!(
                    "Stored initialization segment ({} bytes)",
                    segment.data.len()
                );
                Self::verify_esds_in_init_segment(&segment.data);
            }

            debug!(
                "Segment {}: type={}, size={} bytes, keyframe={}",
                index,
                segment.media_type,
                segment.data.len(),
                segment.is_keyframe
            );
        }

        info!(
            "Prepared {} stream segments",
            stream_segments.len()
        );

        // Store prepared segments for streaming
        *self.video_samples.lock().await = Some(stream_segments);

        Ok(())
    }

    #[cfg(feature = "ffmpeg")]
    async fn _load_video_with_ffmpeg(
        &self,
        _ffmpeg_reader: FFmpegVideoReader,
    ) -> Result<(), String> {
        // TODO: Re-implement when FFmpeg thread safety issues are resolved
        Err("FFmpeg support temporarily disabled".to_string())
    }

    /// Send a video segment to connected WebSocket clients
    pub async fn send_video_segment(&self, data: Vec<u8>, is_keyframe: bool) -> Result<(), String> {
        // Only send if there are active receivers
        if self.segment_sender.receiver_count() == 0 {
            return Ok(()); // No clients connected, skip silently
        }

        // Get next sequence number
        let sequence = {
            let mut seq = self.segment_sequence.lock().await;
            let current = *seq;
            *seq += 1;
            current
        };

        let timestamp_us = chrono::Utc::now().timestamp_micros() as u64;
        let segment = StreamSegment::video(
            self.segment_stream_id.clone(),
            sequence,
            timestamp_us,
            0, // duration unknown for live segments
            data,
            is_keyframe,
        );

        match self.segment_sender.send(segment) {
            Ok(_) => Ok(()),
            Err(broadcast::error::SendError(_)) => {
                // All receivers dropped, which is fine
                Ok(())
            }
        }
    }

    /// Send an audio segment to connected WebSocket clients
    pub async fn send_audio_segment(&self, data: Vec<u8>) -> Result<(), String> {
        // Only send if there are active receivers
        if self.segment_sender.receiver_count() == 0 {
            return Ok(()); // No clients connected, skip silently
        }

        // Get next sequence number
        let sequence = {
            let mut seq = self.segment_sequence.lock().await;
            let current = *seq;
            *seq += 1;
            current
        };

        let timestamp_us = chrono::Utc::now().timestamp_micros() as u64;
        let segment = StreamSegment::audio(
            self.segment_stream_id.clone(),
            sequence,
            timestamp_us,
            0, // duration unknown for live segments
            data,
        );

        match self.segment_sender.send(segment) {
            Ok(_) => Ok(()),
            Err(broadcast::error::SendError(_)) => {
                // All receivers dropped, which is fine
                Ok(())
            }
        }
    }

    /// Verify ESDS configuration in initialization segment for Chrome compatibility
    /// Chrome requires: objectTypeIndication=0x40 AND audioObjectType=2
    fn verify_esds_in_init_segment(data: &[u8]) {
        // Search for 'esds' box
        let mut i = 0;
        while i < data.len().saturating_sub(30) {
            if &data[i..i + 4] == b"esds" {
                info!("📊 ESDS Verification: Found ESDS box at offset {}", i);

                // Search for DecoderConfigDescriptor (tag 0x04)
                for j in i + 4..std::cmp::min(i + 60, data.len().saturating_sub(15)) {
                    if data[j] == 0x04 {
                        // Skip tag and length bytes
                        let mut k = j + 1;
                        while k < data.len() && (data[k] & 0x80) != 0 {
                            k += 1;
                        }
                        k += 1; // Skip last length byte

                        if k < data.len() {
                            let object_type_indication = data[k];
                            info!(
                                "📊 ESDS Verification: objectTypeIndication=0x{:02X} at offset {}",
                                object_type_indication, k
                            );

                            // Find DecSpecificInfoDescriptor (tag 0x05) for AudioSpecificConfig
                            for m in k + 13..std::cmp::min(k + 30, data.len().saturating_sub(5)) {
                                if data[m] == 0x05 {
                                    let mut n = m + 1;
                                    while n < data.len() && (data[n] & 0x80) != 0 {
                                        n += 1;
                                    }
                                    n += 1;

                                    if n < data.len() {
                                        let asc_byte = data[n];
                                        let audio_object_type = (asc_byte >> 3) & 0x1F;
                                        info!(
                                            "📊 ESDS Verification: audioObjectType={} (raw byte: 0x{:02X}) at offset {}",
                                            audio_object_type, asc_byte, n
                                        );

                                        // Final Chrome compatibility check
                                        if object_type_indication == 0x40 && audio_object_type == 2
                                        {
                                            info!("✅ ESDS Verification: Chrome-compatible (objectTypeIndication=0x40, audioObjectType=2)");
                                        } else {
                                            warn!(
                                                "⚠️ ESDS Verification: NOT Chrome-compatible! Expected 0x40/2, got 0x{:02X}/{}",
                                                object_type_indication, audio_object_type
                                            );
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        break;
                    }
                }
                return;
            }
            i += 1;
        }
        info!("📊 ESDS Verification: No ESDS box found (video-only or non-AAC audio)");
    }
}

/// Standalone P2P receive loop that processes overlay events and forwards
/// segments to a broadcast channel. Used by both `start_p2p_segment_listener`
/// (StreamManager lifetime) and dynamic per-WebSocket subscriptions.
///
/// If `stop_flag` is None, the loop runs until the overlay stops producing
/// events (suitable for tasks that are cancelled by dropping the JoinHandle).
async fn run_p2p_receive_loop(
    overlay: Arc<Libp2pOverlay>,
    target_stream_id: StreamId,
    segment_sender: broadcast::Sender<StreamSegment>,
    init_segment: Arc<Mutex<Option<Vec<u8>>>>,
    stream_discovery: Option<Arc<StreamDiscovery<Libp2pOverlay>>>,
    stop_flag: Option<Arc<Mutex<bool>>>,
) {
    loop {
        // Check stop flag if provided
        if let Some(ref flag) = stop_flag {
            if !*flag.lock().await {
                break;
            }
        }

        if let Some(event) = overlay.next_event().await {
            match event {
                OverlayEvent::StreamData {
                    stream_id: recv_stream_id,
                    data,
                    ..
                } => {
                    let recv_id_str =
                        String::from_utf8_lossy(recv_stream_id.as_bytes())
                            .to_string();
                    if recv_id_str != target_stream_id.as_str().unwrap_or_default() {
                        continue;
                    }
                    // Try to deserialize as WireSegment (new format)
                    match WireSegment::from_bytes(&data) {
                        Ok(wire_segment) => {
                            let segment: StreamSegment = wire_segment.into();

                            if segment.media_type == MediaType::Initialization {
                                let mut init = init_segment.lock().await;
                                *init = Some(segment.data.to_vec());
                                info!(
                                    "Received P2P init segment ({} bytes)",
                                    segment.data.len()
                                );
                            }

                            if segment_sender.receiver_count() > 0 {
                                let _ = segment_sender.send(segment);
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to parse P2P data as \
                                 WireSegment: {}",
                                e
                            );
                        }
                    }
                }
                OverlayEvent::StreamAnnounced { data, .. } => {
                    if let Some(ref discovery) = stream_discovery {
                        match discovery.process_announcement(&data).await {
                            Ok(Some(ann)) => {
                                info!(
                                    "Discovered stream: {}",
                                    ann.summary()
                                );
                            }
                            Ok(None) => {} // expired, ignored
                            Err(e) => {
                                warn!(
                                    "Failed to process stream \
                                     announcement: {}",
                                    e
                                );
                            }
                        }
                    }
                }
                _ => {}
            }
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

// ── Relay WebSocket proxy for WASM browser clients ──────────────────

/// JSON messages exchanged with WASM browsers over /ws/relay
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum RelayMessage {
    #[serde(rename = "subscribe")]
    Subscribe { stream_id: String },
    #[serde(rename = "unsubscribe")]
    Unsubscribe { stream_id: String },
    #[serde(rename = "publish")]
    Publish { stream_id: String },
    #[serde(rename = "list_streams")]
    ListStreams,
    #[serde(rename = "stream_list")]
    StreamList { streams: Vec<RelayStreamInfo> },
    #[serde(rename = "error")]
    Error { message: String },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RelayStreamInfo {
    stream_id: String,
    publisher: Option<String>,
}

/// WebSocket handler for WASM browser relay connections
async fn relay_websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> Response {
    ws.on_upgrade(move |socket| handle_relay_websocket(socket, state))
}

/// Handle a single WASM relay WebSocket connection
async fn handle_relay_websocket(socket: WebSocket, state: AppState) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let sm = &state.stream_manager;

    // Per-connection subscription: spawns a task that forwards P2P data
    let mut relay_task: Option<tokio::task::JoinHandle<()>> = None;
    let (binary_tx, mut binary_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (text_tx, mut text_rx) = mpsc::unbounded_channel::<String>();

    // Spawn a task that forwards frames to the browser
    let forward_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(data) = binary_rx.recv() => {
                    if ws_sender.send(Message::Binary(data)).await.is_err() {
                        break;
                    }
                }
                Some(text) = text_rx.recv() => {
                    if ws_sender.send(Message::Text(text)).await.is_err() {
                        break;
                    }
                }
                else => break,
            }
        }
    });

    while let Some(Ok(msg)) = ws_receiver.next().await {
        match msg {
            Message::Text(text) => {
                let parsed: Result<RelayMessage, _> = serde_json::from_str(&text);
                match parsed {
                    Ok(RelayMessage::Subscribe { stream_id }) => {
                        info!(
                            "WASM relay: subscribe to stream '{}'",
                            stream_id
                        );
                        // Cancel any existing relay task
                        if let Some(handle) = relay_task.take() {
                            handle.abort();
                        }

                        // Start forwarding P2P data for this stream
                        if let Some(ref overlay) = sm.overlay {
                            let overlay = Arc::clone(overlay);
                            let sid = stream_id.clone();
                            let tx = binary_tx.clone();

                            // Subscribe on the overlay
                            let overlay_sid =
                                OverlayStreamId::from_string(&sid);
                            if let Err(e) =
                                overlay.subscribe_stream(&overlay_sid).await
                            {
                                warn!(
                                    "Relay subscribe failed: {}",
                                    e
                                );
                            }

                            // Send init segment if available
                            {
                                let init = sm.initialization_segment.lock().await;
                                if let Some(ref data) = *init {
                                    let _ = tx.send(data.clone());
                                }
                            }

                            // Spawn task to forward overlay events
                            relay_task = Some(tokio::spawn(async move {
                                let target = OverlayStreamId::from_string(&sid);
                                loop {
                                    if let Some(event) =
                                        overlay.next_event().await
                                    {
                                        if let OverlayEvent::StreamData {
                                            stream_id: recv_id,
                                            data,
                                            ..
                                        } = event
                                        {
                                            let recv_str =
                                                String::from_utf8_lossy(
                                                    recv_id.as_bytes(),
                                                )
                                                .to_string();
                                            let target_str = target
                                                .as_str()
                                                .unwrap_or_default();
                                            if recv_str == target_str {
                                                if tx.send(data).is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    } else {
                                        tokio::time::sleep(
                                            Duration::from_millis(50),
                                        )
                                        .await;
                                    }
                                }
                            }));
                        } else {
                            // No overlay — subscribe to the broadcast channel
                            let mut rx = sm.segment_sender.subscribe();
                            let tx = binary_tx.clone();
                            relay_task = Some(tokio::spawn(async move {
                                while let Ok(segment) = rx.recv().await {
                                    let wire: WireSegment = (&segment).into();
                                    if let Ok(bytes) = wire.to_bytes() {
                                        if tx.send(bytes).is_err() {
                                            break;
                                        }
                                    }
                                }
                            }));
                        }
                    }
                    Ok(RelayMessage::Unsubscribe { stream_id }) => {
                        info!(
                            "WASM relay: unsubscribe from stream '{}'",
                            stream_id
                        );
                        if let Some(handle) = relay_task.take() {
                            handle.abort();
                        }
                    }
                    Ok(RelayMessage::Publish { stream_id }) => {
                        info!(
                            "WASM relay: publish stream '{}'",
                            stream_id
                        );
                        // Browser is publishing — announce stream
                        if let Some(ref overlay) = sm.overlay {
                            let sid =
                                OverlayStreamId::from_string(&stream_id);
                            if let Err(e) =
                                overlay.publish_stream(&sid).await
                            {
                                warn!(
                                    "Relay publish announce failed: {}",
                                    e
                                );
                            }
                        }
                    }
                    Ok(RelayMessage::ListStreams) => {
                        let streams = if let Some(ref discovery) =
                            sm.stream_discovery
                        {
                            discovery
                                .list_streams()
                                .await
                                .iter()
                                .map(|ann| RelayStreamInfo {
                                    stream_id: ann.stream_id.to_string(),
                                    publisher: Some(
                                        String::from_utf8_lossy(
                                            &ann.publisher_id,
                                        )
                                        .to_string(),
                                    ),
                                })
                                .collect()
                        } else {
                            vec![]
                        };

                        let resp = RelayMessage::StreamList { streams };
                        if let Ok(json) = serde_json::to_string(&resp) {
                            let _ = text_tx.send(json);
                        }
                    }
                    Ok(_) => {} // ignore other message types from client
                    Err(e) => {
                        warn!(
                            "WASM relay: invalid JSON from client: {}",
                            e
                        );
                    }
                }
            }
            Message::Binary(data) => {
                // Browser is publishing binary stream data
                if let Some(ref overlay) = sm.overlay {
                    if let Some(ref sid) = sm.stream_id {
                        let overlay_sid = OverlayStreamId::from_string(
                            sid.as_str().unwrap_or_default(),
                        );
                        if let Err(e) = overlay
                            .publish_stream_data(&overlay_sid, data)
                            .await
                        {
                            warn!("Relay publish data failed: {}", e);
                        }
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    // Clean up
    if let Some(handle) = relay_task.take() {
        handle.abort();
    }
    forward_handle.abort();
    info!("WASM relay client disconnected");
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
        let signaling_state = Arc::new(SignalingState::default());

        let app_state = AppState {
            config: config.clone(),
            stream_manager,
            clients,
            signaling_state,
            moderation: None,
            relay_stats: None,
        };

        Self { config, app_state }
    }

    pub fn new_with_state(config: WebServerConfig, app_state: AppState) -> Self {
        Self { config, app_state }
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
            .route("/ws/relay", get(relay_websocket_handler))
            .route("/webrtc/signal", get(webrtc_signaling_handler))
            .route("/api/stream/start", post(start_stream_handler))
            .route("/api/stream/stop", post(stop_stream_handler))
            .route("/api/stream/status", get(stream_status_handler))
            .route("/api/streams", get(list_streams_handler))
            .route("/api/moderation/block-peer", post(mod_block_peer_handler))
            .route(
                "/api/moderation/block-peer/:peer_id",
                delete(mod_unblock_peer_handler),
            )
            .route(
                "/api/moderation/blocked-peers",
                get(mod_blocked_peers_handler),
            )
            .route(
                "/api/moderation/flag-stream",
                post(mod_flag_stream_handler),
            )
            .route(
                "/api/moderation/flagged-streams",
                get(mod_flagged_streams_handler),
            )
            .route("/api/moderation/import", post(mod_import_handler))
            .route("/api/moderation/export", get(mod_export_handler))
            .route("/api/stats", get(stats_handler))
            .with_state(self.app_state.clone());

        // Add CORS if enabled
        if self.config.enable_cors {
            router = router.layer(ServiceBuilder::new().layer(CorsLayer::permissive()));
        }

        // Serve static files (web viewer)
        if self.config.web_root.exists() {
            info!("Serving static files from: {:?}", self.config.web_root);
            router = router.fallback_service(
                ServeDir::new(&self.config.web_root)
                    .append_index_html_on_directories(true)
            );
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
/// Accepts an optional `stream_id` query parameter (e.g. `/stream?stream_id=X`).
async fn websocket_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Response {
    let stream_id = params.get("stream_id").cloned();
    if let Some(ref id) = stream_id {
        info!("WebSocket connection requested stream_id={}", id);
    }
    ws.on_upgrade(move |socket| handle_websocket(socket, state, stream_id))
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
async fn handle_websocket(
    socket: WebSocket,
    state: AppState,
    requested_stream_id: Option<String>,
) {
    let client_id = uuid::Uuid::new_v4().to_string();
    if let Some(ref sid) = requested_stream_id {
        info!(
            "New WebSocket client connected: {} (stream_id={})",
            client_id, sid
        );
    } else {
        info!("New WebSocket client connected: {}", client_id);
    }

    // Split the socket into sender and receiver
    let (mut sender, mut receiver) = socket.split();

    // If the client requested a specific stream and we have an overlay but no
    // matching subscription yet (browse mode), dynamically subscribe now.
    let _dynamic_listener_handle = if let Some(ref sid) = requested_stream_id {
        if state.stream_manager.overlay.is_some()
            && state.stream_manager.stream_id.is_none()
        {
            info!(
                "Dynamic subscription: subscribing to stream {} via overlay",
                sid
            );
            let overlay = state.stream_manager.overlay.as_ref().unwrap().clone();
            let target_stream_id = StreamId::new(sid.clone());
            let overlay_stream_id = OverlayStreamId::from_bytes(
                sid.as_bytes().to_vec(),
            );

            // Subscribe to the GossipSub topic
            if let Err(e) = overlay.subscribe_stream(&overlay_stream_id).await {
                warn!("Failed to subscribe to stream {}: {}", sid, e);
            }
            // Join the topic so we receive data
            if let Err(e) = overlay.publish_stream(&overlay_stream_id).await {
                warn!("Failed to join GossipSub topic for {}: {}", sid, e);
            }

            // Spawn a per-connection P2P receive loop
            let segment_sender = state.stream_manager.segment_sender.clone();
            let init_segment =
                Arc::clone(&state.stream_manager.initialization_segment);
            let discovery = state.stream_manager.stream_discovery.clone();

            let handle = tokio::spawn(async move {
                run_p2p_receive_loop(
                    overlay,
                    target_stream_id,
                    segment_sender,
                    init_segment,
                    discovery,
                    None, // runs until task is dropped
                )
                .await;
            });
            Some(handle)
        } else {
            None
        }
    } else {
        None
    };

    // Subscribe to stream segments
    let mut segment_receiver = state.stream_manager.segment_sender.subscribe();

    // Send initial stream info with detected codec information
    // In browse/subscriber mode, codec info may not be available locally —
    // the client will receive it from the P2P init segment instead.
    let _has_local_codec_info =
        state.stream_manager.video_codec_info.lock().await.is_some();

    let (video_codec, video_mime) =
        if let Some((codec, mime)) = &*state.stream_manager.video_codec_info.lock().await {
            (codec.clone(), Some(mime.clone()))
        } else {
            (
                "H.264".to_string(),
                Some("video/mp4; codecs=\"avc1.42E01E\"".to_string()),
            )
        };

    // Check if we actually have an audio track
    let audio_info =
        if let Some((codec, mime)) = &*state.stream_manager.audio_codec_info.lock().await {
            info!(
                "🔍 WEBSOCKET DEBUG: Using cached audio codec info: {} -> {}",
                codec, mime
            );
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

    info!(
        "🔍 WEBSOCKET DEBUG: Final stream configuration - Video: {:?}, Audio: {:?}",
        video_mime,
        audio_info.as_ref().map(|a| &a.mime_type)
    );

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
        info!(
            "🔍 WEBSOCKET DEBUG: Serialized stream_info message: {}",
            message
        );
        if let Err(e) = sender.send(Message::Text(message)).await {
            error!("Failed to send initial stream info: {}", e);
            return;
        }
        info!("🔍 WEBSOCKET DEBUG: Successfully sent stream_info message to client");
    }

    // Send initialization segments to client
    // Use separate video/audio init segments for Chrome MSE buffer separation
    {
        const MAX_CHUNK_SIZE: usize = 512 * 1024; // 512KB chunks

        let video_init =
            state.stream_manager.video_init_segment.lock().await.clone();
        let audio_init =
            state.stream_manager.audio_init_segment.lock().await.clone();

        if let Some(ref video_init_data) = video_init {
            // Send video init segment
            info!(
                "Sending video init segment to client {} ({} bytes)",
                client_id,
                video_init_data.len()
            );

            let chunk_info = ClientMessage::ChunkInfo {
                data: ChunkInfo {
                    chunk_type: "init_video".to_string(),
                    size: video_init_data.len(),
                    timestamp: 0,
                },
            };

            if let Ok(info_message) = serde_json::to_string(&chunk_info) {
                if let Err(e) =
                    sender.send(Message::Text(info_message)).await
                {
                    error!("Failed to send video init chunk info: {}", e);
                    return;
                }
            }

            if video_init_data.len() > MAX_CHUNK_SIZE {
                let mut offset = 0;
                let mut chunk_num = 0;
                while offset < video_init_data.len() {
                    let chunk_end = std::cmp::min(
                        offset + MAX_CHUNK_SIZE,
                        video_init_data.len(),
                    );
                    let chunk = &video_init_data[offset..chunk_end];
                    chunk_num += 1;

                    let chunk_info = ClientMessage::ChunkInfo {
                        data: ChunkInfo {
                            chunk_type: format!(
                                "init_video_chunk_{}",
                                chunk_num
                            ),
                            size: chunk.len(),
                            timestamp: offset as u64,
                        },
                    };
                    if let Ok(info_message) =
                        serde_json::to_string(&chunk_info)
                    {
                        if let Err(e) =
                            sender.send(Message::Text(info_message)).await
                        {
                            error!(
                                "Failed to send chunk {} info: {}",
                                chunk_num, e
                            );
                            return;
                        }
                    }
                    if let Err(e) =
                        sender.send(Message::Binary(chunk.to_vec())).await
                    {
                        error!(
                            "Failed to send video init chunk {}: {}",
                            chunk_num, e
                        );
                        return;
                    }
                    offset = chunk_end;
                }
            } else if let Err(e) = sender
                .send(Message::Binary(video_init_data.clone()))
                .await
            {
                error!("Failed to send video init segment: {}", e);
                return;
            }

            info!(
                "Successfully sent video init segment to client {}",
                client_id
            );

            // Send audio init segment if available
            if let Some(ref audio_init_data) = audio_init {
                info!(
                    "Sending audio init segment to client {} ({} bytes)",
                    client_id,
                    audio_init_data.len()
                );

                let chunk_info = ClientMessage::ChunkInfo {
                    data: ChunkInfo {
                        chunk_type: "init_audio".to_string(),
                        size: audio_init_data.len(),
                        timestamp: 0,
                    },
                };

                if let Ok(info_message) =
                    serde_json::to_string(&chunk_info)
                {
                    if let Err(e) =
                        sender.send(Message::Text(info_message)).await
                    {
                        error!(
                            "Failed to send audio init chunk info: {}",
                            e
                        );
                        return;
                    }
                }

                if let Err(e) = sender
                    .send(Message::Binary(audio_init_data.clone()))
                    .await
                {
                    error!("Failed to send audio init segment: {}", e);
                    return;
                }

                info!(
                    "Successfully sent audio init segment to client {}",
                    client_id
                );
            }
        } else if let Some(init_segment) =
            &*state.stream_manager.initialization_segment.lock().await
        {
            // Fallback: use combined init segment
            info!(
                "Sending combined init segment to client {} ({} bytes)",
                client_id,
                init_segment.len()
            );

            let chunk_info = ClientMessage::ChunkInfo {
                data: ChunkInfo {
                    chunk_type: "initialization".to_string(),
                    size: init_segment.len(),
                    timestamp: 0,
                },
            };

            if let Ok(info_message) = serde_json::to_string(&chunk_info) {
                if let Err(e) =
                    sender.send(Message::Text(info_message)).await
                {
                    error!(
                        "Failed to send initialization chunk info: {}",
                        e
                    );
                    return;
                }
            }

            if init_segment.len() > MAX_CHUNK_SIZE {
                let mut offset = 0;
                let mut chunk_num = 0;
                while offset < init_segment.len() {
                    let chunk_end = std::cmp::min(
                        offset + MAX_CHUNK_SIZE,
                        init_segment.len(),
                    );
                    let chunk = &init_segment[offset..chunk_end];
                    chunk_num += 1;

                    let chunk_info = ClientMessage::ChunkInfo {
                        data: ChunkInfo {
                            chunk_type: format!(
                                "initialization_chunk_{}",
                                chunk_num
                            ),
                            size: chunk.len(),
                            timestamp: offset as u64,
                        },
                    };
                    if let Ok(info_message) =
                        serde_json::to_string(&chunk_info)
                    {
                        if let Err(e) =
                            sender.send(Message::Text(info_message)).await
                        {
                            error!(
                                "Failed to send chunk {} info: {}",
                                chunk_num, e
                            );
                            return;
                        }
                    }
                    if let Err(e) =
                        sender.send(Message::Binary(chunk.to_vec())).await
                    {
                        error!(
                            "Failed to send init chunk {}: {}",
                            chunk_num, e
                        );
                        return;
                    }
                    offset = chunk_end;
                }

                let completion_info = ClientMessage::ChunkInfo {
                    data: ChunkInfo {
                        chunk_type: "initialization_complete".to_string(),
                        size: init_segment.len(),
                        timestamp: 0,
                    },
                };
                if let Ok(info_message) =
                    serde_json::to_string(&completion_info)
                {
                    if let Err(e) =
                        sender.send(Message::Text(info_message)).await
                    {
                        error!(
                            "Failed to send completion marker: {}",
                            e
                        );
                        return;
                    }
                }
            } else if let Err(e) = sender
                .send(Message::Binary(init_segment.clone()))
                .await
            {
                error!("Failed to send initialization segment: {}", e);
                return;
            }

            info!(
                "Successfully sent combined init segment to client {}",
                client_id
            );
        } else {
            warn!(
                "No initialization segment available for client {}",
                client_id
            );
        }
    }

    // Shared flag so the reader task can signal the writer to stop
    let client_disconnected = Arc::new(tokio::sync::Notify::new());

    // Spawn task to handle incoming messages from client (reader side)
    let client_id_clone = client_id.clone();
    let disconnect_notify = client_disconnected.clone();
    tokio::spawn(async move {
        while let Some(message) = receiver.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    debug!(
                        "Received text from client {}: {}",
                        client_id_clone, text
                    );
                }
                Ok(Message::Binary(_)) => {
                    debug!(
                        "Received binary data from client {}",
                        client_id_clone
                    );
                }
                Ok(Message::Pong(_)) => {
                    debug!("Pong from client {}", client_id_clone);
                }
                Ok(Message::Close(_)) => {
                    info!("Client {} disconnected", client_id_clone);
                    break;
                }
                Err(e) => {
                    warn!(
                        "WebSocket read error for client {}: {} — \
                         cleaning up connection",
                        client_id_clone, e
                    );
                    break;
                }
                _ => {}
            }
        }
        // Signal the writer loop that this client is gone
        disconnect_notify.notify_one();
    });

    // Backpressure / drop tracking
    let mut dropped_segments: u64 = 0;
    let mut last_heartbeat = tokio::time::Instant::now();
    let heartbeat_interval = tokio::time::Duration::from_secs(30);

    // Handle outgoing stream segments to client
    // Convert StreamSegment → JSON at the WebSocket boundary
    loop {
        tokio::select! {
            // Client reader task signalled disconnect
            _ = client_disconnected.notified() => {
                info!(
                    "Client {} reader closed — stopping writer \
                     (dropped {} segments)",
                    client_id, dropped_segments
                );
                break;
            }
            segment_result = segment_receiver.recv() => {
                match segment_result {
                    Ok(segment) => {
                        let message = match segment.media_type {
                            MediaType::Video | MediaType::Initialization => {
                                // Determine chunk type for JSON message
                                let chunk_type = if segment.media_type == MediaType::Initialization {
                                    "initialization".to_string()
                                } else if segment.is_keyframe {
                                    "video-keyframe".to_string()
                                } else {
                                    "video".to_string()
                                };

                                // Send chunk info first
                                let chunk_info = ClientMessage::ChunkInfo {
                                    data: ChunkInfo {
                                        chunk_type,
                                        size: segment.data.len(),
                                        timestamp: segment.pts_ms(),
                                    },
                                };

                                if let Ok(info_message) = serde_json::to_string(&chunk_info) {
                                    if sender.send(Message::Text(info_message)).await.is_err() {
                                        dropped_segments += 1;
                                        warn!(
                                            "Client {} send failed — \
                                             removing dead connection \
                                             (total dropped: {})",
                                            client_id, dropped_segments
                                        );
                                        break;
                                    }
                                }

                                // Add a small delay to prevent overwhelming the client's SourceBuffer
                                // Chrome's SourceBuffer needs time to process each segment
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                                // Then send the actual data
                                Message::Binary(segment.data.to_vec())
                            }
                            MediaType::Audio => {
                                let chunk_info = ClientMessage::ChunkInfo {
                                    data: ChunkInfo {
                                        chunk_type: "audio".to_string(),
                                        size: segment.data.len(),
                                        timestamp: segment.pts_ms(),
                                    },
                                };

                                if let Ok(info_message) = serde_json::to_string(&chunk_info) {
                                    if sender.send(Message::Text(info_message)).await.is_err() {
                                        dropped_segments += 1;
                                        warn!(
                                            "Client {} send failed — \
                                             removing dead connection \
                                             (total dropped: {})",
                                            client_id, dropped_segments
                                        );
                                        break;
                                    }
                                }

                                Message::Binary(segment.data.to_vec())
                            }
                            MediaType::Metadata => {
                                // Parse metadata JSON from segment data
                                if let Ok(meta_str) = std::str::from_utf8(&segment.data) {
                                    if let Ok(meta) = serde_json::from_str::<serde_json::Value>(meta_str) {
                                        let stream_info = ClientMessage::StreamInfo {
                                            data: StreamInfo {
                                                video: Some(VideoInfo {
                                                    width: meta.get("video_width").and_then(|v| v.as_u64()).unwrap_or(640) as u32,
                                                    height: meta.get("video_height").and_then(|v| v.as_u64()).unwrap_or(480) as u32,
                                                    fps: meta.get("video_fps").and_then(|v| v.as_f64()).unwrap_or(30.0) as f32,
                                                    codec: "H.264".to_string(),
                                                    mime_type: Some("video/mp4; codecs=\"avc1.42E01E\"".to_string()),
                                                }),
                                                audio: Some(AudioInfo {
                                                    sample_rate: meta.get("audio_sample_rate").and_then(|v| v.as_u64()).unwrap_or(48000) as u32,
                                                    channels: meta.get("audio_channels").and_then(|v| v.as_u64()).unwrap_or(2) as u16,
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
                                    } else {
                                        continue;
                                    }
                                } else {
                                    continue;
                                }
                            }
                        };

                        if sender.send(message).await.is_err() {
                            dropped_segments += 1;
                            warn!(
                                "Client {} send failed — removing dead \
                                 connection (total dropped: {})",
                                client_id, dropped_segments
                            );
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        dropped_segments += n;
                        warn!(
                            "Client {} lagged: skipped {} segments \
                             (total dropped: {})",
                            client_id, n, dropped_segments
                        );
                        // Continue — the receiver auto-advances past
                        // the gap so we'll get the next available segment
                        continue;
                    }
                    Err(_) => {
                        // Channel closed, exit gracefully
                        break;
                    }
                }
            }
            // Heartbeat ping + end-of-stream check
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {
                // Send keepalive ping if interval has elapsed
                let now = tokio::time::Instant::now();
                if now.duration_since(last_heartbeat) >= heartbeat_interval {
                    if sender.send(Message::Ping(vec![]))
                        .await
                        .is_err()
                    {
                        warn!(
                            "Heartbeat ping failed for client {} — \
                             removing dead connection",
                            client_id
                        );
                        break;
                    }
                    last_heartbeat = now;
                }

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

    if dropped_segments > 0 {
        info!(
            "Client {} total dropped segments: {}",
            client_id, dropped_segments
        );
    }

    // Clean up dynamic P2P listener if one was spawned
    if let Some(handle) = _dynamic_listener_handle {
        handle.abort();
        info!(
            "Stopped dynamic P2P listener for client {}",
            client_id
        );
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

/// API endpoint to list available streams in the network
///
/// Returns a list of streams known to this node, including:
/// - Locally published streams
/// - Streams discovered via P2P network
async fn list_streams_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut streams = Vec::new();

    // Add local stream if streaming
    let is_streaming = *state.stream_manager.is_streaming.lock().await;
    if is_streaming {
        if let Some(stream_id) = &state.stream_manager.stream_id {
            let video_info = state.stream_manager.video_codec_info.lock().await;
            let audio_info = state.stream_manager.audio_codec_info.lock().await;

            streams.push(serde_json::json!({
                "stream_id": stream_id.as_str().unwrap_or_default(),
                "title": format!("Local Stream - {}", stream_id.as_str().unwrap_or_default()),
                "is_local": true,
                "video_codec": video_info.as_ref().map(|(c, _)| c.clone()),
                "audio_codec": audio_info.as_ref().map(|(c, _)| c.clone()),
                "status": "streaming"
            }));
        }
    }

    // Add streams discovered from P2P network via StreamDiscovery
    if let Some(ref discovery) = state.stream_manager.stream_discovery {
        let local_stream_id = state.stream_manager.stream_id.as_ref()
            .map(|id| id.to_string_lossy());

        for ann in discovery.list_streams().await {
            let ann_stream_id = ann.stream_id.to_string();
            // Skip if this is our own local stream (already listed above)
            if let Some(ref local_id) = local_stream_id {
                if &ann_stream_id == local_id {
                    continue;
                }
            }
            streams.push(serde_json::json!({
                "stream_id": ann_stream_id,
                "title": ann.title,
                "is_local": false,
                "video_codec": ann.video_codec,
                "audio_codec": ann.audio_codec,
                "video_width": ann.video_width,
                "video_height": ann.video_height,
                "started_at": ann.started_at,
                "status": "streaming"
            }));
        }
    }

    Json(serde_json::json!({
        "streams": streams,
        "count": streams.len(),
        "p2p_enabled": state.stream_manager.enable_p2p
    }))
}

// ============================================================================
// WebRTC Signaling Handlers
// ============================================================================

/// WebSocket handler for WebRTC signaling connections
///
/// This endpoint handles WebSocket connections for WebRTC signaling,
/// relaying SDP offers/answers and ICE candidates between peers.
async fn webrtc_signaling_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(|socket| handle_webrtc_signaling(socket, state))
}

/// Handle individual WebRTC signaling connections
///
/// This function:
/// 1. Assigns a unique peer ID to the connection
/// 2. Receives and relays signaling messages between peers
/// 3. Manages peer registration and discovery for streams
async fn handle_webrtc_signaling(socket: WebSocket, state: AppState) {
    let peer_id = uuid::Uuid::new_v4().to_string();
    info!("New WebRTC signaling client connected: {}", peer_id);

    // Split the socket into sender and receiver
    let (mut sender, mut receiver) = socket.split();

    // Create channel for sending messages to this peer
    let (tx, mut rx) = mpsc::unbounded_channel::<SignalingMessage>();

    // Register this peer's sender
    {
        let mut senders = state.signaling_state.peer_senders.write().await;
        senders.insert(peer_id.clone(), tx);
    }

    // Track which stream this peer is in (if any)
    let mut current_stream_id: Option<String> = None;

    // Spawn task to forward messages from channel to WebSocket
    let peer_id_clone = peer_id.clone();
    let sender_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match serde_json::to_string(&msg) {
                Ok(json) => {
                    if let Err(e) = sender.send(Message::Text(json)).await {
                        debug!("Failed to send signaling message to {}: {}", peer_id_clone, e);
                        break;
                    }
                }
                Err(e) => {
                    warn!("Failed to serialize signaling message: {}", e);
                }
            }
        }
    });

    // Process incoming messages
    while let Some(message) = receiver.next().await {
        match message {
            Ok(Message::Text(text)) => {
                match serde_json::from_str::<SignalingMessage>(&text) {
                    Ok(signal_msg) => {
                        handle_signaling_message(
                            &state,
                            &peer_id,
                            &mut current_stream_id,
                            signal_msg,
                        )
                        .await;
                    }
                    Err(e) => {
                        warn!("Invalid signaling message from {}: {}", peer_id, e);
                        // Send error back to peer
                        let error_msg = SignalingMessage::Error {
                            message: format!("Invalid message format: {}", e),
                        };
                        let senders = state.signaling_state.peer_senders.read().await;
                        if let Some(sender) = senders.get(&peer_id) {
                            let _ = sender.send(error_msg);
                        }
                    }
                }
            }
            Ok(Message::Close(_)) => {
                info!("WebRTC signaling client {} disconnected", peer_id);
                break;
            }
            Err(e) => {
                error!("WebSocket error for signaling client {}: {}", peer_id, e);
                break;
            }
            _ => {}
        }
    }

    // Cleanup on disconnect
    sender_task.abort();
    cleanup_signaling_peer(&state, &peer_id, &current_stream_id).await;
}

/// Process a signaling message from a peer
async fn handle_signaling_message(
    state: &AppState,
    peer_id: &str,
    current_stream_id: &mut Option<String>,
    message: SignalingMessage,
) {
    match message {
        SignalingMessage::PeerJoin {
            peer_id: _,
            stream_id,
            is_publisher,
        } => {
            // Check moderation before allowing join
            if let Some(ref moderation) = state.moderation {
                if !moderation.is_peer_allowed(peer_id).await {
                    warn!("Rejected signaling from blocked peer: {}", peer_id);
                    let error_msg = SignalingMessage::Error {
                        message: "You have been blocked from this node".to_string(),
                    };
                    let senders = state.signaling_state.peer_senders.read().await;
                    if let Some(sender) = senders.get(peer_id) {
                        let _ = sender.send(error_msg);
                    }
                    return;
                }
            }

            info!(
                "Peer {} joining stream {} (publisher: {})",
                peer_id, stream_id, is_publisher
            );

            // Add peer to stream
            {
                let mut streams = state.signaling_state.streams.write().await;
                let peers = streams.entry(stream_id.clone()).or_insert_with(Vec::new);

                // Remove if already exists (rejoin)
                peers.retain(|p| p.peer_id != peer_id);

                peers.push(SignalingPeer {
                    peer_id: peer_id.to_string(),
                    is_publisher,
                    can_relay: false,
                });

                debug!(
                    "Stream {} now has {} peers",
                    stream_id,
                    peers.len()
                );
            }

            *current_stream_id = Some(stream_id.clone());

            // Send list of existing peers to the new peer
            let peer_list = {
                let streams = state.signaling_state.streams.read().await;
                if let Some(peers) = streams.get(&stream_id) {
                    peers
                        .iter()
                        .filter(|p| p.peer_id != peer_id)
                        .map(|p| PeerInfo {
                            peer_id: p.peer_id.clone(),
                            is_publisher: p.is_publisher,
                            can_relay: p.can_relay,
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            };

            let peer_list_msg = SignalingMessage::PeerList { peers: peer_list };
            let senders = state.signaling_state.peer_senders.read().await;
            if let Some(sender) = senders.get(peer_id) {
                let _ = sender.send(peer_list_msg);
            }

            // Notify other peers about the new peer
            let join_notification = SignalingMessage::PeerJoin {
                peer_id: peer_id.to_string(),
                stream_id: stream_id.clone(),
                is_publisher,
            };

            let streams = state.signaling_state.streams.read().await;
            if let Some(peers) = streams.get(&stream_id) {
                for other_peer in peers.iter().filter(|p| p.peer_id != peer_id) {
                    if let Some(sender) = senders.get(&other_peer.peer_id) {
                        let _ = sender.send(join_notification.clone());
                    }
                }
            }
        }

        SignalingMessage::PeerLeave { peer_id: _ } => {
            info!("Peer {} leaving stream", peer_id);
            cleanup_signaling_peer(state, peer_id, current_stream_id).await;
            *current_stream_id = None;
        }

        SignalingMessage::Offer {
            peer_id: _,
            target_peer_id,
            sdp,
        } => {
            // Drop offers from blocked peers
            if let Some(ref moderation) = state.moderation {
                if !moderation.is_peer_allowed(peer_id).await {
                    debug!("Dropping offer from blocked peer: {}", peer_id);
                    return;
                }
            }

            if let Some(target_id) = target_peer_id {
                debug!("Relaying offer from {} to {}", peer_id, target_id);
                let offer = SignalingMessage::Offer {
                    peer_id: peer_id.to_string(),
                    target_peer_id: Some(target_id.clone()),
                    sdp,
                };

                let senders = state.signaling_state.peer_senders.read().await;
                if let Some(sender) = senders.get(&target_id) {
                    let _ = sender.send(offer);
                } else {
                    warn!("Target peer {} not found for offer relay", target_id);
                }
            }
        }

        SignalingMessage::Answer {
            peer_id: _,
            target_peer_id,
            sdp,
        } => {
            debug!("Relaying answer from {} to {}", peer_id, target_peer_id);
            let answer = SignalingMessage::Answer {
                peer_id: peer_id.to_string(),
                target_peer_id: target_peer_id.clone(),
                sdp,
            };

            let senders = state.signaling_state.peer_senders.read().await;
            if let Some(sender) = senders.get(&target_peer_id) {
                let _ = sender.send(answer);
            } else {
                warn!("Target peer {} not found for answer relay", target_peer_id);
            }
        }

        SignalingMessage::IceCandidate {
            peer_id: _,
            target_peer_id,
            candidate,
            sdp_mid,
            sdp_mline_index,
        } => {
            debug!("Relaying ICE candidate from {} to {}", peer_id, target_peer_id);
            let ice = SignalingMessage::IceCandidate {
                peer_id: peer_id.to_string(),
                target_peer_id: target_peer_id.clone(),
                candidate,
                sdp_mid,
                sdp_mline_index,
            };

            let senders = state.signaling_state.peer_senders.read().await;
            if let Some(sender) = senders.get(&target_peer_id) {
                let _ = sender.send(ice);
            } else {
                debug!("Target peer {} not found for ICE relay", target_peer_id);
            }
        }

        SignalingMessage::PeerUpdate {
            peer_id: _,
            stream_id: _,
            is_publisher: updated_publisher,
            can_relay,
        } => {
            info!(
                "Peer {} updated: is_publisher={}, can_relay={}",
                peer_id, updated_publisher, can_relay
            );

            // Update the peer's state and collect peer IDs to notify
            let stream_id_for_update = current_stream_id.clone();
            let peers_to_notify = if let Some(ref sid) = stream_id_for_update {
                let mut streams = state.signaling_state.streams.write().await;
                if let Some(peers) = streams.get_mut(sid) {
                    if let Some(p) = peers.iter_mut().find(|p| p.peer_id == peer_id) {
                        p.is_publisher = updated_publisher;
                        p.can_relay = can_relay;
                    }
                    peers
                        .iter()
                        .filter(|p| p.peer_id != peer_id)
                        .map(|p| p.peer_id.clone())
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            // Broadcast update to other peers
            if !peers_to_notify.is_empty() {
                let update_msg = SignalingMessage::PeerUpdate {
                    peer_id: peer_id.to_string(),
                    stream_id: stream_id_for_update,
                    is_publisher: updated_publisher,
                    can_relay,
                };
                let senders = state.signaling_state.peer_senders.read().await;
                for other_id in &peers_to_notify {
                    if let Some(sender) = senders.get(other_id) {
                        let _ = sender.send(update_msg.clone());
                    }
                }
            }
        }

        SignalingMessage::PeerList { .. } | SignalingMessage::Error { .. } => {
            // These are server-to-client messages, ignore if received from client
            debug!("Ignoring server-only message type from client {}", peer_id);
        }
    }
}

/// Clean up a peer's presence in the signaling system
async fn cleanup_signaling_peer(
    state: &AppState,
    peer_id: &str,
    current_stream_id: &Option<String>,
) {
    // Remove from peer senders
    {
        let mut senders = state.signaling_state.peer_senders.write().await;
        senders.remove(peer_id);
    }

    // Remove from stream
    if let Some(stream_id) = current_stream_id {
        let mut streams = state.signaling_state.streams.write().await;
        if let Some(peers) = streams.get_mut(stream_id) {
            peers.retain(|p| p.peer_id != peer_id);

            // Notify remaining peers
            let leave_msg = SignalingMessage::PeerLeave {
                peer_id: peer_id.to_string(),
            };

            let senders = state.signaling_state.peer_senders.read().await;
            for other_peer in peers.iter() {
                if let Some(sender) = senders.get(&other_peer.peer_id) {
                    let _ = sender.send(leave_msg.clone());
                }
            }

            // Clean up empty streams
            if peers.is_empty() {
                streams.remove(stream_id);
                debug!("Removed empty stream: {}", stream_id);
            }
        }
    }

    info!("Cleaned up signaling peer: {}", peer_id);
}

/// Kick a peer from all signaling rooms and close their sender channel.
///
/// Used when a peer is blocked via the moderation API to immediately
/// disconnect them from WebRTC signaling.
async fn kick_peer_from_signaling(state: &AppState, peer_id: &str) {
    // Find all streams this peer is in
    let streams_to_notify: Vec<(String, Vec<String>)> = {
        let mut streams = state.signaling_state.streams.write().await;
        let mut result = Vec::new();
        for (stream_id, peers) in streams.iter_mut() {
            if peers.iter().any(|p| p.peer_id == peer_id) {
                peers.retain(|p| p.peer_id != peer_id);
                let remaining: Vec<String> =
                    peers.iter().map(|p| p.peer_id.clone()).collect();
                result.push((stream_id.clone(), remaining));
            }
        }
        // Clean up empty streams
        streams.retain(|_, peers| !peers.is_empty());
        result
    };

    // Notify remaining peers with PeerLeave
    let leave_msg = SignalingMessage::PeerLeave {
        peer_id: peer_id.to_string(),
    };
    {
        let senders = state.signaling_state.peer_senders.read().await;
        for (_stream_id, remaining_peers) in &streams_to_notify {
            for other_id in remaining_peers {
                if let Some(sender) = senders.get(other_id) {
                    let _ = sender.send(leave_msg.clone());
                }
            }
        }
    }

    // Remove the peer's sender channel (closes their WebSocket)
    {
        let mut senders = state.signaling_state.peer_senders.write().await;
        senders.remove(peer_id);
    }

    if !streams_to_notify.is_empty() {
        info!(
            "Kicked blocked peer {} from {} signaling stream(s)",
            peer_id,
            streams_to_notify.len()
        );
    }
}

// ============================================================================
// Moderation API Handlers
// ============================================================================

/// Request body for blocking a peer
#[derive(Debug, Deserialize)]
struct BlockPeerRequest {
    peer_id: String,
    reason: Option<String>,
}

/// Request body for flagging a stream
#[derive(Debug, Deserialize)]
struct FlagStreamRequest {
    stream_id: String,
    reason: String,
    details: Option<String>,
    reporter: Option<String>,
}

fn mod_not_enabled() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "error",
        "message": "Moderation not enabled"
    }))
}

fn parse_flag_reason(s: &str) -> OpenBroadcastNetwork_core::moderation::FlagReason {
    use OpenBroadcastNetwork_core::moderation::FlagReason;
    match s.to_lowercase().as_str() {
        "spam" => FlagReason::Spam,
        "inappropriate" => FlagReason::Inappropriate,
        "copyright" => FlagReason::Copyright,
        "abuse" => FlagReason::Abuse,
        other => FlagReason::Other(other.to_string()),
    }
}

async fn mod_block_peer_handler(
    State(state): State<AppState>,
    Json(body): Json<BlockPeerRequest>,
) -> impl IntoResponse {
    let Some(ref moderation) = state.moderation else {
        return mod_not_enabled();
    };
    moderation.block_peer(&body.peer_id, body.reason).await;

    // Kick the peer from signaling immediately
    kick_peer_from_signaling(&state, &body.peer_id).await;

    Json(serde_json::json!({
        "status": "ok",
        "message": format!("Blocked peer: {}", body.peer_id)
    }))
}

async fn mod_unblock_peer_handler(
    State(state): State<AppState>,
    Path(peer_id): Path<String>,
) -> impl IntoResponse {
    let Some(ref moderation) = state.moderation else {
        return mod_not_enabled();
    };
    let removed = moderation.unblock_peer(&peer_id).await;
    Json(serde_json::json!({
        "status": "ok",
        "removed": removed
    }))
}

async fn mod_blocked_peers_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let Some(ref moderation) = state.moderation else {
        return mod_not_enabled();
    };
    let peers = moderation.blocked_peers().await;
    Json(serde_json::json!({
        "status": "ok",
        "blocked_peers": peers,
        "count": peers.len()
    }))
}

async fn mod_flag_stream_handler(
    State(state): State<AppState>,
    Json(body): Json<FlagStreamRequest>,
) -> impl IntoResponse {
    let Some(ref moderation) = state.moderation else {
        return mod_not_enabled();
    };
    let reason = parse_flag_reason(&body.reason);
    moderation
        .flag_stream(&body.stream_id, reason, body.details, body.reporter)
        .await;
    Json(serde_json::json!({
        "status": "ok",
        "message": format!("Flagged stream: {}", body.stream_id)
    }))
}

async fn mod_flagged_streams_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let Some(ref moderation) = state.moderation else {
        return mod_not_enabled();
    };
    let flags = moderation.flagged_streams().await;
    Json(serde_json::json!({
        "status": "ok",
        "flagged_streams": flags,
        "count": flags.len()
    }))
}

async fn mod_import_handler(
    State(state): State<AppState>,
    Json(list): Json<OpenBroadcastNetwork_core::moderation::ModerationList>,
) -> impl IntoResponse {
    let Some(ref moderation) = state.moderation else {
        return mod_not_enabled();
    };
    moderation.import_list(list).await;
    Json(serde_json::json!({
        "status": "ok",
        "message": "Imported moderation list"
    }))
}

async fn mod_export_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let Some(ref moderation) = state.moderation else {
        return mod_not_enabled();
    };
    let list = moderation.export_list().await;
    Json(serde_json::json!(list))
}

// ============================================================================
// Stats API Handler
// ============================================================================

/// API endpoint returning relay, signaling, and WebRTC statistics
async fn stats_handler(State(state): State<AppState>) -> impl IntoResponse {
    // Relay stats (from RelayNode if available)
    let relay = if let Some(ref rs) = state.relay_stats {
        let s = rs.read().await;
        serde_json::json!({
            "chunks_relayed": s.chunks_relayed,
            "bytes_relayed": s.bytes_relayed,
            "avg_chunk_size": s.avg_chunk_size,
            "active_streams": s.active_streams,
            "connected_peers": s.connected_peers,
            "incoming_bandwidth_bps": s.incoming_bandwidth,
            "outgoing_bandwidth_bps": s.outgoing_bandwidth,
            "per_stream_chunks": s.per_stream_chunks,
        })
    } else {
        serde_json::json!(null)
    };

    // Signaling stats (always available)
    let (sig_peers, sig_streams) = {
        let senders = state.signaling_state.peer_senders.read().await;
        let streams = state.signaling_state.streams.read().await;
        (senders.len(), streams.len())
    };

    // WebRTC peer connection count: sum of peers across all stream rooms
    let webrtc_peers: usize = {
        let streams = state.signaling_state.streams.read().await;
        streams.values().map(|peers| peers.len()).sum()
    };

    Json(serde_json::json!({
        "status": "ok",
        "relay": relay,
        "signaling": {
            "connected_peers": sig_peers,
            "active_streams": sig_streams,
        },
        "webrtc": {
            "peer_connections": webrtc_peers,
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
                <li><strong>GET /api/streams</strong> - List available P2P streams</li>
                <li><strong>POST /api/stream/start</strong> - Start streaming</li>
                <li><strong>POST /api/stream/stop</strong> - Stop streaming</li>
                <li><strong>WS /stream</strong> - WebSocket for chunk streaming</li>
            </ul>
        </div>
    </div>
</body>
</html>
"#;
