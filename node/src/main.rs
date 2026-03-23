//! CLI relay node for OpenBroadcastNetwork.
#![allow(non_snake_case)]

use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::signal;
use tokio::sync::{Mutex, RwLock};
use tokio::time;
use tracing::{debug, error, info, warn};
use tracing_subscriber::fmt;
use OpenBroadcastNetwork_core::media::publisher::StreamPublisher;
use OpenBroadcastNetwork_core::media::segment::StreamId;
use OpenBroadcastNetwork_core::moderation::{ModerationConfig, ModerationManager};
use OpenBroadcastNetwork_core::overlay::interface::{Overlay, OverlayConfig};
use OpenBroadcastNetwork_core::overlay::libp2p::impl_core::Libp2pOverlay;
use OpenBroadcastNetwork_core::overlay::peer::{LocalPeerId, PeerRole};
use OpenBroadcastNetwork_core::prelude::*;
use OpenBroadcastNetwork_node::visualization::*;
use OpenBroadcastNetwork_node::{StreamingDemo, StreamingDemoConfig};

mod bootstrap_server;
mod web_server;
use web_server::{WebServer, WebServerConfig};

/// Decentralized Streaming Relay Node CLI
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Config file path
    #[clap(short, long, value_parser)]
    config: Option<PathBuf>,

    /// Enable debug logging
    #[clap(short, long, action)]
    debug: bool,

    /// Subcommands
    #[clap(subcommand)]
    command: Commands,
}

/// CLI Subcommands
#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a relay node
    Run {
        /// Bootstrap nodes to connect to
        #[clap(short, long, value_delimiter = ',')]
        bootstrap: Option<Vec<String>>,

        /// Listen addresses
        #[clap(short, long, default_value = "0.0.0.0:9000")]
        listen: String,

        /// Node role (relay, edge, source)
        #[clap(short, long, default_value = "relay")]
        role: String,

        /// Enable mDNS discovery
        #[clap(short, long, action)]
        mdns: bool,

        /// Enable DHT discovery
        #[clap(short, long, action)]
        dht: bool,

        /// Enable geo-aware rebalancing
        #[clap(short = 'g', long, action)]
        geo_aware: bool,
    },

    /// View network status and topology
    Status {
        /// Connect to a running node at this address
        #[clap(short, long, default_value = "127.0.0.1:9000")]
        node: String,
    },

    /// List active streams
    ListStreams {
        /// Connect to a running node at this address
        #[clap(short, long, default_value = "127.0.0.1:9000")]
        node: String,
    },

    /// Visualize the network topology
    Visualize {
        /// Connect to a running node at this address
        #[clap(short, long, default_value = "127.0.0.1:9000")]
        node: String,

        /// Output format (text, json, dot)
        #[clap(short, long, default_value = "text")]
        format: String,

        /// Output file (or stdout if not specified)
        #[clap(short, long)]
        output: Option<PathBuf>,
    },

    /// Run streaming demo with synthetic content or stream a real video file
    Stream {
        /// Bootstrap nodes to connect to
        #[clap(short, long, value_delimiter = ',')]
        bootstrap: Option<Vec<String>>,

        /// Listen addresses
        #[clap(short, long, default_value = "0.0.0.0:9001")]
        listen: String,

        /// Stream title
        #[clap(short, long, default_value = "Demo Stream")]
        title: String,

        /// Path to MP4 video file to stream (if provided, streams real video instead of synthetic)
        #[clap(long)]
        video: Option<String>,

        /// Video width (synthetic mode only)
        #[clap(long, default_value = "640")]
        width: u32,

        /// Video height (synthetic mode only)
        #[clap(long, default_value = "480")]
        height: u32,

        /// Video FPS (synthetic mode only)
        #[clap(long, default_value = "30")]
        fps: u32,

        /// Duration in seconds (synthetic mode only)
        #[clap(short, long, default_value = "60")]
        duration: u64,

        /// Enable DHT discovery
        #[clap(short = 'D', long, action)]
        dht: bool,
    },

    /// Start web viewer server for codec testing
    WebViewer {
        /// Web server host
        #[clap(long, default_value = "127.0.0.1")]
        host: String,

        /// Web server port
        #[clap(short, long, default_value = "8080")]
        port: u16,

        /// Path to web viewer files
        #[clap(long, default_value = "web_viewer")]
        web_root: String,

        /// Bootstrap nodes to connect to
        #[clap(short, long, value_delimiter = ',')]
        bootstrap: Option<Vec<String>>,

        /// Enable CORS
        #[clap(long, action)]
        cors: bool,

        /// Path to MP4 video file to stream
        #[clap(long)]
        video: Option<String>,

        /// Connect to P2P stream with this Stream ID (subscriber mode)
        #[clap(long)]
        stream_id: Option<String>,

        /// Publish local video to P2P network (publisher mode)
        #[clap(long, action)]
        publish: bool,

        /// Enable DHT discovery for P2P mode
        #[clap(short = 'D', long, action)]
        dht: bool,
    },

    /// Publish a video file to the P2P network
    Publish {
        /// Path to the video file to stream
        video: PathBuf,

        /// Stream title
        #[clap(short, long)]
        title: Option<String>,

        /// Web server port
        #[clap(short, long, default_value = "8080")]
        port: u16,

        /// Bootstrap nodes to connect to
        #[clap(short, long, value_delimiter = ',')]
        bootstrap: Option<Vec<String>>,

        /// Enable DHT discovery
        #[clap(short = 'D', long, action)]
        dht: bool,
    },

    /// Run as a bootstrap server for peer discovery
    BootstrapServer {
        /// Port to listen on
        #[clap(short, long, default_value = "9000")]
        port: u16,

        /// Listen address
        #[clap(short, long, default_value = "0.0.0.0")]
        listen: String,

        /// Peer expiration time in seconds
        #[clap(long, default_value = "3600")]
        peer_expiration: u64,

        /// Maximum number of peers to track
        #[clap(long, default_value = "1000")]
        max_peers: usize,

        /// Enable circuit relay v2 server for NAT traversal
        #[clap(long, action, default_value = "true")]
        relay_server: bool,

        /// Maximum relay reservations
        #[clap(long, default_value = "128")]
        max_reservations: usize,

        /// Maximum active relay circuits
        #[clap(long, default_value = "512")]
        max_circuits: usize,
    },
}

/// Running node instance with overlay
#[allow(dead_code)]
struct RunningNode {
    /// When the node started
    start_time: SystemTime,
    /// The overlay network instance
    overlay: Arc<Libp2pOverlay>,
    /// Node role
    role: PeerRole,
    /// Moderation manager
    moderation: Arc<ModerationManager>,
}

impl RunningNode {
    async fn new(
        role: PeerRole,
        enable_dht: bool,
        enable_geo_aware: bool,
        bootstrap_peers: Vec<String>,
    ) -> Result<Self, anyhow::Error> {
        let enable_bootstrap = !bootstrap_peers.is_empty();
        let config = OverlayConfig {
            local_peer_id: LocalPeerId::new_random(),
            bootstrap_peers,
            enable_kademlia: enable_dht,
            enable_bootstrap_discovery: enable_bootstrap,
            enable_dht_discovery: enable_dht,
            max_connections: 50,
            connection_timeout: Duration::from_secs(30),
            enable_geo_aware,
            ..Default::default()
        };

        let mut overlay = Libp2pOverlay::new(config).await?;

        // Create moderation manager with auto-persist
        let mod_config = ModerationConfig {
            persistence_path: Some(PathBuf::from("moderation.json")),
            auto_persist: true,
            ..Default::default()
        };
        let moderation = Arc::new(ModerationManager::new(mod_config));

        // Load existing moderation state if present
        let persist_path = PathBuf::from("moderation.json");
        if persist_path.exists() {
            if let Err(e) = moderation.load_from_file(&persist_path).await {
                warn!("Failed to load moderation state: {}", e);
            }
        }

        // Wire moderation into the overlay for connection enforcement
        overlay.set_moderation(moderation.clone());

        Ok(Self {
            start_time: SystemTime::now(),
            overlay: Arc::new(overlay),
            role,
            moderation,
        })
    }
}

/// Runs a relay node with the specified configuration
async fn run_relay_node(
    bootstrap_nodes: Vec<String>,
    listen_addr: String,
    role: String,
    _enable_mdns: bool,
    enable_dht: bool,
    geo_aware: bool,
) -> Result<(), anyhow::Error> {
    info!("Starting relay node on {}", listen_addr);

    // Map role string to PeerRole
    let peer_role = match role.to_lowercase().as_str() {
        "relay" => PeerRole::Relay,
        "edge" | "consumer" => PeerRole::Consumer,
        "source" | "publisher" => PeerRole::Publisher,
        "hybrid" => PeerRole::HybridRelay,
        "gateway" => PeerRole::Gateway,
        _ => {
            warn!("Unknown role '{}', defaulting to relay", role);
            PeerRole::Relay
        }
    };

    info!("Node role: {:?}", peer_role);

    // Create the running node with overlay
    let node = RunningNode::new(peer_role, enable_dht, geo_aware, bootstrap_nodes).await?;
    let peer_id = node.overlay.local_peer_id();
    info!("Peer ID: {}", peer_id);

    // Start the overlay
    node.overlay.start().await?;
    info!("Overlay network started");

    // Register shutdown signal handler
    let running = Arc::new(Mutex::new(true));
    let r = running.clone();

    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("Shutdown signal received");
                let mut lock = r.lock().await;
                *lock = false;
            }
            Err(err) => {
                error!("Unable to listen for shutdown signal: {}", err);
            }
        }
    });

    // Main event loop
    let interval = Duration::from_secs(5); // Update stats every 5 seconds
    let mut interval_timer = time::interval(interval);

    info!("Relay node started successfully");

    while *running.lock().await {
        interval_timer.tick().await;

        // Get and log stats
        if let Ok(stats) = node.overlay.stats().await {
            if let Ok(uptime) = SystemTime::now().duration_since(node.start_time) {
                debug!(
                    "Node uptime: {}, Connected peers: {}, Active streams: {}",
                    format_duration(uptime),
                    stats.connected_peers,
                    stats.active_streams
                );
            }
        }
    }

    info!("Shutting down relay node...");
    node.overlay.stop().await?;
    info!("Node stopped successfully");
    Ok(())
}

/// Run streaming demo with synthetic content
#[allow(clippy::too_many_arguments)]
async fn run_streaming_demo(
    bootstrap_nodes: Vec<String>,
    listen_addr: String,
    title: String,
    width: u32,
    height: u32,
    fps: u32,
    duration: u64,
    enable_dht: bool,
) -> Result<(), anyhow::Error> {
    info!("Starting streaming demo on {}", listen_addr);
    info!(
        "Stream: '{}' ({}x{} @ {}fps for {}s)",
        title, width, height, fps, duration
    );

    // Create the overlay network first
    let node = RunningNode::new(PeerRole::Publisher, enable_dht, false, bootstrap_nodes).await?;
    let peer_id = node.overlay.local_peer_id();
    info!("Publisher Peer ID: {}", peer_id);

    // Start the overlay
    node.overlay.start().await?;
    info!("Overlay network started");

    // Create demo configuration
    let demo_config = StreamingDemoConfig {
        title,
        video_width: width,
        video_height: height,
        video_fps: fps,
        audio_sample_rate: 48000,
        audio_channels: 2,
        duration_seconds: duration,
    };

    // Create and start streaming demo
    let mut demo = StreamingDemo::new(demo_config, node.overlay.clone()).await?;
    info!("Streaming demo created successfully");

    info!("Starting media generation and streaming...");
    demo.start().await?;

    info!("Streaming demo completed successfully");

    node.overlay.stop().await?;
    info!("Node stopped successfully");
    Ok(())
}

async fn run_video_stream(
    bootstrap_nodes: Vec<String>,
    listen_addr: String,
    title: String,
    video_path: String,
    enable_dht: bool,
) -> Result<(), anyhow::Error> {
    info!(
        "Starting video stream from {} on {}",
        video_path, listen_addr
    );

    let video_file = PathBuf::from(&video_path);
    if !video_file.exists() {
        return Err(anyhow::anyhow!("Video file not found: {}", video_path));
    }

    let node = RunningNode::new(PeerRole::Publisher, enable_dht, false, bootstrap_nodes).await?;
    let peer_id = node.overlay.local_peer_id();
    info!("Publisher Peer ID: {}", peer_id);

    node.overlay.start().await?;
    info!("Overlay network started");

    let mut publisher = StreamPublisher::new(node.overlay.clone(), title);
    let stream_id = publisher.stream_id().clone();
    info!("Stream ID: {}", stream_id);

    let running = Arc::new(Mutex::new(true));
    let r = running.clone();

    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("Shutdown signal received");
                let mut lock = r.lock().await;
                *lock = false;
            }
            Err(err) => {
                error!("Unable to listen for shutdown signal: {}", err);
            }
        }
    });

    info!("Starting video stream...");
    match publisher.publish_from_file(&video_file).await {
        Ok(()) => {
            info!("Video stream completed successfully");
        }
        Err(e) => {
            error!("Video stream failed: {}", e);
        }
    }

    node.overlay.stop().await?;
    info!("Node stopped successfully");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_web_viewer(
    host: String,
    port: u16,
    web_root: String,
    bootstrap_nodes: Vec<String>,
    enable_cors: bool,
    video_file: Option<String>,
    stream_id: Option<String>,
    publish: bool,
    enable_dht: bool,
) -> Result<(), anyhow::Error> {
    info!("Starting web viewer server on {}:{}", host, port);

    // Determine P2P mode: subscribing, publishing, or browse (bootstrap only)
    let enable_p2p = stream_id.is_some()
        || (publish && video_file.is_some())
        || !bootstrap_nodes.is_empty();

    // Create web server configuration
    let config = WebServerConfig {
        host: host.clone(),
        port,
        web_root: PathBuf::from(web_root),
        enable_cors,
        video_file: video_file.as_ref().map(PathBuf::from),
        enable_p2p,
        p2p_fallback: true,
    };

    // Create server with P2P support
    let (server, stream_manager, p2p_node) = if let Some(sid) = stream_id.clone() {
        // Subscriber mode: connect to an existing P2P stream
        info!("📡 P2P subscriber mode enabled for stream: {}", sid);

        // Create P2P overlay for receiving streams
        let node = RunningNode::new(
            PeerRole::Consumer,
            enable_dht,
            false,
            bootstrap_nodes.clone(),
        )
        .await?;
        node.overlay.start().await?;

        // Connect to bootstrap nodes
        for bootstrap in &bootstrap_nodes {
            info!("Connecting to bootstrap node: {}", bootstrap);
            if let Err(e) = Overlay::connect_peer(node.overlay.as_ref(), bootstrap.as_str()).await {
                warn!("Failed to connect to bootstrap {}: {}", bootstrap, e);
            }
        }

        let stream_id_obj = StreamId::new(sid.clone());
        let stream_manager = Arc::new(web_server::StreamManager::with_p2p(
            node.overlay.clone(),
            stream_id_obj,
        ));

        let config_clone = config.clone();
        let app_state = web_server::AppState {
            config: config_clone,
            stream_manager: stream_manager.clone(),
            clients: Arc::new(RwLock::new(HashMap::new())),
            signaling_state: Arc::new(web_server::SignalingState::default()),
            moderation: Some(node.moderation.clone()),
            relay_stats: None,
        };

        let server = web_server::WebServer::new_with_state(config, app_state);
        info!("📡 Subscribing to stream: {}", sid);
        info!("Other nodes can publish with: --stream-id {}", sid);
        (server, stream_manager, Some(node))
    } else if publish && video_file.is_some() {
        // Publisher mode: create a new stream and publish local video to P2P
        info!("📡 P2P publisher mode enabled");

        // Create P2P overlay for publishing streams
        let node = RunningNode::new(
            PeerRole::Publisher,
            enable_dht,
            false,
            bootstrap_nodes.clone(),
        )
        .await?;
        node.overlay.start().await?;

        // Connect to bootstrap nodes so other peers can discover this stream
        for bootstrap in &bootstrap_nodes {
            info!("Connecting to bootstrap node: {}", bootstrap);
            if let Err(e) = Overlay::connect_peer(node.overlay.as_ref(), bootstrap.as_str()).await {
                warn!("Failed to connect to bootstrap {}: {}", bootstrap, e);
            }
        }

        // Generate a new stream ID for publishing
        let stream_id_obj = StreamId::generate();
        info!("📡 Publishing stream with ID: {}", stream_id_obj);
        info!("Other nodes can subscribe with: --stream-id {}", stream_id_obj);

        let stream_manager = Arc::new(web_server::StreamManager::with_p2p(
            node.overlay.clone(),
            stream_id_obj,
        ));

        let config_clone = config.clone();
        let app_state = web_server::AppState {
            config: config_clone,
            stream_manager: stream_manager.clone(),
            clients: Arc::new(RwLock::new(HashMap::new())),
            signaling_state: Arc::new(web_server::SignalingState::default()),
            moderation: Some(node.moderation.clone()),
            relay_stats: None,
        };

        let server = web_server::WebServer::new_with_state(config, app_state);
        (server, stream_manager, Some(node))
    } else if !bootstrap_nodes.is_empty() {
        // Browse mode: connect to P2P network for stream discovery,
        // but don't subscribe to a specific stream yet.
        info!("📡 P2P browse mode enabled — discover and pick a stream");

        let node = RunningNode::new(
            PeerRole::Consumer,
            enable_dht,
            false,
            bootstrap_nodes.clone(),
        )
        .await?;
        node.overlay.start().await?;

        for bootstrap in &bootstrap_nodes {
            info!("Connecting to bootstrap node: {}", bootstrap);
            if let Err(e) =
                Overlay::connect_peer(node.overlay.as_ref(), bootstrap.as_str()).await
            {
                warn!("Failed to connect to bootstrap {}: {}", bootstrap, e);
            }
        }

        let stream_manager = Arc::new(
            web_server::StreamManager::with_discovery(node.overlay.clone()),
        );

        let config_clone = config.clone();
        let app_state = web_server::AppState {
            config: config_clone,
            stream_manager: stream_manager.clone(),
            clients: Arc::new(RwLock::new(HashMap::new())),
            signaling_state: Arc::new(web_server::SignalingState::default()),
            moderation: Some(node.moderation.clone()),
            relay_stats: None,
        };

        let server = web_server::WebServer::new_with_state(config, app_state);
        (server, stream_manager, Some(node))
    } else {
        // Regular mode without P2P
        let server = WebServer::new(config);
        let stream_manager = server.get_stream_manager();
        (server, stream_manager, None)
    };

    info!("Web viewer available at http://{}:{}/", host, port);
    info!(
        "WebSocket streaming endpoint: ws://{}:{}/stream",
        host, port
    );

    // Load video file if provided (with fallback to synthetic streaming)
    let video_loaded = if let Some(video_path) = &video_file {
        info!("Loading video file: {}", video_path);
        match stream_manager
            .load_video_file(&PathBuf::from(video_path))
            .await
        {
            Ok(_) => {
                info!("Successfully loaded video file for streaming");
                true
            }
            Err(e) => {
                warn!(
                    "Failed to load video file: {}. Falling back to synthetic streaming.",
                    e
                );
                false
            }
        }
    } else {
        false
    };

    // Start a demo task that generates test video chunks or streams real video
    let stream_manager_clone = stream_manager.clone();
    tokio::spawn(async move {
        // Wait a bit for the server to start
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Start the streaming session
        if let Err(e) = stream_manager_clone.start_streaming().await {
            error!("Failed to start streaming: {}", e);
            return;
        }

        if video_loaded {
            // Stream real video from loaded samples
            info!("Starting real video streaming");

            let samples = {
                let samples_guard = stream_manager_clone.video_samples.lock().await;
                samples_guard.clone()
            };

            if let Some(samples) = samples {
                info!(
                    "Streaming {} samples from real video (will loop continuously)",
                    samples.len()
                );

                let start_time = std::time::Instant::now();
                let mut loop_count = 0;

                // Loop the video content continuously
                loop {
                    // If no clients are connected, wait before starting the loop
                    while stream_manager_clone.segment_sender.receiver_count() == 0 {
                        debug!("No clients connected, waiting...");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }

                    info!(
                        "Client(s) connected, starting video loop #{}",
                        loop_count + 1
                    );

                    for (index, segment) in samples.iter().enumerate() {
                        // Check if clients are still connected
                        if stream_manager_clone.segment_sender.receiver_count() == 0 {
                            warn!("All clients disconnected during streaming");
                            break; // Break inner loop to wait for new clients
                        }

                        // Skip initialization segments — sent once per client connection
                        if segment.is_init() {
                            continue;
                        }

                        // Calculate adjusted timestamp for looping
                        let loop_duration = Duration::from_secs_f64(samples.len() as f64 / 30.0);
                        let segment_ts = Duration::from_micros(segment.pts_us);
                        let adjusted_timestamp = segment_ts + (loop_duration * loop_count);

                        // Use precise timing with a deadline
                        let target_time = start_time + adjusted_timestamp;
                        tokio::time::sleep_until(tokio::time::Instant::from_std(target_time)).await;

                        if segment.is_video() {
                            if let Err(e) = stream_manager_clone
                                .send_video_segment(segment.data.to_vec(), segment.is_keyframe)
                                .await
                            {
                                warn!("Failed to send video segment: {}", e);
                                break;
                            }

                            if index % 10 == 0 {
                                debug!(
                                    "Sent video segment {} at {:?} ({} bytes, keyframe: {})",
                                    index,
                                    adjusted_timestamp,
                                    segment.data.len(),
                                    segment.is_keyframe
                                );
                            }
                        } else if segment.is_audio() {
                            if let Err(e) = stream_manager_clone
                                .send_audio_segment(segment.data.to_vec())
                                .await
                            {
                                warn!("Failed to send audio segment: {}", e);
                                break;
                            }
                        }
                    }

                    loop_count += 1;
                    info!("Completed video loop #{}", loop_count);

                    if loop_count >= 10 {
                        // Limit to 10 loops to prevent infinite streaming
                        break;
                    }
                }

                info!(
                    "Finished streaming real video (completed {} loops)",
                    loop_count
                );
            } else {
                warn!("No video samples loaded, falling back to demo mode");
            }
        } else {
            info!("No video file provided. Please provide a video file with --video option.");
            info!("Example: cargo run -p OpenBroadcastNetwork-node web-viewer --video sample_video.mp4");

            // For now, just wait without generating demo content
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                if stream_manager_clone.segment_sender.receiver_count() > 0 {
                    info!("Clients connected but no video file provided. Use --video option to stream a file.");
                }
            }
        }
    });

    // Set up graceful shutdown signal handler
    let shutdown_signal = async {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("Received Ctrl+C, shutting down gracefully...");
            }
            Err(err) => {
                error!("Unable to listen for shutdown signal: {}", err);
            }
        }
    };

    // Start the web server with graceful shutdown
    tokio::select! {
        result = server.start() => {
            if let Err(e) = result {
                error!("Web server error: {}", e);
            }
        }
        _ = shutdown_signal => {
            info!("Shutdown signal received, stopping server...");
        }
    }

    // Clean up P2P node if it was created
    if let Some(node) = p2p_node {
        info!("Stopping P2P overlay...");
        node.overlay.stop().await?;
    }

    info!("Web viewer server stopped successfully");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Parse command line arguments
    let cli = Cli::parse();

    // Setup logging with simplified configuration
    fmt()
        .with_max_level(if cli.debug {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .with_target(true)
        .init();

    info!(
        "Decentralized Streaming Relay Node v{}",
        env!("CARGO_PKG_VERSION")
    );

    match &cli.command {
        Commands::Run {
            bootstrap,
            listen,
            role,
            mdns,
            dht,
            geo_aware,
        } => {
            let bootstrap_nodes = bootstrap.clone().unwrap_or_else(Vec::new);
            run_relay_node(
                bootstrap_nodes,
                listen.clone(),
                role.clone(),
                *mdns,
                *dht,
                *geo_aware,
            )
            .await?;
        }
        Commands::Status { node } => {
            info!("Connecting to node at {}", node);

            // For demonstration, create a temporary overlay to show stats
            let demo_node = RunningNode::new(PeerRole::Relay, false, false, vec![]).await?;
            demo_node.overlay.start().await?;

            // Get stats and display
            if let Ok(stats) = demo_node.overlay.stats().await {
                if let Ok(uptime) = SystemTime::now().duration_since(demo_node.start_time) {
                    println!("{}", create_network_status(&stats, uptime));
                }
            }

            demo_node.overlay.stop().await?;
        }
        Commands::ListStreams { node } => {
            let url = format!("http://{}/api/streams", node);
            info!("Querying streams from {}", url);

            match reqwest::get(&url).await {
                Ok(resp) => {
                    let data: serde_json::Value = resp.json().await?;
                    let streams = data["streams"].as_array();
                    match streams {
                        Some(streams) if !streams.is_empty() => {
                            println!(
                                "{:<40} {:<20} {:<15} {:<10}",
                                "Stream ID", "Title", "Codecs", "Resolution"
                            );
                            println!("{}", "-".repeat(85));
                            for s in streams {
                                let id = s["stream_id"]
                                    .as_str()
                                    .unwrap_or("unknown");
                                let title = s["title"]
                                    .as_str()
                                    .unwrap_or("untitled");
                                let vc = s["video_codec"]
                                    .as_str()
                                    .unwrap_or("");
                                let ac = s["audio_codec"]
                                    .as_str()
                                    .unwrap_or("");
                                let codecs = format!("{}/{}", vc, ac);
                                let w = s["video_width"].as_u64().unwrap_or(0);
                                let h = s["video_height"].as_u64().unwrap_or(0);
                                let res = if w > 0 {
                                    format!("{}x{}", w, h)
                                } else {
                                    String::new()
                                };
                                println!(
                                    "{:<40} {:<20} {:<15} {:<10}",
                                    id, title, codecs, res
                                );
                            }
                        }
                        _ => println!("No active streams found on {}", node),
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Failed to connect to node at {}: {}",
                        node, e
                    );
                    eprintln!(
                        "Make sure a web-viewer is running on that address."
                    );
                }
            }
        }
        Commands::Visualize {
            node,
            format,
            output,
        } => {
            info!("Connecting to node at {}", node);

            // For demonstration, create a temporary overlay and show topology
            let demo_node = RunningNode::new(PeerRole::Relay, false, false, vec![]).await?;
            demo_node.overlay.start().await?;

            // Get connected peers and convert to HashMap
            let connected_peers = demo_node
                .overlay
                .connected_peers()
                .await
                .unwrap_or_default();
            let mut peers = HashMap::new();
            for peer in connected_peers {
                // Convert LocalPeerId to PeerId for the map
                let pid: libp2p::PeerId = (&peer.id).into();
                peers.insert(pid, peer);
            }

            let viz = match format.as_str() {
                "text" => {
                    let mut output_text = String::new();
                    output_text.push_str("== NETWORK TOPOLOGY ==\n\n");
                    if let Ok(stats) = demo_node.overlay.stats().await {
                        output_text.push_str(&create_network_status(
                            &stats,
                            SystemTime::now()
                                .duration_since(demo_node.start_time)
                                .unwrap_or_default(),
                        ));
                    }
                    output_text.push('\n');
                    output_text.push_str(&create_peer_table(&peers, true));
                    output_text
                }
                "dot" => generate_dot_graph(&peers, None),
                "json" => {
                    let json_data = serde_json::json!({
                        "topology": {
                            "peers": peers.len(),
                            "streams": 0,
                            "timestamp": SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
                        }
                    });
                    serde_json::to_string_pretty(&json_data).unwrap()
                }
                _ => {
                    return Err(anyhow::anyhow!("Unsupported format: {}", format));
                }
            };

            demo_node.overlay.stop().await?;

            // Output visualization
            if let Some(path) = output {
                std::fs::write(path, viz)?;
                println!("Visualization written to {}", path.display());
            } else {
                println!("{}", viz);
            }
        }
        Commands::Stream {
            bootstrap,
            listen,
            title,
            video,
            width,
            height,
            fps,
            duration,
            dht,
        } => {
            let bootstrap_nodes = bootstrap.clone().unwrap_or_else(Vec::new);
            if let Some(video_path) = video {
                run_video_stream(
                    bootstrap_nodes,
                    listen.clone(),
                    title.clone(),
                    video_path.clone(),
                    *dht,
                )
                .await?;
            } else {
                run_streaming_demo(
                    bootstrap_nodes,
                    listen.clone(),
                    title.clone(),
                    *width,
                    *height,
                    *fps,
                    *duration,
                    *dht,
                )
                .await?;
            }
        }
        Commands::WebViewer {
            host,
            port,
            web_root,
            bootstrap,
            cors,
            video,
            stream_id,
            publish,
            dht,
        } => {
            let bootstrap_nodes = bootstrap.clone().unwrap_or_else(Vec::new);
            run_web_viewer(
                host.clone(),
                *port,
                web_root.clone(),
                bootstrap_nodes,
                *cors,
                video.clone(),
                stream_id.clone(),
                *publish,
                *dht,
            )
            .await?;
        }
        Commands::Publish {
            video,
            title,
            port,
            bootstrap,
            dht,
        } => {
            let bootstrap_nodes = bootstrap.clone().unwrap_or_default();
            let video_str = video.to_string_lossy().to_string();
            let stream_title = title
                .clone()
                .unwrap_or_else(|| {
                    video
                        .file_stem()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_else(|| "Live Stream".to_string())
                });
            info!("Publishing: \"{}\" from {}", stream_title, video_str);
            run_web_viewer(
                "127.0.0.1".to_string(),
                *port,
                "web_viewer".to_string(),
                bootstrap_nodes,
                false,
                Some(video_str),
                None,
                true,
                *dht,
            )
            .await?;
        }
        Commands::BootstrapServer {
            port,
            listen,
            peer_expiration,
            max_peers,
            relay_server,
            max_reservations,
            max_circuits,
        } => {
            let config = bootstrap_server::BootstrapServerConfig {
                listen_addr: listen.clone(),
                port: *port,
                max_peers: *max_peers,
                peer_expiration: Duration::from_secs(*peer_expiration),
                enable_relay: *relay_server,
                max_reservations: *max_reservations,
                max_circuits: *max_circuits,
            };

            let mut server = bootstrap_server::BootstrapServer::new(config);
            server.run().await.map_err(|e| anyhow::anyhow!("{}", e))?;
        }
    }

    Ok(())
}
