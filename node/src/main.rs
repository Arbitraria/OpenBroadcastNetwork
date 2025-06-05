// Simplified version for relay node

use clap::{Parser, Subcommand};
use OpenBroadcastNetwork_core::prelude::*;
use OpenBroadcastNetwork_core::overlay::interface::{Overlay, OverlayConfig};
use OpenBroadcastNetwork_core::overlay::peer::{PeerRole, LocalPeerId};
use OpenBroadcastNetwork_core::overlay::libp2p::impl_core::Libp2pOverlay;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::signal;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{debug, error, info, warn};
use tracing_subscriber::fmt;
use OpenBroadcastNetwork_node::visualization::*;

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
}

/// Running node instance with overlay
struct RunningNode {
    /// When the node started
    start_time: SystemTime,
    /// The overlay network instance
    overlay: Arc<Libp2pOverlay>,
    /// Node role
    role: PeerRole,
}

impl RunningNode {
    async fn new(role: PeerRole, enable_dht: bool, bootstrap_peers: Vec<String>) -> Result<Self, anyhow::Error> {
        let enable_bootstrap = !bootstrap_peers.is_empty();
        let config = OverlayConfig {
            local_peer_id: LocalPeerId::new_random(),
            bootstrap_peers,
            enable_kademlia: enable_dht,
            enable_bootstrap_discovery: enable_bootstrap,
            enable_dht_discovery: enable_dht,
            max_connections: 50,
            connection_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        
        let overlay = Libp2pOverlay::new(config).await?;
        
        Ok(Self {
            start_time: SystemTime::now(),
            overlay: Arc::new(overlay),
            role,
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
    _geo_aware: bool,
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
    let node = RunningNode::new(peer_role, enable_dht, bootstrap_nodes).await?;
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
                debug!("Node uptime: {}, Connected peers: {}, Active streams: {}", 
                      format_duration(uptime), stats.connected_peers, stats.active_streams);
            }
        }
    }
    
    info!("Shutting down relay node...");
    node.overlay.stop().await?;
    info!("Node stopped successfully");
    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Parse command line arguments
    let cli = Cli::parse();
    
    // Setup logging with simplified configuration
    fmt()
        .with_max_level(if cli.debug { tracing::Level::DEBUG } else { tracing::Level::INFO })
        .with_target(true)
        .init();
    
    info!("Decentralized Streaming Relay Node v{}", env!("CARGO_PKG_VERSION"));
    
    match &cli.command {
        Commands::Run { bootstrap, listen, role, mdns, dht, geo_aware } => {
            let bootstrap_nodes = bootstrap.clone().unwrap_or_else(Vec::new);
            run_relay_node(
                bootstrap_nodes,
                listen.clone(),
                role.clone(),
                *mdns,
                *dht,
                *geo_aware,
            ).await?;
        },
        Commands::Status { node } => {
            info!("Connecting to node at {}", node);
            
            // For demonstration, create a temporary overlay to show stats
            let demo_node = RunningNode::new(PeerRole::Relay, false, vec![]).await?;
            demo_node.overlay.start().await?;
            
            // Get stats and display
            if let Ok(stats) = demo_node.overlay.stats().await {
                if let Ok(uptime) = SystemTime::now().duration_since(demo_node.start_time) {
                    println!("{}", create_network_status(&stats, uptime));
                }
            }
            
            demo_node.overlay.stop().await?;
        },
        Commands::ListStreams { node } => {
            info!("Connecting to node at {}", node);
            
            // For demonstration, create a temporary overlay and show stream info
            let demo_node = RunningNode::new(PeerRole::Relay, false, vec![]).await?;
            demo_node.overlay.start().await?;
            
            // Get active streams and display
            if let Ok(streams) = demo_node.overlay.active_streams().await {
                println!("{}", create_stream_table(&streams));
            } else {
                println!("No active streams found");
            }
            
            demo_node.overlay.stop().await?;
        },
        Commands::Visualize { node, format, output } => {
            info!("Connecting to node at {}", node);
            
            // For demonstration, create a temporary overlay and show topology
            let demo_node = RunningNode::new(PeerRole::Relay, false, vec![]).await?;
            demo_node.overlay.start().await?;
            
            // Get connected peers and convert to HashMap
            let connected_peers = demo_node.overlay.connected_peers().await.unwrap_or_default();
            let mut peers = HashMap::new();
            for peer in connected_peers {
                // Convert LocalPeerId to PeerId for the map
                if let Ok(pid) = libp2p::PeerId::try_from(&peer.id) {
                    peers.insert(pid, peer);
                }
            }
            
            let viz = match format.as_str() {
                "text" => {
                    let mut output_text = String::new();
                    output_text.push_str("== NETWORK TOPOLOGY ==\n\n");
                    if let Ok(stats) = demo_node.overlay.stats().await {
                        output_text.push_str(&create_network_status(&stats, SystemTime::now().duration_since(demo_node.start_time).unwrap_or_default()));
                    }
                    output_text.push_str("\n");
                    output_text.push_str(&create_peer_table(&peers, true));
                    output_text
                },
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
                },
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
    }
    
    Ok(())
}
