// Add visualization module
mod visualization;

use clap::{Parser, Subcommand};
use decentralized_stream_core::prelude::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::signal;
use tokio::time;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};
use visualization::{create_peer_table, create_stream_table, format_bytes, format_duration, PeerDisplayInfo, StreamInfo, visualize_tree};

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

/// Node operational metrics
#[derive(Debug, Clone, Default)]
struct NodeMetrics {
    /// When the node started
    start_time: Instant,
    /// Active connections
    connections: usize,
    /// Known peers
    known_peers: usize,
    /// Active streams
    active_streams: usize,
    /// Relayed bytes (total)
    relayed_bytes: u64,
    /// Current bandwidth usage (bytes/sec)
    bandwidth_bps: u64,
    /// Relay score
    relay_score: f32,
}

/// Runs a relay node with the specified configuration
async fn run_relay_node(
    bootstrap_nodes: Vec<String>,
    listen_addr: String,
    role: String,
    enable_mdns: bool,
    enable_dht: bool,
    geo_aware: bool,
) -> Result<(), anyhow::Error> {
    info!("Starting relay node on {}", listen_addr);
    
    // Parse the listen address
    let addr = listen_addr.parse::<SocketAddr>()?;
    
    // Setup node metrics
    let metrics = NodeMetrics {
        start_time: Instant::now(),
        ..Default::default()
    };
    
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
    
    // Setup a network configuration
    let mut network_config = NetworkConfig::default();
    network_config.listen_addresses = vec![format!("/ip4/{}/tcp/{}", addr.ip(), addr.port())];
    
    // Setup topology configuration
    let mut topology_config = TopologyConfig::default();
    topology_config.region_aware = geo_aware;
    
    // Setup hybrid overlay configuration
    let mut overlay_config = HybridOverlayConfig::default();
    overlay_config.network_config = network_config;
    overlay_config.topology_config = topology_config;
    overlay_config.default_role = peer_role;
    overlay_config.geo_aware = geo_aware;
    
    // Create and start the overlay
    let overlay = HybridOverlay::new(overlay_config).await?;
    overlay.start().await?;
    
    // Setup discovery mechanism
    if enable_mdns {
        let mut mdns = MdnsDiscovery::new();
        mdns.start()?;
        info!("mDNS discovery enabled");
    }
    
    if enable_dht {
        let config = DhtDiscoveryConfig {
            bootstrap_peers: bootstrap_nodes.iter()
                .map(|addr| addr.parse().expect("Invalid multiaddr"))
                .collect(),
            ..Default::default()
        };
        
        let mut dht = DhtDiscovery::with_config(config);
        dht.start()?;
        info!("DHT discovery enabled with {} bootstrap peers", bootstrap_nodes.len());
    }
    
    // If neither is enabled, use bootstrap discovery
    if !enable_mdns && !enable_dht && !bootstrap_nodes.is_empty() {
        let mut bootstrap_config = BootstrapDiscoveryConfig::default();
        bootstrap_config.bootstrap_nodes = bootstrap_nodes.iter()
            .filter_map(|addr| addr.parse::<SocketAddr>().ok())
            .collect();
            
        let mut bootstrap = BootstrapDiscovery::with_config(bootstrap_config);
        bootstrap.start()?;
        info!("Bootstrap discovery enabled with {} nodes", bootstrap_config.bootstrap_nodes.len());
    }
    
    info!("Relay node running with role: {:?}", peer_role);
    
    // Setup periodic status display
    let mut status_interval = time::interval(Duration::from_secs(30));
    
    // Process events and handle signals
    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("Received Ctrl+C, shutting down...");
                break;
            }
            _ = status_interval.tick() => {
                // Display node status
                if let Ok(stats) = overlay.stats().await {
                    info!("Status: {} peers, {} streams, {}MB relayed", 
                        stats.connected_peers,
                        stats.active_streams,
                        stats.outgoing_bandwidth / 1_000_000);
                }
            }
            Some(event) = overlay.next_event() => {
                match event {
                    OverlayEvent::StreamPublished { stream_id, publisher } => {
                        info!("New stream published: {:?} by {}", stream_id, publisher);
                    }
                    OverlayEvent::StreamRelayed { stream_id, source, target } => {
                        debug!("Relaying stream {:?} from {} to {}", stream_id, source, target);
                    }
                    OverlayEvent::StreamStopped { stream_id, reason } => {
                        info!("Stream stopped: {:?} - {}", stream_id, reason);
                    }
                    OverlayEvent::PeerConnected { peer_id, info } => {
                        info!("Peer connected: {} ({})", peer_id, info.role);
                    }
                    OverlayEvent::PeerDisconnected { peer_id, reason } => {
                        info!("Peer disconnected: {} - {}", peer_id, reason);
                    }
                    OverlayEvent::Error(e) => {
                        error!("Overlay error: {}", e);
                    }
                    _ => {}
                }
            }
        }
    }
    
    Ok(())
}

/// Generate a text visualization of the network topology
fn visualize_topology_text(streams: Vec<StreamId>, peers: HashMap<PeerId, PeerInfo>) -> String {
    let mut output = String::new();
    
    output.push_str(&format!("Network Topology - {} streams, {} peers\n\n", 
        streams.len(), peers.len()));
    
    // Display streams
    output.push_str("Active Streams:\n");
    for (i, stream_id) in streams.iter().enumerate() {
        output.push_str(&format!("  {}. {}\n", i+1, stream_id));
    }
    output.push('\n');
    
    // Display peers by role
    let mut publishers = Vec::new();
    let mut relays = Vec::new();
    let mut consumers = Vec::new();
    
    for (peer_id, info) in &peers {
        match info.role {
            PeerRole::Publisher => publishers.push((peer_id, info)),
            PeerRole::Relay | PeerRole::HybridRelay => relays.push((peer_id, info)),
            PeerRole::Consumer => consumers.push((peer_id, info)),
            _ => {}
        }
    }
    
    output.push_str(&format!("Publishers ({})\n", publishers.len()));
    for (peer_id, info) in publishers {
        output.push_str(&format!("  {} - {}\n", peer_id, info.addresses.join(", ")));
    }
    
    output.push_str(&format!("\nRelays ({})\n", relays.len()));
    for (peer_id, info) in relays {
        output.push_str(&format!("  {} - {}\n", peer_id, info.addresses.join(", ")));
    }
    
    output.push_str(&format!("\nConsumers ({})\n", consumers.len()));
    for (peer_id, info) in consumers {
        output.push_str(&format!("  {} - {}\n", peer_id, info.addresses.join(", ")));
    }
    
    output
}

/// Generate a DOT format visualization for use with Graphviz
fn visualize_topology_dot(streams: Vec<StreamId>, peers: HashMap<PeerId, PeerInfo>) -> String {
    let mut output = String::new();
    
    output.push_str("digraph network_topology {\n");
    output.push_str("  rankdir=LR;\n");
    output.push_str("  node [shape=box style=filled];\n\n");
    
    // Add nodes with different colors based on role
    for (peer_id, info) in &peers {
        let (color, shape) = match info.role {
            PeerRole::Publisher => ("lightblue", "ellipse"),
            PeerRole::Relay => ("lightgreen", "box"),
            PeerRole::HybridRelay => ("lightyellow", "box"),
            PeerRole::Consumer => ("pink", "circle"),
            _ => ("gray", "box"),
        };
        
        let label = format!("{}\n{}", peer_id, info.addresses.first().unwrap_or(&String::new()));
        output.push_str(&format!("  \"{}\" [label=\"{}\" fillcolor={} shape={}];\n", 
            peer_id, label, color, shape));
    }
    
    output.push_str("\n  // Connections would be added here in a real implementation\n");
    output.push_str("  // Example: publisher -> relay -> consumer\n");
    
    // In a real implementation, we would add actual edges based on the topology
    // This is just a placeholder
    if let (Some(publisher), Some(relay), Some(consumer)) = (
        peers.iter().find(|(_, info)| info.role == PeerRole::Publisher),
        peers.iter().find(|(_, info)| info.role == PeerRole::Relay),
        peers.iter().find(|(_, info)| info.role == PeerRole::Consumer),
    ) {
        output.push_str(&format!("  \"{}\" -> \"{}\" [label=\"stream\"];\n", 
            publisher.0, relay.0));
        output.push_str(&format!("  \"{}\" -> \"{}\" [label=\"stream\"];\n", 
            relay.0, consumer.0));
    }
    
    output.push_str("}\n");
    
    output
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Parse command line arguments
    let cli = Cli::parse();
    
    // Setup logging
    let log_level = if cli.debug { "debug" } else { "info" };
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("decentralized_stream_node={}", log_level)));
        
    fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();
    
    // Handle subcommands
    match cli.command {
        Commands::Run { 
            bootstrap, 
            listen, 
            role,
            mdns,
            dht,
            geo_aware,
        } => {
            run_relay_node(
                bootstrap.unwrap_or_default(),
                listen,
                role,
                mdns,
                dht,
                geo_aware,
            ).await?;
        }
        
        Commands::Status { node } => {
            println!("Connecting to node at {}...", node);
            println!("Status feature not fully implemented yet.");
            // In a real implementation, would connect to the node and display status
            println!("In a real implementation, this would show:");
            println!("- Node uptime");
            println!("- Connected peers");
            println!("- Active streams");
            println!("- Bandwidth usage");
            println!("- System resources");
        }
        
        Commands::ListStreams { node } => {
            println!("Connecting to node at {}...", node);
            println!("Stream listing feature not fully implemented yet.");
            // In a real implementation, would connect to the node and list streams
            println!("In a real implementation, this would list:");
            println!("- Stream IDs");
            println!("- Publishers");
            println!("- Subscriber counts");
            println!("- Bandwidth usage per stream");
        }
        
        Commands::Visualize { node, format, output } => {
            println!("Connecting to node at {}...", node);
            println!("Visualization feature not fully implemented yet.");
            
            // Placeholder for real data
            let stream_infos = vec![
                StreamInfo {
                    id: StreamId::from_string("test-stream-1"),
                    publisher: PeerId::from_string("publisher-1"),
                    subscribers: 2,
                    bandwidth_bps: 5_000_000,
                    age_seconds: 3600,
                },
                StreamInfo {
                    id: StreamId::from_string("test-stream-2"),
                    publisher: PeerId::from_string("publisher-1"),
                    subscribers: 1,
                    bandwidth_bps: 2_500_000,
                    age_seconds: 1800,
                },
            ];
            
            let mut peers = HashMap::new();
            peers.insert(PeerId::from_string("publisher-1"), PeerInfo {
                id: PeerId::from_string("publisher-1"),
                addresses: vec!["192.168.1.10:9000".to_string()],
                role: PeerRole::Publisher,
                protocols: vec!["stream/1.0".to_string()],
                metadata: HashMap::new(),
                latency_ms: Some(10),
                last_seen: 0,
                region: Some("us-west".to_string()),
                bandwidth_capacity: Some(5_000_000),
            });
            
            peers.insert(PeerId::from_string("relay-1"), PeerInfo {
                id: PeerId::from_string("relay-1"),
                addresses: vec!["192.168.1.20:9000".to_string()],
                role: PeerRole::Relay,
                protocols: vec!["stream/1.0".to_string()],
                metadata: HashMap::new(),
                latency_ms: Some(20),
                last_seen: 0,
                region: Some("us-west".to_string()),
                bandwidth_capacity: Some(10_000_000),
            });
            
            peers.insert(PeerId::from_string("consumer-1"), PeerInfo {
                id: PeerId::from_string("consumer-1"),
                addresses: vec!["192.168.1.30:9000".to_string()],
                role: PeerRole::Consumer,
                protocols: vec!["stream/1.0".to_string()],
                metadata: HashMap::new(),
                latency_ms: Some(30),
                last_seen: 0,
                region: Some("us-west".to_string()),
                bandwidth_capacity: Some(2_000_000),
            });
            
            // Create sample connection mapping for tree visualization
            let mut connections = HashMap::new();
            connections.insert(
                PeerId::from_string("publisher-1"), 
                vec![PeerId::from_string("relay-1")]
            );
            connections.insert(
                PeerId::from_string("relay-1"), 
                vec![PeerId::from_string("consumer-1")]
            );
            
            // Create list of display info for tabular output
            let peer_displays: Vec<PeerDisplayInfo> = peers.values()
                .map(|p| {
                    let mut display: PeerDisplayInfo = p.clone().into();
                    display.connections = connections.get(&p.id)
                        .map(|c| c.len())
                        .unwrap_or(0);
                    display
                })
                .collect();
            
            // Generate visualization based on requested format
            let viz = match format.as_str() {
                "text" => {
                    let mut output = String::new();
                    output.push_str("== STREAM INFORMATION ==\n\n");
                    output.push_str(&create_stream_table(&stream_infos));
                    output.push_str("\n\n== PEER INFORMATION ==\n\n");
                    output.push_str(&create_peer_table(&peer_displays));
                    output.push_str("\n\n== NETWORK TOPOLOGY ==\n\n");
                    
                    // Find publisher to use as root
                    let root_id = peers.values()
                        .find(|p| p.role == PeerRole::Publisher)
                        .map(|p| p.id.clone())
                        .unwrap_or_else(|| peers.keys().next().unwrap().clone());
                        
                    output.push_str(&visualize_tree(&peers, &root_id, &connections));
                    output
                },
                "dot" => visualization::dot_to_ascii(&visualize_topology_dot(
                    stream_infos.iter().map(|s| s.id.clone()).collect(),
                    peers.clone()
                )),
                "json" => {
                    // Create a more comprehensive structure for JSON output
                    let json_data = serde_json::json!({
                        "streams": stream_infos,
                        "peers": peer_displays,
                        "connections": connections,
                    });
                    serde_json::to_string_pretty(&json_data).unwrap()
                },
                _ => {
                    return Err(anyhow::anyhow!("Unsupported format: {}", format));
                }
            };
            
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