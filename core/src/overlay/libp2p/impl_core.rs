//! Core struct and implementation for the libp2p overlay
//!
//! This module provides the core `Libp2pOverlay` struct and its implementation methods.
//! It serves as the primary integration point between our application and the libp2p networking stack.
//!
//! # Architecture Overview
//!
//! The implementation follows a layered architecture:
//!
//! 1. **Network Layer (`Libp2pOverlay`)** - Implements the `Overlay` trait and manages
//!    the libp2p Swarm. This is the entry point for network operations.
//!
//! 2. **Behavior Layer (`OverlayBehavior`)** - Combines multiple libp2p protocols
//!    (Gossipsub, Kademlia, Identify, Relay, AutoNAT, DCUtR) into a unified behavior.
//!
//! 3. **Management Layer** - Specialized components that handle specific aspects:
//!    - `TopologyManager` - Manages peer connections and network topology
//!    - `RelayManager` - Handles stream relaying between peers
//!    - `MeshNetwork` - Manages mesh network connections
//!
//! # NAT Traversal
//!
//! This implementation includes NAT traversal support via:
//! - Circuit Relay v2 (relay client behavior)
//! - AutoNAT for NAT status detection
//! - DCUtR for direct connection upgrade through relay
//!
//! # Concurrency Pattern
//!
//! This implementation uses a specific concurrency pattern for thread safety:
//! - `Arc<T>` for shared ownership of managers across threads
//! - `Arc<Mutex<T>>` for exclusive access to mutable shared state
//! - `RwLock<T>` for data structures that are read frequently but written to occasionally

// Core library imports
use futures::channel::mpsc;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};

// External dependencies
use libp2p::core::upgrade;
use libp2p::gossipsub::{self, Behaviour as Gossipsub, MessageAuthenticity, ValidationMode};
use libp2p::identify::{Behaviour as Identify, Config as IdentifyConfig};
use libp2p::identity;
use libp2p::kad::{store::MemoryStore, Behaviour as Kademlia, Config as KademliaConfig};
use libp2p::noise;
use libp2p::relay;
use libp2p::tcp::Config as GenTcpConfig;
use libp2p::Multiaddr;
use libp2p::PeerId as Libp2pPeerId;
use libp2p::{autonat, dcutr};
use libp2p::{Swarm, SwarmBuilder, Transport};
use tracing::{debug, info, warn};

// Local crate imports
use crate::discovery::{
    BootstrapDiscoveryConfig, DhtDiscoveryConfig, DiscoveryManager, DiscoveryManagerConfig,
};
use crate::moderation::ModerationManager;
use crate::overlay::interface::{OverlayConfig, OverlayError, OverlayEvent, StreamId};
use crate::overlay::libp2p::behavior::OverlayBehavior;
use crate::overlay::mesh::{MeshConfig, MeshNetwork};
use crate::overlay::peer::{LocalPeerId, Peer};
use crate::overlay::relay::{RelayConfig, RelayManager};
use crate::overlay::topology::{TopologyConfig, TopologyManager};

use crate::overlay::libp2p::topics;

/// libp2p implementation of the overlay network
pub struct Libp2pOverlay {
    /// Configuration
    pub config: OverlayConfig,
    /// The libp2p peer ID for this node
    ///
    /// Since LocalPeerId now wraps libp2p::PeerId directly, we only need
    /// to store one copy. Use `local_peer_id()` to get the LocalPeerId wrapper.
    pub peer_id: Libp2pPeerId,
    /// The keypair for this node (needed for relay transport)
    keypair: identity::Keypair,
    /// Swarm
    pub swarm: Arc<Mutex<Option<Swarm<OverlayBehavior>>>>,
    /// Topology manager
    pub topology: Arc<TopologyManager>,
    /// Relay manager
    pub relay: Arc<RelayManager>,
    /// Mesh network
    pub mesh: Arc<MeshNetwork>,
    /// Discovery manager
    pub discovery: Arc<Mutex<DiscoveryManager>>,
    /// Peers
    pub peers: Arc<RwLock<HashMap<LocalPeerId, Peer>>>,
    /// Active streams
    pub streams: Arc<RwLock<HashSet<StreamId>>>,
    /// Event channel sender
    pub event_tx: mpsc::Sender<OverlayEvent>,
    /// Event channel receiver
    pub event_rx: Arc<Mutex<mpsc::Receiver<OverlayEvent>>>,
    /// Is the overlay running
    pub running: Arc<RwLock<bool>>,
    /// Worker task handle
    pub worker_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Optional moderation manager for peer blocking/whitelisting
    pub moderation: Option<Arc<ModerationManager>>,
}

impl Clone for Libp2pOverlay {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            peer_id: self.peer_id,
            keypair: self.keypair.clone(),
            swarm: self.swarm.clone(),
            topology: self.topology.clone(),
            relay: self.relay.clone(),
            mesh: self.mesh.clone(),
            discovery: self.discovery.clone(),
            peers: self.peers.clone(),
            streams: self.streams.clone(),
            event_tx: self.event_tx.clone(),
            event_rx: self.event_rx.clone(),
            running: self.running.clone(),
            worker_task: self.worker_task.clone(),
            moderation: self.moderation.clone(),
        }
    }
}

impl Libp2pOverlay {
    /// Create a new libp2p-based overlay
    pub async fn new(config: OverlayConfig) -> Result<Self, OverlayError> {
        // Create the libp2p identity
        let keypair = identity::Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();

        // Create channel for events
        let (event_tx, event_rx) = mpsc::channel(100);

        // Topology configuration - propagate geo-aware setting from OverlayConfig
        let topology_config = TopologyConfig {
            enable_geo_aware: config.enable_geo_aware,
            ..TopologyConfig::default()
        };

        let topology = Arc::new(TopologyManager::new(
            peer_id,
            topology_config,
            None, // No metrics for now
        ));

        // Relay configuration
        let relay_config = RelayConfig::default();

        let relay = Arc::new(RelayManager::new(peer_id, relay_config, topology.clone()));

        // Mesh configuration
        let mesh_config = MeshConfig::default();

        let mesh = Arc::new(MeshNetwork::new(peer_id, mesh_config));

        // Create discovery manager configuration
        let mut discovery_config = DiscoveryManagerConfig {
            enable_bootstrap: config.enable_bootstrap_discovery,
            enable_dht: config.enable_dht_discovery,
            ..DiscoveryManagerConfig::default()
        };

        // Parse bootstrap peers once upfront
        let parsed_bootstrap: Vec<Multiaddr> = config
            .bootstrap_peers
            .iter()
            .filter_map(|peer_str| {
                peer_str.parse::<Multiaddr>().map_or_else(
                    |_| {
                        warn!("Invalid bootstrap peer address: {}", peer_str);
                        None
                    },
                    Some,
                )
            })
            .collect();

        // Configure bootstrap discovery if enabled
        if config.enable_bootstrap_discovery {
            discovery_config.bootstrap_config = Some(BootstrapDiscoveryConfig {
                bootstrap_nodes: parsed_bootstrap.clone(),
                ..BootstrapDiscoveryConfig::default()
            });
        }

        // Configure DHT discovery if enabled
        if config.enable_dht_discovery {
            discovery_config.dht_config = Some(DhtDiscoveryConfig {
                bootstrap_peers: parsed_bootstrap,
                ..DhtDiscoveryConfig::default()
            });
        }

        let discovery = Arc::new(Mutex::new(DiscoveryManager::new(discovery_config)));

        // Swarm is created lazily when start() is called, but we initialize the container here
        let swarm = Arc::new(Mutex::new(None));

        Ok(Self {
            config,
            peer_id,
            keypair,
            swarm,
            topology,
            relay,
            mesh,
            discovery,
            peers: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(RwLock::new(HashSet::new())),
            event_tx,
            event_rx: Arc::new(Mutex::new(event_rx)),
            running: Arc::new(RwLock::new(false)),
            worker_task: Arc::new(Mutex::new(None)),
            moderation: None,
        })
    }

    /// Get the local peer ID as a LocalPeerId wrapper (zero-cost)
    pub fn local_peer_id(&self) -> LocalPeerId {
        LocalPeerId::from(self.peer_id)
    }

    /// Set the moderation manager for peer blocking enforcement
    pub fn set_moderation(&mut self, mgr: Arc<ModerationManager>) {
        self.moderation = Some(mgr);
    }

    /// Initialize the swarm with behaviors including NAT traversal support
    pub async fn init_swarm(&self) -> Result<Swarm<OverlayBehavior>, OverlayError> {
        // Clone the keypair for use in transport and behaviors
        let local_key = self.keypair.clone();
        let local_peer_id = local_key.public().to_peer_id();

        // Create TCP transport
        let tcp_transport =
            libp2p::tcp::tokio::Transport::new(GenTcpConfig::default().nodelay(true));

        // Create relay client transport for NAT traversal
        // This allows us to connect through relay servers when direct connections fail
        let (relay_transport, relay_client) = relay::client::new(local_peer_id);

        // Combine transports: TCP first, relay as fallback
        // OrTransport tries the first transport, then falls back to the second
        let transport = tcp_transport
            .or_transport(relay_transport)
            .upgrade(upgrade::Version::V1)
            .authenticate(
                noise::Config::new(&local_key)
                    .map_err(|e| OverlayError::Other(format!("Noise init failed: {}", e)))?,
            )
            .multiplex(libp2p::yamux::Config::default())
            .boxed();

        // Create Gossipsub behavior
        let gossipsub_config = libp2p::gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .validation_mode(ValidationMode::Strict)
            .mesh_n(2)
            .mesh_n_low(1)
            .mesh_n_high(4)
            .mesh_outbound_min(1)
            .build()
            .map_err(|e| OverlayError::Other(format!("Invalid gossipsub config: {}", e)))?;

        let gossipsub = Gossipsub::new(
            MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config,
        )
        .map_err(|e| OverlayError::Other(format!("Failed to create gossipsub: {}", e)))?;

        // Create Kademlia behavior
        let store = MemoryStore::new(self.peer_id);
        let kademlia_config = KademliaConfig::default();
        let kademlia = Kademlia::with_config(self.peer_id, store, kademlia_config);

        // Create Identify behavior
        let identify_config = IdentifyConfig::new(
            "/open-broadcast-network/1.0.0".to_string(),
            local_key.public(),
        );
        let identify = Identify::new(identify_config);

        // Create AutoNAT behavior for automatic NAT detection
        // AutoNAT probes other peers to determine if we're reachable from the internet
        let autonat_config = autonat::Config {
            // Use bootstrap peers as servers for NAT probing
            boot_delay: Duration::from_secs(10),
            refresh_interval: Duration::from_secs(60),
            retry_interval: Duration::from_secs(30),
            throttle_server_period: Duration::from_secs(5),
            only_global_ips: true,
            ..Default::default()
        };
        let autonat = autonat::Behaviour::new(local_peer_id, autonat_config);

        // Create DCUtR behavior for direct connection upgrade through relay
        // This attempts to hole-punch and establish a direct connection when both
        // peers are behind NATs
        let dcutr = dcutr::Behaviour::new(local_peer_id);

        info!("NAT traversal enabled: relay client, autonat, dcutr");

        // Combine behaviors
        let behavior =
            OverlayBehavior::new(gossipsub, kademlia, identify, relay_client, autonat, dcutr);

        // Create Swarm
        let swarm = SwarmBuilder::with_existing_identity(local_key)
            .with_tokio()
            .with_other_transport(|_key| transport)
            .unwrap()
            .with_behaviour(|_| behavior)
            .map_err(|e| {
                OverlayError::Other(format!("Failed to build swarm with behavior: {}", e))
            })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        Ok(swarm)
    }

    /// Run the swarm
    pub async fn run_swarm(&self) -> Result<(), OverlayError> {
        let mut swarm_lock = self.swarm.lock().await;

        if swarm_lock.is_some() {
            return Ok(());
        }

        // Initialize the swarm
        let mut swarm = self.init_swarm().await?;

        // Setup listeners
        swarm
            .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
            .map_err(|e| OverlayError::Other(format!("Failed to listen: {:?}", e)))?;

        // Connect to bootstrap peers
        for addr_str in &self.config.bootstrap_peers {
            match addr_str.parse::<Multiaddr>() {
                Ok(addr) => {
                    debug!("Dialing bootstrap peer: {}", addr);
                    match swarm.dial(addr.clone()) {
                        Ok(_) => {}
                        Err(e) => warn!("Failed to dial bootstrap peer: {}", e),
                    }
                }
                Err(e) => warn!("Invalid bootstrap address {}: {}", addr_str, e),
            }
        }

        // Start the discovery topic
        let discovery_topic = gossipsub::IdentTopic::new(topics::discovery());
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&discovery_topic)
            .map_err(|e| OverlayError::Other(format!("Failed to subscribe to discovery: {}", e)))?;

        // Subscribe to stream announcement topic for stream discovery
        let announce_topic =
            gossipsub::IdentTopic::new(crate::discovery::stream_discovery::STREAM_ANNOUNCE_TOPIC);
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&announce_topic)
            .map_err(|e| {
                OverlayError::Other(format!("Failed to subscribe to announce topic: {}", e))
            })?;
        info!("Subscribed to stream announce topic");

        // Store the swarm
        *swarm_lock = Some(swarm);

        Ok(())
    }

    /// Get the listen addresses from the swarm
    pub async fn get_listen_addrs(&self) -> Vec<Multiaddr> {
        let swarm_lock = self.swarm.lock().await;
        match &*swarm_lock {
            Some(swarm) => swarm.listeners().cloned().collect(),
            None => Vec::new(),
        }
    }

    /// Initialize the topology manager
    pub async fn init_topology(&self) -> Result<(), OverlayError> {
        debug!("Initializing topology manager");
        // This method would typically initialize the topology manager
        // For example, start any background tasks, load peer lists, etc.
        Ok(())
    }

    /// Stop the topology manager
    pub async fn stop_topology(&self) -> Result<(), OverlayError> {
        debug!("Stopping topology manager");
        Ok(())
    }

    /// Initialize the relay manager
    pub async fn init_relay(&self) -> Result<(), OverlayError> {
        debug!("Initializing relay manager");
        // This method would typically initialize the relay manager
        Ok(())
    }

    /// Stop the relay manager
    pub async fn stop_relay(&self) -> Result<(), OverlayError> {
        debug!("Stopping relay manager");
        Ok(())
    }

    /// Initialize the mesh network
    pub async fn init_mesh(&self) -> Result<(), OverlayError> {
        debug!("Initializing mesh network");
        // This method would typically initialize the mesh network
        Ok(())
    }

    /// Stop the mesh network
    pub async fn stop_mesh(&self) -> Result<(), OverlayError> {
        debug!("Stopping mesh network");
        Ok(())
    }
}
