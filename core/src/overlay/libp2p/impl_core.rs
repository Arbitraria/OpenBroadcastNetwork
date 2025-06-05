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
//!    (Gossipsub, Kademlia, Identify) into a unified behavior.
//!
//! 3. **Management Layer** - Specialized components that handle specific aspects:
//!    - `TopologyManager` - Manages peer connections and network topology
//!    - `RelayManager` - Handles stream relaying between peers
//!    - `MeshNetwork` - Manages mesh network connections
//!
//! # Concurrency Pattern
//!
//! This implementation uses a specific concurrency pattern for thread safety:
//! - `Arc<T>` for shared ownership of managers across threads
//! - `Arc<Mutex<T>>` for exclusive access to mutable shared state
//! - `RwLock<T>` for data structures that are read frequently but written to occasionally

// Core library imports
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use std::pin::Pin;
use std::future::Future;
use futures::{FutureExt, StreamExt, SinkExt};
use futures::channel::mpsc;
use tokio::sync::{Mutex, RwLock};

// External dependencies
use libp2p::gossipsub::{self, Behaviour as Gossipsub, Event as GossipsubEvent, IdentTopic, MessageAuthenticity, MessageId, ConfigBuilder, ValidationMode};
use libp2p::identify::{Behaviour as Identify, Event as IdentifyEvent, Config as IdentifyConfig};
use libp2p::kad::{Behaviour as Kademlia, Event as KademliaEvent, Config as KademliaConfig, store::MemoryStore, QueryResult};
use libp2p::tcp::Config as GenTcpConfig;
use libp2p::noise;
use libp2p::core::upgrade;
use libp2p::identity;
use libp2p::{Transport, SwarmBuilder, Swarm};
use libp2p::swarm::{SwarmEvent, NetworkBehaviour};
use libp2p::Multiaddr;
use libp2p::PeerId as Libp2pPeerId;
use tracing::{info, debug, warn, error};
use tokio::time;

// Local crate imports
use crate::overlay::interface::{OverlayConfig, OverlayError, OverlayEvent, StreamId};
use crate::overlay::peer::{Peer, LocalPeerId, PeerInfo, ConnectionStatus};
use crate::overlay::topology::{TopologyConfig, TopologyManager};
use crate::overlay::relay::{RelayManager, RelayNode, RelayConfig, StreamChunk};
use crate::overlay::mesh::{MeshNetwork, MeshConfig, StreamMesh, MeshStats};
use crate::overlay::libp2p::behavior::{OverlayBehavior, OverlayBehaviorEvent};

// Import LocalPeerId directly
use crate::overlay::libp2p::types::{to_libp2p_peer_id, from_libp2p_peer_id};
// PeerId is already imported as Libp2pPeerId from libp2p on line 50
use crate::overlay::libp2p::topics;
use crate::overlay::libp2p::overlay_utils;
use std::convert::TryFrom;

/// libp2p implementation of the overlay network
pub struct Libp2pOverlay {
    /// Configuration
    pub config: OverlayConfig,
    /// Local peer ID
    pub local_peer_id: LocalPeerId,
    /// libp2p peer ID
    pub libp2p_peer_id: Libp2pPeerId,
    /// Swarm
    pub swarm: Arc<Mutex<Option<Swarm<OverlayBehavior>>>>,
    /// Topology manager
    pub topology: Arc<TopologyManager>,
    /// Relay manager
    pub relay: Arc<RelayManager>,
    /// Mesh network
    pub mesh: Arc<MeshNetwork>,
    /// Peers
    pub peers: RwLock<HashMap<LocalPeerId, Peer>>,
    /// Active streams
    pub streams: RwLock<HashSet<StreamId>>,
    /// Event channel sender
    pub event_tx: mpsc::Sender<OverlayEvent>,
    /// Event channel receiver
    pub event_rx: Mutex<mpsc::Receiver<OverlayEvent>>,
    /// Is the overlay running
    pub running: RwLock<bool>,
    /// Worker task handle - renamed from worker to worker_task
    pub worker_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Libp2pOverlay {
    /// Create a new libp2p-based overlay
    pub async fn new(config: OverlayConfig) -> Result<Self, OverlayError> {
        // Create the libp2p identity
        let local_key = identity::Keypair::generate_ed25519();
        let libp2p_peer_id = local_key.public().to_peer_id();
        let local_peer_id = LocalPeerId::from(libp2p_peer_id);
        
        // Create channel for events
        let (event_tx, event_rx) = mpsc::channel(100);
        
        // Topology configuration
        let topology_config = TopologyConfig::default();
        
        let topology = Arc::new(
            TopologyManager::new(
                libp2p_peer_id,
                topology_config,
                None // No metrics for now
            )
        );
        
        // Relay configuration
        let relay_config = RelayConfig::default();
        
        let relay = Arc::new(
            RelayManager::new(
                libp2p_peer_id,
                relay_config,
                topology.clone()
            )
        );
        
        // Mesh configuration
        let mesh_config = MeshConfig::default();
        
        let mesh = Arc::new(MeshNetwork::new(config.local_peer_id.clone(), mesh_config));
        
        // Create the Libp2pOverlay
        let overlay = Self {
            local_peer_id: config.local_peer_id.clone(),
            libp2p_peer_id,
            config,
            swarm: Arc::new(Mutex::new(None)),
            topology,
            relay,
            mesh,
            peers: RwLock::new(HashMap::new()),
            streams: RwLock::new(HashSet::new()),
            event_tx,
            event_rx: Mutex::new(event_rx),
            running: RwLock::new(false),
            worker_task: Mutex::new(None),
        };
        
        Ok(overlay)
    }
    
    /// Initialize the swarm
    pub async fn init_swarm(&self) -> Result<Swarm<OverlayBehavior>, OverlayError> {
        // Create a key pair for authentication
        let local_key = identity::Keypair::generate_ed25519();
        let libp2p_peer_id = local_key.public().to_peer_id();
        
        // Set up gossipsub
        let gossipsub_config = ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .validation_mode(ValidationMode::Strict)
            .build()
            .expect("Valid gossipsub config");
            
        let gossipsub = Gossipsub::new(
            MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config
        ).expect("Valid gossipsub");
        
        // Set up kademlia
        let store = MemoryStore::new(libp2p_peer_id);
        let kademlia = Kademlia::new(libp2p_peer_id, store);
        
        // Set up identify
        let identify = Identify::new(
            IdentifyConfig::new(
                "/obn/0.1.0".to_string(),
                local_key.public()
            )
        );
        
        // Create the behavior
        let behavior = OverlayBehavior::new(
            gossipsub,
            kademlia,
            identify,
        );
        
        // Build the swarm with new libp2p 0.53.0 API
        let swarm = SwarmBuilder::with_existing_identity(local_key)
            .with_tokio()
            .with_tcp(
                GenTcpConfig::default().nodelay(true), 
                noise::Config::new,
                libp2p::yamux::Config::default
            )
            .map_err(|e| OverlayError::Other(format!("Failed to build swarm with TCP: {}", e)))?
            .with_behaviour(|_| behavior)
            .map_err(|e| OverlayError::Other(format!("Failed to build swarm with behavior: {}", e)))?
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
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
            .map_err(|e| OverlayError::Other(format!("Failed to listen: {:?}", e)))?;
        
        // Connect to bootstrap peers
        for addr_str in &self.config.bootstrap_peers {
            match addr_str.parse::<Multiaddr>() {
                Ok(addr) => {
                    debug!("Dialing bootstrap peer: {}", addr);
                    match swarm.dial(addr.clone()) {
                        Ok(_) => {},
                        Err(e) => warn!("Failed to dial bootstrap peer: {}", e),
                    }
                },
                Err(e) => warn!("Invalid bootstrap address {}: {}", addr_str, e),
            }
        }
        
        // Start the discovery topic
        let discovery_topic = gossipsub::IdentTopic::new(topics::discovery());
        swarm.behaviour_mut().gossipsub.subscribe(&discovery_topic)
            .map_err(|e| OverlayError::Other(format!("Failed to subscribe to discovery: {}", e)))?;
        
        // Store the swarm
        *swarm_lock = Some(swarm);
        
        Ok(())
    }

    /// Initialize the topology manager
    pub async fn init_topology(&self) -> Result<(), OverlayError> {
        debug!("Initializing topology manager");
        // This method would typically initialize the topology manager
        // For example, start any background tasks, load peer lists, etc.
        Ok(())
    }

    /// Initialize the relay manager
    pub async fn init_relay(&self) -> Result<(), OverlayError> {
        debug!("Initializing relay manager");
        // Initialize the relay manager
        // For example, start background tasks, configure relay settings, etc.
        Ok(())
    }

    /// Initialize the mesh network
    pub async fn init_mesh(&self) -> Result<(), OverlayError> {
        debug!("Initializing mesh network");
        // Initialize the mesh network
        // For example, establish initial connections, set up routing tables, etc.
        Ok(())
    }

    /// Stop the topology manager
    pub async fn stop_topology(&self) -> Result<(), OverlayError> {
        debug!("Stopping topology manager");
        // Stop any background tasks, clean up resources, etc.
        Ok(())
    }

    /// Stop the relay manager
    pub async fn stop_relay(&self) -> Result<(), OverlayError> {
        debug!("Stopping relay manager");
        // Stop any background tasks, clean up resources, etc.
        Ok(())
    }

    /// Stop the mesh network
    pub async fn stop_mesh(&self) -> Result<(), OverlayError> {
        debug!("Stopping mesh network");
        // Stop any background tasks, clean up resources, etc.
        Ok(())
    }

    // NOTE: process_swarm_events method moved to event_handlers.rs
}
