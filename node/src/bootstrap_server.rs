use futures_util::StreamExt;
use libp2p::{
    gossipsub::{
        Behaviour as Gossipsub, ConfigBuilder as GossipsubConfigBuilder, Event as GossipsubEvent,
        IdentTopic, MessageAuthenticity, ValidationMode,
    },
    identify::{Behaviour as Identify, Config as IdentifyConfig, Event as IdentifyEvent},
    identity::Keypair,
    kad::{
        store::MemoryStore, Behaviour as Kademlia, Config as KademliaConfig,
        Event as KademliaEvent, Mode,
    },
    noise,
    relay::{self, Config as RelayConfig},
    swarm::SwarmEvent,
    tcp::Config as GenTcpConfig,
    Multiaddr, PeerId, SwarmBuilder,
};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Combined network behavior for the bootstrap server using derive macro
/// for proper connection handler composition.
#[derive(libp2p::swarm::NetworkBehaviour)]
#[behaviour(to_swarm = "BootstrapBehaviorEvent")]
struct BootstrapBehavior {
    identify: Identify,
    kademlia: Kademlia<MemoryStore>,
    /// Circuit relay v2 server behavior for NAT traversal
    relay: relay::Behaviour,
    /// Gossipsub so the bootstrap server can act as a mesh hub for stream
    /// data: publishers and subscribers usually only know each other through
    /// the bootstrap, so without gossipsub here they can never form a mesh.
    gossipsub: Gossipsub,
}

/// Events emitted by the combined bootstrap behavior
#[derive(Debug)]
enum BootstrapBehaviorEvent {
    Identify(IdentifyEvent),
    Kademlia(KademliaEvent),
    Relay(relay::Event),
    Gossipsub(GossipsubEvent),
}

impl From<IdentifyEvent> for BootstrapBehaviorEvent {
    fn from(event: IdentifyEvent) -> Self {
        BootstrapBehaviorEvent::Identify(event)
    }
}

impl From<KademliaEvent> for BootstrapBehaviorEvent {
    fn from(event: KademliaEvent) -> Self {
        BootstrapBehaviorEvent::Kademlia(event)
    }
}

impl From<relay::Event> for BootstrapBehaviorEvent {
    fn from(event: relay::Event) -> Self {
        BootstrapBehaviorEvent::Relay(event)
    }
}

impl From<GossipsubEvent> for BootstrapBehaviorEvent {
    fn from(event: GossipsubEvent) -> Self {
        BootstrapBehaviorEvent::Gossipsub(event)
    }
}

/// Configuration for the bootstrap server
pub struct BootstrapServerConfig {
    /// Address to listen on (e.g., "0.0.0.0")
    pub listen_addr: String,
    /// Port to listen on
    pub port: u16,
    /// Maximum number of peers to track
    pub max_peers: usize,
    /// How long before a peer is considered expired
    pub peer_expiration: Duration,
    /// Whether to enable relay server functionality
    pub enable_relay: bool,
    /// Maximum number of relay reservations
    pub max_reservations: usize,
    /// Maximum number of active relay circuits
    pub max_circuits: usize,
}

impl Default for BootstrapServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0".to_string(),
            port: 9000,
            max_peers: 1000,
            peer_expiration: Duration::from_secs(3600),
            enable_relay: true,
            max_reservations: 128,
            max_circuits: 512,
        }
    }
}

/// Information about a tracked peer
struct TrackedPeer {
    /// When this peer was last seen
    last_seen: Instant,
}

/// Bootstrap server for peer discovery with optional relay server functionality
///
/// This server acts as a rendezvous point for nodes in the network.
/// Nodes connect to exchange peer information, enabling peer discovery
/// without requiring manual address configuration.
///
/// When relay is enabled, this server also acts as a Circuit Relay v2 server,
/// allowing peers behind NATs to be reachable through this server.
pub struct BootstrapServer {
    config: BootstrapServerConfig,
    peers: HashMap<PeerId, TrackedPeer>,
    /// Stream data topics this bootstrap is currently subscribed to. Tracked
    /// so we can subscribe lazily when remote peers join a new stream's mesh.
    subscribed_stream_topics: HashSet<String>,
}

impl BootstrapServer {
    /// Create a new bootstrap server with the given configuration
    pub fn new(config: BootstrapServerConfig) -> Self {
        Self {
            config,
            peers: HashMap::new(),
            subscribed_stream_topics: HashSet::new(),
        }
    }

    /// Run the bootstrap server
    ///
    /// This starts the libp2p swarm and listens for incoming connections.
    /// Connected peers are tracked and their information is exchanged via
    /// the Identify protocol. Kademlia DHT enables automatic peer discovery
    /// after initial bootstrap connection.
    ///
    /// When relay is enabled, the server also handles relay reservations and
    /// circuit connections for NAT traversal.
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();

        let identify = Identify::new(IdentifyConfig::new(
            "/open-broadcast-network/1.0.0".to_string(),
            keypair.public(),
        ));

        // Configure Kademlia DHT for peer discovery
        let store = MemoryStore::new(peer_id);
        let kad_config = KademliaConfig::default();
        let mut kademlia = Kademlia::with_config(peer_id, store, kad_config);
        // Set to server mode so this node participates in DHT routing
        kademlia.set_mode(Some(Mode::Server));

        // Configure Circuit Relay v2 server for NAT traversal
        let relay_config = RelayConfig {
            max_reservations: self.config.max_reservations,
            max_circuits: self.config.max_circuits,
            reservation_duration: Duration::from_secs(3600), // 1 hour reservations
            max_circuit_duration: Duration::from_secs(3600 * 2), // 2 hour max circuit
            max_circuit_bytes: u64::MAX, // Essentially unlimited bytes per circuit
            ..Default::default()
        };
        let relay = relay::Behaviour::new(peer_id, relay_config);

        // Match the gossipsub config used by client overlay nodes (see
        // core/src/overlay/libp2p/impl_core.rs) so this bootstrap can join
        // their meshes and forward stream data.
        let gossipsub_config = GossipsubConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .validation_mode(ValidationMode::Strict)
            .mesh_n(2)
            .mesh_n_low(1)
            .mesh_n_high(4)
            .mesh_outbound_min(1)
            .max_transmit_size(4 * 1024 * 1024)
            .build()
            .map_err(|e| format!("Invalid gossipsub config: {}", e))?;

        let mut gossipsub = Gossipsub::new(
            MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .map_err(|e| format!("Failed to create gossipsub: {}", e))?;

        // Subscribe to the discovery/announce topics that all overlay nodes
        // use, so this bootstrap is in their initial mesh and can relay any
        // future stream data subscriptions back to other peers.
        for topic in ["obn/streams/announce", "discovery"] {
            gossipsub
                .subscribe(&IdentTopic::new(topic))
                .map_err(|e| format!("Failed to subscribe to {}: {}", topic, e))?;
        }

        let behavior = BootstrapBehavior {
            identify,
            kademlia,
            relay,
            gossipsub,
        };

        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                GenTcpConfig::default().nodelay(true),
                noise::Config::new,
                libp2p::yamux::Config::default,
            )?
            .with_behaviour(|_| behavior)?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(300)))
            .build();

        let listen_addr: Multiaddr =
            format!("/ip4/{}/tcp/{}", self.config.listen_addr, self.config.port).parse()?;

        swarm.listen_on(listen_addr)?;

        info!("🚀 Bootstrap server starting...");
        info!("Peer ID: {}", peer_id);
        info!("DHT: Kademlia enabled for peer discovery");
        if self.config.enable_relay {
            info!(
                "🔗 Relay: Circuit relay v2 enabled (max {} reservations, {} circuits)",
                self.config.max_reservations, self.config.max_circuits
            );
        }
        info!(
            "Max peers: {}, Peer expiration: {:?}",
            self.config.max_peers, self.config.peer_expiration
        );

        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                event = swarm.select_next_some() => {
                    self.handle_event(&mut swarm, event);
                }
                _ = cleanup_interval.tick() => {
                    self.cleanup_expired_peers();
                }
            }
        }
    }

    /// Handle a swarm event
    fn handle_event(
        &mut self,
        swarm: &mut libp2p::Swarm<BootstrapBehavior>,
        event: SwarmEvent<BootstrapBehaviorEvent>,
    ) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("📡 Listening on: {}", address);
                // Print the full relay address for clients to use
                info!(
                    "🔗 Relay address: {}/p2p/{}",
                    address,
                    swarm.local_peer_id()
                );
            }

            SwarmEvent::ConnectionEstablished {
                peer_id,
                endpoint,
                num_established,
                ..
            } => {
                info!(
                    "✅ Peer connected: {} (connections: {})",
                    peer_id, num_established
                );
                debug!("  Endpoint: {:?}", endpoint);

                // Add the peer's address to Kademlia routing table
                let addr = endpoint.get_remote_address().clone();
                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                debug!("  Added to DHT routing table");
            }

            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                cause,
                ..
            } => {
                if num_established == 0 {
                    info!("❌ Peer disconnected: {}", peer_id);
                    if let Some(cause) = cause {
                        debug!("  Cause: {:?}", cause);
                    }
                } else {
                    debug!(
                        "Connection closed for {}, {} remaining",
                        peer_id, num_established
                    );
                }
            }

            SwarmEvent::Behaviour(BootstrapBehaviorEvent::Identify(IdentifyEvent::Received {
                peer_id,
                info,
            })) => {
                info!(
                    "📋 Identified peer {}: {} addresses",
                    peer_id,
                    info.listen_addrs.len()
                );
                debug!("  Protocol: {}", info.protocol_version);
                debug!("  Agent: {}", info.agent_version);

                // Add all peer's listen addresses to Kademlia DHT
                for addr in &info.listen_addrs {
                    debug!("  Address: {}", addr);
                    swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, addr.clone());
                }
                info!(
                    "🌐 Added {} addresses to DHT for peer {}",
                    info.listen_addrs.len(),
                    peer_id
                );

                // Track the peer if we haven't exceeded max_peers
                if self.peers.len() < self.config.max_peers || self.peers.contains_key(&peer_id) {
                    self.peers.insert(
                        peer_id,
                        TrackedPeer {
                            last_seen: Instant::now(),
                        },
                    );
                    info!("📊 Tracking {} peers", self.peers.len());
                } else {
                    warn!(
                        "Max peers reached ({}), not tracking new peer",
                        self.config.max_peers
                    );
                }
            }

            SwarmEvent::Behaviour(BootstrapBehaviorEvent::Identify(IdentifyEvent::Sent {
                peer_id,
            })) => {
                debug!("Sent identify info to {}", peer_id);
            }

            SwarmEvent::Behaviour(BootstrapBehaviorEvent::Identify(IdentifyEvent::Pushed {
                peer_id,
                info,
            })) => {
                debug!(
                    "Pushed identify info to {}: {} addresses",
                    peer_id,
                    info.listen_addrs.len()
                );
            }

            SwarmEvent::Behaviour(BootstrapBehaviorEvent::Identify(IdentifyEvent::Error {
                peer_id,
                error,
            })) => {
                warn!("Identify error with {}: {}", peer_id, error);
            }

            SwarmEvent::Behaviour(BootstrapBehaviorEvent::Kademlia(event)) => match &event {
                KademliaEvent::RoutingUpdated {
                    peer, addresses, ..
                } => {
                    info!(
                        "🔄 DHT routing updated: peer {} with {} addresses",
                        peer,
                        addresses.len()
                    );
                }
                KademliaEvent::InboundRequest { request } => {
                    debug!("DHT inbound request: {:?}", request);
                }
                KademliaEvent::OutboundQueryProgressed { id, result, .. } => {
                    debug!("DHT query {:?} progressed: {:?}", id, result);
                }
                KademliaEvent::ModeChanged { new_mode } => {
                    info!("DHT mode changed to: {:?}", new_mode);
                }
                _ => {
                    debug!("DHT event: {:?}", event);
                }
            },

            // Handle gossipsub events: keep the bootstrap server subscribed
            // to every stream topic some peer joins, so it can forward stream
            // data between publishers and subscribers that only know each
            // other through this bootstrap.
            SwarmEvent::Behaviour(BootstrapBehaviorEvent::Gossipsub(event)) => match event {
                GossipsubEvent::Subscribed { peer_id, topic } => {
                    debug!("Peer {} subscribed to {}", peer_id, topic);
                    let topic_str = topic.as_str();
                    if topic_str.starts_with("stream/")
                        && !self.subscribed_stream_topics.contains(topic_str)
                    {
                        let ident = IdentTopic::new(topic_str);
                        if let Err(e) = swarm.behaviour_mut().gossipsub.subscribe(&ident) {
                            warn!("Failed to subscribe to {}: {}", topic_str, e);
                        } else {
                            info!("🔁 Bootstrap now relaying {}", topic_str);
                            self.subscribed_stream_topics.insert(topic_str.to_string());
                        }
                    }
                }
                GossipsubEvent::Unsubscribed { peer_id, topic } => {
                    debug!("Peer {} unsubscribed from {}", peer_id, topic);
                }
                GossipsubEvent::Message { .. } => {
                    // Bootstrap forwards messages but does not consume them.
                }
                GossipsubEvent::GossipsubNotSupported { peer_id } => {
                    debug!("Peer {} does not support gossipsub", peer_id);
                }
            },

            // Handle relay events
            SwarmEvent::Behaviour(BootstrapBehaviorEvent::Relay(event)) => {
                #[allow(deprecated)]
                match event {
                    relay::Event::ReservationReqAccepted {
                        src_peer_id,
                        renewed,
                    } => {
                        if renewed {
                            info!("🔗 Relay reservation renewed for {}", src_peer_id);
                        } else {
                            info!("🔗 Relay reservation accepted for {}", src_peer_id);
                        }
                    }
                    relay::Event::ReservationReqDenied { src_peer_id } => {
                        warn!("🔗 Relay reservation denied for {}", src_peer_id);
                    }
                    relay::Event::ReservationTimedOut { src_peer_id } => {
                        info!("🔗 Relay reservation timed out for {}", src_peer_id);
                    }
                    relay::Event::CircuitReqAccepted {
                        src_peer_id,
                        dst_peer_id,
                    } => {
                        info!("🔗 Circuit established: {} -> {}", src_peer_id, dst_peer_id);
                    }
                    relay::Event::CircuitReqDenied {
                        src_peer_id,
                        dst_peer_id,
                    } => {
                        warn!("🔗 Circuit denied: {} -> {}", src_peer_id, dst_peer_id);
                    }
                    // Handle deprecated events
                    _ => {
                        debug!("🔗 Relay event: {:?}", event);
                    }
                }
            }

            SwarmEvent::IncomingConnection {
                local_addr,
                send_back_addr,
                ..
            } => {
                debug!(
                    "Incoming connection: local={}, remote={}",
                    local_addr, send_back_addr
                );
            }

            SwarmEvent::IncomingConnectionError {
                local_addr,
                send_back_addr,
                error,
                ..
            } => {
                warn!(
                    "Incoming connection error: local={}, remote={}, error={}",
                    local_addr, send_back_addr, error
                );
            }

            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(peer_id) = peer_id {
                    warn!("Outgoing connection error to {}: {}", peer_id, error);
                } else {
                    warn!("Outgoing connection error: {}", error);
                }
            }

            SwarmEvent::Dialing { peer_id, .. } => {
                if let Some(peer_id) = peer_id {
                    debug!("Dialing peer: {}", peer_id);
                }
            }

            _ => {
                debug!("Other swarm event: {:?}", event);
            }
        }
    }

    /// Remove peers that haven't been seen recently
    fn cleanup_expired_peers(&mut self) {
        let now = Instant::now();
        let before = self.peers.len();

        self.peers.retain(|peer_id, peer| {
            let expired = now.duration_since(peer.last_seen) >= self.config.peer_expiration;
            if expired {
                debug!("Removing expired peer: {}", peer_id);
            }
            !expired
        });

        let removed = before - self.peers.len();
        if removed > 0 {
            info!("🧹 Cleaned up {} expired peers", removed);
            info!("📊 Now tracking {} peers", self.peers.len());
        }
    }
}

/// Format a duration for display
fn _format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    let secs = secs % 60;

    if hours > 0 {
        format!("{}h {}m {}s", hours, mins, secs)
    } else if mins > 0 {
        format!("{}m {}s", mins, secs)
    } else {
        format!("{}s", secs)
    }
}
