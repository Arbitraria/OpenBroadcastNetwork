use futures_util::StreamExt;
use libp2p::{
    core::Endpoint,
    identify::{Behaviour as Identify, Config as IdentifyConfig, Event as IdentifyEvent},
    identity::Keypair,
    kad::{
        store::MemoryStore, Behaviour as Kademlia, Config as KademliaConfig,
        Event as KademliaEvent, Mode,
    },
    noise,
    swarm::{
        ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, SwarmEvent, THandlerInEvent,
        ToSwarm,
    },
    tcp::Config as GenTcpConfig,
    Multiaddr, PeerId, SwarmBuilder,
};
use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Combined network behavior for the bootstrap server
struct BootstrapBehavior {
    identify: Identify,
    kademlia: Kademlia<MemoryStore>,
    events: VecDeque<BootstrapBehaviorEvent>,
}

impl BootstrapBehavior {
    fn new(identify: Identify, kademlia: Kademlia<MemoryStore>) -> Self {
        Self {
            identify,
            kademlia,
            events: VecDeque::new(),
        }
    }

    fn queue_event(&mut self, event: BootstrapBehaviorEvent) {
        self.events.push_back(event);
    }
}

/// Events emitted by the combined bootstrap behavior
#[derive(Debug)]
enum BootstrapBehaviorEvent {
    Identify(IdentifyEvent),
    Kademlia(KademliaEvent),
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

impl NetworkBehaviour for BootstrapBehavior {
    type ConnectionHandler = libp2p::swarm::dummy::ConnectionHandler;
    type ToSwarm = BootstrapBehaviorEvent;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, ConnectionDenied> {
        let _ = self.identify.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        );
        let _ = self.kademlia.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        );
        Ok(libp2p::swarm::dummy::ConnectionHandler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
    ) -> Result<libp2p::swarm::THandler<Self>, ConnectionDenied> {
        let _ = self.identify.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
        );
        let _ = self.kademlia.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
        );
        Ok(libp2p::swarm::dummy::ConnectionHandler)
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        self.identify.on_swarm_event(event.clone());
        self.kademlia.on_swarm_event(event);
    }

    fn on_connection_handler_event(
        &mut self,
        _peer_id: PeerId,
        _connection_id: ConnectionId,
        _event: THandlerInEvent<Self>,
    ) {
        // No-op since we're using dummy connection handlers
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        // Check pending events
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(ToSwarm::GenerateEvent(event));
        }

        // Poll Identify
        if let Poll::Ready(ToSwarm::GenerateEvent(event)) = self.identify.poll(cx) {
            self.queue_event(BootstrapBehaviorEvent::Identify(event));
            return Poll::Ready(ToSwarm::GenerateEvent(self.events.pop_front().unwrap()));
        }

        // Poll Kademlia
        if let Poll::Ready(ToSwarm::GenerateEvent(event)) = self.kademlia.poll(cx) {
            self.queue_event(BootstrapBehaviorEvent::Kademlia(event));
            return Poll::Ready(ToSwarm::GenerateEvent(self.events.pop_front().unwrap()));
        }

        Poll::Pending
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
}

/// Information about a tracked peer
struct TrackedPeer {
    /// The peer's advertised listen addresses
    listen_addrs: Vec<Multiaddr>,
    /// When this peer was last seen
    last_seen: Instant,
    /// The peer's protocol version
    protocol_version: String,
    /// The peer's agent version
    agent_version: String,
}

/// Bootstrap server for peer discovery
///
/// This server acts as a rendezvous point for nodes in the network.
/// Nodes connect to exchange peer information, enabling peer discovery
/// without requiring manual address configuration.
pub struct BootstrapServer {
    config: BootstrapServerConfig,
    peers: HashMap<PeerId, TrackedPeer>,
}

impl BootstrapServer {
    /// Create a new bootstrap server with the given configuration
    pub fn new(config: BootstrapServerConfig) -> Self {
        Self {
            config,
            peers: HashMap::new(),
        }
    }

    /// Run the bootstrap server
    ///
    /// This starts the libp2p swarm and listens for incoming connections.
    /// Connected peers are tracked and their information is exchanged via
    /// the Identify protocol. Kademlia DHT enables automatic peer discovery
    /// after initial bootstrap connection.
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

        let behavior = BootstrapBehavior::new(identify, kademlia);

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

        let listen_addr: Multiaddr = format!(
            "/ip4/{}/tcp/{}",
            self.config.listen_addr, self.config.port
        )
        .parse()?;

        swarm.listen_on(listen_addr)?;

        info!("🚀 Bootstrap server starting...");
        info!("Peer ID: {}", peer_id);
        info!("DHT: Kademlia enabled for peer discovery");
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
                info!("🌐 Added {} addresses to DHT for peer {}", info.listen_addrs.len(), peer_id);

                // Track the peer if we haven't exceeded max_peers
                if self.peers.len() < self.config.max_peers || self.peers.contains_key(&peer_id) {
                    self.peers.insert(
                        peer_id,
                        TrackedPeer {
                            listen_addrs: info.listen_addrs,
                            last_seen: Instant::now(),
                            protocol_version: info.protocol_version,
                            agent_version: info.agent_version,
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

            SwarmEvent::Behaviour(BootstrapBehaviorEvent::Kademlia(event)) => {
                match &event {
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
