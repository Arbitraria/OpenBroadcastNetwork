//! LibP2P-based network implementation
//!
//! This module provides the networking foundation for the overlay using libp2p.

use futures::StreamExt;
use libp2p::{
    core::upgrade,
    gossipsub::{
        self, Behaviour as Gossipsub, ConfigBuilder as GossipsubConfigBuilder, MessageAuthenticity,
        MessageId, TopicHash, ValidationMode,
    },
    identity, noise,
    swarm::SwarmEvent,
    tcp, PeerId, Swarm, SwarmBuilder, Transport,
};
use std::collections::HashSet;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Configuration for the libp2p network
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Local node listening address
    pub listen_address: String,
    /// Whether to enable Kademlia DHT
    pub enable_kademlia: bool,
    /// Bootstrap nodes for Kademlia
    pub bootstrap_nodes: Vec<String>,
    /// Message validation mode for GossipSub
    pub validation_mode: ValidationMode,
    /// Topic message history size
    pub history_size: usize,
    /// Heartbeat interval in seconds
    pub heartbeat_interval: u64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            listen_address: "/ip4/0.0.0.0/tcp/0".to_string(),

            enable_kademlia: false,
            bootstrap_nodes: Vec::new(),
            validation_mode: ValidationMode::Strict,
            history_size: 50,
            heartbeat_interval: 1,
        }
    }
}

/// Events emitted by the network layer
#[derive(Debug)]
pub enum NetworkEvent {
    /// New peer connected
    PeerConnected(PeerId),
    /// Peer disconnected
    PeerDisconnected(PeerId),
    /// Message received on a topic
    MessageReceived(TopicHash, PeerId, Vec<u8>),
    /// Error occurred
    Error(String),
}

/// Network statistics
#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    /// Number of connected peers
    pub connected_peers_count: usize,
    /// Number of subscribed topics
    pub subscribed_topics_count: usize,
    /// Total messages sent
    pub messages_sent: u64,
    /// Total messages received
    pub messages_received: u64,
}

/// Internal statistics counters
#[derive(Debug, Default)]
struct StatsCounters {
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
}

/// A wrapper around libp2p Swarm for our network functionality
pub struct Network {
    /// The libp2p Swarm
    swarm: Swarm<gossipsub::Behaviour>,
    /// Our local peer ID
    local_peer_id: PeerId,
    /// Subscribed topics
    subscribed_topics: HashSet<TopicHash>,
    /// Channel for network events
    event_sender: mpsc::Sender<NetworkEvent>,
    /// Channel for receiving network events
    event_receiver: mpsc::Receiver<NetworkEvent>,
    /// Is the network running
    running: bool,
    /// Statistics counters
    stats_counters: StatsCounters,
}

impl Network {
    /// Create a new network instance
    pub async fn new(config: NetworkConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Create a random keypair for our identity
        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        info!("Local peer id: {}", local_peer_id);

        // Create a transport with Noise for encryption and Yamux for multiplexing
        let _transport = tcp::tokio::Transport::new(tcp::Config::default())
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::Config::new(&local_key)?)
            .multiplex(libp2p::yamux::Config::default())
            .boxed();

        // Create a GossipSub configuration
        let gossipsub_config = GossipsubConfigBuilder::default()
            .validation_mode(config.validation_mode.clone())
            .message_id_fn(|message: &gossipsub::Message| MessageId::from(message.data.clone()))
            .heartbeat_interval(Duration::from_secs(config.heartbeat_interval))
            .history_length(config.history_size)
            .build()
            .expect("Valid GossipSub configuration");

        // Create a GossipSub behaviour
        let gossipsub = Gossipsub::new(
            MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config,
        )?;

        // Create a Swarm to manage peers and events using new libp2p 0.53.0 API
        let mut swarm = SwarmBuilder::with_existing_identity(local_key)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                libp2p::yamux::Config::default,
            )
            .map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to build swarm: {}", e),
                ))
            })?
            .with_behaviour(|_| gossipsub)
            .map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to add behavior: {}", e),
                ))
            })?
            .build();

        // Listen on the configured address
        swarm.listen_on(config.listen_address.parse()?)?;

        // Create a channel for network events
        let (event_sender, event_receiver) = mpsc::channel(100);

        Ok(Self {
            swarm,
            local_peer_id,
            subscribed_topics: HashSet::new(),
            event_sender,
            event_receiver,
            running: false,
            stats_counters: StatsCounters::default(),
        })
    }

    /// Get our local peer ID
    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    /// Subscribe to a topic
    pub fn subscribe(&mut self, topic: &str) -> Result<TopicHash, Box<dyn Error + Send + Sync>> {
        let topic = gossipsub::IdentTopic::new(topic);
        match self.swarm.behaviour_mut().subscribe(&topic) {
            Ok(_) => {
                let topic_hash = topic.hash();
                self.subscribed_topics.insert(topic_hash.clone());
                info!("Subscribed to topic: {:?}", topic);
                Ok(topic_hash)
            }
            Err(e) => {
                error!("Failed to subscribe to topic: {}", e);
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to subscribe: {}", e),
                )))
            }
        }
    }

    /// Unsubscribe from a topic
    pub fn unsubscribe(&mut self, topic: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let topic = gossipsub::IdentTopic::new(topic);
        match self.swarm.behaviour_mut().unsubscribe(&topic) {
            Ok(_) => {
                self.subscribed_topics.remove(&topic.hash());
                info!("Unsubscribed from topic: {:?}", topic);
                Ok(())
            }
            Err(e) => {
                error!("Failed to unsubscribe from topic: {}", e);
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to unsubscribe: {}", e),
                )))
            }
        }
    }

    /// Publish a message to a topic
    pub fn publish(
        &mut self,
        topic: &str,
        data: Vec<u8>,
    ) -> Result<MessageId, Box<dyn Error + Send + Sync>> {
        let topic = gossipsub::IdentTopic::new(topic);
        match self.swarm.behaviour_mut().publish(topic.clone(), data) {
            Ok(message_id) => {
                debug!("Published message to topic: {:?}", topic);
                self.stats_counters
                    .messages_sent
                    .fetch_add(1, Ordering::Relaxed);
                Ok(message_id)
            }
            Err(e) => {
                error!("Failed to publish to topic: {}", e);
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to publish: {}", e),
                )))
            }
        }
    }

    /// Start the network event loop
    pub async fn run(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.running = true;

        while self.running {
            tokio::select! {
                event = self.swarm.next() => {
                    if let Some(event) = event {
                    match event {
                        SwarmEvent::Behaviour(gossipsub::Event::Message {
                            propagation_source,
                            message_id,
                            message,
                        }) => {
                            debug!(
                                "Got message: {} from {}",
                                message_id,
                                propagation_source
                            );

                            self.stats_counters.messages_received.fetch_add(1, Ordering::Relaxed);
                            if let Err(e) = self.event_sender.send(NetworkEvent::MessageReceived(
                                message.topic,
                                message.source.unwrap_or(propagation_source),
                                message.data,
                            )).await {
                                error!("Failed to send network event: {}", e);
                            }
                        },
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!("Listening on {address}");
                        },
                        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                            info!("Connected to {peer_id}");
                            if let Err(e) = self.event_sender.send(NetworkEvent::PeerConnected(peer_id)).await {
                                error!("Failed to send network event: {}", e);
                            }
                        },
                        SwarmEvent::ConnectionClosed { peer_id, .. } => {
                            info!("Disconnected from {peer_id}");
                            if let Err(e) = self.event_sender.send(NetworkEvent::PeerDisconnected(peer_id)).await {
                                error!("Failed to send network event: {}", e);
                            }
                        },
                        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                            if let Some(peer_id) = peer_id {
                                warn!("Failed to connect to {peer_id}: {error}");
                            } else {
                                warn!("Failed to connect: {error}");
                            }
                            if let Err(e) = self.event_sender.send(NetworkEvent::Error(error.to_string())).await {
                                error!("Failed to send network event: {}", e);
                            }
                        },
                        _ => {}
                    }
                    }
                }
                // Handle external shutdown signal
                else => {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Stop the network
    pub fn stop(&mut self) {
        self.running = false;
    }

    /// Check if the network is running
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Get the next network event (if any)
    pub async fn next_event(&mut self) -> Option<NetworkEvent> {
        self.event_receiver.recv().await
    }

    /// Get network statistics
    pub async fn stats(&self) -> Result<NetworkStats, Box<dyn Error + Send + Sync>> {
        let connected_peers_count = self.swarm.connected_peers().count();
        let subscribed_topics_count = self.subscribed_topics.len();
        let messages_sent = self.stats_counters.messages_sent.load(Ordering::Relaxed);
        let messages_received = self
            .stats_counters
            .messages_received
            .load(Ordering::Relaxed);

        Ok(NetworkStats {
            connected_peers_count,
            subscribed_topics_count,
            messages_sent,
            messages_received,
        })
    }
}
