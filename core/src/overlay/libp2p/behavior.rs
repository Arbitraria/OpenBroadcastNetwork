//! Network behavior definitions for libp2p
//!
//! This module defines the composite network behavior for the libp2p overlay
//! by combining multiple sub-behaviors (Gossipsub, Kademlia, Identify, Relay, AutoNAT, DCUtR)
//! into a unified behavior that can be used with the libp2p Swarm.
//!
//! # Architecture Overview
//!
//! The `OverlayBehavior` struct implements the `NetworkBehaviour` trait from libp2p,
//! which is required by the Swarm to handle network events. It uses the derive macro
//! to combine multiple sub-behaviors:
//!
//! - **Gossipsub** - Used for publish/subscribe message broadcasting
//! - **Kademlia** - Used for distributed hash table and peer discovery
//! - **Identify** - Used for exchanging node information with peers
//! - **Relay Client** - Used for circuit relay v2 client functionality (NAT traversal)
//! - **AutoNAT** - Used for automatic NAT status detection
//! - **DCUtR** - Used for direct connection upgrade through relay
//!
//! # Integration Points
//!
//! This behavior is used by:
//! - `Libp2pOverlay` in `overlay/libp2p_impl.rs` - Primary consumer that creates a Swarm with this behavior
//!
//! # Event Handling
//!
//! The behavior converts events from sub-behaviors into `OverlayBehaviorEvent` variants,
//! which are then processed by the `Libp2pOverlay` event loop. This abstraction allows
//! the rest of the application to work with high-level overlay events instead of
//! low-level protocol events.
//!
//! # NAT Traversal
//!
//! This module provides NAT traversal capabilities through:
//! - **Relay Client**: Enables peers behind NATs to be reachable via relay servers
//! - **AutoNAT**: Automatically detects whether the peer is behind a NAT
//! - **DCUtR**: Upgrades relayed connections to direct connections when possible

// Import libp2p core components
use libp2p::{
    autonat, dcutr,
    gossipsub::{Behaviour as Gossipsub, Event as GossipsubEvent},
    identify::{Behaviour as Identify, Event as IdentifyEvent},
    kad::{store::MemoryStore, Behaviour as Kademlia, Event as KademliaEvent},
    relay,
};

/// Combined network behavior for the overlay using the derive macro
/// for proper connection handler composition.
#[derive(libp2p::swarm::NetworkBehaviour)]
#[behaviour(to_swarm = "OverlayBehaviorEvent")]
pub struct OverlayBehavior {
    /// Gossipsub for pub/sub communication
    pub gossipsub: Gossipsub,
    /// Kademlia for DHT and peer discovery
    pub kademlia: Kademlia<MemoryStore>,
    /// Identify protocol
    pub identify: Identify,
    /// Relay client for NAT traversal via circuit relay v2
    pub relay_client: relay::client::Behaviour,
    /// AutoNAT for automatic NAT status detection
    pub autonat: autonat::Behaviour,
    /// DCUtR for direct connection upgrade through relay
    pub dcutr: dcutr::Behaviour,
}

impl OverlayBehavior {
    /// Create a new OverlayBehavior with the provided sub-behaviors
    pub fn new(
        gossipsub: Gossipsub,
        kademlia: Kademlia<MemoryStore>,
        identify: Identify,
        relay_client: relay::client::Behaviour,
        autonat: autonat::Behaviour,
        dcutr: dcutr::Behaviour,
    ) -> Self {
        Self {
            gossipsub,
            kademlia,
            identify,
            relay_client,
            autonat,
            dcutr,
        }
    }
}

/// Events emitted by the OverlayBehavior
#[derive(Debug)]
pub enum OverlayBehaviorEvent {
    /// Gossipsub events
    Gossipsub(GossipsubEvent),
    /// Kademlia events
    Kademlia(KademliaEvent),
    /// Identify events
    Identify(IdentifyEvent),
    /// Relay client events
    RelayClient(relay::client::Event),
    /// AutoNAT events
    Autonat(autonat::Event),
    /// DCUtR events
    Dcutr(dcutr::Event),
}

// From implementations for event conversions (required by the derive macro)
impl From<GossipsubEvent> for OverlayBehaviorEvent {
    fn from(event: GossipsubEvent) -> Self {
        OverlayBehaviorEvent::Gossipsub(event)
    }
}

impl From<KademliaEvent> for OverlayBehaviorEvent {
    fn from(event: KademliaEvent) -> Self {
        OverlayBehaviorEvent::Kademlia(event)
    }
}

impl From<IdentifyEvent> for OverlayBehaviorEvent {
    fn from(event: IdentifyEvent) -> Self {
        OverlayBehaviorEvent::Identify(event)
    }
}

impl From<relay::client::Event> for OverlayBehaviorEvent {
    fn from(event: relay::client::Event) -> Self {
        OverlayBehaviorEvent::RelayClient(event)
    }
}

impl From<autonat::Event> for OverlayBehaviorEvent {
    fn from(event: autonat::Event) -> Self {
        OverlayBehaviorEvent::Autonat(event)
    }
}

impl From<dcutr::Event> for OverlayBehaviorEvent {
    fn from(event: dcutr::Event) -> Self {
        OverlayBehaviorEvent::Dcutr(event)
    }
}
