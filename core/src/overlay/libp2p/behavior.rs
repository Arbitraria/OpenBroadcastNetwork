//! Network behavior definitions for libp2p
//!
//! This module defines the composite network behavior for the libp2p overlay
//! by combining multiple sub-behaviors (Gossipsub, Kademlia, MDNS, Identify)
//! into a unified behavior that can be used with the libp2p Swarm.
//!
//! # Architecture Overview
//!
//! The `OverlayBehavior` struct implements the `NetworkBehaviour` trait from libp2p,
//! which is required by the Swarm to handle network events. It uses the type-map pattern
//! to combine multiple sub-behaviors:
//!
//! - **Gossipsub** - Used for publish/subscribe message broadcasting
//! - **Kademlia** - Used for distributed hash table and peer discovery
//! - **MDNS** - Used for local network peer discovery
//! - **Identify** - Used for exchanging node information with peers
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
//! # Implementation Notes
//!
//! - The `NetworkBehaviour` trait implementation uses a dummy connection handler
//!   since actual connection handling is delegated to the sub-behaviors.
//! - Event handling is done through pattern matching on events from sub-behaviors
//!   and converting them to the appropriate `OverlayBehaviorEvent` variant.

// Standard library imports
use std::task::{Context, Poll};
use std::collections::VecDeque;

// Import libp2p core components
// Import libp2p from the root like in libp2p_impl.rs
use libp2p::{PeerId, Multiaddr, swarm::{NetworkBehaviour, ToSwarm, FromSwarm, THandlerInEvent, ConnectionId, ConnectionDenied}};
use libp2p::core::Endpoint;

// Import protocol types using the same paths as libp2p_impl.rs
use libp2p::gossipsub::{self, Gossipsub, GossipsubEvent};
use libp2p::identify::{Identify, IdentifyEvent};
use libp2p::kad::{Kademlia, KademliaEvent, store::MemoryStore};
use libp2p::mdns::{Mdns, MdnsEvent};


/// Combined network behavior for the overlay
pub struct OverlayBehavior {
    /// Gossipsub for pub/sub communication
    pub gossipsub: Gossipsub,
    /// Kademlia for DHT and peer discovery
    pub kademlia: Kademlia<MemoryStore>,
    /// mDNS for local peer discovery
    pub mdns: Mdns,
    /// Identify protocol
    pub identify: Identify,
    /// Queue of events to emit
    events: VecDeque<OverlayBehaviorEvent>,
}

impl OverlayBehavior {
    /// Create a new OverlayBehavior with the provided sub-behaviors
    pub fn new(
        gossipsub: Gossipsub,
        kademlia: Kademlia<MemoryStore>,
        mdns: Mdns,
        identify: Identify,
    ) -> Self {
        Self {
            gossipsub,
            kademlia,
            mdns,
            identify,
            events: VecDeque::new(),
        }
    }
    
    /// Add an event to the queue
    pub fn queue_event(&mut self, event: OverlayBehaviorEvent) {
        self.events.push_back(event);
    }
    
    /// Process events from the Gossipsub sub-behavior
    fn handle_gossipsub_event(&mut self, event: GossipsubEvent) {
        self.queue_event(OverlayBehaviorEvent::Gossipsub(event));
    }

    /// Process events from the Mdns sub-behavior
    fn handle_mdns_event(&mut self, event: MdnsEvent) {
        self.queue_event(OverlayBehaviorEvent::Mdns(event));
    }

    /// Process events from the Kademlia sub-behavior
    fn handle_kademlia_event(&mut self, event: KademliaEvent) {
        self.queue_event(OverlayBehaviorEvent::Kademlia(event));
    }

    /// Process events from the Identify sub-behavior
    fn handle_identify_event(&mut self, event: IdentifyEvent) {
        self.queue_event(OverlayBehaviorEvent::Identify(event));
    }
}

/// Events emitted by the OverlayBehavior
#[derive(Debug)]
pub enum OverlayBehaviorEvent {
    /// Gossipsub events
    Gossipsub(GossipsubEvent),
    /// mDNS events
    Mdns(MdnsEvent),
    /// Kademlia events
    Kademlia(KademliaEvent),
    /// Identify events
    Identify(IdentifyEvent),
}

// Implement NetworkBehaviour trait with modern API
impl NetworkBehaviour for OverlayBehavior {
    type ConnectionHandler = libp2p::swarm::dummy::ConnectionHandler;
    type ToSwarm = OverlayBehaviorEvent;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, ConnectionDenied> {
        // Delegate to sub-behaviors
        // We'll ignore any errors from sub-behaviors and only propagate if all fail
        let mut all_denied = true;
        
        if self.gossipsub.handle_established_inbound_connection(
            connection_id, peer, local_addr, remote_addr).is_ok() {
            all_denied = false;
        }
        
        if self.kademlia.handle_established_inbound_connection(
            connection_id, peer, local_addr, remote_addr).is_ok() {
            all_denied = false;
        }
        
        if self.mdns.handle_established_inbound_connection(
            connection_id, peer, local_addr, remote_addr).is_ok() {
            all_denied = false;
        }
        
        if self.identify.handle_established_inbound_connection(
            connection_id, peer, local_addr, remote_addr).is_ok() {
            all_denied = false;
        }
        
        if all_denied {
            return Err(ConnectionDenied::Denied);
        }
        
        Ok(libp2p::swarm::dummy::ConnectionHandler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
    ) -> Result<libp2p::swarm::THandler<Self>, ConnectionDenied> {
        // Delegate to sub-behaviors
        // Similar to inbound, ignore individual errors unless all fail
        let mut all_denied = true;
        
        if self.gossipsub.handle_established_outbound_connection(
            connection_id, peer, addr, role_override).is_ok() {
            all_denied = false;
        }
        
        if self.kademlia.handle_established_outbound_connection(
            connection_id, peer, addr, role_override).is_ok() {
            all_denied = false;
        }
        
        if self.mdns.handle_established_outbound_connection(
            connection_id, peer, addr, role_override).is_ok() {
            all_denied = false;
        }
        
        if self.identify.handle_established_outbound_connection(
            connection_id, peer, addr, role_override).is_ok() {
            all_denied = false;
        }
        
        if all_denied {
            return Err(ConnectionDenied::Denied);
        }
        
        Ok(libp2p::swarm::dummy::ConnectionHandler)
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        // Delegate swarm events to sub-behaviors
        self.gossipsub.on_swarm_event(event.clone());
        self.kademlia.on_swarm_event(event.clone());
        self.mdns.on_swarm_event(event.clone());
        self.identify.on_swarm_event(event);
    }

    fn on_connection_handler_event(
        &mut self, 
        _peer_id: PeerId,
        _connection_id: ConnectionId,
        _event: THandlerInEvent<Self>,
    ) {
        // No-op since we're using dummy connection handlers
        // In a real implementation, we would dispatch to sub-behaviors
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        // Check if we have any pending events in our queue
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(ToSwarm::GenerateEvent(event));
        }
        
        // Poll Gossipsub
        if let Poll::Ready(ToSwarm::GenerateEvent(event)) = self.gossipsub.poll(cx) {
            self.handle_gossipsub_event(event);
            return Poll::Ready(ToSwarm::GenerateEvent(self.events.pop_front().unwrap()));
        }
        
        // Poll Kademlia
        if let Poll::Ready(ToSwarm::GenerateEvent(event)) = self.kademlia.poll(cx) {
            self.handle_kademlia_event(event);
            return Poll::Ready(ToSwarm::GenerateEvent(self.events.pop_front().unwrap()));
        }
        
        // Poll MDNS
        if let Poll::Ready(ToSwarm::GenerateEvent(event)) = self.mdns.poll(cx) {
            self.handle_mdns_event(event);
            return Poll::Ready(ToSwarm::GenerateEvent(self.events.pop_front().unwrap()));
        }
        
        // Poll Identify
        if let Poll::Ready(ToSwarm::GenerateEvent(event)) = self.identify.poll(cx) {
            self.handle_identify_event(event);
            return Poll::Ready(ToSwarm::GenerateEvent(self.events.pop_front().unwrap()));
        }
        
        // Handle other ToSwarm variants from sub-behaviors (like ListenOn, etc.)
        // This is a simplified implementation - a more complete one would handle all variants
        
        Poll::Pending
    }
}

// From implementations for event conversions
impl From<GossipsubEvent> for OverlayBehaviorEvent {
    fn from(event: GossipsubEvent) -> Self {
        OverlayBehaviorEvent::Gossipsub(event)
    }
}

impl From<MdnsEvent> for OverlayBehaviorEvent {
    fn from(event: MdnsEvent) -> Self {
        OverlayBehaviorEvent::Mdns(event)
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
