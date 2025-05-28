//! Decentralized Streaming Core Library
//!
//! This crate provides the core functionality for a decentralized streaming CDN.
//! It implements a hybrid tree-mesh overlay network for peer-to-peer streaming
//! with support for WebRTC, QUIC, and other transport protocols.

/// Network transport layer implementations for peer connections
/// 
/// Provides abstractions for different transport protocols like WebRTC and QUIC,
/// with a unified interface for establishing connections and transmitting data.
pub mod transport;
/// Peer discovery mechanisms for finding and connecting to other nodes
/// 
/// Implements various discovery protocols like mDNS, DHT, and bootstrap servers
/// to locate peers in the decentralized network.
pub mod discovery;
/// Overlay network topologies for efficient data distribution
/// 
/// Implements tree and mesh-based overlay networks optimized for streaming media
/// with support for adaptive connections based on network conditions.
pub mod overlay;
/// Cryptographic utilities for secure communications
/// 
/// Provides functions for encryption, signing, and verification to ensure
/// data integrity and confidentiality across the network.
pub mod crypto;
/// Media processing pipeline for handling streaming content
/// 
/// Implements a modular pipeline for processing, encoding, and transmitting
/// media data with support for various codecs and formats.
pub mod media;
/// Publish-subscribe system for distributing messages across the network
/// 
/// Provides a topic-based pub/sub implementation using Gossipsub protocol
/// for efficient message propagation with validation and flood protection.
pub mod pubsub;
/// Configuration structures for all system components
/// 
/// Defines configuration options for transport, discovery, overlay networks,
/// and other components with sensible defaults and validation.
pub mod config;
/// Telemetry and metrics collection for system monitoring
/// 
/// Provides tools for collecting, aggregating, and reporting metrics
/// on system performance, network health, and resource usage.
pub mod telemetry;

/// Re-exports of commonly used types
pub mod prelude {
    // Pub/Sub module re-exports
    pub use crate::pubsub::{
        PubSub, PubSubEvent, PubSubError, PubSubStats,
        Topic, TopicId, StreamTopic,
        Message, MessageId, MessageType, MessagePayload,
        GossipSubService, GossipSubConfig,
    };
    
    // Discovery module re-exports
    pub use crate::discovery::{
        Discovery, DiscoveryEvent, DiscoveryError, PeerInfo,
        MdnsDiscovery, MdnsDiscoveryConfig,
        DhtDiscovery, DhtDiscoveryConfig,
        BootstrapDiscovery, BootstrapDiscoveryConfig,
    };
    
    // Overlay module re-exports
    pub use crate::overlay::{
        Overlay, OverlayEvent, OverlayError,
        Peer, PeerId, PeerInfo as OverlayPeerInfo, PeerRole,
    };
    
    // Standard library re-exports for convenience
    pub use std::sync::Arc;
    pub use futures::StreamExt;
    pub use futures::SinkExt;
    pub use libp2p;
}

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
