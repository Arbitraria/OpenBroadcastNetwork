#![allow(non_snake_case)]

//! Decentralized Streaming Core Library
//!
//! This crate provides the core functionality for a decentralized streaming CDN.
//! It implements a hybrid tree-mesh overlay network for peer-to-peer streaming
//! with support for WebRTC, QUIC, and other transport protocols.

/// Configuration structures for all system components
///
/// Defines configuration options for transport, discovery, overlay networks,
/// and other components with sensible defaults and validation.
pub mod config;
/// Cryptographic utilities for secure communications
///
/// Provides functions for encryption, signing, and verification to ensure
/// data integrity and confidentiality across the network.
pub mod crypto;
/// Peer discovery mechanisms for finding and connecting to other nodes
///
/// Implements various discovery protocols like Kademlia, DHT, and bootstrap servers
/// to locate peers in the decentralized network.
pub mod discovery;
/// Media processing pipeline for handling streaming content
///
/// Implements a modular pipeline for processing, encoding, and transmitting
/// media data with support for various codecs and formats.
pub mod media;
/// Overlay network topologies for efficient data distribution
///
/// Implements tree and mesh-based overlay networks optimized for streaming media
/// with support for adaptive connections based on network conditions.
pub mod overlay;
/// Publish-subscribe system for distributing messages across the network
///
/// Provides a topic-based pub/sub implementation using Gossipsub protocol
/// for efficient message propagation with validation and flood protection.
pub mod pubsub;
/// Telemetry and metrics collection for system monitoring
///
/// Provides tools for collecting, aggregating, and reporting metrics
/// on system performance, network health, and resource usage.
pub mod telemetry;
/// Network transport layer implementations for peer connections
///
/// Provides abstractions for different transport protocols like WebRTC and QUIC,
/// with a unified interface for establishing connections and transmitting data.
pub mod transport;

/// Test reporting utilities for collecting and analyzing test results
///
/// Provides structures and utilities for generating comprehensive test reports
/// with timing data, status tracking, and serialization support.
pub mod test_report;

/// Re-exports of commonly used types
pub mod prelude {
    // Pub/Sub module re-exports
    pub use crate::pubsub::{
        GossipSubConfig, GossipSubService, Message, MessageId, MessagePayload, MessageType, PubSub,
        PubSubError, PubSubEvent, PubSubStats, StreamTopic, Topic, TopicId,
    };

    // Discovery module re-exports
    pub use crate::discovery::{
        BootstrapDiscovery, BootstrapDiscoveryConfig, DhtDiscovery, DhtDiscoveryConfig, Discovery,
        DiscoveryError, DiscoveryEvent, PeerInfo,
    };

    // Overlay module re-exports
    pub use crate::overlay::{
        Overlay, OverlayError, OverlayEvent, Peer, PeerId, PeerInfo as OverlayPeerInfo, PeerRole,
    };

    // Standard library re-exports for convenience
    pub use futures::SinkExt;
    pub use futures::StreamExt;
    pub use libp2p;
    pub use std::sync::Arc;
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
