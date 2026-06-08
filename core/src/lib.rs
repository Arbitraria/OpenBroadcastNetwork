#![allow(non_snake_case)]

//! Decentralized Streaming Core Library
//!
//! This crate provides the core functionality for a decentralized streaming CDN.
//! It implements a hybrid tree-mesh overlay network for peer-to-peer streaming
//! with support for WebRTC, QUIC, and other transport protocols.

/// Configuration structures for all system components
pub mod config;
/// Cryptographic utilities for secure communications
pub mod crypto;
/// Media processing pipeline for handling streaming content
pub mod media;
/// Overlay network topologies for efficient data distribution
pub mod overlay;
/// Publish-subscribe system for distributing messages across the network
pub mod pubsub;
/// Telemetry and metrics collection for system monitoring
pub mod telemetry;

/// Peer discovery mechanisms (native only — requires libp2p + tokio)
#[cfg(not(target_arch = "wasm32"))]
pub mod discovery;
/// Per-node moderation: peer blocking, stream flagging (native only)
#[cfg(not(target_arch = "wasm32"))]
pub mod moderation;
/// Network transport layer implementations (native only)
#[cfg(not(target_arch = "wasm32"))]
pub mod transport;

/// Re-exports of commonly used types
pub mod prelude {
    // Pub/Sub module re-exports (platform-independent types)
    pub use crate::pubsub::{
        Message, MessageId, MessagePayload, MessageType, PubSub, PubSubError, PubSubEvent,
        PubSubStats, StreamTopic, Topic, TopicId,
    };
    // Native-only pubsub re-exports
    #[cfg(not(target_arch = "wasm32"))]
    pub use crate::pubsub::{GossipSubConfig, GossipSubService};

    // Discovery module re-exports (native only)
    #[cfg(not(target_arch = "wasm32"))]
    pub use crate::discovery::{
        BootstrapDiscovery, BootstrapDiscoveryConfig, DhtDiscovery, DhtDiscoveryConfig, Discovery,
        DiscoveryError, DiscoveryEvent, PeerInfo,
    };

    // Overlay module re-exports
    #[cfg(not(target_arch = "wasm32"))]
    pub use crate::overlay::PeerId;
    pub use crate::overlay::{
        Overlay, OverlayError, OverlayEvent, Peer, PeerInfo as OverlayPeerInfo, PeerRole,
    };

    // Standard library re-exports for convenience
    pub use futures::SinkExt;
    pub use futures::StreamExt;
    #[cfg(not(target_arch = "wasm32"))]
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
