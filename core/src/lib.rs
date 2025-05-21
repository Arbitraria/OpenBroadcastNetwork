//! Decentralized Streaming Core Library
//!
//! This crate provides the core functionality for a decentralized streaming CDN.
//! It implements a hybrid tree-mesh overlay network for peer-to-peer streaming
//! with support for WebRTC, QUIC, and other transport protocols.

pub mod transport;
pub mod discovery;
pub mod overlay;
pub mod crypto;
pub mod media;
pub mod pubsub;
pub mod config;
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
