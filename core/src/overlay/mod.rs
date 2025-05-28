//! Overlay network module for decentralized streaming
//!
//! This module handles the peer-to-peer overlay network topology,
//! implementing a tree-mesh hybrid for efficient stream relay. The overlay
//! network is responsible for organizing peers into an optimal structure for
//! distributing streaming content with minimal latency and maximum throughput.
//!
//! The hybrid approach combines:
//! - **Tree-based overlay**: Optimized for efficient one-to-many distribution
//!   with deterministic paths and predictable latency
//! - **Mesh-based overlay**: Provides resilience through redundant connections
//!   and adaptability to changing network conditions
//!
//! The overlay network automatically adapts its structure based on network metrics,
//! peer capabilities, and geographic location to optimize streaming performance.

/// Core interfaces and types for the overlay network
pub mod interface;
/// Network topology management and organization
pub mod topology;
/// Relay functionality for forwarding streams between peers
pub mod relay;
/// Performance metrics collection and analysis for overlay optimization
pub mod metrics;
/// Peer management, roles, and connection handling
pub mod peer;
/// Mesh-based overlay implementation for resilient connections
pub mod mesh;
/// Tree-based overlay implementation for efficient distribution
pub mod tree;

#[cfg(test)]
mod tests;

// Re-export main types
pub use interface::{Overlay, OverlayEvent, OverlayError};
pub use topology::{TopologyManager, TopologyConfig, RelayTree};
pub use peer::{Peer, PeerInfo, PeerRole, PeerConnection, LocalPeerId};
pub use libp2p::PeerId;
pub use relay::{RelayNode, RelayManager, RelayStats}; 