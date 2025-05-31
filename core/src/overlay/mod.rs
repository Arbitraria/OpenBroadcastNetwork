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
/// Peer management, roles, and connection handling
pub mod peer;
/// Tree-based overlay implementation for efficient distribution
pub mod tree;
/// Mesh-based overlay implementation for resilient connections
pub mod mesh;
// Import discovery module from crate root
// The discovery module is defined at the crate level, not in the overlay
/// Performance metrics collection and analysis for overlay optimization
pub mod metrics;
/// Relay functionality for forwarding streams between peers
pub mod relay;
/// libp2p implementation of the overlay network
pub mod libp2p_impl;
/// Implementation of the overlay network using libp2p (modular version)
pub mod libp2p;
/// Hybrid tree-mesh overlay implementation (modular version)
pub mod hybrid;
/// Legacy relay exports for backward compatibility
pub mod relay_redirector;
/// Network topology management and organization (refactored into a modular structure)
pub mod topology;
/// Module for testing dynamic overlay features
#[cfg(test)]
pub mod dynamic_tests;

#[cfg(test)]
mod tests;

// Re-export main types
pub use interface::{Overlay, OverlayEvent, OverlayError};
// Re-exporting topology types
pub use topology::{TopologyManager, TopologyConfig};
pub use peer::{Peer, PeerInfo, PeerRole, PeerConnection, LocalPeerId};
// Re-export PeerId from libp2p
pub use ::libp2p::PeerId;
// Re-export our overlay implementation
pub use crate::overlay::libp2p_impl::Libp2pOverlay;
pub use relay::{RelayNode, RelayManager, RelayStats};
// Re-export hybrid overlay types
pub use hybrid::{HybridOverlay, HybridOverlayConfig, StreamMetadata, StreamQuality};