//! Overlay network module for decentralized streaming
//!
//! This module handles the peer-to-peer overlay network topology,
//! implementing a tree-mesh hybrid for efficient stream relay.

/// Core interfaces and types for the overlay network
pub mod interface;
/// Peer management, roles, and connection handling
pub mod peer;

// ── Native-only submodules (require libp2p + tokio) ──

/// Module for testing dynamic overlay features
#[cfg(all(not(target_arch = "wasm32"), test))]
pub mod dynamic_tests;
/// Mesh-based overlay implementation
#[cfg(not(target_arch = "wasm32"))]
pub mod mesh;
/// Network configuration and utilities
#[cfg(not(target_arch = "wasm32"))]
pub mod network;
/// Tree-based overlay implementation
#[cfg(not(target_arch = "wasm32"))]
pub mod tree;
// NOTE: The hybrid overlay module has been archived. The libp2p
// implementation is the active overlay used by the system.
/// Implementation of the overlay network using libp2p
#[cfg(not(target_arch = "wasm32"))]
pub mod libp2p;
/// Performance metrics collection and analysis
#[cfg(not(target_arch = "wasm32"))]
pub mod metrics;
/// Relay functionality for forwarding streams between peers
#[cfg(not(target_arch = "wasm32"))]
pub mod relay;
/// Network topology management and organization
#[cfg(not(target_arch = "wasm32"))]
pub mod topology;

// ── WASM overlay (WebSocket-based relay client) ──

#[cfg(target_arch = "wasm32")]
pub mod wasm;

#[cfg(test)]
mod tests;

// Re-export main types (platform-independent)
pub use interface::{Overlay, OverlayError, OverlayEvent};
pub use peer::{LocalPeerId, Peer, PeerConnection, PeerInfo, PeerRole};

// Native-only re-exports
#[cfg(not(target_arch = "wasm32"))]
pub use crate::overlay::libp2p::impl_core::Libp2pOverlay;
#[cfg(not(target_arch = "wasm32"))]
pub use ::libp2p::PeerId;
#[cfg(not(target_arch = "wasm32"))]
pub use network::NetworkConfig;
#[cfg(not(target_arch = "wasm32"))]
pub use topology::{TopologyConfig, TopologyManager};
/// The default overlay implementation for the current platform.
#[cfg(not(target_arch = "wasm32"))]
pub type DefaultOverlay = Libp2pOverlay;
#[cfg(not(target_arch = "wasm32"))]
pub use relay::{RelayManager, RelayNode, RelayStats};
