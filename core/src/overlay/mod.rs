// Overlay network module for decentralized streaming
//
// This module handles the peer-to-peer overlay network topology,
// implementing a tree-mesh hybrid for efficient stream relay.

pub mod interface;
pub mod topology;
pub mod relay;
pub mod metrics;
pub mod peer;
pub mod mesh;

#[cfg(test)]
mod tests;

// Re-export main types
pub use interface::{Overlay, OverlayEvent, OverlayError};
pub use topology::{TopologyManager, TopologyConfig, RelayTree};
pub use peer::{Peer, PeerId, PeerInfo, PeerRole, PeerConnection};
pub use relay::{RelayNode, RelayManager, RelayStats}; 