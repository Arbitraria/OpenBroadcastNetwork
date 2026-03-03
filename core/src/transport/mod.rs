//! Transport layer implementation
//!
//! This module provides abstractions over different network transport protocols
//! including WebRTC and QUIC. The transport layer is responsible for establishing
//! connections between peers and transmitting data reliably across the network.
//!
//! The transport system is designed with the following principles:
//! - **Protocol Agnostic**: Common interface for different transport protocols
//! - **Connection Oriented**: All communications are based on established connections
//! - **Reliability**: Ensures data is delivered correctly and in order
//! - **Security**: Provides encrypted and authenticated connections
//! - **NAT Traversal**: Support for connecting peers behind NATs and firewalls

/// Core interfaces and types for transport implementations
pub mod interface;
/// WebRTC signaling types for browser-to-browser P2P connections
pub mod webrtc_signaling;
/// WebRTC-based transport implementation for browser compatibility
// pub mod webrtc;  // Disabled - using browser-native WebRTC via signaling
/// QUIC protocol transport for high-performance connections
pub mod quic;

#[cfg(test)]
mod tests;

// Re-export the main interface types
pub use interface::{Connection, ConnectionId, Transport, TransportError, TransportEvent};

// Re-export signaling types for WebRTC P2P
pub use webrtc_signaling::{SignalingMessage, PeerInfo};
// pub use quic::QuicTransport;
