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

/// WebRTC-based transport implementation for browser compatibility
pub mod webrtc;
/// QUIC protocol transport for high-performance connections
pub mod quic;
/// Core interfaces and types for transport implementations
pub mod interface;

#[cfg(test)]
mod tests;

// Re-export the main interface types
pub use interface::{Transport, TransportEvent, TransportError, Connection, ConnectionId};

// Re-export concrete implementations
// pub use webrtc::WebRtcTransport;
// pub use quic::QuicTransport; 