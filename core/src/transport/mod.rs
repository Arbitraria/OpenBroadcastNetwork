//! Transport layer implementation
//!
//! This module provides abstractions over different network transport protocols
//! including WebRTC and QUIC.

pub mod webrtc;
pub mod quic;
pub mod interface;

#[cfg(test)]
mod tests;

// Re-export the main interface types
pub use interface::{Transport, TransportEvent, TransportError, Connection, ConnectionId};

// Re-export concrete implementations
// pub use webrtc::WebRtcTransport;
// pub use quic::QuicTransport; 