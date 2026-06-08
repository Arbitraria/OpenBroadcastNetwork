//! libp2p implementation of the overlay network
//!
//! This module implements the Overlay trait using libp2p.

// Module definitions
pub mod behavior;
pub mod event_handlers;
pub mod impl_core;
pub mod overlay_trait;
pub mod overlay_utils;
pub mod swarm;
pub mod topics;
pub mod types;
pub mod utils;

// Re-exports
pub use impl_core::Libp2pOverlay;
pub use types::{from_libp2p_peer_id, to_libp2p_peer_id};
