//! libp2p implementation of the overlay network
//!
//! This module implements the Overlay trait using libp2p.

// Module definitions
pub mod behavior;
pub mod impl_core;
pub mod swarm;
pub mod event_handlers;
pub mod overlay_trait;
pub mod topics;
pub mod types;
pub mod utils;
pub mod overlay_utils;
pub mod peer_manager;
pub mod mesh_manager;
pub mod relay_manager;

// Re-exports
pub use impl_core::Libp2pOverlay;
pub use types::{to_libp2p_peer_id, from_libp2p_peer_id};
