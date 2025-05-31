//! libp2p implementation of the overlay network
//!
//! This module implements the Overlay trait using libp2p.

// Module definitions
pub mod behavior;
pub mod topics;
pub mod utils;

// Re-exports
pub use crate::overlay::libp2p_impl::Libp2pOverlay;
