//! Compatibility shim for libp2p overlay implementation
//!
//! This module re-exports the libp2p overlay implementation from the modularized files.
//! It exists for backward compatibility with code that imported Libp2pOverlay from
//! this module. New code should import directly from overlay::libp2p::impl_core.
//!
//! This compatibility shim is part of the modularization effort to organize the code
//! according to the project structure guidelines in the Wind Surf Rules.

// Re-export the Libp2pOverlay from the modularized implementation
pub use crate::overlay::libp2p::impl_core::Libp2pOverlay;
