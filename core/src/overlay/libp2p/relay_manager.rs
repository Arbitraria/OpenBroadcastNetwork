//! Relay node/subscriber logic for libp2p overlay

/// Re-export relay functionality from existing modules
pub use crate::overlay::relay::*;

/// Create a relay manager
pub fn create_relay_manager() -> Result<(), crate::overlay::interface::OverlayError> {
    Ok(())
}
