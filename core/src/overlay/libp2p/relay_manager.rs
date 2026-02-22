//! Relay node/subscriber logic for libp2p overlay

/// Re-export relay functionality from existing modules
#[cfg(feature = "libp2p")]
pub use crate::overlay::relay::*;

/// Stub implementation when libp2p feature is disabled
#[cfg(not(feature = "libp2p"))]
pub mod stub {
    use crate::overlay::interface::OverlayError;

    pub fn create_relay_manager() -> Result<(), OverlayError> {
        Err(OverlayError::ProtocolError(
            "libp2p feature not enabled".to_string(),
        ))
    }
}
