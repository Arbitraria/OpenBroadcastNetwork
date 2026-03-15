//! Mesh network management for libp2p overlay

/// Re-export mesh functionality from existing modules
pub use crate::overlay::mesh::*;

/// Create a mesh manager
pub fn create_mesh_manager() -> Result<(), crate::overlay::interface::OverlayError> {
    Ok(())
}
