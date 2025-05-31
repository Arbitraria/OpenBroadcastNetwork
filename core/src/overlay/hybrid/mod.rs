//! Hybrid tree-mesh overlay implementation
//!
//! This module combines the tree and mesh overlays to provide an efficient
//! and resilient distribution network.

// Module definitions
mod config;
mod types;
mod overlay;

// Re-exports for public API
pub use config::HybridOverlayConfig;
pub use types::{StreamMetadata, StreamQuality};
pub use overlay::HybridOverlay;
