//! Hybrid tree-mesh overlay implementation
//!
//! This module combines the tree and mesh overlays to provide an efficient
//! and resilient distribution network.

// Module definitions
mod config;
mod overlay;
mod types;

// Re-exports for public API
pub use config::HybridOverlayConfig;
pub use overlay::HybridOverlay;
pub use types::{StreamMetadata, StreamQuality};
