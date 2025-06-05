//! Topology management for the overlay network
//!
//! This module handles the organization of peers into an efficient
//! tree-mesh hybrid structure for stream relay.

// Export our sub-modules
mod geo;
mod config;
mod health;
mod manager;

// Re-export public components
pub use geo::{Region, GeoLocation};
pub use config::{TopologyConfig, RelayScoreWeights};
pub use health::ConnectionHealth;
pub use manager::TopologyManager;
