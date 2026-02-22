//! Topology management for the overlay network
//!
//! This module handles the organization of peers into an efficient
//! tree-mesh hybrid structure for stream relay.

// Export our sub-modules
mod config;
mod geo;
mod health;
mod manager;

// Re-export public components
pub use config::{RelayScoreWeights, TopologyConfig};
pub use geo::{GeoLocation, Region};
pub use health::ConnectionHealth;
pub use manager::TopologyManager;
