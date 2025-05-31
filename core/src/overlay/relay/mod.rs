//! Stream relay implementation
//!
//! This module handles the relaying of stream data between peers.
//! It provides mechanisms for efficient data distribution in the overlay network.

// Export our sub-modules
mod types;
mod config;
mod stats;
mod stream;
mod node;
mod manager;

// Re-export public components
pub use types::StreamChunk;
pub use config::RelayConfig;
pub use stats::RelayStats;
pub use stream::StreamRelay;
pub use node::RelayNode;
pub use manager::RelayManager;
