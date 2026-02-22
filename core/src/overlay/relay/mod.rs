//! Stream relay implementation
//!
//! This module handles the relaying of stream data between peers.
//! It provides mechanisms for efficient data distribution in the overlay network.

// Export our sub-modules
mod config;
mod manager;
mod node;
mod stats;
mod stream;
mod types;

// Re-export public components
pub use config::RelayConfig;
pub use manager::RelayManager;
pub use node::RelayNode;
pub use stats::RelayStats;
pub use stream::StreamRelay;
pub use types::StreamChunk;
