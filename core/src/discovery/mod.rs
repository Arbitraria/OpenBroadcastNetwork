//! Peer discovery implementation
//!
//! This module provides mechanisms for discovering peers in the network.

pub mod bootstrap;
pub mod dht;
pub mod mdns;
pub mod interface;

#[cfg(test)]
mod tests;

// Re-export the main interface types
pub use interface::{Discovery, DiscoveryEvent, DiscoveryError, PeerInfo};

// Re-export the discovery implementations
pub use bootstrap::{BootstrapDiscovery, BootstrapDiscoveryConfig};
pub use dht::{DhtDiscovery, DhtDiscoveryConfig};
pub use mdns::{MdnsDiscovery, MdnsDiscoveryConfig};

// Will be exported once implemented
// pub use bootstrap::BootstrapDiscovery;
// pub use dht::DhtDiscovery;
// pub use mdns::MdnsDiscovery; 