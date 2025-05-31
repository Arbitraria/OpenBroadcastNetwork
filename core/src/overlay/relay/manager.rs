//! Relay manager implementation
//!
//! This module provides a high-level manager for coordinating stream data relay
//! between peers in the overlay network. It works in conjunction with the topology
//! manager to ensure efficient data distribution.
//!
//! # Responsibilities
//!
//! The `RelayManager` has several key responsibilities:
//!
//! 1. **Stream Creation** - Setting up new data streams for publishers
//! 2. **Data Relay** - Forwarding stream chunks to appropriate recipients
//! 3. **Buffer Management** - Handling temporary storage of stream chunks
//! 4. **Flow Control** - Managing bandwidth utilization and backpressure
//! 5. **Stream Lifecycle** - Handling stream creation and termination
//!
//! # Architecture
//!
//! The `RelayManager` works by:
//!
//! - Managing a collection of relay nodes (typically one per peer)
//! - Maintaining stream routing tables based on topology information
//! - Buffering stream chunks for reliable delivery
//! - Coordinating with the topology manager for peer connections
//!
//! # Dependencies
//!
//! This component depends on:
//! - `overlay::topology::TopologyManager` - For peer connection and stream tree information
//! - `overlay::interface` - For stream IDs and error types
//! - `overlay::relay::config` - For relay configuration parameters
//! - `overlay::relay::node` - For the actual relay node implementation
//!
//! # Integration Points
//!
//! The `RelayManager` is primarily used by:
//! - `Libp2pOverlay` in `overlay/libp2p_impl.rs` - For stream creation and management
//! - Used indirectly by network event handlers to process stream data
//!
//! # Thread Safety
//!
//! This component is designed to be thread-safe using:
//! - `Arc<RelayManager>` for shared ownership
//! - Delegation to thread-safe `RelayNode` instances
//! - Async-aware methods for all public operations

use libp2p::PeerId;
use crate::overlay::interface::OverlayError;
use crate::overlay::topology::TopologyManager;
use std::sync::Arc;

use super::config::RelayConfig;
use super::node::RelayNode;

/// A manager for handling multiple relay nodes
pub struct RelayManager {
    /// Local peer ID
    local_peer_id: PeerId,
    /// Configuration
    config: RelayConfig,
    /// Topology manager for overlay network management
    topology: Arc<TopologyManager>,
    /// Local relay node
    relay_node: Arc<RelayNode>,
}

impl RelayManager {
    /// Create a new relay manager
    pub fn new(
        local_peer_id: PeerId,
        config: RelayConfig,
        topology: Arc<TopologyManager>,
    ) -> Self {
        let relay_node = Arc::new(RelayNode::new(
            local_peer_id.clone(),
            config.clone(),
            topology.clone(),
        ));
        
        Self {
            local_peer_id,
            config,
            topology,
            relay_node,
        }
    }
    
    /// Get the relay node
    pub fn relay_node(&self) -> Arc<RelayNode> {
        self.relay_node.clone()
    }
    
    /// Start the relay manager
    pub async fn start(&self) -> Result<(), OverlayError> {
        // Start the relay node
        self.relay_node.start().await?;
        
        Ok(())
    }
    
    /// Stop the relay manager
    pub async fn stop(&self) -> Result<(), OverlayError> {
        // Stop the relay node
        self.relay_node.stop().await?;
        
        Ok(())
    }
}
