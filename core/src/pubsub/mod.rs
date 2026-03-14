//! Publish-Subscribe module for decentralized streaming
//!
//! This module implements topic-based publish-subscribe functionality
//! using libp2p GossipSub for efficient message propagation.

/// GossipSub protocol configuration and service
pub mod gossipsub;
/// Core pub/sub trait definitions
pub mod interface;
/// Message types and serialization
pub mod message;
/// Pub/sub performance metrics
pub mod metrics;
/// Topic management and stream topic helpers
pub mod topic;
/// Message validation pipeline
pub mod validation;

#[cfg(test)]
mod tests;

// Re-export main types
pub use gossipsub::{GossipSubConfig, GossipSubService};
pub use interface::{PubSub, PubSubError, PubSubEvent};
pub use message::{Message, MessageId, MessagePayload, MessageType};
pub use metrics::{PubSubMetrics, PubSubStats};
pub use topic::{StreamTopic, Topic, TopicId};
pub use validation::{MessageValidator, ValidationResult};
