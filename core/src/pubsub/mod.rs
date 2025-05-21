// Publish-Subscribe module for decentralized streaming
//
// This module implements topic-based publish-subscribe functionality
// using libp2p GossipSub for efficient message propagation.

pub mod interface;
pub mod gossipsub;
pub mod topic;
pub mod message;
pub mod validation;
pub mod metrics;

#[cfg(test)]
mod tests;

// Re-export main types
pub use interface::{PubSub, PubSubEvent, PubSubError};
pub use gossipsub::{GossipSubConfig, GossipSubService};
pub use topic::{Topic, TopicId, StreamTopic};
pub use message::{Message, MessageId, MessageType, MessagePayload};
pub use validation::{MessageValidator, ValidationResult};
pub use metrics::{PubSubMetrics, PubSubStats}; 