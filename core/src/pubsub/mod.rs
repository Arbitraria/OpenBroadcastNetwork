// Publish-Subscribe module for decentralized streaming
//
// This module implements topic-based publish-subscribe functionality
// using libp2p GossipSub for efficient message propagation.

pub mod gossipsub;
pub mod interface;
pub mod message;
pub mod metrics;
pub mod topic;
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
