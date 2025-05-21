// Publish-Subscribe module for the decentralized streaming system
//
// This module implements the pub/sub mechanism for stream control and metadata.

pub mod interface;
pub mod message;
pub mod topic;
pub mod gossip;
pub mod control;
pub mod serialization;

#[cfg(test)]
mod tests;

// Re-export the main types
pub use interface::{PubSub, PubSubEvent, PubSubError, Publisher, Subscriber};
pub use message::{Message, MessageId, MessageKind};
pub use topic::{Topic, TopicId, TopicFilter};
pub use control::{ControlMessage, ControlCommand, ControlResponse}; 