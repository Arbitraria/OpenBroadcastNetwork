use std::fmt::{Display, Formatter};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::pubsub::topic::{Topic, TopicId};
use crate::pubsub::message::{Message, MessageId};
use crate::overlay::peer::PeerId;

/// Error types for pub/sub operations
#[derive(Debug, thiserror::Error)]
pub enum PubSubError {
    #[error("Topic subscription error: {0}")]
    SubscriptionError(String),
    
    #[error("Message publish error: {0}")]
    PublishError(String),
    
    #[error("Network error: {0}")]
    NetworkError(String),
    
    #[error("Invalid topic: {0}")]
    InvalidTopic(String),
    
    #[error("Message validation error: {0}")]
    ValidationError(String),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Events emitted by the PubSub system
#[derive(Debug, Clone)]
pub enum PubSubEvent {
    /// A new message was received on a topic
    MessageReceived {
        topic: TopicId,
        message: Arc<Message>,
        source: Option<PeerId>,
    },
    
    /// A message was successfully published to a topic
    MessagePublished {
        topic: TopicId,
        message_id: MessageId,
    },
    
    /// A new peer has joined a topic
    PeerJoined {
        topic: TopicId,
        peer: PeerId,
    },
    
    /// A peer has left a topic
    PeerLeft {
        topic: TopicId,
        peer: PeerId,
    },
    
    /// Topic subscription changed
    SubscriptionChanged {
        topic: TopicId,
        subscribed: bool,
    },
    
    /// Metrics event with statistics
    MetricsUpdate {
        messages_received: usize,
        messages_sent: usize,
        unique_peers: usize,
    },
}

/// Configuration for a PubSub service
pub trait PubSubConfig: Send + Sync {
    /// Maximum message size in bytes
    fn max_message_size(&self) -> usize;
    
    /// Heartbeat interval in milliseconds
    fn heartbeat_interval(&self) -> u64;
    
    /// Message validation timeout in milliseconds
    fn validation_timeout(&self) -> u64;
    
    /// Maximum number of topic subscriptions
    fn max_subscriptions(&self) -> usize;
}

/// Core publish-subscribe interface
pub trait PubSub: Send + Sync {
    /// Subscribe to a topic
    fn subscribe(&mut self, topic: &Topic) -> Result<(), PubSubError>;
    
    /// Unsubscribe from a topic
    fn unsubscribe(&mut self, topic_id: &TopicId) -> Result<(), PubSubError>;
    
    /// Publish a message to a topic
    fn publish(&mut self, topic_id: &TopicId, data: Vec<u8>) -> Result<MessageId, PubSubError>;
    
    /// Publish a pre-created message to a topic
    fn publish_message(&mut self, topic_id: &TopicId, message: Message) -> Result<MessageId, PubSubError>;
    
    /// Get the list of peers subscribed to a topic
    fn list_peers(&self, topic_id: &TopicId) -> Vec<PeerId>;
    
    /// Get the list of topics this node is subscribed to
    fn list_subscriptions(&self) -> Vec<TopicId>;
    
    /// Start the PubSub service
    fn start(&mut self) -> Result<(), PubSubError>;
    
    /// Stop the PubSub service
    fn stop(&mut self) -> Result<(), PubSubError>;
}

/// Asynchronous pubsub interface
pub trait AsyncPubSub: Send + Sync {
    /// Subscribe to a topic asynchronously
    fn subscribe<'a>(&'a mut self, topic: &'a Topic) 
        -> Pin<Box<dyn Future<Output = Result<(), PubSubError>> + Send + 'a>>;
    
    /// Unsubscribe from a topic asynchronously
    fn unsubscribe<'a>(&'a mut self, topic_id: &'a TopicId)
        -> Pin<Box<dyn Future<Output = Result<(), PubSubError>> + Send + 'a>>;
    
    /// Publish a message to a topic asynchronously
    fn publish<'a>(&'a mut self, topic_id: &'a TopicId, data: Vec<u8>)
        -> Pin<Box<dyn Future<Output = Result<MessageId, PubSubError>> + Send + 'a>>;
    
    /// Get event stream for pubsub events
    fn event_stream<'a>(&'a mut self) 
        -> Pin<Box<dyn Future<Output = Result<PubSubEventStream, PubSubError>> + Send + 'a>>;
}

/// Event stream for receiving PubSub events
pub struct PubSubEventStream {
    // Implementation details will be added later
}

impl PubSubEventStream {
    /// Get the next event
    pub async fn next(&mut self) -> Option<PubSubEvent> {
        None // Placeholder implementation
    }
} 