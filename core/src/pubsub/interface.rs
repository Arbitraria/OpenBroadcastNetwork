use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::pubsub::topic::{Topic, TopicId};
use crate::pubsub::message::{Message, MessageId};
use crate::overlay::peer::LocalPeerId;

/// Error types for pub/sub operations
#[derive(Debug, thiserror::Error)]
pub enum PubSubError {
    #[error("Topic subscription error: {0}")]
    /// Error that occurs when subscribing to a topic fails
    SubscriptionError(String),
    
    #[error("Message publish error: {0}")]
    /// Error that occurs when publishing a message fails
    PublishError(String),
    
    #[error("Network error: {0}")]
    /// Error related to underlying network issues
    NetworkError(String),
    
    #[error("Invalid topic: {0}")]
    /// Error that occurs when a topic identifier is invalid
    InvalidTopic(String),
    
    #[error("Message validation error: {0}")]
    /// Error that occurs when message validation fails
    ValidationError(String),
    
    #[error("Peer ID error: {0}")]
    /// Error related to invalid peer identifiers
    PeerIdError(String),
    
    #[error("IO error: {0}")]
    /// Error from the underlying I/O operations
    IoError(#[from] std::io::Error),
}

/// Events emitted by the PubSub system
#[derive(Debug, Clone)]
pub enum PubSubEvent {
    /// A new message was received on a topic
    MessageReceived {
        /// The topic the message was received on
        topic: TopicId,
        /// The received message content
        message: Arc<Message>,
        /// The peer that sent this message, if known
        source: Option<LocalPeerId>,
    },
    
    /// A message was successfully published to a topic
    MessagePublished {
        /// The topic the message was published to
        topic: TopicId,
        /// The unique identifier of the published message
        message_id: MessageId,
    },
    
    /// A new peer has joined a topic
    PeerJoined {
        /// The topic the peer joined
        topic: TopicId,
        /// The identifier of the peer that joined
        peer_id: LocalPeerId,
    },
    
    /// A peer has left a topic
    PeerLeft {
        /// The topic the peer left
        topic: TopicId,
        /// The identifier of the peer that left
        peer_id: LocalPeerId,
    },
    
    /// Topic subscription changed for the local node
    SubscriptionChanged {
        /// The topic whose subscription status changed
        topic: TopicId,
        /// Whether the local node is now subscribed (true) or unsubscribed (false)
        subscribed: bool,
    },
    
    /// Periodic update of pub/sub system metrics
    MetricsUpdate {
        /// The total number of messages received since system start
        messages_received: usize,
        /// The total number of messages sent since system start
        messages_sent: usize,
        /// The number of unique peers currently connected
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

/// Core publish-subscribe interface for topic-based message distribution
/// 
/// This trait defines the synchronous interface for interacting with the publish-subscribe
/// system. Implementations handle message routing, peer connections, and data validation.
/// For asynchronous operations, see the `AsyncPubSub` trait.
pub trait PubSub: Send + Sync {
    /// Subscribe to a topic
    /// 
    /// Subscribes the local node to messages published on the specified topic.
    /// Once subscribed, the node will receive messages published to this topic.
    /// 
    /// # Arguments
    /// * `topic` - The topic to subscribe to
    /// 
    /// # Returns
    /// * `Ok(())` if subscription was successful
    /// * `Err(PubSubError)` if subscription failed
    fn subscribe(&mut self, topic: &Topic) -> Result<(), PubSubError>;
    
    /// Unsubscribe from a topic
    /// 
    /// Stops receiving messages from the specified topic. After unsubscribing,
    /// the node will no longer receive messages published to this topic.
    /// 
    /// # Arguments
    /// * `topic_id` - The ID of the topic to unsubscribe from
    /// 
    /// # Returns
    /// * `Ok(())` if unsubscription was successful
    /// * `Err(PubSubError)` if unsubscription failed
    fn unsubscribe(&mut self, topic_id: &TopicId) -> Result<(), PubSubError>;
    
    /// Publish a message to a topic
    /// 
    /// Creates a message from the provided data and publishes it to the specified topic.
    /// The message will be propagated to all nodes subscribed to the topic.
    /// 
    /// # Arguments
    /// * `topic_id` - The ID of the topic to publish to
    /// * `data` - The raw message data to publish
    /// 
    /// # Returns
    /// * `Ok(MessageId)` with the ID of the published message if successful
    /// * `Err(PubSubError)` if publishing failed
    fn publish(&mut self, topic_id: &TopicId, data: Vec<u8>) -> Result<MessageId, PubSubError>;
    
    /// Publish a pre-created message to a topic
    fn publish_message(&mut self, topic_id: &TopicId, message: Message) -> Result<MessageId, PubSubError>;
    
    /// Get the list of peers subscribed to a topic
    fn list_peers(&self, topic_id: &TopicId) -> Vec<LocalPeerId>;
    
    /// Get the list of topics this node is subscribed to
    fn list_subscriptions(&self) -> Vec<TopicId>;
    
    /// Start the PubSub service
    fn start(&mut self) -> Result<(), PubSubError>;
    
    /// Stop the PubSub service
    fn stop(&mut self) -> Result<(), PubSubError>;
}

/// Asynchronous publish-subscribe interface for non-blocking operations
/// 
/// This trait provides an asynchronous API for interacting with the publish-subscribe
/// system, allowing for non-blocking operations in an async runtime environment.
/// It mirrors the functionality of the synchronous `PubSub` trait but with Future-based
/// return values for integration with async/await patterns.
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

use tokio::sync::mpsc;

/// Event stream for receiving PubSub events
pub struct PubSubEventStream {
    receiver: mpsc::Receiver<PubSubEvent>,
}

impl PubSubEventStream {
    /// Create a new PubSubEventStream
    pub fn new(receiver: mpsc::Receiver<PubSubEvent>) -> Self {
        Self { receiver }
    }

    /// Get the next event
    pub async fn next(&mut self) -> Option<PubSubEvent> {
        self.receiver.recv().await
    }
} 