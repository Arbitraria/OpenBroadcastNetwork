//! Core interfaces for the pub/sub system
//!
//! This module defines the fundamental interfaces for the publish-subscribe mechanism.

use crate::pubsub::message::{Message, MessageId};
use crate::pubsub::topic::{Topic, TopicId, TopicFilter};
use std::fmt;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// PubSub errors that can occur
#[derive(Debug, thiserror::Error)]
pub enum PubSubError {
    /// Topic validation error
    #[error("Invalid topic: {0}")]
    InvalidTopic(String),
    
    /// Message validation error
    #[error("Invalid message: {0}")]
    InvalidMessage(String),
    
    /// Network error
    #[error("Network error: {0}")]
    NetworkError(String),
    
    /// Subscription error
    #[error("Subscription error: {0}")]
    SubscriptionError(String),
    
    /// Timeout error
    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),
    
    /// System is stopping
    #[error("System is stopping")]
    Stopping,
    
    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    
    /// Other error
    #[error("Error: {0}")]
    Other(String),
}

/// PubSub events emitted by the system
#[derive(Debug, Clone)]
pub enum PubSubEvent {
    /// A new message has been received on a topic
    MessageReceived {
        /// Topic the message was received on
        topic: Topic,
        /// The message
        message: Message,
    },
    
    /// A subscription has been established
    Subscribed {
        /// Topic subscribed to
        topic: Topic,
    },
    
    /// A subscription has been removed
    Unsubscribed {
        /// Topic unsubscribed from
        topic: Topic,
    },
    
    /// A peer has connected
    PeerConnected {
        /// Peer ID
        peer_id: String,
    },
    
    /// A peer has disconnected
    PeerDisconnected {
        /// Peer ID
        peer_id: String,
    },
    
    /// System state change
    StateChange {
        /// New state
        state: String,
    },
}

/// Message validation result
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationResult {
    /// Message is valid
    Accept,
    /// Message is invalid and should be rejected
    Reject,
    /// Message should be ignored
    Ignore,
}

/// Options for publishing messages
#[derive(Debug, Clone)]
pub struct PublishOptions {
    /// Time-to-live for the message in seconds (0 means no TTL)
    pub ttl: u32,
    /// Whether to retain the message on the topic
    pub retain: bool,
    /// Quality of service level
    pub qos: QosLevel,
    /// Message priority (0-255, higher is more important)
    pub priority: u8,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            ttl: 0,                 // No TTL by default
            retain: false,          // Don't retain by default
            qos: QosLevel::AtMostOnce, // Default QoS
            priority: 128,          // Default priority (middle)
        }
    }
}

/// Options for subscribing to topics
#[derive(Debug, Clone)]
pub struct SubscribeOptions {
    /// Whether to receive messages published before the subscription
    pub include_existing: bool,
    /// Quality of service level
    pub qos: QosLevel,
    /// Subscription priority (for multiple subscribers)
    pub priority: u8,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            include_existing: false,     // Don't include existing messages by default
            qos: QosLevel::AtMostOnce,   // Default QoS
            priority: 128,               // Default priority (middle)
        }
    }
}

/// Quality of Service levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QosLevel {
    /// At most once delivery (fire and forget)
    AtMostOnce,
    /// At least once delivery (acknowledged)
    AtLeastOnce,
    /// Exactly once delivery (acknowledged with deduplication)
    ExactlyOnce,
}

/// Statistics for a publisher
#[derive(Debug, Clone, Default)]
pub struct PublisherStats {
    /// Number of messages published
    pub messages_published: u64,
    /// Number of bytes published
    pub bytes_published: u64,
    /// Number of active topics
    pub active_topics: usize,
    /// Number of errors
    pub errors: u64,
}

/// Statistics for a subscriber
#[derive(Debug, Clone, Default)]
pub struct SubscriberStats {
    /// Number of messages received
    pub messages_received: u64,
    /// Number of bytes received
    pub bytes_received: u64,
    /// Number of subscriptions
    pub subscriptions: usize,
    /// Number of message processing errors
    pub errors: u64,
}

/// A unique subscription ID
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub Vec<u8>);

/// Publisher interface for publishing messages to topics
pub trait Publisher {
    /// Publish a message to a topic
    fn publish(&self, topic: &Topic, data: Vec<u8>, options: PublishOptions) 
        -> Pin<Box<dyn Future<Output = Result<MessageId, PubSubError>> + Send>>;
    
    /// Check if a topic exists
    fn topic_exists(&self, topic: &Topic) 
        -> Pin<Box<dyn Future<Output = Result<bool, PubSubError>> + Send>>;
    
    /// Get publisher statistics
    fn stats(&self) -> PublisherStats;
}

/// Subscriber interface for subscribing to topics
pub trait Subscriber {
    /// Subscribe to a topic
    fn subscribe(&self, topic: &Topic, options: SubscribeOptions) 
        -> Pin<Box<dyn Future<Output = Result<SubscriptionId, PubSubError>> + Send>>;
    
    /// Unsubscribe from a topic
    fn unsubscribe(&self, subscription_id: &SubscriptionId) 
        -> Pin<Box<dyn Future<Output = Result<(), PubSubError>> + Send>>;
    
    /// Get the next message from subscribed topics
    fn next_message(&self) 
        -> Pin<Box<dyn Future<Output = Option<(Topic, Message)>> + Send>>;
    
    /// Get subscriber statistics
    fn stats(&self) -> SubscriberStats;
}

/// Combined PubSub interface
pub trait PubSub: Publisher + Subscriber {
    /// Start the pubsub system
    fn start(&self) -> Pin<Box<dyn Future<Output = Result<(), PubSubError>> + Send>>;
    
    /// Stop the pubsub system
    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), PubSubError>> + Send>>;
    
    /// Check if the system is running
    fn is_running(&self) -> bool;
    
    /// Get the next event from the system
    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<PubSubEvent>> + Send>>;
    
    /// List all known topics
    fn list_topics(&self) -> Pin<Box<dyn Future<Output = Result<Vec<Topic>, PubSubError>> + Send>>;
    
    /// Get topics matching a filter
    fn get_topics(&self, filter: &TopicFilter) 
        -> Pin<Box<dyn Future<Output = Result<Vec<Topic>, PubSubError>> + Send>>;
} 