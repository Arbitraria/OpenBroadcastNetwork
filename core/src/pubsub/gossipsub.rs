use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

// Tokio imports
use tokio::sync::mpsc::{self, Sender};

// External libraries
use async_trait::async_trait;
use libp2p::identity::Keypair;

// Import basic types from gossipsub module
use libp2p::gossipsub::IdentTopic as LibP2PTopic;
use libp2p::gossipsub::MessageAuthenticity;
use libp2p::gossipsub::MessageId as LibP2PMessageId;

// In libp2p 0.53.0, the Gossipsub behaviour and its components are in libp2p_gossipsub crate
// which is re-exported as libp2p::gossipsub
use libp2p::gossipsub::Behaviour as Gossipsub;
// Import only the components we actually use
use libp2p::gossipsub::ConfigBuilder;
use libp2p::gossipsub::Message as GossipsubMessage;

// Internal imports
use crate::overlay::peer::LocalPeerId;
use crate::pubsub::interface::{AsyncPubSub, PubSub, PubSubConfig, PubSubError, PubSubEvent};
use crate::pubsub::message::{Message, MessageId};
use crate::pubsub::metrics::PubSubMetrics;
use crate::pubsub::topic::{Topic, TopicId};
use crate::pubsub::validation::{BasicValidator, MessageValidator, ValidationResult};

/// Configuration for GossipSub
#[derive(Debug, Clone)]
pub struct GossipSubConfig {
    /// Maximum message size in bytes
    pub max_message_size: usize,

    /// Heartbeat interval in milliseconds
    pub heartbeat_interval: u64,

    /// Message validation timeout in milliseconds
    pub validation_timeout: u64,

    /// Maximum number of topic subscriptions
    pub max_subscriptions: usize,

    /// Number of mesh peers to maintain per topic
    pub mesh_n: usize,

    /// Higher threshold for mesh peers
    pub mesh_n_high: usize,

    /// Lower threshold for mesh peers
    pub mesh_n_low: usize,

    /// Fan-out time-to-live in seconds
    pub fanout_ttl: u64,

    /// Gossip factor (percentage of non-mesh peers to gossip to)
    pub gossip_factor: f64,
}

impl Default for GossipSubConfig {
    fn default() -> Self {
        Self {
            max_message_size: 1024 * 1024, // 1MB
            heartbeat_interval: 1000,      // 1 second
            validation_timeout: 5000,      // 5 seconds
            max_subscriptions: 50,         // Maximum 50 topics
            mesh_n: 6,                     // Maintain ~6 peers per topic
            mesh_n_high: 12,               // Maximum 12 peers per topic
            mesh_n_low: 4,                 // Minimum 4 peers per topic
            fanout_ttl: 60,                // 60 seconds
            gossip_factor: 0.25,           // Gossip to 25% of peers
        }
    }
}

impl PubSubConfig for GossipSubConfig {
    fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    fn heartbeat_interval(&self) -> u64 {
        self.heartbeat_interval
    }

    fn validation_timeout(&self) -> u64 {
        self.validation_timeout
    }

    fn max_subscriptions(&self) -> usize {
        self.max_subscriptions
    }
}

use std::fmt;

/// GossipSub service implementation
#[allow(dead_code)]
pub struct GossipSubService {
    /// Configuration
    config: GossipSubConfig,

    /// Subscribed topics
    topics: RwLock<HashMap<TopicId, Topic>>,

    /// Message validator
    validator: Arc<dyn MessageValidator>,

    /// Metrics collector
    metrics: Arc<PubSubMetrics>,

    /// Event sender
    event_sender: Mutex<Option<Sender<PubSubEvent>>>,

    /// Node keypair for message signing
    keypair: Keypair,

    /// Peers connected to this node
    peers: RwLock<HashSet<LocalPeerId>>,

    /// Is the service started
    started: std::sync::atomic::AtomicBool,
}

impl fmt::Debug for GossipSubService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GossipSubService")
            .field("config", &self.config)
            .field("topics", &self.topics)
            .field("metrics", &self.metrics)
            .field("keypair", &"[redacted]")
            .field("peers", &self.peers)
            .finish()
    }
}

impl GossipSubService {
    /// Create a new GossipSub service
    pub fn new(keypair: Keypair) -> Self {
        Self::with_config(keypair, GossipSubConfig::default())
    }

    /// Create a new GossipSub service with custom configuration
    pub fn with_config(keypair: Keypair, config: GossipSubConfig) -> Self {
        Self {
            config,
            topics: RwLock::new(HashMap::new()),
            validator: Arc::new(BasicValidator::new()),
            metrics: Arc::new(PubSubMetrics::new()),
            event_sender: Mutex::new(None),
            keypair,
            peers: RwLock::new(HashSet::new()),
            started: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Set the message validator
    pub fn with_validator<V: MessageValidator + 'static>(mut self, validator: V) -> Self {
        self.validator = Arc::new(validator);
        self
    }

    /// Create a new gossipsub protocol with current configuration
    #[allow(dead_code)]
    fn create_gossipsub(&self) -> Gossipsub {
        // Use the ConfigBuilder pattern to configure Gossipsub
        let gossipsub_config = ConfigBuilder::default()
            .mesh_n(self.config.mesh_n)
            .mesh_n_low(self.config.mesh_n_low)
            .mesh_n_high(self.config.mesh_n_high)
            .gossip_factor(self.config.gossip_factor)
            .fanout_ttl(Duration::from_secs(self.config.fanout_ttl))
            .heartbeat_interval(Duration::from_millis(self.config.heartbeat_interval))
            .max_transmit_size(self.config.max_message_size)
            .build()
            .expect("Valid gossipsub config");

        // Create the gossipsub instance
        Gossipsub::new(
            MessageAuthenticity::Signed(self.keypair.clone()),
            gossipsub_config,
        )
        .expect("Failed to create gossipsub")
    }

    /// Convert our Topic to libp2p Topic
    #[allow(dead_code)]
    fn to_libp2p_topic(topic_id: &TopicId) -> LibP2PTopic {
        LibP2PTopic::new(topic_id.0.clone())
    }

    /// Convert libp2p PeerId to SerializablePeerId for Message compatibility
    #[allow(dead_code)]
    fn from_libp2p_peer_id(peer_id: &libp2p::PeerId) -> crate::pubsub::message::SerializablePeerId {
        // Convert to string representation for serialization
        crate::pubsub::message::SerializablePeerId(peer_id.to_string())
    }

    /// Convert our LocalPeerId to libp2p PeerId
    #[allow(dead_code)]
    fn to_libp2p_peer_id(peer_id: &LocalPeerId) -> Result<libp2p::PeerId, PubSubError> {
        // Use the TryFrom trait implementation
        libp2p::PeerId::try_from(peer_id).map_err(|e| PubSubError::PeerIdError(e.to_string()))
    }

    /// Convert Message to GossipsubMessage
    #[allow(dead_code)]
    fn to_gossipsub_message(
        &self,
        message: &Message,
    ) -> Result<(Vec<u8>, Option<LibP2PMessageId>), PubSubError> {
        // Serialize the message
        let payload = match serde_json::to_vec(message) {
            Ok(data) => data,
            Err(e) => {
                return Err(PubSubError::PublishError(format!(
                    "Failed to serialize message: {}",
                    e
                )))
            }
        };

        if payload.len() > self.config.max_message_size {
            return Err(PubSubError::PublishError(format!(
                "Message size ({} bytes) exceeds maximum ({} bytes)",
                payload.len(),
                self.config.max_message_size
            )));
        }

        // Create a message ID
        let message_id = LibP2PMessageId::from(message.id.0.as_bytes().to_vec());

        Ok((payload, Some(message_id)))
    }

    /// Convert GossipsubMessage to our Message
    #[allow(dead_code)]
    fn from_gossipsub_message(
        &self,
        topic_id: TopicId,
        gossipsub_msg: &GossipsubMessage,
        source: Option<libp2p::PeerId>,
    ) -> Result<Message, PubSubError> {
        // Try to deserialize the message
        match serde_json::from_slice::<Message>(&gossipsub_msg.data) {
            Ok(mut message) => {
                // Update the source if available
                if let Some(peer_id) = source {
                    message.publisher = Some(Self::from_libp2p_peer_id(&peer_id));
                }

                Ok(message)
            }
            Err(_) => {
                // If we can't deserialize as our Message type, create a simple binary message
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis() as u64;

                // In libp2p 0.53.0, the message_id field is no longer directly accessible
                // Use a timestamp-based ID as a fallback
                let id = MessageId(format!("msg-{}-{}", topic_id, timestamp));

                let mut message = Message::binary(topic_id, gossipsub_msg.data.clone());
                message.id = id;

                if let Some(peer_id) = source {
                    message.publisher = Some(Self::from_libp2p_peer_id(&peer_id));
                }

                Ok(message)
            }
        }
    }
}

impl PubSub for GossipSubService {
    fn subscribe(&mut self, topic: &Topic) -> Result<(), PubSubError> {
        // Check if we're already subscribed
        let mut topics = self
            .topics
            .write()
            .map_err(|_| PubSubError::SubscriptionError("Failed to acquire lock".to_string()))?;

        if topics.contains_key(&topic.id) {
            return Ok(());
        }

        // Check if we've hit the subscription limit
        if topics.len() >= self.config.max_subscriptions {
            return Err(PubSubError::SubscriptionError(format!(
                "Maximum subscriptions ({}) reached",
                self.config.max_subscriptions
            )));
        }

        // Store the topic
        topics.insert(topic.id.clone(), topic.clone());

        // Update subscription metrics
        self.metrics.update_subscription_count(topics.len());

        Ok(())
    }

    fn unsubscribe(&mut self, topic_id: &TopicId) -> Result<(), PubSubError> {
        let mut topics = self
            .topics
            .write()
            .map_err(|_| PubSubError::SubscriptionError("Failed to acquire lock".to_string()))?;

        if topics.remove(topic_id).is_some() {
            // Update subscription metrics
            self.metrics.update_subscription_count(topics.len());
        }

        Ok(())
    }

    fn publish(&mut self, topic_id: &TopicId, data: Vec<u8>) -> Result<MessageId, PubSubError> {
        // Create a message
        let message = Message::binary(topic_id.clone(), data);

        self.publish_message(topic_id, message)
    }

    fn publish_message(
        &mut self,
        _topic_id: &TopicId,
        message: Message,
    ) -> Result<MessageId, PubSubError> {
        // Validate the message locally
        match self.validator.validate(&message, None) {
            ValidationResult::Accept => {
                // The message is valid, continue
            }
            ValidationResult::Reject => {
                return Err(PubSubError::ValidationError(
                    "Message rejected by local validator".to_string(),
                ));
            }
            ValidationResult::Ignore => {
                return Err(PubSubError::ValidationError(
                    "Message ignored by local validator".to_string(),
                ));
            }
            ValidationResult::Pending => {
                // Should we block and wait for async validation?
                // For simplicity, we'll just accept it for now
            }
        }

        // Return the message ID
        Ok(message.id.clone())
    }

    fn list_peers(&self, _topic_id: &TopicId) -> Vec<LocalPeerId> {
        match self.peers.read() {
            Ok(peers) => peers.iter().cloned().collect(),
            Err(_) => Vec::new(),
        }
    }

    fn list_subscriptions(&self) -> Vec<TopicId> {
        match self.topics.read() {
            Ok(topics) => topics.keys().cloned().collect(),
            Err(_) => Vec::new(),
        }
    }

    fn start(&mut self) -> Result<(), PubSubError> {
        // Check if already started
        if self.started.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }

        // Create event channel
        let (tx, _) = mpsc::channel(100);

        // Store sender
        let mut event_sender = self
            .event_sender
            .lock()
            .map_err(|_| PubSubError::NetworkError("Failed to acquire lock".to_string()))?;

        *event_sender = Some(tx);

        // Mark as started
        self.started
            .store(true, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    fn stop(&mut self) -> Result<(), PubSubError> {
        if !self.started.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }

        // Clear event sender
        let mut event_sender = self
            .event_sender
            .lock()
            .map_err(|_| PubSubError::NetworkError("Failed to acquire lock".to_string()))?;

        *event_sender = None;

        // Mark as stopped
        self.started
            .store(false, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }
}

#[async_trait]
impl AsyncPubSub for GossipSubService {
    fn subscribe<'a>(
        &'a mut self,
        topic: &'a Topic,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), PubSubError>> + Send + 'a>>
    {
        Box::pin(async move { PubSub::subscribe(self, topic) })
    }

    fn unsubscribe<'a>(
        &'a mut self,
        topic_id: &'a TopicId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), PubSubError>> + Send + 'a>>
    {
        Box::pin(async move { PubSub::unsubscribe(self, topic_id) })
    }

    fn publish<'a>(
        &'a mut self,
        topic_id: &'a TopicId,
        data: Vec<u8>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<MessageId, PubSubError>> + Send + 'a>,
    > {
        Box::pin(async move { PubSub::publish(self, topic_id, data) })
    }

    fn event_stream<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<PubSubEventStream, PubSubError>> + Send + 'a>,
    > {
        Box::pin(async move {
            // Create a new channel
            let (tx, rx) = mpsc::channel(100);

            // Store in the service
            let mut event_sender = self
                .event_sender
                .lock()
                .map_err(|_| PubSubError::NetworkError("Failed to acquire lock".to_string()))?;

            *event_sender = Some(tx);

            // Create and return the event stream
            Ok(PubSubEventStream::new(rx))
        })
    }
}

// Import PubSubEventStream from interface module for clarity
use crate::pubsub::interface::PubSubEventStream;

// Extension methods for PubSubEventStream, if needed in the future
// Currently handled directly in the interface.rs file
