//! Gossip-based publish/subscribe implementation
//!
//! This module provides a gossip-protocol based implementation of the pub/sub system.

use crate::pubsub::interface::{
    PubSub, Publisher, Subscriber, PubSubEvent, PubSubError, PublishOptions,
    SubscribeOptions, SubscriptionId, PublisherStats, SubscriberStats, ValidationResult, QosLevel
};
use crate::pubsub::message::{Message, MessageId, MessageKind};
use crate::pubsub::topic::{Topic, TopicId, TopicFilter};
use crate::pubsub::serialization::{MessageSerializer, SerializationFormat};

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::time;
use rand::Rng;
use tracing::{debug, error, info, warn};

/// Configuration for the gossip-based pub/sub
#[derive(Debug, Clone)]
pub struct GossipPubSubConfig {
    /// Local node ID
    pub node_id: String,
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Message cache time (how long to keep message IDs for deduplication)
    pub message_cache_time: Duration,
    /// Message validation timeout
    pub validation_timeout: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Gossip interval (how often to propagate messages)
    pub gossip_interval: Duration,
    /// Maximum number of messages to keep in history per topic
    pub history_size: usize,
    /// Maximum number of peers to gossip to per round
    pub gossip_factor: usize,
    /// Whether to use explicit peer exchange
    pub explicit_peer_exchange: bool,
    /// Serialization format to use
    pub serialization_format: SerializationFormat,
}

impl Default for GossipPubSubConfig {
    fn default() -> Self {
        let mut rng = rand::thread_rng();
        let node_id = format!("node-{}", rng.gen::<u64>());
        
        Self {
            node_id,
            max_message_size: 1024 * 1024, // 1 MB
            message_cache_time: Duration::from_secs(300), // 5 minutes
            validation_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(10),
            gossip_interval: Duration::from_secs(1),
            history_size: 100,
            gossip_factor: 3, // Gossip to 3 peers per round
            explicit_peer_exchange: true,
            serialization_format: SerializationFormat::Json,
        }
    }
}

/// A peer in the gossip network
#[derive(Debug, Clone)]
struct Peer {
    /// Peer ID
    id: String,
    /// Last seen time
    last_seen: Instant,
    /// Topic subscriptions
    subscriptions: HashSet<TopicId>,
}

/// A received message with metadata
#[derive(Debug, Clone)]
struct ReceivedMessage {
    /// The message
    message: Message,
    /// The topic it was received on
    topic: Topic,
    /// When it was received
    received_at: Instant,
}

/// Cache entry for message deduplication
#[derive(Debug)]
struct MessageCacheEntry {
    /// Message ID
    id: MessageId,
    /// First seen time
    first_seen: Instant,
    /// Topics this message was seen on
    topics: HashSet<TopicId>,
}

/// Subscription entry
#[derive(Debug)]
struct Subscription {
    /// Subscription ID
    id: SubscriptionId,
    /// Topic
    topic: Topic,
    /// Options
    options: SubscribeOptions,
    /// When the subscription was created
    created_at: Instant,
}

/// Topic data
#[derive(Debug)]
struct TopicData {
    /// The topic
    topic: Topic,
    /// Subscribers
    subscribers: HashSet<SubscriptionId>,
    /// Message history
    history: VecDeque<Message>,
    /// Whether this node is publishing to this topic
    is_publishing: bool,
}

/// The gossip-based pub/sub implementation
pub struct GossipPubSub {
    /// Configuration
    config: GossipPubSubConfig,
    /// Known peers
    peers: RwLock<HashMap<String, Peer>>,
    /// Topics
    topics: RwLock<HashMap<TopicId, TopicData>>,
    /// Subscriptions
    subscriptions: RwLock<HashMap<SubscriptionId, Subscription>>,
    /// Message cache for deduplication
    message_cache: Mutex<HashMap<MessageId, MessageCacheEntry>>,
    /// Retained messages by topic
    retained_messages: RwLock<HashMap<TopicId, Message>>,
    /// Message validation handlers
    validators: RwLock<HashMap<TopicId, Box<dyn Fn(&Message) -> ValidationResult + Send + Sync>>>,
    /// Publisher statistics
    publisher_stats: Mutex<PublisherStats>,
    /// Subscriber statistics
    subscriber_stats: Mutex<SubscriberStats>,
    /// Channel for publishing messages
    publish_tx: mpsc::Sender<(Topic, Message)>,
    /// Channel for receiving messages
    publish_rx: Mutex<mpsc::Receiver<(Topic, Message)>>,
    /// Channel for events
    event_tx: mpsc::Sender<PubSubEvent>,
    /// Channel for receiving events
    event_rx: Mutex<mpsc::Receiver<PubSubEvent>>,
    /// Message inbox for subscribers
    inbox: Mutex<VecDeque<(Topic, Message)>>,
    /// Is the system running
    running: RwLock<bool>,
    /// Heartbeat task handle
    heartbeat_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Gossip task handle
    gossip_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Cleanup task handle
    cleanup_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl GossipPubSub {
    /// Create a new gossip-based pub/sub
    pub fn new(config: GossipPubSubConfig) -> Self {
        let (publish_tx, publish_rx) = mpsc::channel(100);
        let (event_tx, event_rx) = mpsc::channel(100);
        
        Self {
            config,
            peers: RwLock::new(HashMap::new()),
            topics: RwLock::new(HashMap::new()),
            subscriptions: RwLock::new(HashMap::new()),
            message_cache: Mutex::new(HashMap::new()),
            retained_messages: RwLock::new(HashMap::new()),
            validators: RwLock::new(HashMap::new()),
            publisher_stats: Mutex::new(PublisherStats::default()),
            subscriber_stats: Mutex::new(SubscriberStats::default()),
            publish_tx,
            publish_rx: Mutex::new(publish_rx),
            event_tx,
            event_rx: Mutex::new(event_rx),
            inbox: Mutex::new(VecDeque::new()),
            running: RwLock::new(false),
            heartbeat_task: Mutex::new(None),
            gossip_task: Mutex::new(None),
            cleanup_task: Mutex::new(None),
        }
    }
    
    /// Add a peer to the network
    pub async fn add_peer(&self, peer_id: &str) -> Result<(), PubSubError> {
        let mut peers = self.peers.write().await;
        
        if peer_id == self.config.node_id {
            return Err(PubSubError::Other("Cannot add self as peer".to_string()));
        }
        
        if !peers.contains_key(peer_id) {
            let peer = Peer {
                id: peer_id.to_string(),
                last_seen: Instant::now(),
                subscriptions: HashSet::new(),
            };
            
            peers.insert(peer_id.to_string(), peer);
            
            // Emit event
            let event = PubSubEvent::PeerConnected {
                peer_id: peer_id.to_string(),
            };
            let _ = self.event_tx.send(event).await;
        }
        
        Ok(())
    }
    
    /// Remove a peer from the network
    pub async fn remove_peer(&self, peer_id: &str) -> Result<(), PubSubError> {
        let mut peers = self.peers.write().await;
        
        if peers.remove(peer_id).is_some() {
            // Emit event
            let event = PubSubEvent::PeerDisconnected {
                peer_id: peer_id.to_string(),
            };
            let _ = self.event_tx.send(event).await;
        }
        
        Ok(())
    }
    
    /// Handle a message received from a peer
    pub async fn handle_message(&self, topic: Topic, message: Message, peer_id: &str) -> Result<(), PubSubError> {
        // Update peer last seen time
        {
            let mut peers = self.peers.write().await;
            if let Some(peer) = peers.get_mut(peer_id) {
                peer.last_seen = Instant::now();
            }
        }
        
        // Check if we've already seen this message
        let is_new_message = {
            let mut cache = self.message_cache.lock().await;
            
            if let Some(entry) = cache.get_mut(&message.id) {
                // Already seen this message, but might be on a new topic
                let is_new_topic = entry.topics.insert(topic.id.clone());
                
                // Update the entry's time
                entry.first_seen = Instant::now();
                
                is_new_topic
            } else {
                // New message
                let mut topics = HashSet::new();
                topics.insert(topic.id.clone());
                
                cache.insert(message.id.clone(), MessageCacheEntry {
                    id: message.id.clone(),
                    first_seen: Instant::now(),
                    topics,
                });
                
                true
            }
        };
        
        if !is_new_message {
            // We've already processed this message
            return Ok(());
        }
        
        // Validate the message
        let is_valid = {
            let validators = self.validators.read().await;
            if let Some(validator) = validators.get(&topic.id) {
                match validator(&message) {
                    ValidationResult::Accept => true,
                    ValidationResult::Reject => {
                        warn!("Message rejected by validator: {:?}", message.id);
                        false
                    },
                    ValidationResult::Ignore => {
                        // Just ignore, don't propagate
                        return Ok(());
                    }
                }
            } else {
                // No validator, accept by default
                true
            }
        };
        
        if !is_valid {
            return Err(PubSubError::InvalidMessage("Message rejected by validator".to_string()));
        }
        
        // Store the message in the topic history
        {
            let mut topics = self.topics.write().await;
            
            if let Some(topic_data) = topics.get_mut(&topic.id) {
                // Add to history
                topic_data.history.push_back(message.clone());
                
                // Trim history if needed
                while topic_data.history.len() > self.config.history_size {
                    topic_data.history.pop_front();
                }
            } else {
                // Create new topic
                let mut history = VecDeque::new();
                history.push_back(message.clone());
                
                let topic_data = TopicData {
                    topic: topic.clone(),
                    subscribers: HashSet::new(),
                    history,
                    is_publishing: false,
                };
                
                topics.insert(topic.id.clone(), topic_data);
            }
        }
        
        // If it's a retained message, store it
        if message.retain {
            let mut retained = self.retained_messages.write().await;
            retained.insert(topic.id.clone(), message.clone());
        }
        
        // Deliver to local subscribers
        {
            let topics = self.topics.read().await;
            let subscriptions = self.subscriptions.read().await;
            
            if let Some(topic_data) = topics.get(&topic.id) {
                for sub_id in &topic_data.subscribers {
                    if let Some(subscription) = subscriptions.get(sub_id) {
                        // Add to inbox
                        let mut inbox = self.inbox.lock().await;
                        inbox.push_back((topic.clone(), message.clone()));
                        
                        // Update stats
                        let mut stats = self.subscriber_stats.lock().await;
                        stats.messages_received += 1;
                        stats.bytes_received += message.payload.len() as u64;
                    }
                }
            }
        }
        
        // Emit message received event
        let event = PubSubEvent::MessageReceived {
            topic: topic.clone(),
            message: message.clone(),
        };
        let _ = self.event_tx.send(event).await;
        
        // TODO: Forward to interested peers (gossip)
        
        Ok(())
    }
    
    /// Add a message validator for a topic
    pub async fn add_validator<F>(&self, topic: &Topic, validator: F) -> Result<(), PubSubError>
    where
        F: Fn(&Message) -> ValidationResult + Send + Sync + 'static,
    {
        let mut validators = self.validators.write().await;
        validators.insert(topic.id.clone(), Box::new(validator));
        Ok(())
    }
    
    /// Remove a message validator for a topic
    pub async fn remove_validator(&self, topic: &Topic) -> Result<(), PubSubError> {
        let mut validators = self.validators.write().await;
        validators.remove(&topic.id);
        Ok(())
    }
    
    /// Start the heartbeat task
    async fn start_heartbeat(&self) {
        let mut heartbeat_task = self.heartbeat_task.lock().await;
        
        // Don't start if already running
        if heartbeat_task.is_some() {
            return;
        }
        
        let interval = self.config.heartbeat_interval;
        let node_id = self.config.node_id.clone();
        
        // TODO: Implement heartbeat mechanism to keep track of peer liveness
        
        let task = tokio::spawn(async move {
            let mut interval = time::interval(interval);
            
            loop {
                interval.tick().await;
                
                // Heartbeat logic would go here
                // - Send heartbeats to peers
                // - Check for unresponsive peers
            }
        });
        
        *heartbeat_task = Some(task);
    }
    
    /// Start the gossip task
    async fn start_gossip(&self) {
        let mut gossip_task = self.gossip_task.lock().await;
        
        // Don't start if already running
        if gossip_task.is_some() {
            return;
        }
        
        let interval = self.config.gossip_interval;
        let node_id = self.config.node_id.clone();
        let gossip_factor = self.config.gossip_factor;
        
        // Clone the necessary reference
        let topics_ref = self.topics.clone();
        let peers_ref = self.peers.clone();
        let message_cache_ref = self.message_cache.clone();
        
        let task = tokio::spawn(async move {
            let mut interval = time::interval(interval);
            
            loop {
                interval.tick().await;
                
                // Gossip logic would go here
                // - Select peers to gossip to
                // - Select messages to gossip
                // - Send messages to selected peers
                
                // For now, this is just a placeholder
            }
        });
        
        *gossip_task = Some(task);
    }
    
    /// Start the cleanup task
    async fn start_cleanup(&self) {
        let mut cleanup_task = self.cleanup_task.lock().await;
        
        // Don't start if already running
        if cleanup_task.is_some() {
            return;
        }
        
        let cache_time = self.config.message_cache_time;
        
        // Clone the necessary reference
        let message_cache_ref = self.message_cache.clone();
        
        let task = tokio::spawn(async move {
            let interval = time::interval(Duration::from_secs(60)); // Run cleanup every minute
            let mut interval_stream = interval;
            
            loop {
                interval_stream.tick().await;
                
                // Clean up old message cache entries
                let now = Instant::now();
                let mut cache = message_cache_ref.lock().await;
                
                cache.retain(|_, entry| {
                    now.duration_since(entry.first_seen) < cache_time
                });
            }
        });
        
        *cleanup_task = Some(task);
    }
}

#[async_trait::async_trait]
impl Publisher for GossipPubSub {
    fn publish(&self, topic: &Topic, data: Vec<u8>, options: PublishOptions) 
        -> Pin<Box<dyn Future<Output = Result<MessageId, PubSubError>> + Send>> {
        
        let topic = topic.clone();
        let publish_tx = self.publish_tx.clone();
        let config = self.config.clone();
        let stats = self.publisher_stats.clone();
        
        Box::pin(async move {
            if data.len() > config.max_message_size {
                return Err(PubSubError::InvalidMessage(
                    format!("Message too large: {} bytes (max {})", data.len(), config.max_message_size)
                ));
            }
            
            // Create message
            let message = Message::new_publication(data)
                .with_ttl(options.ttl)
                .with_retain(options.retain)
                .with_source(&config.node_id)
                .with_priority(options.priority);
                
            // Update stats
            {
                let mut stats_guard = stats.lock().await;
                stats_guard.messages_published += 1;
                stats_guard.bytes_published += message.payload.len() as u64;
            }
            
            // Send to publish channel
            if let Err(e) = publish_tx.send((topic, message.clone())).await {
                return Err(PubSubError::Other(format!("Failed to publish: {}", e)));
            }
            
            // Return the message ID
            Ok(message.id)
        })
    }
    
    fn topic_exists(&self, topic: &Topic) 
        -> Pin<Box<dyn Future<Output = Result<bool, PubSubError>> + Send>> {
        
        let topic_id = topic.id.clone();
        let topics = self.topics.clone();
        
        Box::pin(async move {
            let topics_guard = topics.read().await;
            Ok(topics_guard.contains_key(&topic_id))
        })
    }
    
    fn stats(&self) -> PublisherStats {
        futures::executor::block_on(async {
            self.publisher_stats.lock().await.clone()
        })
    }
}

#[async_trait::async_trait]
impl Subscriber for GossipPubSub {
    fn subscribe(&self, topic: &Topic, options: SubscribeOptions) 
        -> Pin<Box<dyn Future<Output = Result<SubscriptionId, PubSubError>> + Send>> {
        
        let topic = topic.clone();
        let topics = self.topics.clone();
        let subscriptions = self.subscriptions.clone();
        let stats = self.subscriber_stats.clone();
        let retained = self.retained_messages.clone();
        let inbox = self.inbox.clone();
        let event_tx = self.event_tx.clone();
        
        Box::pin(async move {
            // Generate a subscription ID
            let subscription_id = SubscriptionId(format!("sub-{}", rand::random::<u64>()).into_bytes());
            
            // Create subscription
            let subscription = Subscription {
                id: subscription_id.clone(),
                topic: topic.clone(),
                options: options.clone(),
                created_at: Instant::now(),
            };
            
            // Update topics
            {
                let mut topics_guard = topics.write().await;
                
                if let Some(topic_data) = topics_guard.get_mut(&topic.id) {
                    // Add to existing topic
                    topic_data.subscribers.insert(subscription_id.clone());
                } else {
                    // Create new topic
                    let topic_data = TopicData {
                        topic: topic.clone(),
                        subscribers: {
                            let mut set = HashSet::new();
                            set.insert(subscription_id.clone());
                            set
                        },
                        history: VecDeque::new(),
                        is_publishing: false,
                    };
                    
                    topics_guard.insert(topic.id.clone(), topic_data);
                }
            }
            
            // Add to subscriptions
            {
                let mut subs_guard = subscriptions.write().await;
                subs_guard.insert(subscription_id.clone(), subscription);
            }
            
            // Update stats
            {
                let mut stats_guard = stats.lock().await;
                stats_guard.subscriptions += 1;
            }
            
            // If include_existing and topic has retained message, deliver it
            if options.include_existing {
                let retained_guard = retained.read().await;
                if let Some(message) = retained_guard.get(&topic.id) {
                    let mut inbox_guard = inbox.lock().await;
                    inbox_guard.push_back((topic.clone(), message.clone()));
                }
            }
            
            // Emit event
            let event = PubSubEvent::Subscribed {
                topic: topic.clone(),
            };
            let _ = event_tx.send(event).await;
            
            Ok(subscription_id)
        })
    }
    
    fn unsubscribe(&self, subscription_id: &SubscriptionId) 
        -> Pin<Box<dyn Future<Output = Result<(), PubSubError>> + Send>> {
        
        let subscription_id = subscription_id.clone();
        let topics = self.topics.clone();
        let subscriptions = self.subscriptions.clone();
        let stats = self.subscriber_stats.clone();
        let event_tx = self.event_tx.clone();
        
        Box::pin(async move {
            // Get the subscription
            let topic = {
                let subs_guard = subscriptions.read().await;
                
                if let Some(sub) = subs_guard.get(&subscription_id) {
                    sub.topic.clone()
                } else {
                    return Err(PubSubError::SubscriptionError(
                        format!("Subscription not found: {:?}", subscription_id)
                    ));
                }
            };
            
            // Remove from topics
            {
                let mut topics_guard = topics.write().await;
                
                if let Some(topic_data) = topics_guard.get_mut(&topic.id) {
                    topic_data.subscribers.remove(&subscription_id);
                    
                    // Remove topic if no subscribers and not publishing
                    if topic_data.subscribers.is_empty() && !topic_data.is_publishing {
                        topics_guard.remove(&topic.id);
                    }
                }
            }
            
            // Remove from subscriptions
            {
                let mut subs_guard = subscriptions.write().await;
                subs_guard.remove(&subscription_id);
            }
            
            // Update stats
            {
                let mut stats_guard = stats.lock().await;
                if stats_guard.subscriptions > 0 {
                    stats_guard.subscriptions -= 1;
                }
            }
            
            // Emit event
            let event = PubSubEvent::Unsubscribed {
                topic,
            };
            let _ = event_tx.send(event).await;
            
            Ok(())
        })
    }
    
    fn next_message(&self) 
        -> Pin<Box<dyn Future<Output = Option<(Topic, Message)>> + Send>> {
        
        let inbox = self.inbox.clone();
        
        Box::pin(async move {
            let mut inbox_guard = inbox.lock().await;
            inbox_guard.pop_front()
        })
    }
    
    fn stats(&self) -> SubscriberStats {
        futures::executor::block_on(async {
            self.subscriber_stats.lock().await.clone()
        })
    }
}

#[async_trait::async_trait]
impl PubSub for GossipPubSub {
    fn start(&self) -> Pin<Box<dyn Future<Output = Result<(), PubSubError>> + Send>> {
        Box::pin(async {
            let mut running = self.running.write().await;
            
            if *running {
                return Err(PubSubError::Other("Already running".to_string()));
            }
            
            *running = true;
            
            // Start background tasks
            self.start_heartbeat().await;
            self.start_gossip().await;
            self.start_cleanup().await;
            
            // Emit state change event
            let event = PubSubEvent::StateChange {
                state: "running".to_string(),
            };
            let _ = self.event_tx.send(event).await;
            
            info!("GossipPubSub started with node ID: {}", self.config.node_id);
            
            Ok(())
        })
    }
    
    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), PubSubError>> + Send>> {
        Box::pin(async {
            let mut running = self.running.write().await;
            
            if !*running {
                return Err(PubSubError::Other("Not running".to_string()));
            }
            
            *running = false;
            
            // Stop background tasks
            {
                let mut heartbeat_task = self.heartbeat_task.lock().await;
                if let Some(task) = heartbeat_task.take() {
                    task.abort();
                }
            }
            
            {
                let mut gossip_task = self.gossip_task.lock().await;
                if let Some(task) = gossip_task.take() {
                    task.abort();
                }
            }
            
            {
                let mut cleanup_task = self.cleanup_task.lock().await;
                if let Some(task) = cleanup_task.take() {
                    task.abort();
                }
            }
            
            // Emit state change event
            let event = PubSubEvent::StateChange {
                state: "stopped".to_string(),
            };
            let _ = self.event_tx.send(event).await;
            
            info!("GossipPubSub stopped");
            
            Ok(())
        })
    }
    
    fn is_running(&self) -> bool {
        futures::executor::block_on(async {
            *self.running.read().await
        })
    }
    
    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<PubSubEvent>> + Send>> {
        let event_rx = self.event_rx.clone();
        
        Box::pin(async move {
            let mut rx = event_rx.lock().await;
            rx.recv().await
        })
    }
    
    fn list_topics(&self) -> Pin<Box<dyn Future<Output = Result<Vec<Topic>, PubSubError>> + Send>> {
        let topics = self.topics.clone();
        
        Box::pin(async move {
            let topics_guard = topics.read().await;
            let topic_list = topics_guard.values()
                .map(|data| data.topic.clone())
                .collect();
                
            Ok(topic_list)
        })
    }
    
    fn get_topics(&self, filter: &TopicFilter) 
        -> Pin<Box<dyn Future<Output = Result<Vec<Topic>, PubSubError>> + Send>> {
        
        let filter = filter.clone();
        let topics = self.topics.clone();
        
        Box::pin(async move {
            let topics_guard = topics.read().await;
            let filtered_topics = topics_guard.values()
                .filter(|data| data.topic.matches(&filter))
                .map(|data| data.topic.clone())
                .collect();
                
            Ok(filtered_topics)
        })
    }
} 