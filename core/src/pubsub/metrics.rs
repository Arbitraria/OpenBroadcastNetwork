use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use crate::pubsub::message::MessageId;
use crate::pubsub::topic::TopicId;
use libp2p::PeerId;

/// Time window for calculating rates
const TIME_WINDOW_SECONDS: u64 = 60;

/// Statistics for the pub/sub system
#[derive(Debug, Clone, Default)]
pub struct PubSubStats {
    /// Total number of messages received
    pub messages_received: usize,

    /// Total number of messages sent
    pub messages_sent: usize,

    /// Total number of rejected messages
    pub messages_rejected: usize,

    /// Total number of duplicated messages
    pub messages_duplicated: usize,

    /// Total number of expired messages
    pub messages_expired: usize,

    /// Total number of valid messages
    pub messages_valid: usize,

    /// Total bytes received
    pub bytes_received: usize,

    /// Total bytes sent
    pub bytes_sent: usize,

    /// Number of active subscriptions
    pub active_subscriptions: usize,

    /// Number of unique peers
    pub unique_peers: usize,

    /// Messages per topic
    pub messages_per_topic: HashMap<String, usize>,

    /// Messages per second (over the last minute)
    pub messages_per_second: f64,

    /// Bytes per second (over the last minute)
    pub bytes_per_second: f64,
}

/// Metrics collector for a pub/sub system
#[derive(Debug)]
pub struct PubSubMetrics {
    /// Total messages received
    messages_received: AtomicUsize,

    /// Total messages sent
    messages_sent: AtomicUsize,

    /// Total messages rejected
    messages_rejected: AtomicUsize,

    /// Total messages duplicated
    messages_duplicated: AtomicUsize,

    /// Total messages expired
    messages_expired: AtomicUsize,

    /// Total valid messages
    messages_valid: AtomicUsize,

    /// Total bytes received
    bytes_received: AtomicUsize,

    /// Total bytes sent
    bytes_sent: AtomicUsize,

    /// Active subscriptions
    active_subscriptions: AtomicUsize,

    /// Per-topic metrics
    topic_metrics: RwLock<HashMap<TopicId, TopicMetrics>>,

    /// Per-peer metrics
    peer_metrics: RwLock<HashMap<PeerId, PeerMetrics>>,

    /// Time-series data for rates
    time_series: RwLock<TimeSeriesData>,

    /// Known unique message IDs (for duplicate detection)
    message_ids: RwLock<lru::LruCache<MessageId, Instant>>,
}

impl Default for PubSubMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubMetrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            messages_received: AtomicUsize::new(0),
            messages_sent: AtomicUsize::new(0),
            messages_rejected: AtomicUsize::new(0),
            messages_duplicated: AtomicUsize::new(0),
            messages_expired: AtomicUsize::new(0),
            messages_valid: AtomicUsize::new(0),
            bytes_received: AtomicUsize::new(0),
            bytes_sent: AtomicUsize::new(0),
            active_subscriptions: AtomicUsize::new(0),
            topic_metrics: RwLock::new(HashMap::new()),
            peer_metrics: RwLock::new(HashMap::new()),
            time_series: RwLock::new(TimeSeriesData::new()),
            message_ids: RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(10_000).unwrap(),
            )),
        }
    }

    /// Record a received message
    pub fn record_message_received(
        &self,
        topic_id: &TopicId,
        peer_id: Option<&PeerId>,
        size: usize,
    ) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(size, Ordering::Relaxed);

        // Update time series
        if let Ok(mut time_series) = self.time_series.write() {
            time_series.add_message_received(size);
        }

        // Update topic metrics
        if let Ok(mut topic_metrics) = self.topic_metrics.write() {
            let metrics = topic_metrics
                .entry(topic_id.clone())
                .or_insert_with(TopicMetrics::new);

            metrics.messages_received.fetch_add(1, Ordering::Relaxed);
            metrics.bytes_received.fetch_add(size, Ordering::Relaxed);
        }

        // Update peer metrics if available
        if let Some(peer_id) = peer_id {
            if let Ok(mut peer_metrics) = self.peer_metrics.write() {
                let metrics = peer_metrics
                    .entry(peer_id.clone())
                    .or_insert_with(PeerMetrics::new);

                metrics.messages_received.fetch_add(1, Ordering::Relaxed);
                metrics.bytes_received.fetch_add(size, Ordering::Relaxed);
            }
        }
    }

    /// Record a sent message
    pub fn record_message_sent(&self, topic_id: &TopicId, peer_id: Option<&PeerId>, size: usize) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(size, Ordering::Relaxed);

        // Update time series
        if let Ok(mut time_series) = self.time_series.write() {
            time_series.add_message_sent(size);
        }

        // Update topic metrics
        if let Ok(mut topic_metrics) = self.topic_metrics.write() {
            let metrics = topic_metrics
                .entry(topic_id.clone())
                .or_insert_with(TopicMetrics::new);

            metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
            metrics.bytes_sent.fetch_add(size, Ordering::Relaxed);
        }

        // Update peer metrics if available
        if let Some(peer_id) = peer_id {
            if let Ok(mut peer_metrics) = self.peer_metrics.write() {
                let metrics = peer_metrics
                    .entry(peer_id.clone())
                    .or_insert_with(PeerMetrics::new);

                metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                metrics.bytes_sent.fetch_add(size, Ordering::Relaxed);
            }
        }
    }

    /// Record a rejected message
    pub fn record_message_rejected(&self, topic_id: &TopicId) {
        self.messages_rejected.fetch_add(1, Ordering::Relaxed);

        // Update topic metrics
        if let Ok(mut topic_metrics) = self.topic_metrics.write() {
            let metrics = topic_metrics
                .entry(topic_id.clone())
                .or_insert_with(TopicMetrics::new);

            metrics.messages_rejected.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a duplicated message
    pub fn record_message_duplicated(&self, topic_id: &TopicId) {
        self.messages_duplicated.fetch_add(1, Ordering::Relaxed);

        // Update topic metrics
        if let Ok(mut topic_metrics) = self.topic_metrics.write() {
            let metrics = topic_metrics
                .entry(topic_id.clone())
                .or_insert_with(TopicMetrics::new);

            metrics.messages_duplicated.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Check if a message is a duplicate
    pub fn is_duplicate(&self, message_id: &MessageId) -> bool {
        if let Ok(mut cache) = self.message_ids.write() {
            if cache.contains(message_id) {
                self.messages_duplicated.fetch_add(1, Ordering::Relaxed);
                return true;
            }

            // Add to cache with current time
            cache.put(message_id.clone(), Instant::now());
            false
        } else {
            // If we can't access the cache, assume it's not a duplicate
            false
        }
    }

    /// Update subscription count
    pub fn update_subscription_count(&self, count: usize) {
        self.active_subscriptions.store(count, Ordering::Relaxed);
    }

    /// Get current statistics
    pub fn get_stats(&self) -> PubSubStats {
        let mut stats = PubSubStats {
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_rejected: self.messages_rejected.load(Ordering::Relaxed),
            messages_duplicated: self.messages_duplicated.load(Ordering::Relaxed),
            messages_expired: self.messages_expired.load(Ordering::Relaxed),
            messages_valid: self.messages_valid.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            active_subscriptions: self.active_subscriptions.load(Ordering::Relaxed),
            unique_peers: 0,
            messages_per_topic: HashMap::new(),
            messages_per_second: 0.0,
            bytes_per_second: 0.0,
        };

        // Calculate messages per topic
        if let Ok(topic_metrics) = self.topic_metrics.read() {
            for (topic_id, metrics) in topic_metrics.iter() {
                stats.messages_per_topic.insert(
                    topic_id.0.clone(),
                    metrics.messages_received.load(Ordering::Relaxed),
                );
            }
        }

        // Count unique peers
        if let Ok(peer_metrics) = self.peer_metrics.read() {
            stats.unique_peers = peer_metrics.len();
        }

        // Calculate rates
        if let Ok(time_series) = self.time_series.read() {
            let (msg_rate, byte_rate) = time_series.calculate_rates();
            stats.messages_per_second = msg_rate;
            stats.bytes_per_second = byte_rate;
        }

        stats
    }

    /// Clean up old cached message IDs
    pub fn cleanup_message_cache(&self, max_age: Duration) {
        if let Ok(mut cache) = self.message_ids.write() {
            let now = Instant::now();

            // Create a vector of message IDs to remove
            let to_remove: Vec<_> = cache
                .iter()
                .filter_map(|(id, timestamp)| {
                    if now.duration_since(*timestamp) >= max_age {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Remove old entries
            for id in to_remove {
                cache.pop(&id);
            }
        }
    }
}

/// Metrics for a specific topic
#[derive(Debug)]
struct TopicMetrics {
    /// Messages received on this topic
    messages_received: AtomicUsize,

    /// Messages sent on this topic
    messages_sent: AtomicUsize,

    /// Messages rejected on this topic
    messages_rejected: AtomicUsize,

    /// Duplicated messages on this topic
    messages_duplicated: AtomicUsize,

    /// Bytes received on this topic
    bytes_received: AtomicUsize,

    /// Bytes sent on this topic
    bytes_sent: AtomicUsize,
}

impl TopicMetrics {
    fn new() -> Self {
        Self {
            messages_received: AtomicUsize::new(0),
            messages_sent: AtomicUsize::new(0),
            messages_rejected: AtomicUsize::new(0),
            messages_duplicated: AtomicUsize::new(0),
            bytes_received: AtomicUsize::new(0),
            bytes_sent: AtomicUsize::new(0),
        }
    }
}

/// Metrics for a specific peer
#[derive(Debug)]
struct PeerMetrics {
    /// Messages received from this peer
    messages_received: AtomicUsize,

    /// Messages sent to this peer
    messages_sent: AtomicUsize,

    /// Bytes received from this peer
    bytes_received: AtomicUsize,

    /// Bytes sent to this peer
    bytes_sent: AtomicUsize,

    /// First seen timestamp
    first_seen: Instant,

    /// Last seen timestamp
    last_seen: RwLock<Instant>,
}

impl PeerMetrics {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            messages_received: AtomicUsize::new(0),
            messages_sent: AtomicUsize::new(0),
            bytes_received: AtomicUsize::new(0),
            bytes_sent: AtomicUsize::new(0),
            first_seen: now,
            last_seen: RwLock::new(now),
        }
    }

    /// Update the last seen timestamp
    fn update_last_seen(&self) {
        if let Ok(mut last_seen) = self.last_seen.write() {
            *last_seen = Instant::now();
        }
    }
}

/// Time-series data for calculating rates
#[derive(Debug)]
struct TimeSeriesData {
    /// Bucket size in seconds
    bucket_size: u64,

    /// Number of buckets to keep
    num_buckets: usize,

    /// Current bucket index
    current_bucket: usize,

    /// Last bucket update time
    last_update: Instant,

    /// Message count per bucket
    message_counts: Vec<usize>,

    /// Byte count per bucket
    byte_counts: Vec<usize>,
}

impl TimeSeriesData {
    fn new() -> Self {
        let bucket_size = 1; // 1 second per bucket
        let num_buckets = TIME_WINDOW_SECONDS as usize;

        Self {
            bucket_size,
            num_buckets,
            current_bucket: 0,
            last_update: Instant::now(),
            message_counts: vec![0; num_buckets],
            byte_counts: vec![0; num_buckets],
        }
    }

    /// Rotate buckets if needed
    fn rotate_if_needed(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs();

        if elapsed >= self.bucket_size {
            let buckets_to_rotate =
                std::cmp::min((elapsed / self.bucket_size) as usize, self.num_buckets);

            for _ in 0..buckets_to_rotate {
                self.current_bucket = (self.current_bucket + 1) % self.num_buckets;
                self.message_counts[self.current_bucket] = 0;
                self.byte_counts[self.current_bucket] = 0;
            }

            self.last_update = now;
        }
    }

    /// Add a received message
    fn add_message_received(&mut self, size: usize) {
        self.rotate_if_needed();
        self.message_counts[self.current_bucket] += 1;
        self.byte_counts[self.current_bucket] += size;
    }

    /// Add a sent message
    fn add_message_sent(&mut self, size: usize) {
        // For now, we track both sent and received in the same buckets
        self.add_message_received(size);
    }

    /// Calculate message and byte rates
    fn calculate_rates(&self) -> (f64, f64) {
        let mut total_messages = 0;
        let mut total_bytes = 0;

        for i in 0..self.num_buckets {
            total_messages += self.message_counts[i];
            total_bytes += self.byte_counts[i];
        }

        let time_window = TIME_WINDOW_SECONDS as f64;
        let msg_rate = total_messages as f64 / time_window;
        let byte_rate = total_bytes as f64 / time_window;

        (msg_rate, byte_rate)
    }
}
