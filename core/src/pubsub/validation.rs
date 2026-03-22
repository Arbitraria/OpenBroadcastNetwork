use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
#[cfg(target_arch = "wasm32")]
use instant::Instant;

use crate::media::wire_format::WireSegment;
use crate::pubsub::message::{Message, SerializablePeerId};
use crate::pubsub::topic::TopicId;

// On native, PeerId is libp2p::PeerId. On wasm, use String as a stand-in.
#[cfg(not(target_arch = "wasm32"))]
use libp2p::PeerId;
#[cfg(target_arch = "wasm32")]
type PeerId = String;

/// Result of message validation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationResult {
    /// Message is valid and should be processed and forwarded
    Accept,

    /// Message is invalid and should be rejected
    Reject,

    /// Message validation is pending (async validation in progress)
    Pending,

    /// Message should be ignored but not rejected (e.g., duplicate)
    Ignore,
}

/// Interface for validating messages
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait MessageValidator: Send + Sync {
    /// Synchronously validate a message (fast path)
    ///
    /// This method should return quickly. If validation requires
    /// asynchronous operations, return Pending and perform
    /// the validation in the async_validate method.
    fn validate(&self, message: &Message, source: Option<&PeerId>) -> ValidationResult;

    /// Asynchronously validate a message (called after validate returns Pending)
    async fn async_validate(
        &self,
        message: Arc<Message>,
        source: Option<PeerId>,
    ) -> ValidationResult;
}

/// Basic message validator with size and rate limiting
#[derive(Debug, Clone)]
pub struct BasicValidator {
    /// Maximum allowed message size in bytes
    max_message_size: usize,

    /// Maximum messages per second per peer
    max_messages_per_second: usize,

    /// Topic-specific validation rules
    topic_rules: std::collections::HashMap<TopicId, TopicValidationRules>,
}

/// Topic-specific validation rules
#[derive(Debug, Clone)]
pub struct TopicValidationRules {
    /// Maximum message size for this topic (overrides global)
    pub max_message_size: Option<usize>,

    /// Allowed publishers (empty means anyone can publish)
    pub allowed_publishers: Vec<PeerId>,

    /// Is message validation required for this topic
    pub validation_required: bool,
}

impl Default for BasicValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl BasicValidator {
    /// Create a new validator with default settings
    pub fn new() -> Self {
        Self {
            max_message_size: 1024 * 1024, // 1MB
            max_messages_per_second: 100,
            topic_rules: std::collections::HashMap::new(),
        }
    }

    /// Set the maximum message size
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Set the maximum messages per second per peer
    pub fn with_max_messages_per_second(mut self, count: usize) -> Self {
        self.max_messages_per_second = count;
        self
    }

    /// Add validation rules for a topic
    pub fn with_topic_rules(mut self, topic_id: TopicId, rules: TopicValidationRules) -> Self {
        self.topic_rules.insert(topic_id, rules);
        self
    }

    /// Get the maximum message size for a topic
    fn get_max_message_size(&self, topic_id: &TopicId) -> usize {
        if let Some(rules) = self.topic_rules.get(topic_id) {
            if let Some(size) = rules.max_message_size {
                return size;
            }
        }
        self.max_message_size
    }

    /// Check if a publisher is allowed for a topic
    fn is_publisher_allowed(&self, topic_id: &TopicId, publisher: &SerializablePeerId) -> bool {
        if let Some(rules) = self.topic_rules.get(topic_id) {
            if !rules.allowed_publishers.is_empty() {
                // For now, just compare publisher IDs as strings
                // In a real implementation, we would convert and use proper PeerId comparison
                let publisher_str = &publisher.0;
                return rules
                    .allowed_publishers
                    .iter()
                    .any(|p| p.to_string() == *publisher_str);
            }
        }
        true
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl MessageValidator for BasicValidator {
    fn validate(&self, message: &Message, _source: Option<&PeerId>) -> ValidationResult {
        // Check message size
        let max_size = self.get_max_message_size(&message.topic);
        if message.size() > max_size {
            return ValidationResult::Reject;
        }

        // Check if the message has expired
        if message.is_expired() {
            return ValidationResult::Ignore;
        }

        // Check publisher is allowed if specified
        if let Some(publisher) = &message.publisher {
            if !self.is_publisher_allowed(&message.topic, publisher) {
                return ValidationResult::Reject;
            }
        }

        // For now, we'll accept all messages by default
        // Rate limiting would be implemented with a time window counter
        ValidationResult::Accept
    }

    async fn async_validate(
        &self,
        _message: Arc<Message>,
        _source: Option<PeerId>,
    ) -> ValidationResult {
        // The basic validator performs all checks synchronously
        // This is a placeholder for more complex async validation
        ValidationResult::Accept
    }
}

/// Validator for stream data messages.
///
/// Validates wire format deserialization, enforces per-source rate limits,
/// and detects sequence gaps/duplicates.
pub struct StreamDataValidator {
    /// Maximum messages per second per source peer
    max_messages_per_second: usize,
    /// Per-source rate tracking: (message count, window start)
    rate_tracker: std::sync::Mutex<HashMap<String, (usize, Instant)>>,
    /// Per-stream last-seen sequence number for gap detection
    sequence_tracker: std::sync::Mutex<HashMap<String, u64>>,
}

impl StreamDataValidator {
    /// Create a new stream data validator
    pub fn new(max_messages_per_second: usize) -> Self {
        Self {
            max_messages_per_second,
            rate_tracker: std::sync::Mutex::new(HashMap::new()),
            sequence_tracker: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Check if a topic looks like a stream data topic
    fn is_stream_data_topic(topic: &TopicId) -> bool {
        topic.0.starts_with("stream/") && topic.0.ends_with("/data")
    }

    /// Check rate limit for a source peer. Returns true if allowed.
    fn check_rate_limit(&self, source_id: &str) -> bool {
        let mut tracker = self.rate_tracker.lock().unwrap();
        let now = Instant::now();

        let entry = tracker
            .entry(source_id.to_string())
            .or_insert((0, now));

        // Reset window if more than 1 second has passed
        if now.duration_since(entry.1).as_secs() >= 1 {
            *entry = (1, now);
            return true;
        }

        entry.0 += 1;
        entry.0 <= self.max_messages_per_second
    }

    /// Track sequence number, warn on gaps/duplicates.
    /// Returns Ignore for duplicates, Accept otherwise (gaps are logged).
    fn check_sequence(
        &self,
        stream_key: &str,
        sequence: u64,
    ) -> ValidationResult {
        let mut tracker = self.sequence_tracker.lock().unwrap();
        if let Some(last_seq) = tracker.get(stream_key) {
            if sequence == *last_seq {
                return ValidationResult::Ignore; // duplicate
            }
            if sequence < *last_seq {
                warn!(
                    "Out-of-order segment: got seq {} after {} for {}",
                    sequence, last_seq, stream_key
                );
            } else if sequence > last_seq + 1 {
                warn!(
                    "Sequence gap: expected {} got {} for {}",
                    last_seq + 1,
                    sequence,
                    stream_key
                );
            }
        }
        tracker.insert(stream_key.to_string(), sequence);
        ValidationResult::Accept
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl MessageValidator for StreamDataValidator {
    fn validate(
        &self,
        message: &Message,
        source: Option<&PeerId>,
    ) -> ValidationResult {
        // Only validate stream data topics
        if !Self::is_stream_data_topic(&message.topic) {
            return ValidationResult::Accept;
        }

        // Rate limit check
        let source_id = source
            .map(|p| p.to_string())
            .or_else(|| message.publisher.as_ref().map(|p| p.0.clone()))
            .unwrap_or_default();

        if !source_id.is_empty() && !self.check_rate_limit(&source_id) {
            warn!("Rate limit exceeded for source {}", source_id);
            return ValidationResult::Reject;
        }

        // Try to deserialize as WireSegment to validate structure
        let payload_bytes = message.payload.as_bytes();
        match WireSegment::from_bytes(&payload_bytes) {
            Ok(wire) => {
                // Check version compatibility
                if !wire.is_compatible() {
                    warn!(
                        "Incompatible wire format version: {}",
                        wire.version
                    );
                    return ValidationResult::Reject;
                }

                // Validate media type is known
                if wire.media_type_enum().is_none() {
                    warn!("Unknown media type: {}", wire.media_type);
                    return ValidationResult::Reject;
                }

                // Check sequence continuity
                let stream_key = format!(
                    "{}:{}",
                    hex::encode(&wire.stream_id),
                    wire.track_id
                );
                self.check_sequence(&stream_key, wire.sequence)
            }
            Err(e) => {
                warn!(
                    "Invalid WireSegment on stream data topic: {}",
                    e
                );
                ValidationResult::Reject
            }
        }
    }

    async fn async_validate(
        &self,
        _message: Arc<Message>,
        _source: Option<PeerId>,
    ) -> ValidationResult {
        ValidationResult::Accept
    }
}

/// Validator that delegates to multiple other validators
#[derive(Default)]
pub struct CompositeValidator {
    validators: Vec<Box<dyn MessageValidator>>,
}

impl CompositeValidator {
    /// Create a new empty composite validator
    pub fn new() -> Self {
        Self {
            validators: Vec::new(),
        }
    }

    /// Add a validator to the chain
    pub fn add_validator<V: MessageValidator + 'static>(&mut self, validator: V) {
        self.validators.push(Box::new(validator));
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl MessageValidator for CompositeValidator {
    fn validate(&self, message: &Message, source: Option<&PeerId>) -> ValidationResult {
        let mut pending = false;

        for validator in &self.validators {
            match validator.validate(message, source) {
                ValidationResult::Reject => return ValidationResult::Reject,
                ValidationResult::Pending => pending = true,
                ValidationResult::Ignore => return ValidationResult::Ignore,
                ValidationResult::Accept => continue,
            }
        }

        if pending {
            ValidationResult::Pending
        } else {
            ValidationResult::Accept
        }
    }

    async fn async_validate(
        &self,
        message: Arc<Message>,
        source: Option<PeerId>,
    ) -> ValidationResult {
        for validator in &self.validators {
            match validator.validate(&message, source.as_ref()) {
                ValidationResult::Reject => return ValidationResult::Reject,
                ValidationResult::Ignore => return ValidationResult::Ignore,
                _ => {
                    // Continue with async validation for this validator
                    let result = validator
                        .async_validate(message.clone(), source.clone())
                        .await;
                    match result {
                        ValidationResult::Reject => return ValidationResult::Reject,
                        ValidationResult::Ignore => return ValidationResult::Ignore,
                        _ => continue,
                    }
                }
            }
        }

        ValidationResult::Accept
    }
}
