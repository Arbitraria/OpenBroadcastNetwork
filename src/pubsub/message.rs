//! Message types for the pub/sub system
//!
//! This module defines the message types and related structures.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use serde::{Serialize, Deserialize};

/// A unique message identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub Vec<u8>);

impl MessageId {
    /// Create a new random message ID
    pub fn new_random() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut bytes = vec![0u8; 16];
        rng.fill(&mut bytes[..]);
        Self(bytes)
    }
    
    /// Create a message ID from a byte vector
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
    
    /// Get the inner bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Message types that can be sent
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageKind {
    /// Regular published message
    Publication,
    /// Control message
    Control,
    /// Acknowledgment of a message
    Acknowledgment,
    /// Request for a message retransmission
    RetransmissionRequest,
    /// System message
    System,
}

/// Message properties
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MessageProperties {
    /// Optional content type
    pub content_type: Option<String>,
    /// Optional content encoding
    pub content_encoding: Option<String>,
    /// User-defined headers
    pub headers: HashMap<String, String>,
    /// Correlation ID for request-response patterns
    pub correlation_id: Option<String>,
    /// Reply-to topic for request-response patterns
    pub reply_to: Option<String>,
}

/// A message in the pub/sub system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Message ID
    pub id: MessageId,
    /// Message kind
    pub kind: MessageKind,
    /// Message payload
    pub payload: Vec<u8>,
    /// Message properties
    pub properties: MessageProperties,
    /// Creation timestamp
    pub timestamp: SystemTime,
    /// Time-to-live in seconds (0 = no TTL)
    pub ttl: u32,
    /// Whether this message should be retained on the topic
    pub retain: bool,
    /// The source of this message (e.g., node ID)
    pub source: Option<String>,
    /// Priority (0-255, higher is more important)
    pub priority: u8,
}

impl Message {
    /// Create a new message
    pub fn new(kind: MessageKind, payload: Vec<u8>) -> Self {
        Self {
            id: MessageId::new_random(),
            kind,
            payload,
            properties: MessageProperties::default(),
            timestamp: SystemTime::now(),
            ttl: 0,
            retain: false,
            source: None,
            priority: 128,
        }
    }
    
    /// Create a new publication message
    pub fn new_publication(payload: Vec<u8>) -> Self {
        Self::new(MessageKind::Publication, payload)
    }
    
    /// Create a new control message
    pub fn new_control(payload: Vec<u8>) -> Self {
        Self::new(MessageKind::Control, payload)
    }
    
    /// Create a new acknowledgment message
    pub fn new_ack(original_id: &MessageId) -> Self {
        let payload = original_id.0.clone();
        Self::new(MessageKind::Acknowledgment, payload)
    }
    
    /// Check if this message is expired
    pub fn is_expired(&self) -> bool {
        if self.ttl == 0 {
            return false;
        }
        
        if let Ok(elapsed) = self.timestamp.elapsed() {
            elapsed > Duration::from_secs(self.ttl as u64)
        } else {
            false
        }
    }
    
    /// Set a header value
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.properties.headers.insert(key.to_string(), value.to_string());
        self
    }
    
    /// Set the content type
    pub fn with_content_type(mut self, content_type: &str) -> Self {
        self.properties.content_type = Some(content_type.to_string());
        self
    }
    
    /// Set the reply-to topic
    pub fn with_reply_to(mut self, reply_to: &str) -> Self {
        self.properties.reply_to = Some(reply_to.to_string());
        self
    }
    
    /// Set the correlation ID
    pub fn with_correlation_id(mut self, correlation_id: &str) -> Self {
        self.properties.correlation_id = Some(correlation_id.to_string());
        self
    }
    
    /// Set the TTL
    pub fn with_ttl(mut self, ttl: u32) -> Self {
        self.ttl = ttl;
        self
    }
    
    /// Set the retain flag
    pub fn with_retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }
    
    /// Set the source
    pub fn with_source(mut self, source: &str) -> Self {
        self.source = Some(source.to_string());
        self
    }
    
    /// Set the priority
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
} 