use std::fmt::{Display, Formatter};
use std::time::{SystemTime, UNIX_EPOCH};
use std::hash::Hash;
use serde::{Serialize, Deserialize};
use std::str::FromStr;

use libp2p::PeerId;
use crate::pubsub::topic::TopicId;

/// Serializable wrapper for PeerId
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SerializablePeerId(pub String);

impl From<PeerId> for SerializablePeerId {
    fn from(peer_id: PeerId) -> Self {
        SerializablePeerId(peer_id.to_string())
    }
}

impl TryFrom<SerializablePeerId> for PeerId {
    type Error = String;
    
    fn try_from(peer_id: SerializablePeerId) -> Result<Self, Self::Error> {
        PeerId::from_str(&peer_id.0)
            .map_err(|e| format!("Failed to parse PeerId: {}", e))
    }
}

/// Unique identifier for a message
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub String);

impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Type of message content
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    /// Regular data message
    Data,
    
    /// Control message
    Control,
    
    /// Stream metadata message
    Metadata,
    
    /// Stream media segment/chunk
    MediaChunk,
    
    /// Service announcement (peer info, etc.)
    Announcement,
}

/// Payload of a message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessagePayload {
    /// Binary data
    Binary(Vec<u8>),
    
    /// UTF-8 encoded text
    Text(String),
    
    /// JSON encoded data
    Json(serde_json::Value),
}

impl MessagePayload {
    /// Get the payload size in bytes
    pub fn size(&self) -> usize {
        match self {
            MessagePayload::Binary(data) => data.len(),
            MessagePayload::Text(text) => text.len(),
            MessagePayload::Json(json) => json.to_string().len(),
        }
    }
    
    /// Convert the payload to bytes
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            MessagePayload::Binary(data) => data.clone(),
            MessagePayload::Text(text) => text.as_bytes().to_vec(),
            MessagePayload::Json(json) => json.to_string().into_bytes(),
        }
    }
}

/// A message in the pub/sub system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Unique message identifier
    pub id: MessageId,
    
    /// Topic this message belongs to
    pub topic: TopicId,
    
    /// Publisher of the message
    pub publisher: Option<SerializablePeerId>,
    
    /// Message type
    pub message_type: MessageType,
    
    /// Message payload
    pub payload: MessagePayload,
    
    /// Sequence number (optional, for ordering)
    pub sequence: Option<u64>,
    
    /// Timestamp when the message was created (milliseconds since epoch)
    pub timestamp: u64,
    
    /// Time-to-live for the message in milliseconds (0 means no expiry)
    pub ttl: u64,
    
    /// Custom headers as key-value pairs
    pub headers: std::collections::HashMap<String, String>,
}

impl Message {
    /// Create a new data message with binary payload
    pub fn binary(topic: TopicId, data: Vec<u8>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        
        let id = generate_message_id(&topic, &data, timestamp);
        
        Self {
            id,
            topic,
            publisher: None,
            message_type: MessageType::Data,
            payload: MessagePayload::Binary(data),
            sequence: None,
            timestamp,
            ttl: 0,
            headers: std::collections::HashMap::new(),
        }
    }
    
    /// Create a new data message with text payload
    pub fn text<S: Into<String>>(topic: TopicId, text: S) -> Self {
        let text_str = text.into();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        
        let id = generate_message_id(&topic, text_str.as_bytes(), timestamp);
        
        Self {
            id,
            topic,
            publisher: None,
            message_type: MessageType::Data,
            payload: MessagePayload::Text(text_str),
            sequence: None,
            timestamp,
            ttl: 0,
            headers: std::collections::HashMap::new(),
        }
    }
    
    /// Create a new data message with JSON payload
    pub fn json(topic: TopicId, json: serde_json::Value) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        
        let json_bytes = json.to_string().into_bytes();
        let id = generate_message_id(&topic, &json_bytes, timestamp);
        
        Self {
            id,
            topic,
            publisher: None,
            message_type: MessageType::Data,
            payload: MessagePayload::Json(json),
            sequence: None,
            timestamp,
            ttl: 0,
            headers: std::collections::HashMap::new(),
        }
    }
    
    /// Set the publisher of the message
    pub fn with_publisher(mut self, publisher: PeerId) -> Self {
        self.publisher = Some(publisher.into());
        self
    }
    
    /// Set the message type
    pub fn with_type(mut self, message_type: MessageType) -> Self {
        self.message_type = message_type;
        self
    }
    
    /// Set the sequence number
    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = Some(sequence);
        self
    }
    
    /// Set the time-to-live
    pub fn with_ttl(mut self, ttl: u64) -> Self {
        self.ttl = ttl;
        self
    }
    
    /// Add a header to the message
    pub fn with_header<S: Into<String>>(mut self, key: S, value: S) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
    
    /// Check if the message has expired
    pub fn is_expired(&self) -> bool {
        if self.ttl == 0 {
            return false;
        }
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        
        now > self.timestamp + self.ttl
    }
    
    /// Get the size of the message in bytes (approximate)
    pub fn size(&self) -> usize {
        let headers_size: usize = self.headers
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum();
        
        // Base size + payload size + headers size
        32 + self.payload.size() + headers_size
    }
}

/// Generate a unique message ID based on topic, content, and timestamp
fn generate_message_id(topic: &TopicId, data: &[u8], timestamp: u64) -> MessageId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(topic.0.as_bytes());
    hasher.update(data);
    hasher.update(&timestamp.to_be_bytes());
    
    let mut output = [0u8; 16];
    let mut output_reader = hasher.finalize_xof();
    output_reader.fill(&mut output);
    let id = hex::encode(output);
    
    MessageId(id)
} 