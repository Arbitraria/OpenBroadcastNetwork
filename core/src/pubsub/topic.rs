use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Unique identifier for a topic
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicId(pub String);

impl Display for TopicId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Topic metadata and configuration
#[derive(Debug, Clone)]
pub struct Topic {
    /// Unique identifier for the topic
    pub id: TopicId,
    
    /// Human-readable name for the topic
    pub name: String,
    
    /// Description of the topic contents
    pub description: Option<String>,
    
    /// Topic-specific configuration
    pub config: TopicConfig,
}

impl Topic {
    /// Create a new topic with default configuration
    pub fn new<S: Into<String>>(id: S, name: S) -> Self {
        let id_str: String = id.into();
        let name_str: String = name.into();
        
        Self {
            id: TopicId(id_str),
            name: name_str,
            description: None,
            config: TopicConfig::default(),
        }
    }
    
    /// Create a new topic with custom configuration
    pub fn with_config<S: Into<String>>(id: S, name: S, config: TopicConfig) -> Self {
        let id_str: String = id.into();
        let name_str: String = name.into();
        
        Self {
            id: TopicId(id_str),
            name: name_str,
            description: None,
            config,
        }
    }
    
    /// Add a description to the topic
    pub fn with_description<S: Into<String>>(mut self, description: S) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// Configuration for a topic
#[derive(Debug, Clone)]
pub struct TopicConfig {
    /// Message cache history size
    pub history_size: usize,
    
    /// Allowed publishers (empty means anyone can publish)
    pub allowed_publishers: Vec<String>,
    
    /// Is this a private topic (invite only)
    pub is_private: bool,
    
    /// Maximum message size specific to this topic (0 means use global default)
    pub max_message_size: usize,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            history_size: 100,
            allowed_publishers: Vec::new(),
            is_private: false,
            max_message_size: 0,
        }
    }
}

/// A topic specifically for streaming content
#[derive(Debug, Clone)]
pub struct StreamTopic {
    /// Base topic
    pub topic: Topic,
    
    /// Stream identifier
    pub stream_id: String,
    
    /// Stream metadata
    pub metadata: StreamMetadata,
}

impl StreamTopic {
    /// Create a new stream topic
    pub fn new<S: Into<String>>(stream_id: S, name: S) -> Self {
        let stream_id_str: String = stream_id.into();
        let name_str: String = name.into();
        let topic_id = format!("stream/{}", stream_id_str);
        
        Self {
            topic: Topic::new(topic_id, name_str),
            stream_id: stream_id_str,
            metadata: StreamMetadata::default(),
        }
    }
    
    /// Get the topic ID
    pub fn topic_id(&self) -> &TopicId {
        &self.topic.id
    }
    
    /// Set stream metadata
    pub fn with_metadata(mut self, metadata: StreamMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Metadata for a stream
#[derive(Debug, Clone, Default)]
pub struct StreamMetadata {
    /// Content type (e.g., "video/webm")
    pub content_type: Option<String>,
    
    /// Stream title
    pub title: Option<String>,
    
    /// Stream description
    pub description: Option<String>,
    
    /// Creator identifier
    pub creator: Option<String>,
    
    /// Tags for the stream
    pub tags: Vec<String>,
    
    /// Custom metadata as key-value pairs
    pub custom: std::collections::HashMap<String, String>,
}

impl StreamMetadata {
    /// Create new metadata with a title
    pub fn new<S: Into<String>>(title: S) -> Self {
        let mut metadata = Self::default();
        metadata.title = Some(title.into());
        metadata
    }
    
    /// Add content type
    pub fn with_content_type<S: Into<String>>(mut self, content_type: S) -> Self {
        self.content_type = Some(content_type.into());
        self
    }
    
    /// Add a description
    pub fn with_description<S: Into<String>>(mut self, description: S) -> Self {
        self.description = Some(description.into());
        self
    }
    
    /// Add a creator
    pub fn with_creator<S: Into<String>>(mut self, creator: S) -> Self {
        self.creator = Some(creator.into());
        self
    }
    
    /// Add tags
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }
    
    /// Add a custom metadata field
    pub fn with_custom<S: Into<String>>(mut self, key: S, value: S) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }
} 