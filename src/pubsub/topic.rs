//! Topic handling for the pub/sub system
//!
//! This module defines the topic types and matching functionality.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use serde::{Serialize, Deserialize};
use thiserror::Error;

/// Topic parsing error
#[derive(Debug, Error)]
pub enum TopicError {
    /// Invalid topic format
    #[error("Invalid topic format: {0}")]
    InvalidFormat(String),
    
    /// Empty topic name
    #[error("Empty topic name")]
    EmptyName,
    
    /// Invalid character in topic name
    #[error("Invalid character in topic: {0}")]
    InvalidCharacter(char),
    
    /// Topic too long
    #[error("Topic too long (max 255 levels)")]
    TooLong,
}

/// A unique topic identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicId(pub Vec<u8>);

impl TopicId {
    /// Create a topic ID from a byte vector
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
    
    /// Get the inner bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Topic structure for publish/subscribe
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    /// Topic path parts
    pub path: Vec<String>,
    /// Topic ID
    pub id: TopicId,
}

impl Topic {
    /// Create a new topic from path parts
    pub fn new(path: Vec<String>) -> Result<Self, TopicError> {
        if path.is_empty() {
            return Err(TopicError::EmptyName);
        }
        
        if path.len() > 255 {
            return Err(TopicError::TooLong);
        }
        
        // Validate each path component
        for part in &path {
            if part.is_empty() {
                return Err(TopicError::EmptyName);
            }
            
            for c in part.chars() {
                if !c.is_alphanumeric() && c != '_' && c != '-' {
                    return Err(TopicError::InvalidCharacter(c));
                }
            }
        }
        
        // Generate an ID based on the path
        let id_string = path.join("/");
        let id = TopicId(id_string.into_bytes());
        
        Ok(Self { path, id })
    }
    
    /// Create a topic from a string path
    pub fn from_path(path: &str) -> Result<Self, TopicError> {
        // Split the path by /
        let parts: Vec<String> = path
            .split('/')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
            
        Self::new(parts)
    }
    
    /// Check if this topic matches a filter
    pub fn matches(&self, filter: &TopicFilter) -> bool {
        filter.matches(self)
    }
    
    /// Get the topic as a string path
    pub fn as_path(&self) -> String {
        self.path.join("/")
    }
    
    /// Check if this topic is a subtopic of another topic
    pub fn is_subtopic_of(&self, parent: &Topic) -> bool {
        if self.path.len() <= parent.path.len() {
            return false;
        }
        
        // Check if all parent parts match
        for (i, part) in parent.path.iter().enumerate() {
            if &self.path[i] != part {
                return false;
            }
        }
        
        true
    }
    
    /// Get the parent topic
    pub fn parent(&self) -> Option<Topic> {
        if self.path.len() <= 1 {
            return None;
        }
        
        let parent_path = self.path[0..self.path.len()-1].to_vec();
        Topic::new(parent_path).ok()
    }
}

impl FromStr for Topic {
    type Err = TopicError;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Topic::from_path(s)
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_path())
    }
}

impl Ord for Topic {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_path().cmp(&other.as_path())
    }
}

impl PartialOrd for Topic {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Topic {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for Topic {}

impl Hash for Topic {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

/// Topic filter for matching multiple topics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicFilter {
    /// Filter pattern parts
    pub pattern: Vec<String>,
}

impl TopicFilter {
    /// Create a new topic filter
    pub fn new(pattern: Vec<String>) -> Result<Self, TopicError> {
        if pattern.len() > 255 {
            return Err(TopicError::TooLong);
        }
        
        for part in &pattern {
            if part.is_empty() && part != "#" && part != "+" {
                return Err(TopicError::EmptyName);
            }
            
            if part != "#" && part != "+" {
                for c in part.chars() {
                    if !c.is_alphanumeric() && c != '_' && c != '-' {
                        return Err(TopicError::InvalidCharacter(c));
                    }
                }
            }
        }
        
        Ok(Self { pattern })
    }
    
    /// Create a filter from a string pattern
    pub fn from_pattern(pattern: &str) -> Result<Self, TopicError> {
        let parts: Vec<String> = pattern
            .split('/')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
            
        Self::new(parts)
    }
    
    /// Check if a topic matches this filter
    pub fn matches(&self, topic: &Topic) -> bool {
        // Special case for # (match everything)
        if self.pattern.len() == 1 && self.pattern[0] == "#" {
            return true;
        }
        
        // Recursive matching function
        fn matches_recursive(filter: &[String], topic: &[String], position: usize) -> bool {
            // End conditions
            if position == filter.len() {
                return position == topic.len();
            }
            
            let filter_part = &filter[position];
            
            // # wildcard matches everything from here
            if filter_part == "#" {
                return true;
            }
            
            // If topic is shorter than filter, no match
            if position >= topic.len() {
                return false;
            }
            
            let topic_part = &topic[position];
            
            // + wildcard matches exactly one level
            if filter_part == "+" || filter_part == topic_part {
                return matches_recursive(filter, topic, position + 1);
            }
            
            false
        }
        
        matches_recursive(&self.pattern, &topic.path, 0)
    }
    
    /// Get the filter as a string pattern
    pub fn as_pattern(&self) -> String {
        self.pattern.join("/")
    }
}

impl FromStr for TopicFilter {
    type Err = TopicError;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TopicFilter::from_pattern(s)
    }
}

impl fmt::Display for TopicFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_pattern())
    }
} 