//! Configuration for mDNS-based peer discovery

use std::time::Duration;
use tokio::sync::oneshot::Sender as OneshotSender;

/// Default TTL for mDNS records in seconds
const DEFAULT_TTL: u32 = 120;

/// Default buffer size for discovery events
const DEFAULT_EVENT_BUFFER_SIZE: usize = 100;

/// Default peer expiration time in seconds
const DEFAULT_PEER_EXPIRATION: u64 = 300; // 5 minutes

/// Configuration for mDNS discovery
#[derive(Debug)]
pub struct MdnsDiscoveryConfig {
    /// TTL for mDNS records in seconds
    pub ttl: u32,
    
    /// Maximum number of events to buffer
    pub event_buffer_size: usize,
    
    /// Peer expiration time in seconds
    pub peer_expiration: u64,
    
    /// Sender for the shutdown signal (used internally)
    pub shutdown_sender: Option<OneshotSender<()>>,
}

impl Default for MdnsDiscoveryConfig {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_TTL,
            event_buffer_size: DEFAULT_EVENT_BUFFER_SIZE,
            peer_expiration: DEFAULT_PEER_EXPIRATION,
            shutdown_sender: None,
        }
    }
}

impl Clone for MdnsDiscoveryConfig {
    fn clone(&self) -> Self {
        Self {
            ttl: self.ttl,
            event_buffer_size: self.event_buffer_size,
            peer_expiration: self.peer_expiration,
            shutdown_sender: None, // Don't clone the sender as it's not Clone
        }
    }
}

impl MdnsDiscoveryConfig {
    /// Create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Set the TTL for mDNS records
    pub fn with_ttl(mut self, ttl: u32) -> Self {
        self.ttl = ttl;
        self
    }
    
    /// Set the event buffer size
    pub fn with_event_buffer_size(mut self, size: usize) -> Self {
        self.event_buffer_size = size;
        self
    }
    
    /// Set the peer expiration time in seconds
    pub fn with_peer_expiration(mut self, expiration_secs: u64) -> Self {
        self.peer_expiration = expiration_secs;
        self
    }
    
    /// Get the peer expiration duration
    pub(crate) fn peer_expiration_duration(&self) -> Duration {
        Duration::from_secs(self.peer_expiration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MdnsDiscoveryConfig::default();
        
        assert_eq!(config.ttl, DEFAULT_TTL);
        assert_eq!(config.event_buffer_size, DEFAULT_EVENT_BUFFER_SIZE);
        assert_eq!(config.peer_expiration, DEFAULT_PEER_EXPIRATION);
        assert!(config.shutdown_sender.is_none());
    }

    #[test]
    fn test_builder_pattern() {
        let config = MdnsDiscoveryConfig::new()
            .with_ttl(60)
            .with_event_buffer_size(50)
            .with_peer_expiration(60);
        
        assert_eq!(config.ttl, 60);
        assert_eq!(config.event_buffer_size, 50);
        assert_eq!(config.peer_expiration, 60);
    }
}
