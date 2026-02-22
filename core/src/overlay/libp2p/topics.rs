//! Topic handling for the gossipsub protocol
//!
//! This module contains functions for creating and parsing pub/sub topics

use crate::overlay::interface::StreamId;

/// Topics for the pub/sub system
/// Create a control topic for a stream
pub fn stream_control(stream_id: &StreamId) -> String {
    let id_str = hex::encode(stream_id.as_bytes());
    format!("stream/{}/control", id_str)
}

/// Create a data topic for a stream
pub fn stream_data(stream_id: &StreamId) -> String {
    let id_str = hex::encode(stream_id.as_bytes());
    format!("stream/{}/data", id_str)
}

/// Create a discovery topic
pub fn discovery() -> String {
    "discovery".to_string()
}

/// Parse a stream ID from a topic
pub fn parse_stream_id(topic: &str) -> Option<StreamId> {
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() >= 3 && parts[0] == "stream" {
        if let Ok(bytes) = hex::decode(parts[1]) {
            return Some(StreamId::from_bytes(bytes));
        }
    }
    None
}
