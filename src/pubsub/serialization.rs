//! Serialization utilities for the pub/sub system
//!
//! This module provides serialization and deserialization functionality.

use crate::pubsub::message::Message;
use crate::pubsub::control::ControlMessage;
use thiserror::Error;
use serde::{Serialize, Deserialize};

/// Serialization format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// JSON format
    Json,
    /// CBOR format (compact binary)
    Cbor,
    /// MessagePack format
    MessagePack,
    /// Protobuf format
    Protobuf,
}

/// Serialization error
#[derive(Debug, Error)]
pub enum SerializationError {
    /// JSON serialization error
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
    
    /// CBOR serialization error
    #[error("CBOR error: {0}")]
    CborError(String),
    
    /// MessagePack serialization error
    #[error("MessagePack error: {0}")]
    MessagePackError(String),
    
    /// Protobuf serialization error
    #[error("Protobuf error: {0}")]
    ProtobufError(String),
    
    /// Unknown format
    #[error("Unknown format: {0}")]
    UnknownFormat(String),
}

/// Message serializer
pub struct MessageSerializer;

impl MessageSerializer {
    /// Serialize a message to bytes
    pub fn serialize(message: &Message, format: SerializationFormat) -> Result<Vec<u8>, SerializationError> {
        match format {
            SerializationFormat::Json => {
                Ok(serde_json::to_vec(message)?)
            },
            SerializationFormat::Cbor => {
                // Placeholder for CBOR implementation
                // In a real implementation, we would use a CBOR library
                Err(SerializationError::CborError("CBOR serialization not implemented".to_string()))
            },
            SerializationFormat::MessagePack => {
                // Placeholder for MessagePack implementation
                // In a real implementation, we would use rmp_serde
                Err(SerializationError::MessagePackError("MessagePack serialization not implemented".to_string()))
            },
            SerializationFormat::Protobuf => {
                // Placeholder for Protobuf implementation
                // In a real implementation, we would use prost
                Err(SerializationError::ProtobufError("Protobuf serialization not implemented".to_string()))
            },
        }
    }
    
    /// Deserialize bytes to a message
    pub fn deserialize(bytes: &[u8], format: SerializationFormat) -> Result<Message, SerializationError> {
        match format {
            SerializationFormat::Json => {
                Ok(serde_json::from_slice(bytes)?)
            },
            SerializationFormat::Cbor => {
                // Placeholder for CBOR implementation
                Err(SerializationError::CborError("CBOR deserialization not implemented".to_string()))
            },
            SerializationFormat::MessagePack => {
                // Placeholder for MessagePack implementation
                Err(SerializationError::MessagePackError("MessagePack deserialization not implemented".to_string()))
            },
            SerializationFormat::Protobuf => {
                // Placeholder for Protobuf implementation
                Err(SerializationError::ProtobufError("Protobuf deserialization not implemented".to_string()))
            },
        }
    }
    
    /// Determine the format from content type
    pub fn format_from_content_type(content_type: &str) -> SerializationFormat {
        match content_type {
            "application/json" => SerializationFormat::Json,
            "application/cbor" => SerializationFormat::Cbor,
            "application/msgpack" | "application/x-msgpack" => SerializationFormat::MessagePack,
            "application/protobuf" | "application/x-protobuf" => SerializationFormat::Protobuf,
            _ => SerializationFormat::Json, // Default to JSON
        }
    }
}

/// Control message serializer
pub struct ControlMessageSerializer;

impl ControlMessageSerializer {
    /// Serialize a control message to bytes
    pub fn serialize(message: &ControlMessage, format: SerializationFormat) -> Result<Vec<u8>, SerializationError> {
        match format {
            SerializationFormat::Json => {
                Ok(serde_json::to_vec(message)?)
            },
            SerializationFormat::Cbor => {
                // Placeholder for CBOR implementation
                Err(SerializationError::CborError("CBOR serialization not implemented".to_string()))
            },
            SerializationFormat::MessagePack => {
                // Placeholder for MessagePack implementation
                Err(SerializationError::MessagePackError("MessagePack serialization not implemented".to_string()))
            },
            SerializationFormat::Protobuf => {
                // Placeholder for Protobuf implementation
                Err(SerializationError::ProtobufError("Protobuf serialization not implemented".to_string()))
            },
        }
    }
    
    /// Deserialize bytes to a control message
    pub fn deserialize(bytes: &[u8], format: SerializationFormat) -> Result<ControlMessage, SerializationError> {
        match format {
            SerializationFormat::Json => {
                Ok(serde_json::from_slice(bytes)?)
            },
            SerializationFormat::Cbor => {
                // Placeholder for CBOR implementation
                Err(SerializationError::CborError("CBOR deserialization not implemented".to_string()))
            },
            SerializationFormat::MessagePack => {
                // Placeholder for MessagePack implementation
                Err(SerializationError::MessagePackError("MessagePack deserialization not implemented".to_string()))
            },
            SerializationFormat::Protobuf => {
                // Placeholder for Protobuf implementation
                Err(SerializationError::ProtobufError("Protobuf deserialization not implemented".to_string()))
            },
        }
    }
    
    /// Serialize a control message to JSON
    pub fn to_json(message: &ControlMessage) -> Result<String, SerializationError> {
        Ok(serde_json::to_string(message)?)
    }
    
    /// Deserialize JSON to a control message
    pub fn from_json(json: &str) -> Result<ControlMessage, SerializationError> {
        Ok(serde_json::from_str(json)?)
    }
} 