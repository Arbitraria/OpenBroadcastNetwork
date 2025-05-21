use std::sync::Arc;

use libp2p::identity::Keypair;

use crate::pubsub::interface::{PubSub, PubSubConfig, PubSubError};
use crate::pubsub::gossipsub::{GossipSubService, GossipSubConfig};
use crate::pubsub::topic::{Topic, TopicId, TopicConfig, StreamTopic, StreamMetadata};
use crate::pubsub::message::{Message, MessageType, MessagePayload};
use crate::pubsub::validation::{MessageValidator, ValidationResult, BasicValidator};

#[test]
fn test_topic_creation() {
    // Test basic topic creation
    let topic = Topic::new("test-topic", "Test Topic");
    assert_eq!(topic.id.0, "test-topic");
    assert_eq!(topic.name, "Test Topic");
    assert!(topic.description.is_none());
    
    // Test with description
    let topic_with_desc = Topic::new("test-topic-2", "Test Topic 2")
        .with_description("A test topic");
    assert_eq!(topic_with_desc.id.0, "test-topic-2");
    assert_eq!(topic_with_desc.description.unwrap(), "A test topic");
    
    // Test with custom config
    let mut config = TopicConfig::default();
    config.is_private = true;
    
    let topic_with_config = Topic::with_config("private-topic", "Private Topic", config);
    assert_eq!(topic_with_config.id.0, "private-topic");
    assert!(topic_with_config.config.is_private);
}

#[test]
fn test_stream_topic_creation() {
    // Test stream topic creation
    let stream_topic = StreamTopic::new("stream-123", "My Stream");
    assert_eq!(stream_topic.stream_id, "stream-123");
    assert_eq!(stream_topic.topic.name, "My Stream");
    assert_eq!(stream_topic.topic.id.0, "stream/stream-123");
    
    // Test with metadata
    let metadata = StreamMetadata::new("Cool Stream")
        .with_content_type("video/webm")
        .with_creator("user-456")
        .with_tags(vec!["gaming".to_string(), "live".to_string()]);
    
    let stream_topic_with_meta = StreamTopic::new("stream-456", "Cool Stream")
        .with_metadata(metadata);
    
    assert_eq!(stream_topic_with_meta.metadata.title.unwrap(), "Cool Stream");
    assert_eq!(stream_topic_with_meta.metadata.content_type.unwrap(), "video/webm");
    assert_eq!(stream_topic_with_meta.metadata.creator.unwrap(), "user-456");
    assert_eq!(stream_topic_with_meta.metadata.tags.len(), 2);
    assert!(stream_topic_with_meta.metadata.tags.contains(&"gaming".to_string()));
}

#[test]
fn test_message_creation() {
    let topic_id = TopicId("test-topic".to_string());
    
    // Binary message
    let binary_data = vec![1, 2, 3, 4];
    let binary_msg = Message::binary(topic_id.clone(), binary_data.clone());
    
    assert_eq!(binary_msg.topic.0, "test-topic");
    assert_eq!(binary_msg.message_type, MessageType::Data);
    match &binary_msg.payload {
        MessagePayload::Binary(data) => assert_eq!(data, &binary_data),
        _ => panic!("Expected binary payload"),
    }
    
    // Text message
    let text_msg = Message::text(topic_id.clone(), "Hello, world!");
    match &text_msg.payload {
        MessagePayload::Text(text) => assert_eq!(text, "Hello, world!"),
        _ => panic!("Expected text payload"),
    }
    
    // JSON message
    let json_value = serde_json::json!({
        "name": "Test",
        "value": 123
    });
    let json_msg = Message::json(topic_id.clone(), json_value.clone());
    match &json_msg.payload {
        MessagePayload::Json(json) => assert_eq!(json, &json_value),
        _ => panic!("Expected JSON payload"),
    }
    
    // Test builder methods
    let msg = Message::binary(topic_id.clone(), binary_data)
        .with_type(MessageType::MediaChunk)
        .with_sequence(42)
        .with_ttl(60000)
        .with_header("content-type", "application/octet-stream");
    
    assert_eq!(msg.message_type, MessageType::MediaChunk);
    assert_eq!(msg.sequence, Some(42));
    assert_eq!(msg.ttl, 60000);
    assert_eq!(msg.headers.get("content-type").unwrap(), "application/octet-stream");
}

#[test]
fn test_basic_validator() {
    let topic_id = TopicId("test-topic".to_string());
    let validator = BasicValidator::new()
        .with_max_message_size(100);
    
    // Valid message (small)
    let valid_msg = Message::binary(topic_id.clone(), vec![0; 50]);
    assert_eq!(validator.validate(&valid_msg, None), ValidationResult::Accept);
    
    // Invalid message (too large)
    let oversized_msg = Message::binary(topic_id.clone(), vec![0; 200]);
    assert_eq!(validator.validate(&oversized_msg, None), ValidationResult::Reject);
}

#[test]
fn test_gossipsub_service() {
    // Create a keypair for the service
    let keypair = Keypair::generate_ed25519();
    
    // Create GossipSub service
    let mut service = GossipSubService::new(keypair);
    
    // Test topic subscription
    let topic = Topic::new("test-topic", "Test Topic");
    assert!(service.subscribe(&topic).is_ok());
    
    // Test listing subscriptions
    let subscriptions = service.list_subscriptions();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].0, "test-topic");
    
    // Test publishing
    let result = service.publish(&topic.id, vec![1, 2, 3, 4]);
    assert!(result.is_ok());
    
    // Test unsubscribing
    assert!(service.unsubscribe(&topic.id).is_ok());
    assert_eq!(service.list_subscriptions().len(), 0);
}

// Integration test for message flow
#[tokio::test]
async fn test_async_gossipsub() {
    // This test would require a more complex setup with actual network connections
    // For simplicity, we'll just test the async interface methods
    
    // Create a keypair for the service
    let keypair = Keypair::generate_ed25519();
    
    // Create GossipSub service
    let mut service = GossipSubService::new(keypair);
    
    // Test start/stop
    assert!(service.start().is_ok());
    assert!(service.stop().is_ok());
    
    // Test async operations
    let topic = Topic::new("async-topic", "Async Topic");
    let topic_id = topic.id.clone();
    
    let sub_result = service.subscribe(&topic).await;
    assert!(sub_result.is_ok());
    
    // Get event stream
    let stream_result = service.event_stream().await;
    assert!(stream_result.is_ok());
    
    // Publish a message
    let pub_result = service.publish(&topic_id, vec![5, 6, 7, 8]).await;
    assert!(pub_result.is_ok());
    
    // Unsubscribe
    let unsub_result = service.unsubscribe(&topic_id).await;
    assert!(unsub_result.is_ok());
} 