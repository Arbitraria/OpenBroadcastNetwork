//! Tests for the pub/sub system
//!
//! This module contains unit tests for the pub/sub functionality.

#[cfg(test)]
mod tests {
    use super::super::interface::{PubSub, Publisher, Subscriber, PublishOptions, SubscribeOptions};
    use super::super::message::{Message, MessageId};
    use super::super::topic::{Topic, TopicFilter};
    use super::super::gossip::{GossipPubSub, GossipPubSubConfig};
    use std::time::Duration;
    
    /// Test topic creation and validation
    #[test]
    fn test_topic_creation() {
        // Create a topic
        let topic_result = Topic::from_path("stream/video/1080p");
        assert!(topic_result.is_ok());
        
        let topic = topic_result.unwrap();
        assert_eq!(topic.path.len(), 3);
        assert_eq!(topic.path[0], "stream");
        assert_eq!(topic.path[1], "video");
        assert_eq!(topic.path[2], "1080p");
        
        // Test invalid topic
        let invalid_topic = Topic::from_path("stream/video/invalid!");
        assert!(invalid_topic.is_err());
        
        // Test topic matching
        let filter_result = TopicFilter::from_pattern("stream/+/1080p");
        assert!(filter_result.is_ok());
        
        let filter = filter_result.unwrap();
        assert!(topic.matches(&filter));
        
        // Test non-matching topic
        let non_matching_topic = Topic::from_path("stream/audio/stereo").unwrap();
        assert!(!non_matching_topic.matches(&filter));
        
        // Test wildcard filter
        let wildcard_filter = TopicFilter::from_pattern("stream/#").unwrap();
        assert!(topic.matches(&wildcard_filter));
        assert!(non_matching_topic.matches(&wildcard_filter));
    }
    
    /// Test message creation
    #[test]
    fn test_message_creation() {
        let payload = "Hello, world!".as_bytes().to_vec();
        let message = Message::new_publication(payload.clone());
        
        assert_eq!(message.payload, payload);
        assert!(!message.retain);
        assert_eq!(message.ttl, 0);
        
        // Test message with additional properties
        let message_with_props = Message::new_publication(payload.clone())
            .with_retain(true)
            .with_ttl(60)
            .with_content_type("text/plain")
            .with_priority(200);
            
        assert_eq!(message_with_props.payload, payload);
        assert!(message_with_props.retain);
        assert_eq!(message_with_props.ttl, 60);
        assert_eq!(message_with_props.priority, 200);
        assert_eq!(message_with_props.properties.content_type, Some("text/plain".to_string()));
    }
    
    /// Test the gossip-based pub/sub with basic publish and subscribe
    #[tokio::test]
    async fn test_pubsub_basic() {
        // Create pubsub instance
        let config = GossipPubSubConfig {
            node_id: "test-node".to_string(),
            ..Default::default()
        };
        
        let pubsub = GossipPubSub::new(config);
        
        // Start the pubsub system
        pubsub.start().await.expect("Failed to start pubsub");
        
        // Create a topic
        let topic = Topic::from_path("test/topic").unwrap();
        
        // Subscribe to the topic
        let subscription = pubsub.subscribe(&topic, SubscribeOptions::default())
            .await
            .expect("Failed to subscribe");
            
        // Publish a message
        let payload = "Test message".as_bytes().to_vec();
        let message_id = pubsub.publish(&topic, payload.clone(), PublishOptions::default())
            .await
            .expect("Failed to publish");
            
        // Wait a moment for message to be processed
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Receive the message
        let received = pubsub.next_message().await;
        assert!(received.is_some());
        
        let (received_topic, received_message) = received.unwrap();
        assert_eq!(received_topic.as_path(), topic.as_path());
        assert_eq!(received_message.payload, payload);
        
        // Unsubscribe
        pubsub.unsubscribe(&subscription).await.expect("Failed to unsubscribe");
        
        // Stop the pubsub system
        pubsub.stop().await.expect("Failed to stop pubsub");
    }
    
    /// Test multiple subscribers
    #[tokio::test]
    async fn test_multiple_subscribers() {
        // Create pubsub instance
        let config = GossipPubSubConfig {
            node_id: "test-node".to_string(),
            ..Default::default()
        };
        
        let pubsub = GossipPubSub::new(config);
        
        // Start the pubsub system
        pubsub.start().await.expect("Failed to start pubsub");
        
        // Create topics
        let topic1 = Topic::from_path("test/topic1").unwrap();
        let topic2 = Topic::from_path("test/topic2").unwrap();
        
        // Subscribe to both topics
        let sub1 = pubsub.subscribe(&topic1, SubscribeOptions::default())
            .await
            .expect("Failed to subscribe to topic1");
            
        let sub2 = pubsub.subscribe(&topic2, SubscribeOptions::default())
            .await
            .expect("Failed to subscribe to topic2");
            
        // Publish messages to both topics
        let payload1 = "Message 1".as_bytes().to_vec();
        let payload2 = "Message 2".as_bytes().to_vec();
        
        pubsub.publish(&topic1, payload1.clone(), PublishOptions::default())
            .await
            .expect("Failed to publish to topic1");
            
        pubsub.publish(&topic2, payload2.clone(), PublishOptions::default())
            .await
            .expect("Failed to publish to topic2");
            
        // Wait a moment for messages to be processed
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Receive messages
        let received1 = pubsub.next_message().await;
        let received2 = pubsub.next_message().await;
        
        assert!(received1.is_some());
        assert!(received2.is_some());
        
        let received_messages = vec![received1.unwrap(), received2.unwrap()];
        
        // Check that we received both messages
        let received_topic1 = received_messages.iter()
            .find(|(topic, _)| topic.as_path() == topic1.as_path());
        let received_topic2 = received_messages.iter()
            .find(|(topic, _)| topic.as_path() == topic2.as_path());
            
        assert!(received_topic1.is_some());
        assert!(received_topic2.is_some());
        
        let (_, message1) = received_topic1.unwrap();
        let (_, message2) = received_topic2.unwrap();
        
        assert_eq!(message1.payload, payload1);
        assert_eq!(message2.payload, payload2);
        
        // Unsubscribe
        pubsub.unsubscribe(&sub1).await.expect("Failed to unsubscribe from topic1");
        pubsub.unsubscribe(&sub2).await.expect("Failed to unsubscribe from topic2");
        
        // Stop the pubsub system
        pubsub.stop().await.expect("Failed to stop pubsub");
    }
    
    /// Test topic filtering and wildcards
    #[tokio::test]
    async fn test_topic_filtering() {
        // Create pubsub instance
        let config = GossipPubSubConfig {
            node_id: "test-node".to_string(),
            ..Default::default()
        };
        
        let pubsub = GossipPubSub::new(config);
        
        // Start the pubsub system
        pubsub.start().await.expect("Failed to start pubsub");
        
        // Create a wildcard filter
        let filter = TopicFilter::from_pattern("test/+/status").unwrap();
        
        // Create matching and non-matching topics
        let topic1 = Topic::from_path("test/device1/status").unwrap();
        let topic2 = Topic::from_path("test/device2/status").unwrap();
        let topic3 = Topic::from_path("test/device1/data").unwrap();
        
        // Subscribe using the filter directly
        pubsub.subscribe(&topic1, SubscribeOptions::default())
            .await
            .expect("Failed to subscribe to topic1");
            
        pubsub.subscribe(&topic2, SubscribeOptions::default())
            .await
            .expect("Failed to subscribe to topic2");
            
        pubsub.subscribe(&topic3, SubscribeOptions::default())
            .await
            .expect("Failed to subscribe to topic3");
            
        // Get topics matching the filter
        let matching_topics = pubsub.get_topics(&filter).await.unwrap();
        
        // Should match topic1 and topic2 but not topic3
        assert_eq!(matching_topics.len(), 2);
        assert!(matching_topics.iter().any(|t| t.as_path() == topic1.as_path()));
        assert!(matching_topics.iter().any(|t| t.as_path() == topic2.as_path()));
        assert!(!matching_topics.iter().any(|t| t.as_path() == topic3.as_path()));
        
        // Stop the pubsub system
        pubsub.stop().await.expect("Failed to stop pubsub");
    }
} 