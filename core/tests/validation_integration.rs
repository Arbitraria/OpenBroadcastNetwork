//! Validation and stream robustness integration tests
//!
//! Tests for:
//! - Wire format validation (accept valid, reject malformed)
//! - Rate limiting enforcement
//! - Sequence gap/duplicate detection
//! - Stream announcement roundtrip via StreamDiscovery

use OpenBroadcastNetwork_core::media::segment::{MediaType, StreamId, StreamSegment};
use OpenBroadcastNetwork_core::media::wire_format::WireSegment;
use OpenBroadcastNetwork_core::pubsub::message::Message;
use OpenBroadcastNetwork_core::pubsub::topic::TopicId;
use OpenBroadcastNetwork_core::pubsub::validation::{
    BasicValidator, CompositeValidator, MessageValidator, StreamDataValidator, ValidationResult,
};

/// Helper: create a Message whose payload is a valid WireSegment on a
/// stream data topic.
fn make_stream_data_message(sequence: u64) -> Message {
    let wire = WireSegment::new(
        b"test_stream".to_vec(),
        sequence,
        sequence * 33333,
        33333,
        MediaType::Video.to_u8(),
        1,
        sequence == 0,
        vec![0xAA; 128],
    );
    let bytes = wire.to_bytes().expect("serialize");

    let topic = TopicId(format!(
        "stream/{}/data",
        hex::encode(b"test_stream")
    ));
    Message::binary(topic, bytes)
}

// ─── Wire format validation ──────────────────────────────────────

#[test]
fn test_valid_wire_segment_accepted() {
    let validator = StreamDataValidator::new(100);
    let msg = make_stream_data_message(0);
    assert_eq!(validator.validate(&msg, None), ValidationResult::Accept);
}

#[test]
fn test_malformed_wire_segment_rejected() {
    let validator = StreamDataValidator::new(100);

    let topic = TopicId(format!(
        "stream/{}/data",
        hex::encode(b"test_stream")
    ));
    let msg = Message::binary(topic, vec![0xFF, 0x00, 0x01]); // garbage
    assert_eq!(validator.validate(&msg, None), ValidationResult::Reject);
}

#[test]
fn test_non_stream_topic_passes_through() {
    let validator = StreamDataValidator::new(100);
    let topic = TopicId("chat/general".to_string());
    let msg = Message::binary(topic, vec![0xFF, 0x00]);
    // Non-stream topics are accepted regardless of payload
    assert_eq!(validator.validate(&msg, None), ValidationResult::Accept);
}

#[test]
fn test_unknown_media_type_rejected() {
    let wire = WireSegment::new(
        b"test_stream".to_vec(),
        0,
        0,
        33333,
        99, // invalid media type
        1,
        false,
        vec![0xBB; 64],
    );
    let bytes = wire.to_bytes().expect("serialize");
    let topic = TopicId(format!(
        "stream/{}/data",
        hex::encode(b"test_stream")
    ));
    let msg = Message::binary(topic, bytes);

    let validator = StreamDataValidator::new(100);
    assert_eq!(validator.validate(&msg, None), ValidationResult::Reject);
}

// ─── Rate limiting ───────────────────────────────────────────────

#[test]
fn test_rate_limit_enforcement() {
    let validator = StreamDataValidator::new(5); // 5 msg/s
    let peer_id = libp2p::PeerId::random();

    for i in 0u64..5 {
        let msg = make_stream_data_message(i);
        assert_eq!(
            validator.validate(&msg, Some(&peer_id)),
            ValidationResult::Accept,
            "Message {} should be accepted",
            i
        );
    }

    // 6th message in same second window should be rejected
    let msg = make_stream_data_message(5);
    assert_eq!(
        validator.validate(&msg, Some(&peer_id)),
        ValidationResult::Reject
    );
}

// ─── Sequence tracking ──────────────────────────────────────────

#[test]
fn test_duplicate_sequence_ignored() {
    let validator = StreamDataValidator::new(100);

    let msg0 = make_stream_data_message(0);
    assert_eq!(validator.validate(&msg0, None), ValidationResult::Accept);

    // Same sequence again → Ignore (duplicate)
    let msg0_dup = make_stream_data_message(0);
    assert_eq!(
        validator.validate(&msg0_dup, None),
        ValidationResult::Ignore
    );
}

#[test]
fn test_sequential_messages_accepted() {
    let validator = StreamDataValidator::new(100);

    for seq in 0..10 {
        let msg = make_stream_data_message(seq);
        let result = validator.validate(&msg, None);
        assert_eq!(result, ValidationResult::Accept, "seq {} failed", seq);
    }
}

#[test]
fn test_sequence_gap_still_accepted() {
    // Gaps are logged as warnings but still accepted
    let validator = StreamDataValidator::new(100);

    let msg0 = make_stream_data_message(0);
    assert_eq!(validator.validate(&msg0, None), ValidationResult::Accept);

    // Skip to seq 5 — gap should be logged but accepted
    let msg5 = make_stream_data_message(5);
    assert_eq!(validator.validate(&msg5, None), ValidationResult::Accept);
}

// ─── Concurrent subscribers (channel test) ───────────────────────

#[tokio::test]
async fn test_concurrent_subscribers_receive_same_data() {
    use tokio::sync::broadcast;

    let (tx, _) = broadcast::channel::<StreamSegment>(100);

    // 3 subscribers
    let mut rx1 = tx.subscribe();
    let mut rx2 = tx.subscribe();
    let mut rx3 = tx.subscribe();

    // Publish 5 segments
    for i in 0u64..5 {
        let seg = StreamSegment::video(
            StreamId::new("concurrent_test"),
            i,
            i * 33333,
            33333,
            vec![i as u8; 100],
            i == 0,
        );
        tx.send(seg).unwrap();
    }

    // All 3 receivers should get all 5 segments
    for i in 0u64..5 {
        let s1 = rx1.recv().await.unwrap();
        let s2 = rx2.recv().await.unwrap();
        let s3 = rx3.recv().await.unwrap();

        assert_eq!(s1.sequence, i);
        assert_eq!(s2.sequence, i);
        assert_eq!(s3.sequence, i);
        assert_eq!(s1.data.as_ref(), s2.data.as_ref());
        assert_eq!(s2.data.as_ref(), s3.data.as_ref());
    }
}

// ─── Stream announcement roundtrip ──────────────────────────────

#[test]
fn test_stream_announcement_roundtrip() {
    use OpenBroadcastNetwork_core::discovery::stream_discovery::StreamAnnouncement;
    use OpenBroadcastNetwork_core::overlay::interface::StreamId as OverlayStreamId;
    use OpenBroadcastNetwork_core::overlay::peer::LocalPeerId;

    let stream_id = OverlayStreamId::from_string("roundtrip-test");
    let publisher_id = LocalPeerId::new_random();

    let announcement =
        StreamAnnouncement::new(stream_id.clone(), "Test Stream".to_string(), publisher_id)
            .with_video("h264", 1920, 1080)
            .with_audio("aac")
            .with_metadata("category", "gaming");

    // Serialize → deserialize
    let bytes = announcement.to_bytes().expect("serialize announcement");
    let restored =
        StreamAnnouncement::from_bytes(&bytes).expect("deserialize announcement");

    assert_eq!(restored.stream_id, stream_id);
    assert_eq!(restored.title, "Test Stream");
    assert_eq!(restored.video_codec, Some("h264".to_string()));
    assert_eq!(restored.audio_codec, Some("aac".to_string()));
    assert!(!restored.is_expired());
}

// ─── Wire format v2 content hash tests ──────────────────────────

#[test]
fn test_wire_segment_v2_content_hash_roundtrip() {
    let wire = WireSegment::new(
        b"hash_test".to_vec(),
        1,
        33333,
        33333,
        MediaType::Video.to_u8(),
        0,
        true,
        vec![0xDE, 0xAD, 0xBE, 0xEF],
    );

    assert_eq!(wire.content_hash.len(), 16);
    assert!(wire.verify_integrity());

    // Roundtrip through serialization
    let bytes = wire.to_bytes().expect("serialize");
    let restored = WireSegment::from_bytes(&bytes).expect("deserialize");
    assert!(restored.verify_integrity());
    assert_eq!(restored.content_hash, wire.content_hash);
}

#[test]
fn test_verify_integrity_fails_for_tampered_data() {
    let mut wire = WireSegment::new(
        b"tamper_test".to_vec(),
        0,
        0,
        33333,
        MediaType::Video.to_u8(),
        0,
        false,
        vec![1, 2, 3, 4],
    );

    assert!(wire.verify_integrity());

    // Tamper with data after construction
    wire.data[0] = 0xFF;
    assert!(!wire.verify_integrity());
}

#[test]
fn test_wire_segment_from_stream_segment_has_hash() {
    let segment = StreamSegment::video(
        StreamId::new("hash_from_seg"),
        5,
        166665,
        33333,
        vec![0xAA; 200],
        false,
    );

    let wire: WireSegment = segment.into();
    assert_eq!(wire.content_hash.len(), 16);
    assert!(wire.verify_integrity());
}

// ─── CompositeValidator chaining tests ──────────────────────────

#[test]
fn test_composite_validator_chains_basic_and_stream() {
    let mut composite = CompositeValidator::new();
    composite.add_validator(BasicValidator::new().with_max_message_size(64));
    composite.add_validator(StreamDataValidator::new(100));

    // Large message rejected by BasicValidator (size check)
    let topic = TopicId("chat/general".to_string());
    let big_msg = Message::binary(topic, vec![0u8; 128]);
    assert_eq!(
        composite.validate(&big_msg, None),
        ValidationResult::Reject
    );

    // Malformed WireSegment on stream topic rejected by StreamDataValidator
    let stream_topic = TopicId(format!(
        "stream/{}/data",
        hex::encode(b"comp_test")
    ));
    let bad_msg = Message::binary(stream_topic, vec![0xFF; 10]);
    assert_eq!(
        composite.validate(&bad_msg, None),
        ValidationResult::Reject
    );

    // Valid small message on non-stream topic passes both
    let ok_topic = TopicId("chat/ok".to_string());
    let ok_msg = Message::binary(ok_topic, vec![42; 16]);
    assert_eq!(
        composite.validate(&ok_msg, None),
        ValidationResult::Accept
    );
}

// ─── Keypair signing tests ──────────────────────────────────────

#[test]
fn test_sign_and_verify_with_keypair_roundtrip() {
    use OpenBroadcastNetwork_core::crypto::{sign_with_keypair, verify_with_public_key};

    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let data = b"stream segment payload for signing";

    let sig = sign_with_keypair(&keypair, data).expect("sign");
    assert!(!sig.is_empty());

    let valid = verify_with_public_key(&keypair.public(), data, &sig).expect("verify");
    assert!(valid);

    // Wrong data should fail verification
    let invalid = verify_with_public_key(&keypair.public(), b"wrong", &sig).expect("verify");
    assert!(!invalid);

    // Wrong key should fail verification
    let other = libp2p::identity::Keypair::generate_ed25519();
    let wrong_key = verify_with_public_key(&other.public(), data, &sig).expect("verify");
    assert!(!wrong_key);
}

#[test]
fn test_hash_for_signing_deterministic() {
    use OpenBroadcastNetwork_core::crypto::hash_for_signing;

    let h1 = hash_for_signing(b"deterministic");
    let h2 = hash_for_signing(b"deterministic");
    assert_eq!(h1, h2);

    let h3 = hash_for_signing(b"different");
    assert_ne!(h1, h3);
}

#[test]
#[allow(deprecated)]
fn test_deprecated_sign_verify_stubs_compile() {
    use OpenBroadcastNetwork_core::crypto::{sign, verify};
    let _ = sign(&[], &[]);
    let _ = verify(&[], &[], &[]);
}
