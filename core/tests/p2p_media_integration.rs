//! Integration tests for P2P media streaming.
//!
//! Tests the integration between the media pipeline and the P2P overlay network,
//! including segment publishing, subscription, and late-joiner catchup.

use std::collections::VecDeque;
use OpenBroadcastNetwork_core::media::segment::{
    MediaType, SegmentBuilder, StreamId, StreamSegment,
};
use OpenBroadcastNetwork_core::media::wire_format::{ToWireFormat, WireSegment};
use OpenBroadcastNetwork_core::overlay::interface::StreamId as OverlayStreamId;
use OpenBroadcastNetwork_core::overlay::relay::{StreamChunk, StreamRelay};

/// Test that StreamSegment roundtrips correctly through WireFormat
#[test]
fn test_stream_segment_wire_format_roundtrip() {
    let stream_id = StreamId::new("test_stream");

    // Create video segment
    let video_segment = StreamSegment::video(
        stream_id.clone(),
        0,
        0,
        33333, // ~30fps
        vec![0u8; 1000],
        true, // keyframe
    );

    // Serialize to wire format
    let wire_bytes = video_segment.to_wire_bytes().expect("serialize");
    assert!(!wire_bytes.is_empty());

    // Deserialize back
    let wire_segment = WireSegment::from_bytes(&wire_bytes).expect("deserialize");
    let restored: StreamSegment = wire_segment.into();

    // Verify
    assert_eq!(restored.stream_id, video_segment.stream_id);
    assert_eq!(restored.sequence, video_segment.sequence);
    assert_eq!(restored.pts_us, video_segment.pts_us);
    assert_eq!(restored.duration_us, video_segment.duration_us);
    assert_eq!(restored.media_type, MediaType::Video);
    assert!(restored.is_keyframe);
    assert_eq!(restored.data.len(), 1000);
}

/// Test audio segment wire format
#[test]
fn test_audio_segment_wire_format() {
    let stream_id = StreamId::new("audio_test");

    let audio_segment = StreamSegment::audio(stream_id, 10, 1000000, 20000, vec![42u8; 500]);

    let wire_bytes = audio_segment.to_wire_bytes().expect("serialize");
    let wire_segment = WireSegment::from_bytes(&wire_bytes).expect("deserialize");

    assert_eq!(wire_segment.media_type, MediaType::Audio.to_u8());
    assert_eq!(wire_segment.sequence, 10);
    assert!(!wire_segment.is_keyframe);
}

/// Test initialization segment wire format
#[test]
fn test_init_segment_wire_format() {
    let stream_id = StreamId::new("init_test");

    // Fake fMP4 init segment header
    let init_data = vec![
        0x00, 0x00, 0x00, 0x1C, // size
        0x66, 0x74, 0x79, 0x70, // 'ftyp'
        0x69, 0x73, 0x6f, 0x6d, // 'isom'
    ];

    let init_segment = StreamSegment::initialization(stream_id, init_data.clone());

    let wire_bytes = init_segment.to_wire_bytes().expect("serialize");
    let wire_segment = WireSegment::from_bytes(&wire_bytes).expect("deserialize");

    assert_eq!(wire_segment.media_type, MediaType::Initialization.to_u8());
    assert_eq!(wire_segment.sequence, 0);
    assert!(wire_segment.is_keyframe);
    assert_eq!(wire_segment.data, init_data);
}

/// Test SegmentBuilder produces sequential segment numbers
#[test]
fn test_segment_builder_sequencing() {
    let stream_id = StreamId::new("seq_test");
    let mut builder = SegmentBuilder::new(stream_id);

    // Generate video segments
    let v1 = builder.next_video(0, 33333, vec![1], true);
    let v2 = builder.next_video(33333, 33333, vec![2], false);
    let v3 = builder.next_video(66666, 33333, vec![3], false);

    assert_eq!(v1.sequence, 0);
    assert_eq!(v2.sequence, 1);
    assert_eq!(v3.sequence, 2);

    // Audio has separate sequence
    let a1 = builder.next_audio(0, 20000, vec![4]);
    let a2 = builder.next_audio(20000, 20000, vec![5]);

    assert_eq!(a1.sequence, 0);
    assert_eq!(a2.sequence, 1);

    // Video continues its sequence
    let v4 = builder.next_video(99999, 33333, vec![6], true);
    assert_eq!(v4.sequence, 3);
}

/// Test StreamRelay catchup chunks for late joiners
#[test]
fn test_late_joiner_catchup() {
    let stream_id = OverlayStreamId::from_string("catchup_test");
    let publisher = libp2p::PeerId::random();
    let mut relay = StreamRelay::new(stream_id.clone(), publisher);

    // Add init segment
    let init_chunk = StreamChunk {
        id: 0,
        stream_id: stream_id.clone(),
        data: bytes::Bytes::from(vec![0u8; 100]),
        timestamp: 0,
        sequence: 0,
        content_type: "init".to_string(),
        is_keyframe: true,
        source: None,
    };
    relay.add_chunk(init_chunk, 100);

    // Add some video chunks
    for i in 1..=10 {
        let is_keyframe = i % 5 == 1; // Keyframes at 1, 6
        let chunk = StreamChunk {
            id: i,
            stream_id: stream_id.clone(),
            data: bytes::Bytes::from(vec![i as u8; 50]),
            timestamp: i as u64 * 33,
            sequence: i,
            content_type: "video".to_string(),
            is_keyframe,
            source: None,
        };
        relay.add_chunk(chunk, 100);
    }

    // Get catchup chunks
    let catchup = relay.get_catchup_chunks();

    // Should have: init + last keyframe (6) + chunks 7-10
    assert!(!catchup.is_empty());
    assert_eq!(catchup[0].content_type, "init");

    // Find the keyframe
    let keyframe_idx = catchup
        .iter()
        .position(|c| c.sequence == 6 && c.is_keyframe);
    assert!(keyframe_idx.is_some());
}

/// Test StreamRelay keyframe buffer limit
#[test]
fn test_keyframe_buffer_limit() {
    let stream_id = OverlayStreamId::from_string("keyframe_test");
    let publisher = libp2p::PeerId::random();
    let mut relay = StreamRelay::with_keyframe_buffer_size(stream_id.clone(), publisher, 2);

    // Add 5 keyframes
    for i in 0..5 {
        let chunk = StreamChunk {
            id: i * 10,
            stream_id: stream_id.clone(),
            data: bytes::Bytes::from(vec![i as u8; 50]),
            timestamp: i as u64 * 100,
            sequence: i * 10,
            content_type: "video".to_string(),
            is_keyframe: true,
            source: None,
        };
        relay.add_chunk(chunk, 100);
    }

    // Should only have last 2 keyframes
    assert_eq!(relay.keyframe_count(), 2);

    let keyframes = relay.get_keyframes();
    assert_eq!(keyframes[0].sequence, 30); // 4th keyframe
    assert_eq!(keyframes[1].sequence, 40); // 5th keyframe
}

/// Test conversion between StreamSegment and StreamChunk (overlay type)
#[test]
fn test_segment_to_overlay_chunk_conversion() {
    let segment = StreamSegment::video(
        StreamId::new("conv_test"),
        42,
        1000000, // 1 second in microseconds
        33333,
        vec![1, 2, 3, 4, 5],
        true,
    );

    // Convert to overlay chunk
    let chunk = segment.to_overlay_chunk(100);

    assert_eq!(chunk.id, 100);
    assert_eq!(chunk.sequence, 42);
    assert_eq!(chunk.timestamp, 1000); // milliseconds
    assert_eq!(chunk.content_type, "video");
    assert!(chunk.is_keyframe);
    assert_eq!(chunk.data, vec![1, 2, 3, 4, 5]);
}

/// Test conversion from overlay StreamChunk to StreamSegment
#[test]
fn test_overlay_chunk_to_segment_conversion() {
    let stream_id = OverlayStreamId::from_string("conv_test");

    let chunk = StreamChunk {
        id: 1,
        stream_id,
        data: bytes::Bytes::from(vec![10, 20, 30]),
        timestamp: 500, // milliseconds
        sequence: 5,
        content_type: "audio".to_string(),
        is_keyframe: false,
        source: None,
    };

    // Convert to StreamSegment
    let segment: StreamSegment = chunk.into();

    assert_eq!(segment.sequence, 5);
    assert_eq!(segment.pts_us, 500_000); // converted to microseconds
    assert_eq!(segment.media_type, MediaType::Audio);
    assert!(!segment.is_keyframe);
}

/// Test wire format is more compact than JSON
#[test]
fn test_wire_format_efficiency() {
    let segment = StreamSegment::video(
        StreamId::new("efficiency_test_stream"),
        12345,
        98765432100,
        33333,
        vec![0u8; 500], // 500 bytes of data
        true,
    );

    let wire: WireSegment = (&segment).into();

    let bincode_bytes = wire.to_bytes().expect("bincode serialize");
    let json_bytes = serde_json::to_vec(&wire).expect("json serialize");

    // Bincode should be smaller
    assert!(
        bincode_bytes.len() < json_bytes.len(),
        "bincode ({}) should be smaller than json ({})",
        bincode_bytes.len(),
        json_bytes.len()
    );

    // At least 20% smaller
    let ratio = bincode_bytes.len() as f64 / json_bytes.len() as f64;
    assert!(ratio < 0.8, "Expected ratio < 0.8, got {}", ratio);
}

/// Test metadata segment creation
#[test]
fn test_metadata_segment() {
    let stream_id = StreamId::new("meta_test");

    let metadata_json = r#"{"title":"Test Stream","codec":"h264"}"#;
    let segment = StreamSegment::metadata(stream_id.clone(), 0, metadata_json.as_bytes().to_vec());

    assert_eq!(segment.media_type, MediaType::Metadata);
    assert_eq!(segment.sequence, 0);
    assert!(!segment.is_keyframe);
    assert_eq!(segment.track_id, 0);
}
