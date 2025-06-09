# Overlay Network Integration Guide for Phase 2

## Overview

This guide documents how to integrate the media streaming pipeline with the P2P overlay network for distributed video delivery.

## Current Architecture (WebSocket Direct)

```
Video File → MP4Parser → WebSocket → Browser
```

## Target Architecture (P2P Distribution)

```
Video File → MP4Parser → Overlay Network → Peers → Browser
                              ↓
                         GossipSub Topics
                              ↓
                        Relay Management
```

## Integration Points

### 1. Stream Publishing (node/src/web_server.rs)

Current implementation sends segments directly via WebSocket:
```rust
// Current: Direct sending
sender.send(Message::Binary(segment.data)).await
```

Target implementation should publish to overlay:
```rust
// Target: P2P publishing
overlay.publish_to_topic(
    &Topic::new(&stream_id), 
    MediaChunk {
        stream_id: stream_id.clone(),
        sequence: chunk_index,
        timestamp,
        data: segment.data,
        is_keyframe: segment.is_keyframe,
    }
).await?;
```

### 2. Stream Subscription (Peer Side)

Peers need to subscribe to stream topics:
```rust
// Subscribe to stream
overlay.subscribe_to_topic(&Topic::new(&stream_id)).await?;

// Handle incoming chunks
while let Some(chunk) = overlay.receive_chunk().await {
    // Cache chunk locally
    chunk_cache.insert(chunk.sequence, chunk);
    
    // Forward to local WebSocket clients
    broadcast_to_clients(chunk).await;
}
```

### 3. Relay Management Integration

The RelayManager should coordinate chunk distribution:

```rust
// core/src/overlay/relay/manager.rs
impl RelayManager {
    /// Relay a media chunk to downstream peers
    pub async fn relay_chunk(
        &self,
        stream_id: &StreamId,
        chunk: MediaChunk,
    ) -> Result<(), OverlayError> {
        // Get relay tree for stream
        let relay_tree = self.get_stream_tree(stream_id)?;
        
        // Forward to children in tree
        for child in relay_tree.children {
            self.send_to_peer(child, chunk.clone()).await?;
        }
        
        Ok(())
    }
}
```

### 4. GossipSub Topic Structure

Streams should use hierarchical topics:
```
/openbroadcast/stream/{stream_id}/manifest    # Stream metadata
/openbroadcast/stream/{stream_id}/init        # Initialization segment
/openbroadcast/stream/{stream_id}/media       # Media segments
/openbroadcast/stream/{stream_id}/control     # Control messages
```

### 5. Chunk Caching Strategy

Implement a sliding window cache:
```rust
pub struct ChunkCache {
    /// Maximum chunks to keep in memory
    max_size: usize,
    /// LRU cache of chunks
    chunks: LruCache<(StreamId, u64), MediaChunk>,
    /// Stats for cache hits/misses
    stats: CacheStats,
}
```

## Implementation Steps

### Phase 2.1: Basic P2P Streaming
1. Modify web_server.rs to publish chunks to overlay
2. Create peer-side chunk receiver
3. Implement basic chunk caching
4. Test with 2-3 nodes

### Phase 2.2: Relay Tree Integration
1. Integrate with RelayManager for tree-based distribution
2. Implement parent/child relationships
3. Add chunk request/response protocol
4. Handle peer churn and tree rebalancing

### Phase 2.3: Quality and Performance
1. Add adaptive bitrate support
2. Implement chunk prioritization
3. Add bandwidth monitoring
4. Optimize cache management

## Key Files to Modify

### Publishing Side
- `node/src/web_server.rs::stream_video()` - Add overlay publishing
- `node/src/web_server.rs::handle_websocket()` - Keep for edge clients

### Relay Side
- `core/src/overlay/relay/manager.rs` - Add media chunk support
- `core/src/overlay/relay/stream.rs` - Implement chunk forwarding
- `core/src/media/stream.rs` - Add P2P stream types

### Receiving Side
- Create `node/src/p2p_receiver.rs` - Chunk reception logic
- Create `node/src/chunk_cache.rs` - Local caching

## Protocol Messages

### ChunkRequest
```rust
pub struct ChunkRequest {
    pub stream_id: StreamId,
    pub sequence: u64,
    pub requester: LocalPeerId,
}
```

### ChunkResponse
```rust
pub struct ChunkResponse {
    pub stream_id: StreamId,
    pub chunk: Option<MediaChunk>,
    pub available_sequences: Vec<u64>,
}
```

## Testing Strategy

1. **Unit Tests**: Test chunk caching, serialization
2. **Integration Tests**: Test 2-node streaming scenarios
3. **Load Tests**: Test with 10+ nodes and measure latency
4. **Failure Tests**: Test peer disconnection handling

## Performance Considerations

- Chunk size: 64KB for low latency, 256KB for efficiency
- Cache size: 100-500 chunks depending on memory
- Gossip interval: 100-200ms for chunk announcements
- Tree depth: Max 3-4 levels for low latency

## Security Considerations

- Sign chunks with publisher's key
- Verify chunk integrity before caching
- Rate limit chunk requests per peer
- Implement chunk deduplication

This integration will transform OpenBroadcastNetwork from a simple WebSocket streamer to a true P2P content delivery network.