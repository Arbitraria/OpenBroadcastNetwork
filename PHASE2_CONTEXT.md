# Phase 2 Context and Development Guide

## 🎯 Current State Summary (December 8, 2025)

### What's Working
1. **Core P2P Infrastructure**: libp2p 0.53.0 integration complete with DHT discovery, GossipSub messaging
2. **Media Streaming**: Basic MP4 parsing, fragmentation, and MSE-compatible segment generation
3. **Web Viewer**: Universal viewer that handles both chunked and non-chunked initialization segments
4. **Firefox Playback**: Successfully streaming video with AAC audio codecs
5. **AC-3 Detection**: Proper codec detection with fallback to video-only when audio unsupported

### What's Not Working
1. **Chrome Playback**: Persistent "audio object type 0x40 does not match" error despite standards compliance
2. **Mixed Codec Files**: Files with AC-3 audio create "file corrupt" errors when using video-only mode
3. **Proper fMP4 Generation**: Current pseudo-fragmentation doesn't create valid fragmented MP4

## 📁 Critical Files for Phase 2

### Media Processing Pipeline
```
core/src/media/
├── mp4_parser.rs          # Main MP4 parsing and MSE segment generation
├── fragment_parser.rs     # Read-only MP4 box parsing operations
├── fragment_writer.rs     # MSE segment writing and box creation
├── fmp4_converter.rs      # Proper fragmented MP4 converter (NOT INTEGRATED)
├── video_reader.rs        # Video file reading and metadata extraction
└── pipeline.rs           # Media processing pipeline (needs work)
```

### Web Streaming Server
```
node/src/
├── main.rs               # CLI entry point and command handling
└── web_server.rs         # WebSocket streaming server implementation
    - handle_websocket()  # WebSocket connection handler
    - stream_video()      # Video streaming loop
    - Lines 615-700      # WebSocket frame chunking for large segments
```

### Web Viewer
```
web_viewer/
├── universal_viewer.html  # Main viewer with codec fallback logic
│   - Lines 306-321       # Codec fallback implementation
│   - Lines 378-387       # Non-chunked init segment handling
├── firefox_debug.html    # Firefox-specific debugging viewer
└── index.html           # Original viewer (Chrome-focused)
```

## 🔧 Key Technical Challenges

### 1. Chrome MediaSource Validation
**Problem**: Chrome has stricter validation than Firefox for AAC audio codecs
- Expects objectTypeIndication in ESDS to match codec string
- Current files have 0x40 which Chrome rejects
- Extensive testing documented in ATTEMPTED_SOLUTIONS_REFERENCE.md

**Potential Solutions**:
- Use different test videos with Chrome-compatible AAC encoding
- Implement server-side transcoding to ensure compatible codecs
- Create custom MP4 muxer that generates Chrome-friendly ESDS

### 2. Proper Fragmented MP4 Generation
**Current State**: Pseudo-fragmentation that creates simple moof/mdat pairs
**Need**: Proper fMP4 with complete box hierarchy:
```
moof (Movie Fragment)
├── mfhd (Movie Fragment Header)
└── traf (Track Fragment)
    ├── tfhd (Track Fragment Header)
    ├── tfdt (Track Fragment Decode Time)
    └── trun (Track Fragment Run)
mdat (Media Data)
```

**Implementation Path**:
1. Integrate `fmp4_converter.rs` into the media pipeline
2. Replace simple segment generation in `mp4_parser.rs`
3. Ensure proper timing and sample tables

### 3. Multi-Track Handling
**Issue**: Video-only fallback still includes audio track data
**Solution**: Implement proper track filtering when creating segments
- Parse stbl (Sample Table) to identify track boundaries
- Only include requested tracks in moof/mdat generation
- Update tkhd (Track Header) flags appropriately

## 🚀 Phase 2 Development Priorities

### Immediate (Week 1)
1. **Fix Chrome Playback**
   - Test with known Chrome-compatible videos
   - Implement ESDS rewriting if needed
   - Document Chrome-specific requirements

2. **Integrate Proper fMP4 Generation**
   - Complete `fmp4_converter.rs` implementation
   - Replace pseudo-fragmentation
   - Test with both browsers

3. **Improve Error Handling**
   - Better codec mismatch detection
   - Clearer error messages in viewer
   - Automatic fallback strategies

### Short Term (Weeks 2-3)
1. **Enhanced Media Pipeline**
   - Implement `MediaPipeline` trait properly
   - Add quality adaptation logic
   - Support for live streaming

2. **P2P Streaming**
   - Connect media pipeline to overlay network
   - Implement chunk distribution via GossipSub
   - Add peer-to-peer relay functionality

3. **Performance Optimization**
   - Reduce segment generation overhead
   - Implement segment caching
   - Add bandwidth adaptation

### Medium Term (Month 2)
1. **Production Features**
   - Authentication and access control
   - Stream discovery and metadata
   - Analytics and monitoring

2. **Scalability**
   - Geo-aware peer selection
   - Adaptive bitrate streaming
   - CDN-like caching strategies

## 🛠️ Development Workflow Tips

### Testing Video Files
```bash
# Current test files and their codecs:
- mse_compatible_video.mp4  # H.264 + AAC (works in both browsers)
- sample_video.mp4         # H.264 + AAC (may have Chrome issues)
- Stargate*.mp4           # H.264 + AC-3 (no audio in browsers)
- test_*.mp4              # Various test files

# Start server with specific video:
cargo run -p OpenBroadcastNetwork-node web-viewer --video "mse_compatible_video.mp4"
```

### Debugging Commands
```bash
# Check codec detection:
grep -n "Detected.*codec" server_logs.txt

# Check ESDS processing:
grep -n "AudioSpecificConfig" server_logs.txt

# Monitor WebSocket messages:
grep -n "WEBSOCKET DEBUG" server_logs.txt
```

### Browser Testing
1. **Firefox**: More forgiving, supports wider codec range
2. **Chrome**: Strict validation, use for final testing
3. **Check console**: Both viewers have detailed logging

## 📚 Essential Background Reading

1. **ATTEMPTED_SOLUTIONS_REFERENCE.md**: Complete history of codec compatibility attempts
2. **CODEC_COMPATIBILITY_ANALYSIS_FINAL.md**: Deep dive into Chrome codec validation
3. **CODE_ORGANIZATION.md**: Architectural patterns and module structure
4. **TYPE_REFERENCE.md**: Type system and conversion patterns
5. **MEMORIES.md**: Development journey and key insights

## 🎯 Success Metrics for Phase 2

1. ✅ Chrome and Firefox both play video with audio
2. ✅ Proper fragmented MP4 generation
3. ✅ P2P streaming between multiple nodes
4. ✅ < 3 second stream startup time
5. ✅ Adaptive quality based on bandwidth
6. ✅ 95%+ playback success rate

## 🐛 Known Issues and Workarounds

1. **Chrome "object type 0x40" error**
   - Workaround: Use mse_compatible_video.mp4
   - Long-term: Need ESDS rewriting or transcoding

2. **AC-3 audio files**
   - Workaround: Video-only playback in Firefox
   - Long-term: Implement audio transcoding

3. **Large initialization segments**
   - Workaround: WebSocket chunking implemented
   - Long-term: Optimize segment generation

## 💡 Architecture Insights

### Overlay Network Integration
The media pipeline needs to integrate with the overlay network for P2P distribution:
```rust
// Current: Direct WebSocket streaming
web_server -> WebSocket -> Browser

// Target: P2P distribution
web_server -> Overlay Network -> Peers -> Browser
           -> RelayManager -> StreamChunk distribution
           -> GossipSub -> Chunk announcements
```

### Stream Flow Architecture
```
1. Video File -> VideoReader -> MP4Parser
2. MP4Parser -> MSE Segments -> StreamManager
3. StreamManager -> Overlay::publish() -> Peers
4. Peers -> Overlay::subscribe() -> Local Cache
5. Local Cache -> WebSocket -> Browser MSE
```

### Concurrency Model
- Use `Arc<Mutex<T>>` for shared mutable state
- Use `Arc<RwLock<T>>` for read-heavy structures
- All async operations use tokio runtime
- Manager types are Arc-wrapped for sharing

This document provides the essential context for continuing Phase 2 development. The immediate priority is resolving Chrome playback issues and implementing proper fragmented MP4 generation.