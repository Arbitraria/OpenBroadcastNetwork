# Test Files Reference Guide

This document describes all test video files in the OpenBroadcastNetwork project, their characteristics, and browser compatibility.

## Test File Overview

| File | Size | Primary Use | Video Codec | Audio Codec | Browser Support |
|------|------|-------------|-------------|-------------|-----------------|
| `mse_compatible_video.mp4` | 850 MB | MSE testing | H.264 | AAC | ✅ All browsers |
| `Stargate SG1 S01E03.mp4` | 360 MB | AC-3 testing | H.264 | AC-3 | ⚠️ Video-only |
| `bigtroublelittlechina.mp4` | 1.97 GB | Large file test | Unknown | Unknown | ❓ Untested |
| `fragmented_video.mp4` | 257 MB | fMP4 testing | H.264 | AAC | ✅ Should work |
| `sample_video.mp4` | 257 MB | General testing | H.264 | AAC | ⚠️ Chrome issues |
| `video_only.mp4` | 213 MB | No audio test | H.264 | None | ✅ All browsers |
| `test_fragmented.mp4` | 141 KB | Small fMP4 | H.264 | AAC | ✅ All browsers |
| `test_short.mp4` | 146 KB | Quick tests | H.264 | AAC | ✅ All browsers |
| `test_simple.mp4` | 32 KB | Minimal test | H.264 | AAC | ✅ All browsers |
| `test_video.mp4` | 280 KB | Basic testing | H.264 | AAC | ✅ All browsers |

## Detailed File Analysis

### 1. `mse_compatible_video.mp4` ✅ **RECOMMENDED**
- **Purpose**: Primary test file for MSE compatibility
- **Characteristics**:
  - Pre-fragmented MP4 format
  - Small initialization segment (2.7 KB)
  - Compatible AAC audio encoding
  - 1,563 segments total
- **Browser Support**: 
  - ✅ Firefox: Full playback with audio
  - ✅ Chrome: Should work (AAC compatible)
  - ✅ Safari: Expected to work
- **Known Issues**: None
- **Usage**: Best file for testing basic streaming functionality

### 2. `Stargate SG1 S01E03.mp4` ⚠️ **AC-3 AUDIO**
- **Purpose**: Testing unsupported audio codec handling
- **Characteristics**:
  - Regular MP4 (not fragmented)
  - AC-3/Dolby Digital audio track
  - Large initialization segment (1.6 MB)
  - 5,475 segments when fragmented
- **Browser Support**:
  - ⚠️ Firefox: Video-only (no AC-3 support)
  - ❌ Chrome: Video-only (no AC-3 support)
  - ⚠️ Safari: Limited AC-3 support
- **Known Issues**: 
  - Requires WebSocket frame chunking for init segment
  - Audio codec not supported in browsers
- **Usage**: Testing codec fallback and video-only playback

### 3. `sample_video.mp4` ⚠️ **CHROME ISSUES**
- **Purpose**: General testing, but has Chrome compatibility issues
- **Characteristics**:
  - H.264 video with AAC audio
  - May have objectTypeIndication 0x40 in ESDS
- **Browser Support**:
  - ✅ Firefox: Works
  - ❌ Chrome: "audio object type 0x40 does not match" error
- **Known Issues**: Chrome MediaSource validation failure
- **Usage**: Testing Chrome codec validation issues

### 4. `fragmented_video.mp4` ✅ **PRE-FRAGMENTED**
- **Purpose**: Testing with already fragmented MP4
- **Characteristics**:
  - Pre-fragmented format (fMP4)
  - Should have proper moof/mdat structure
  - AAC audio codec
- **Browser Support**: Should work in all browsers
- **Usage**: Testing fragmented MP4 handling

### 5. `video_only.mp4` ✅ **NO AUDIO TRACK**
- **Purpose**: Testing video-only streaming
- **Characteristics**:
  - H.264 video track only
  - No audio complications
  - 213 MB size
- **Browser Support**: Works in all browsers
- **Usage**: Isolating video-related issues

### 6. Test Files (test_*.mp4) ✅ **QUICK TESTING**
- **Purpose**: Small files for rapid testing
- **Characteristics**:
  - Very small sizes (32 KB - 280 KB)
  - Quick to load and stream
  - Standard H.264/AAC codecs
- **Browser Support**: Should work everywhere
- **Usage**: Quick functionality tests

## Testing Recommendations

### For Chrome Testing:
1. Start with `mse_compatible_video.mp4` (most likely to work)
2. Try `test_short.mp4` or `test_simple.mp4` (small, standard encoding)
3. Use `video_only.mp4` to isolate audio codec issues

### For Firefox Testing:
1. Any file except those with AC-3 audio should work
2. `mse_compatible_video.mp4` is proven to work

### For Codec Compatibility Testing:
1. `Stargate SG1 S01E03.mp4` - Tests AC-3 fallback
2. `sample_video.mp4` - Tests Chrome ESDS validation
3. `video_only.mp4` - Baseline video-only test

## How to Use Test Files

```bash
# Start server with specific test file
cargo run -p OpenBroadcastNetwork-node web-viewer --video "test_file.mp4"

# Recommended for initial testing
cargo run -p OpenBroadcastNetwork-node web-viewer --video "mse_compatible_video.mp4"

# For Chrome debugging
cargo run -p OpenBroadcastNetwork-node web-viewer --video "test_simple.mp4"

# For AC-3 codec testing
cargo run -p OpenBroadcastNetwork-node web-viewer --video "Stargate SG1 S01E03.mp4"
```

## Creating New Test Files

If you need to create MSE-compatible test files:

```bash
# Using FFmpeg to create a fragmented MP4
ffmpeg -i input.mp4 -c:v copy -c:a aac -b:a 128k -movflags frag_keyframe+empty_moov output_fragmented.mp4

# Create video-only file
ffmpeg -i input.mp4 -c:v copy -an output_video_only.mp4

# Ensure compatible AAC encoding
ffmpeg -i input.mp4 -c:v copy -c:a aac -profile:a aac_low -ac 2 -ar 48000 output_compatible.mp4
```

## Known Issues by File

### Chrome Specific:
- `sample_video.mp4`: ESDS objectTypeIndication 0x40 rejection
- Potentially others with similar AAC encoding

### Firefox Specific:
- Large init segments (>1MB) require WebSocket chunking
- AC-3 audio not supported

### General:
- Non-fragmented MP4s require conversion to fMP4
- Mixed codec files may cause playback issues

## Test File Selection Guide

```
Need to test basic streaming?
  → Use mse_compatible_video.mp4

Testing Chrome compatibility?
  → Start with test_simple.mp4
  → Then try mse_compatible_video.mp4

Testing codec fallback?
  → Use Stargate SG1 S01E03.mp4

Testing large files?
  → Use bigtroublelittlechina.mp4

Testing fragmented MP4?
  → Use fragmented_video.mp4 or test_fragmented.mp4

Quick functionality test?
  → Use test_short.mp4 or test_simple.mp4
```

This guide should be updated as new test files are added or compatibility issues are discovered.