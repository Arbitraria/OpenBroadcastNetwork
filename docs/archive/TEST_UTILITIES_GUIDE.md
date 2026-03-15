# Test Utilities Guide

This directory contains various testing scripts and utilities developed during the codec compatibility investigation. These tools were crucial in diagnosing and resolving browser playback issues.

## 🔍 Codec Analysis Tools

### ESDS Analysis Scripts
Tools for analyzing AAC codec configuration in MP4 files.

#### `analyze_esds_structure.py`
- **Purpose**: Deep analysis of ESDS (Elementary Stream Descriptor) box structure
- **Usage**: `python analyze_esds_structure.py`
- **Output**: Detailed ESDS box hierarchy and values

#### `proper_esds_analysis.py`
- **Purpose**: Validates ESDS structure against specifications
- **Usage**: `python proper_esds_analysis.py`
- **Key Feature**: Checks objectTypeIndication values

#### `analyze_audiospecificconfig.py`
- **Purpose**: Analyzes AudioSpecificConfig within ESDS
- **Usage**: `python analyze_audiospecificconfig.py`
- **Output**: AAC profile, sample rate, channel configuration

#### `test_esds_detailed.py`
- **Purpose**: Comprehensive ESDS binary data analysis
- **Usage**: `python test_esds_detailed.py`
- **Output**: Byte-by-byte ESDS structure breakdown

### Codec Detection Scripts

#### `test_codec_simple.py`
- **Purpose**: Quick codec detection from server stream
- **Usage**: `python test_codec_simple.py`
- **Output**: Codec string received from WebSocket

#### `comprehensive_codec_test.py`
- **Purpose**: Full codec compatibility test suite
- **Usage**: `python comprehensive_codec_test.py`
- **Tests**: Multiple codec formats and variations

#### `codec_verification_summary.py`
- **Purpose**: Summarizes codec test results
- **Usage**: `python codec_verification_summary.py`
- **Output**: Compatibility matrix

## 🌐 Browser Testing Tools

### Chrome-Specific Tests

#### `test_chrome_codec.py`
- **Purpose**: Automated Chrome codec testing using Selenium
- **Usage**: `python test_chrome_codec.py`
- **Requirements**: Chrome WebDriver
- **Output**: Chrome MediaSource compatibility results

#### `test_browser_codec.js`
- **Purpose**: Puppeteer-based Chrome testing
- **Usage**: `node test_browser_codec.js`
- **Requirements**: Puppeteer npm package
- **Tests**: MediaSource.isTypeSupported() for various codecs

### WebSocket Testing

#### `test_websocket_connection.js`
- **Purpose**: Basic WebSocket connectivity test
- **Usage**: `node test_websocket_connection.js`
- **Output**: Connection status and messages received

#### `test_websocket_order.py`
- **Purpose**: Validates WebSocket message sequence
- **Usage**: `python test_websocket_order.py`
- **Checks**: Proper order of stream_info, chunk_info, binary data

#### `test_simple_connectivity.py`
- **Purpose**: Minimal WebSocket connection test
- **Usage**: `python test_simple_connectivity.py`
- **Output**: First few messages from server

### Browser Test Pages

#### `firefox_mse_test.html`
- **Purpose**: Manual Firefox MSE testing page
- **Usage**: Open in Firefox while server is running
- **Features**: Detailed logging, codec support detection

## 🛠️ Utility Scripts

### Video Preparation

#### `prepare_video.sh`
- **Purpose**: Prepares video files for MSE streaming
- **Usage**: `./prepare_video.sh input.mp4 output.mp4`
- **Actions**: 
  - Converts to fragmented MP4
  - Ensures compatible codecs
  - Optimizes for streaming

#### `fix_codec_format.sh`
- **Purpose**: Attempts to fix codec string formats
- **Usage**: `./fix_codec_format.sh`
- **Note**: Part of the codec investigation process

### Demo Scripts

#### `simple_demo.sh`
- **Purpose**: Quick demo setup
- **Usage**: `./simple_demo.sh`
- **Actions**: Starts server with default test video

#### `demo.sh`
- **Purpose**: Full demo with multiple test scenarios
- **Usage**: `./demo.sh`
- **Features**: Cycles through different test files

## 📊 Testing Workflows

### 1. Codec Compatibility Check
```bash
# Start server
cargo run -p OpenBroadcastNetwork-node web-viewer --video test_video.mp4

# In another terminal, run codec test
python test_utils/test_codec_simple.py

# Check ESDS structure
python test_utils/analyze_esds_structure.py
```

### 2. Browser Compatibility Test
```bash
# Test Chrome support
node test_utils/test_browser_codec.js

# Test WebSocket message order
python test_utils/test_websocket_order.py
```

### 3. Video Preparation
```bash
# Prepare a video for MSE
./test_utils/prepare_video.sh input.mp4 output_mse.mp4

# Test the prepared video
cargo run -p OpenBroadcastNetwork-node web-viewer --video output_mse.mp4
```

## 🔧 Requirements

### Python Scripts
```bash
pip install websocket-client
pip install requests
```

### Node.js Scripts
```bash
npm install ws
npm install puppeteer  # For browser automation
```

### Shell Scripts
- Require `ffmpeg` for video processing
- Require `curl` for HTTP requests

## 📝 Key Findings from Tests

1. **Chrome Codec Validation**: Chrome strictly validates objectTypeIndication must match codec string
2. **ESDS Structure**: AudioSpecificConfig must have correct audioObjectType
3. **WebSocket Sequence**: Must send stream_info before binary data
4. **Frame Size Limits**: Firefox has 1MB WebSocket frame limit

## 🚨 Common Issues Discovered

1. **"object type 0x40 does not match"**: Chrome rejecting AAC with certain ESDS values
2. **WebSocket 1009 error**: Frame size exceeded (Firefox)
3. **Empty buffered ranges**: Codec mismatch or invalid segments
4. **"File corrupt" error**: Mixed codec tracks in video-only mode

These utilities were instrumental in diagnosing and resolving the codec compatibility issues documented in ATTEMPTED_SOLUTIONS_REFERENCE.md.