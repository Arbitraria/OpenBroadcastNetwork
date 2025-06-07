# Test Utilities

This folder contains various test scripts and utilities used during the development of OpenBroadcastNetwork.

## Categories

### Python Test Scripts
- `test_*.py` - Various test scripts for codec compatibility, WebSocket connections, and browser simulation
- `analyze_*.py` - Analysis scripts for MP4/ESDS structure debugging
- `fix_*.py` - Scripts used to diagnose and fix codec issues

### JavaScript Test Scripts
- `test_*.js` - Browser-side test scripts for MediaSource API and WebSocket testing

### Shell Scripts
- `demo.sh` - Interactive demonstration of the streaming network
- `simple_demo.sh` - Simplified demo script
- `prepare_video.sh` - Convert videos to MSE-compatible format
- `fix_codec_format.sh` - Codec string format fixing utility
- `test_websocket_codec.sh` - WebSocket codec testing

## Usage

Most scripts can be run from this directory:

```bash
# Python scripts
python3 test_chrome_codec.py

# Shell scripts (may need to be run from project root)
cd .. && ./test_utils/demo.sh

# JavaScript scripts (for browser testing)
# Copy to web_viewer directory or run with Node.js
```

## Historical Context

These scripts were created during the development process to:
1. Debug Chrome's AAC codec compatibility issues
2. Test WebSocket message ordering and timing
3. Analyze MP4 box structures and ESDS configurations
4. Simulate browser behavior for MediaSource Extensions

Many of these scripts helped solve the "audio object type 0x2 does not match" error that was resolved by fixing the AudioSpecificConfig in ESDS boxes.