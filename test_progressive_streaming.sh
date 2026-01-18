#!/bin/bash

# Test progressive streaming with AC-3 video

set -e

echo "=== Testing Progressive Streaming ==="
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Find an AC-3 video file
AC3_FILE=""
if [ -f "Stargate SG1 S01E03.mp4" ]; then
    AC3_FILE="Stargate SG1 S01E03.mp4"
elif [ -f "bigtroublelittlechina.mp4" ]; then
    AC3_FILE="bigtroublelittlechina.mp4"
else
    echo -e "${RED}Error: No AC-3 video file found${NC}"
    echo "Please ensure 'Stargate SG1 S01E03.mp4' or 'bigtroublelittlechina.mp4' is in the current directory"
    exit 1
fi

echo -e "${GREEN}Using AC-3 video file: $AC3_FILE${NC}"
echo

# Check if the file has AC-3 audio
echo "Checking audio codec..."
if ffprobe -v quiet -print_format json -show_streams "$AC3_FILE" | grep -q "ac3\|ac-3\|eac3"; then
    echo -e "${GREEN}✓ File contains AC-3 audio - perfect for testing progressive transcoding${NC}"
else
    echo -e "${YELLOW}Warning: File may not contain AC-3 audio${NC}"
fi
echo

# Start the server
echo "Starting server with progressive streaming..."
echo -e "${YELLOW}Server will transcode AC-3 to AAC progressively${NC}"
echo

# Kill any existing server
pkill -f "OpenBroadcastNetwork-node" || true
sleep 1

# Start server in background with logging
echo "Starting server..."
RUST_LOG=info,OpenBroadcastNetwork_core::media::streaming_transcoder=debug \
    cargo run -p OpenBroadcastNetwork-node -- web-viewer --video "$AC3_FILE" > test_progressive.log 2>&1 &

SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to start
echo "Waiting for server to initialize..."
sleep 5

# Check if server started
if ! ps -p $SERVER_PID > /dev/null; then
    echo -e "${RED}Server failed to start!${NC}"
    echo "Last 20 lines of log:"
    tail -20 test_progressive.log
    exit 1
fi

echo -e "${GREEN}✓ Server started successfully${NC}"
echo

# Show relevant log lines
echo "Checking for progressive transcoding activity..."
if grep -q "Starting progressive transcoding" test_progressive.log; then
    echo -e "${GREEN}✓ Progressive transcoding initiated${NC}"
    echo
    echo "Transcoding progress:"
    grep -E "(progressive transcoding|FFmpeg process|transcoded segment|initialization segment)" test_progressive.log | tail -10
else
    echo -e "${YELLOW}⚠ Progressive transcoding may not have started${NC}"
    echo "This could mean the file doesn't need transcoding or there was an error"
fi

echo
echo "=== Server is running ==="
echo -e "${GREEN}Web viewer available at: http://127.0.0.1:8080/${NC}"
echo -e "${YELLOW}Open this URL in Chrome or Firefox to test progressive streaming${NC}"
echo
echo "Watch the logs for progressive segment delivery:"
echo "  tail -f test_progressive.log | grep -E '(segment|transcode|progressive)'"
echo
echo "Press Ctrl+C to stop the server"
echo

# Keep script running
trap "echo 'Stopping server...'; kill $SERVER_PID 2>/dev/null; exit" INT
wait $SERVER_PID