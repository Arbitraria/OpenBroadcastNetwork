#!/bin/bash
# Script to prepare a sample video for OpenBroadcastNetwork streaming
# This converts any video to a format suitable for MSE streaming

INPUT_VIDEO="$1"
OUTPUT_VIDEO="sample_video.mp4"

if [ -z "$INPUT_VIDEO" ]; then
    echo "Usage: $0 <input_video_file>"
    echo "Example: $0 my_video.mov"
    echo ""
    echo "This script converts any video file to an MSE-compatible format:"
    echo "- H.264 video codec (baseline profile)"
    echo "- AAC audio codec"
    echo "- MP4 container with fragmented format"
    echo "- Optimized for streaming"
    exit 1
fi

if [ ! -f "$INPUT_VIDEO" ]; then
    echo "❌ Error: Input file '$INPUT_VIDEO' not found"
    exit 1
fi

echo "🎬 Converting $INPUT_VIDEO to MSE-compatible format..."
echo ""

# Convert to H.264/AAC MP4 with proper settings for MSE
ffmpeg -i "$INPUT_VIDEO" \
    -c:v libx264 \
    -profile:v baseline \
    -level 3.0 \
    -pix_fmt yuv420p \
    -preset medium \
    -crf 23 \
    -maxrate 2M \
    -bufsize 4M \
    -c:a aac \
    -ar 48000 \
    -ac 2 \
    -b:a 128k \
    -movflags +faststart+frag_keyframe+empty_moov \
    -f mp4 \
    -y \
    "$OUTPUT_VIDEO"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Successfully created $OUTPUT_VIDEO"
    echo ""
    echo "📊 Video information:"
    ffprobe -v quiet -show_format -show_streams "$OUTPUT_VIDEO" | grep -E "(codec_name|width|height|r_frame_rate|sample_rate|channels|duration|bit_rate)"
    echo ""
    echo "🚀 You can now stream this video with:"
    echo "   cargo run --bin relay-node web-viewer --video $OUTPUT_VIDEO"
else
    echo ""
    echo "❌ Failed to convert video"
    echo "Make sure you have ffmpeg installed:"
    echo "   Ubuntu/Debian: sudo apt install ffmpeg"
    echo "   macOS: brew install ffmpeg"
    echo "   Windows: Download from https://ffmpeg.org/"
    exit 1
fi