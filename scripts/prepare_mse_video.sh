#!/bin/bash
# Prepare MSE-compatible fragmented MP4 from source video

INPUT_VIDEO="${1:-bigtroublelittlechina.mp4}"
OUTPUT_VIDEO="mse_compatible_video.mp4"

echo "Converting $INPUT_VIDEO to MSE-compatible format..."

# Create properly fragmented MP4 with MSE-compatible settings
ffmpeg -i "$INPUT_VIDEO" \
  -c:v copy \
  -c:a aac -b:a 128k \
  -movflags frag_keyframe+empty_moov+default_base_moof+omit_tfhd_offset \
  -frag_duration 2000000 \
  -min_frag_duration 1000000 \
  "$OUTPUT_VIDEO" \
  -y

echo "Created $OUTPUT_VIDEO"

# Analyze the output
echo -e "\nAnalyzing output file structure:"
ffprobe -v error -show_format -show_streams "$OUTPUT_VIDEO" | grep -E "codec_name|codec_tag_string|sample_rate|channels"

# Check if it's properly fragmented
echo -e "\nChecking fragmentation:"
mp4dump "$OUTPUT_VIDEO" 2>/dev/null | head -50 || echo "mp4dump not available, install bento4-tools for detailed analysis"

echo -e "\nFile size comparison:"
ls -lh "$INPUT_VIDEO" "$OUTPUT_VIDEO"