#!/usr/bin/env bash
# Generate test_simple.mp4 — a 30s synthetic test clip with no source file required.
# Used by the README quick start and docker-compose.yml.
set -euo pipefail

OUTPUT="${1:-test_simple.mp4}"

if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "error: ffmpeg not found. Install it first (e.g. 'sudo apt-get install ffmpeg')." >&2
  exit 1
fi

echo "Generating $OUTPUT (30s, 1280x720, H.264 + AAC)..."
ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i "testsrc2=duration=30:size=1280x720:rate=30" \
  -f lavfi -i "sine=frequency=440:duration=30" \
  -c:v libx264 -pix_fmt yuv420p \
  -c:a aac \
  "$OUTPUT"

echo "Created $OUTPUT"
