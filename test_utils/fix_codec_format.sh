#!/bin/bash

# Codec format fix script
# Updates the MP4 parser to use the correct codec format

echo "🔧 OpenBroadcastNetwork Codec Fix"
echo "================================="
echo ""

# Based on extensive research, the issue is that browsers expect different
# codec string formats for object type 0x40 (MPEG-4 Audio)
# 
# The solution is to use mp4a.64.2 (decimal 100) instead of mp4a.40.2

echo "📋 Current codec format: mp4a.40.02"
echo "🎯 Recommended format: mp4a.64.2 (decimal representation of 0x40)"
echo ""

read -p "Apply the codec fix? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🔄 Updating MP4 parser..."
    
    # Update the MP4 parser to use mp4a.64.2 for object type 0x40
    sed -i 's/"mp4a\.40\.02"/"mp4a.64.2"/g' /home/ian/OpenBroadcastNetwork/core/src/media/mp4_parser.rs
    
    # Also update server fallbacks
    sed -i 's/"mp4a\.40\.02"/"mp4a.64.2"/g' /home/ian/OpenBroadcastNetwork/node/src/web_server.rs
    
    echo "✅ Updated codec format to mp4a.64.2"
    echo ""
    echo "📌 Next steps:"
    echo "1. Restart the server"
    echo "2. Test with browser"
    echo ""
    echo "The decimal format (64 = 0x40) is more compatible with browsers"
else
    echo "❌ Fix cancelled"
fi