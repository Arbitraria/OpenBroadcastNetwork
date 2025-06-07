#!/bin/bash

# WebSocket codec testing script
# Tests the OpenBroadcastNetwork server codec detection

echo "🧪 OpenBroadcastNetwork Codec Test"
echo "=================================="
echo ""

# Check if server is running
if ! curl -s http://127.0.0.1:8080/ > /dev/null 2>&1; then
    echo "❌ Server not running at http://127.0.0.1:8080/"
    echo "   Please start the server first with:"
    echo "   cargo run -p OpenBroadcastNetwork-node web-viewer --video sample_video.mp4"
    exit 1
fi

echo "✅ Server is running"
echo ""

# Use node.js to connect to WebSocket and analyze the stream
node << 'EOF'
const WebSocket = require('ws');

console.log('📡 Connecting to WebSocket...');

const ws = new WebSocket('ws://127.0.0.1:8080/stream');
let receivedInit = false;

ws.on('open', () => {
    console.log('✅ Connected to stream');
});

ws.on('message', (data) => {
    if (!receivedInit) {
        if (Buffer.isBuffer(data) && data.length > 100) {
            receivedInit = true;
            console.log(`\n📦 Received initialization segment: ${data.length} bytes`);
            
            // Check first box
            if (data.length >= 8) {
                const size = data.readUInt32BE(0);
                const type = data.toString('ascii', 4, 8);
                console.log(`   First box: ${type} (${size} bytes)`);
                
                if (type === 'ftyp') {
                    console.log('   ✅ Valid MP4 initialization segment');
                    
                    // Look for object type 0x40 in the data
                    let found40 = false;
                    for (let i = 0; i < data.length - 4; i++) {
                        if (data[i] === 0x04 && i + 2 < data.length && data[i + 2] === 0x40) {
                            found40 = true;
                            break;
                        }
                    }
                    
                    if (found40) {
                        console.log('   🎯 Found object type 0x40 in ESDS');
                        console.log('\n📋 Codec Recommendations:');
                        console.log('   • Try: mp4a.40.02 (zero-padded)');
                        console.log('   • Try: mp4a.64.2  (decimal 100)');
                        console.log('   • Try: mp4a.40.5  (if HE-AAC)');
                    }
                }
            }
            
            ws.close();
            process.exit(0);
        } else {
            try {
                const msg = JSON.parse(data.toString());
                if (msg.type === 'stream_info') {
                    console.log('\n📋 Stream info received:');
                    if (msg.data.audio) {
                        console.log(`   Audio: ${msg.data.audio.codec}`);
                        console.log(`   MIME: ${msg.data.audio.mime_type}`);
                    }
                }
            } catch (e) {
                // Not JSON
            }
        }
    }
});

ws.on('error', (err) => {
    console.log(`❌ WebSocket error: ${err.message}`);
    process.exit(1);
});

setTimeout(() => {
    console.log('⏱️  Timeout - no initialization segment received');
    ws.close();
    process.exit(1);
}, 5000);
EOF

echo ""
echo "Test complete!"