#!/usr/bin/env python3
"""
Simple codec testing script for OpenBroadcastNetwork
Tests WebSocket connection and analyzes codec information
"""

import asyncio
import websockets
import json
import struct
from datetime import datetime

async def test_codec():
    uri = "ws://127.0.0.1:8080/stream"
    print(f"🧪 OpenBroadcastNetwork Codec Test")
    print(f"==================================\n")
    print(f"📡 Connecting to {uri}...")
    
    try:
        async with websockets.connect(uri) as websocket:
            print("✅ Connected to WebSocket")
            
            # Wait for messages
            while True:
                message = await websocket.recv()
                
                # Check if binary (initialization segment)
                if isinstance(message, bytes):
                    print(f"\n📦 Received binary data: {len(message)} bytes")
                    
                    if len(message) >= 8:
                        # Parse MP4 box header
                        size = struct.unpack('>I', message[0:4])[0]
                        box_type = message[4:8].decode('ascii', errors='ignore')
                        
                        print(f"   First box: '{box_type}' ({size} bytes)")
                        
                        if box_type == 'ftyp':
                            print("   ✅ Valid MP4 initialization segment detected")
                            
                            # Look for ESDS pattern with object type 0x40
                            found_40 = False
                            for i in range(len(message) - 20):
                                # Look for ESDS box
                                if message[i:i+4] == b'esds':
                                    print(f"   📦 Found ESDS box at offset {i}")
                                    # Look for object type in ESDS
                                    for j in range(i, min(i + 30, len(message) - 2)):
                                        if message[j] == 0x04 and j + 2 < len(message):
                                            obj_type = message[j + 2]
                                            print(f"   🎯 Found object type: 0x{obj_type:02X}")
                                            if obj_type == 0x40:
                                                found_40 = True
                                            break
                            
                            if found_40:
                                print("\n🎵 Detected AAC with object type 0x40")
                                print("\n📋 Codec String Recommendations:")
                                print("   1. mp4a.40.02  - Zero-padded format (RFC 6381)")
                                print("   2. mp4a.64.2   - Decimal 100 representation")
                                print("   3. mp4a.40.2   - Standard format (may fail)")
                                print("   4. mp4a.40.5   - Try if HE-AAC")
                                print("\n💡 Update your MP4 parser to use format #1 or #2")
                            
                            # Success - close connection
                            await websocket.close()
                            return True
                            
                else:
                    # JSON message
                    try:
                        data = json.loads(message)
                        if data.get('type') == 'stream_info':
                            print("\n📋 Stream info from server:")
                            if 'audio' in data.get('data', {}):
                                audio = data['data']['audio']
                                print(f"   Codec: {audio.get('codec', 'Unknown')}")
                                print(f"   MIME: {audio.get('mime_type', 'Unknown')}")
                    except json.JSONDecodeError:
                        pass
                        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

async def main():
    # First check if server is running
    import urllib.request
    try:
        urllib.request.urlopen('http://127.0.0.1:8080/', timeout=1)
    except:
        print("❌ Server not running at http://127.0.0.1:8080/")
        print("   Start it with:")
        print("   cargo run -p OpenBroadcastNetwork-node web-viewer --video sample_video.mp4")
        return
    
    # Run the test
    success = await test_codec()
    
    if success:
        print("\n✅ Test completed successfully!")
    else:
        print("\n❌ Test failed")

if __name__ == "__main__":
    asyncio.run(main())