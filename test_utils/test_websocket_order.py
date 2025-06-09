#!/usr/bin/env python3
"""
Test WebSocket message ordering to understand why we're not receiving binary data
"""

import asyncio
import websockets
import json
import struct

async def test_message_order():
    """Test the order and types of WebSocket messages"""
    print("🔍 Testing WebSocket Message Order")
    print("=" * 50)
    
    try:
        async with websockets.connect("ws://127.0.0.1:8080/stream") as websocket:
            print("✅ WebSocket connected")
            
            message_count = 0
            max_messages = 5  # Limit to first 5 messages
            
            while message_count < max_messages:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=3.0)
                    message_count += 1
                    
                    print(f"\n📨 Message {message_count}:")
                    
                    if isinstance(message, bytes):
                        print(f"   Type: BINARY")
                        print(f"   Size: {len(message)} bytes")
                        
                        # Analyze first 16 bytes
                        if len(message) >= 16:
                            first_bytes = ' '.join(f"{b:02x}" for b in message[:16])
                            print(f"   First 16 bytes: {first_bytes}")
                            
                            # Check if it's MP4
                            if len(message) >= 8:
                                size = struct.unpack('>I', message[0:4])[0]
                                box_type = message[4:8].decode('ascii', errors='ignore')
                                print(f"   MP4 box: '{box_type}' (size: {size})")
                                
                                # Look for ESDS modification
                                if b'esds' in message:
                                    esds_pos = message.find(b'esds')
                                    print(f"   🎯 ESDS found at offset {esds_pos}")
                                    
                                    # Look for object type around ESDS
                                    for i in range(esds_pos, min(esds_pos + 50, len(message) - 2)):
                                        if message[i] == 0x04 and i + 2 < len(message):
                                            obj_type = message[i + 2]
                                            print(f"   📍 Object type: 0x{obj_type:02X} at offset {i + 2}")
                                            if obj_type == 0x02:
                                                print("   ✅ ESDS modification confirmed (0x02)")
                                            elif obj_type == 0x40:
                                                print("   ❌ Original object type (0x40) still present")
                                            break
                        
                    else:
                        print(f"   Type: TEXT/JSON")
                        print(f"   Size: {len(message)} chars")
                        
                        try:
                            data = json.loads(message)
                            msg_type = data.get('type', 'unknown')
                            print(f"   JSON type: {msg_type}")
                            
                            if msg_type == 'stream_info' and 'data' in data:
                                stream_data = data['data']
                                
                                # Handle video info
                                if 'video' in stream_data and stream_data['video']:
                                    video_info = stream_data['video']
                                    codec = video_info.get('codec', 'Unknown')
                                    mime_type = video_info.get('mime_type', 'Unknown')
                                    print(f"   Video: {codec} -> {mime_type}")
                                
                                # Handle audio info (may be None for video-only streams)
                                if 'audio' in stream_data and stream_data['audio']:
                                    audio_info = stream_data['audio']
                                    codec = audio_info.get('codec', 'Unknown')
                                    mime_type = audio_info.get('mime_type', 'Unknown')
                                    print(f"   Audio: {codec} -> {mime_type}")
                                else:
                                    print(f"   📺 Video-only stream (no audio track)")
                                    
                        except json.JSONDecodeError:
                            print(f"   Content: {message[:100]}...")
                
                except asyncio.TimeoutError:
                    print(f"\n⏱️ Timeout waiting for message {message_count + 1}")
                    break
                    
            print(f"\n📊 Received {message_count} messages total")
            
    except Exception as e:
        print(f"❌ WebSocket error: {e}")

if __name__ == "__main__":
    asyncio.run(test_message_order())