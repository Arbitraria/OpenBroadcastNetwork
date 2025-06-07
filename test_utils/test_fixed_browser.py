#!/usr/bin/env python3
"""
Test the fixed browser implementation with proper message sequence handling
"""

import asyncio
import websockets
import json
import struct

async def test_fixed_implementation():
    """Test the complete 3-message sequence as the browser should handle it"""
    print("🧪 Testing Fixed Browser Implementation")
    print("=" * 60)
    
    try:
        async with websockets.connect("ws://127.0.0.1:8080/stream") as websocket:
            print("✅ WebSocket connected")
            
            # Track the expected sequence
            messages_received = []
            expected_sequence = ['stream_info', 'chunk_info', 'binary_init_segment']
            
            for i in range(3):
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    
                    if isinstance(message, bytes):
                        print(f"\n📦 Message {i+1}: BINARY ({len(message)} bytes)")
                        messages_received.append('binary_init_segment')
                        
                        # Analyze initialization segment
                        if len(message) >= 8:
                            size = struct.unpack('>I', message[0:4])[0]
                            box_type = message[4:8].decode('ascii', errors='ignore')
                            print(f"   MP4 box: '{box_type}' (size: {size})")
                            
                            if box_type == 'ftyp':
                                # Look for ESDS modification
                                if b'esds' in message:
                                    esds_pos = message.find(b'esds')
                                    print(f"   🎯 ESDS found at offset {esds_pos}")
                                    
                                    # Check for modified object type
                                    for j in range(esds_pos, min(esds_pos + 50, len(message) - 2)):
                                        if message[j] == 0x04 and j + 2 < len(message):
                                            obj_type = message[j + 2]
                                            if obj_type == 0x02:
                                                print(f"   ✅ ESDS MODIFIED: Object type 0x02 at offset {j + 2}")
                                            elif obj_type == 0x40:
                                                print(f"   ❌ ESDS NOT MODIFIED: Object type 0x40 at offset {j + 2}")
                                            break
                    else:
                        try:
                            data = json.loads(message)
                            msg_type = data.get('type', 'unknown')
                            print(f"\n📋 Message {i+1}: {msg_type.upper()}")
                            messages_received.append(msg_type)
                            
                            if msg_type == 'stream_info':
                                audio = data.get('data', {}).get('audio', {})
                                audio_codec = audio.get('mime_type', 'Unknown')
                                print(f"   🎵 Server audio codec: {audio_codec}")
                                
                            elif msg_type == 'chunk_info':
                                chunk_type = data.get('data', {}).get('chunk_type', 'Unknown')
                                size = data.get('data', {}).get('size', 0)
                                print(f"   📦 Chunk type: {chunk_type} ({size} bytes)")
                                
                        except json.JSONDecodeError:
                            print(f"\n📄 Message {i+1}: TEXT ({len(message)} chars)")
                            messages_received.append('text')
                
                except asyncio.TimeoutError:
                    print(f"\n⏱️ Timeout waiting for message {i+1}")
                    break
            
            # Analyze results
            print(f"\n📊 Analysis Results")
            print("=" * 30)
            print(f"Expected sequence: {expected_sequence}")
            print(f"Received sequence: {messages_received}")
            
            if messages_received == expected_sequence:
                print("✅ PERFECT: Message sequence matches expected pattern")
                print("   → Browser should now handle codec correctly")
                return True
            elif 'stream_info' in messages_received and 'binary_init_segment' in messages_received:
                print("✅ GOOD: Both codec info and binary data received")
                print("   → Browser should be able to create proper SourceBuffer")
                return True
            else:
                print("❌ ISSUE: Missing required messages for proper codec handling")
                return False
                
    except Exception as e:
        print(f"❌ WebSocket error: {e}")
        return False

async def main():
    print("🎯 Testing Fixed Browser Implementation")
    print("Testing the new message sequence handling and codec synchronization")
    print()
    
    success = await test_fixed_implementation()
    
    if success:
        print(f"\n🎉 SUCCESS: Implementation should work!")
        print("   Next step: Test in browser at http://127.0.0.1:8080/")
        print("   Expected browser behavior:")
        print("   1. Receives stream_info with server codec")
        print("   2. Creates audio SourceBuffer with exact server codec")
        print("   3. Receives initialization segment with ESDS modification")
        print("   4. Appends successfully without 'object type' errors")
    else:
        print(f"\n⚠️ ISSUES: Implementation needs more work")
        
    return success

if __name__ == "__main__":
    result = asyncio.run(main())
    exit(0 if result else 1)