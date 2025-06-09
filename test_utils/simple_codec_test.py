#!/usr/bin/env python3
"""
Simple WebSocket test to verify codec information
"""

import websockets
import asyncio
import json
import sys

async def test_websocket():
    uri = "ws://127.0.0.1:8080/stream"
    
    try:
        print(f"Connecting to {uri}...")
        
        # Set a reasonable timeout
        websocket = await asyncio.wait_for(
            websockets.connect(uri), 
            timeout=10
        )
        
        print("Connected successfully!")
        
        # Receive first message
        print("Waiting for first message...")
        message = await asyncio.wait_for(websocket.recv(), timeout=10)
        
        if isinstance(message, str):
            data = json.loads(message)
            print(f"Message type: {data.get('type')}")
            print(f"Full message: {json.dumps(data, indent=2)}")
            
            if 'audio_codec' in data:
                print(f"\n✅ Audio codec: {data['audio_codec']}")
                
                # Check if it matches our expected codec
                expected = 'audio/mp4; codecs="mp4a.40.2"'
                if data['audio_codec'] == expected:
                    print("✅ Codec string matches expected value!")
                else:
                    print(f"❌ Expected: {expected}")
                    
        else:
            print(f"Received binary message: {len(message)} bytes")
            print(f"First 16 bytes: {message[:16].hex()}")
            
        await websocket.close()
        
    except asyncio.TimeoutError:
        print("❌ Connection or message timeout")
    except websockets.exceptions.ConnectionRefused:
        print("❌ Connection refused - server not running?")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_websocket())