#!/usr/bin/env python3
"""
Simulate what the browser should experience with the fixed TypeScript viewer
"""

import asyncio
import websockets
import json
import time

async def simulate_browser_connection():
    """Simulate the browser's WebSocket connection and data flow"""
    
    print("🌐 Simulating browser connection to fixed TypeScript viewer...")
    print("=" * 60)
    
    try:
        async with websockets.connect('ws://127.0.0.1:8080/stream') as ws:
            print("✅ WebSocket connected (like clicking 'Connect' button)")
            
            # Track what the browser would receive
            stream_info_received = False
            init_segment_received = False
            total_data = 0
            message_count = 0
            
            print("\n📋 Expected browser experience:")
            print("1. Connection established ✅")
            print("2. Waiting for stream_info message...")
            
            start_time = time.time()
            while time.time() - start_time < 8:
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    
                    if isinstance(message, str):
                        try:
                            parsed = json.loads(message)
                            message_count += 1
                            msg_type = parsed.get('type', 'unknown')
                            
                            if msg_type == 'stream_info':
                                stream_info_received = True
                                video_codec = parsed.get('video', {}).get('codec', 'none')
                                audio_codec = parsed.get('audio', {}).get('codec', 'none')
                                
                                print(f"✅ 2. Stream info received:")
                                print(f"   📺 Video codec: {video_codec}")
                                print(f"   🔊 Audio codec: {audio_codec}")
                                print("3. MediaSource buffers should be created...")
                                
                            elif msg_type == 'chunk_info':
                                chunk_size = parsed.get('size', 0)
                                chunk_type = parsed.get('chunk_type', 'unknown')
                                print(f"📦 Chunk info: {chunk_size} bytes ({chunk_type})")
                                
                                if chunk_type == 'init':
                                    print("🎯 Initialization segment incoming...")
                                
                        except json.JSONDecodeError:
                            print(f"📨 Non-JSON string: {message[:50]}...")
                            
                    else:
                        # Binary data
                        data_size = len(message)
                        total_data += data_size
                        
                        if not init_segment_received and data_size > 500:
                            # Check if this looks like an init segment
                            header = message[:16]
                            if len(header) >= 8 and header[4:8] == b'ftyp':
                                init_segment_received = True
                                print(f"✅ 4. Initialization segment received: {data_size} bytes")
                                print("   📺 Video element should prepare for playback...")
                                print("5. Waiting for media segments...")
                            else:
                                print(f"📦 Media segment: {data_size} bytes")
                                print("   ▶️ Video should start playing now!")
                        else:
                            print(f"📦 Binary data: {data_size} bytes")
                        
                except asyncio.TimeoutError:
                    continue
            
            print(f"\n📊 Session Summary:")
            print(f"   Messages received: {message_count}")
            print(f"   Total binary data: {total_data} bytes")
            print(f"   Stream info: {'✅' if stream_info_received else '❌'}")
            print(f"   Init segment: {'✅' if init_segment_received else '❌'}")
            
            print(f"\n🎯 Expected Browser State:")
            if stream_info_received and init_segment_received and total_data > 1000:
                print("✅ Video playback should be working!")
                print("✅ Debug panel should show connection details")
                print("✅ Controls should be responsive")
            elif stream_info_received and total_data > 0:
                print("⚠️ Partial success - may have video issues")
            else:
                print("❌ Likely video playback issues")
                
            return stream_info_received and init_segment_received
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

async def main():
    print("🎮 TESTING FIXED TYPESCRIPT VIEWER")
    print("=" * 60)
    
    success = await simulate_browser_connection()
    
    print("\n" + "=" * 60)
    print("🎯 READY FOR BROWSER TESTING!")
    print("=" * 60)
    
    if success:
        print("✅ Server is providing all necessary data")
        print("✅ TypeScript viewer should work correctly")
        print()
        print("🌐 OPEN IN BROWSER:")
        print("   http://127.0.0.1:8080/typescript-viewer-fixed.html")
        print()
        print("🎮 TEST PROCEDURE:")
        print("   1. Click 'Connect' button")
        print("   2. Click 'Debug' button (top-right)")
        print("   3. Watch for video playback to start")
        print("   4. Use Space bar to play/pause")
        print("   5. Check debug panel for real-time stats")
        print()
        print("🔍 WHAT TO LOOK FOR:")
        print("   • Status indicator turns green")
        print("   • Debug panel shows 'Connected'")
        print("   • Buffered ranges appear in debug")
        print("   • Video starts playing automatically")
    else:
        print("❌ Server issues detected - fix server first")

if __name__ == "__main__":
    asyncio.run(main())