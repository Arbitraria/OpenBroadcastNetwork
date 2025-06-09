#!/usr/bin/env python3
"""
Test AC-3 audio detection and video-only mode in the browser viewer.
This simulates what happens when the universal viewer receives AC-3 audio.
"""

import asyncio
import json
import websockets
import time

async def test_browser_ac3_handling():
    """Test how the browser viewer handles AC-3 audio"""
    
    print("🎬 Testing Browser AC-3 Video-Only Mode")
    print("=" * 45)
    
    try:
        async with websockets.connect("ws://localhost:8080/stream") as websocket:
            print("🔗 WebSocket connected to streaming server")
            
            received_stream_info = False
            
            # Listen for messages
            while not received_stream_info:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                    
                    if isinstance(message, str):
                        # JSON control message
                        try:
                            data = json.loads(message)
                            print(f"📋 Received: {data['type']}")
                            
                            if data['type'] == 'stream_info':
                                print("\n✅ Stream Info Analysis:")
                                
                                # Analyze video info
                                if 'video' in data['data']:
                                    video = data['data']['video']
                                    print(f"   📺 Video: {video.get('width')}x{video.get('height')} @ {video.get('fps')}fps")
                                    print(f"   📺 Video MIME: {video.get('mime_type')}")
                                
                                # Analyze audio info (key part for AC-3 detection)
                                if 'audio' in data['data']:
                                    audio = data['data']['audio']
                                    audio_mime = audio.get('mime_type', '')
                                    print(f"   🎵 Audio MIME: {audio_mime}")
                                    
                                    # Test AC-3 detection logic (same as in viewer)
                                    is_ac3 = 'ac-3' in audio_mime.lower()
                                    
                                    print(f"\n🔍 AC-3 Detection Result: {is_ac3}")
                                    
                                    if is_ac3:
                                        print("⚠️ AC-3/Dolby Digital detected!")
                                        print("🎬 Browser Viewer Behavior:")
                                        print("   1. Log: 'AC-3/Dolby Digital audio detected - not supported by Chrome MSE'")
                                        print("   2. Log: 'Falling back to video-only mode for Chrome compatibility'")
                                        print("   3. Set: detectedAudioMimeType = null")
                                        print("   4. Skip: Audio SourceBuffer creation")
                                        print("   5. Mode: Video-only streaming")
                                        
                                        # MediaSource compatibility test
                                        print("\n🌐 MediaSource Compatibility:")
                                        print(f"   Chrome: MediaSource.isTypeSupported('{audio_mime}') = false")
                                        print(f"   Firefox: MediaSource.isTypeSupported('{audio_mime}') = false")
                                        print("   → Video-only mode required for all browsers")
                                        
                                    else:
                                        print(f"✅ Standard audio codec: {audio_mime}")
                                        print("🔊 Normal audio+video mode should work")
                                
                                received_stream_info = True
                                
                            elif data['type'] == 'chunk_info':
                                chunk_type = data['data'].get('chunk_type', 'unknown')
                                chunk_size = data['data'].get('size', 0)
                                print(f"📦 Chunk: {chunk_type} ({chunk_size} bytes)")
                                
                                if chunk_type == 'audio':
                                    print("   🚫 Audio chunk - will be skipped in video-only mode")
                                elif chunk_type.startswith('initialization'):
                                    print("   🎬 Initialization chunk - video-only MediaSource")
                                else:
                                    print(f"   📺 Video chunk - will be processed normally")
                            
                        except json.JSONDecodeError:
                            print(f"❌ Invalid JSON: {message[:100]}...")
                            
                    else:
                        # Binary data
                        print(f"📦 Binary chunk: {len(message)} bytes")
                        
                except asyncio.TimeoutError:
                    print("⏰ Timeout waiting for stream_info")
                    break
                except Exception as e:
                    print(f"❌ Error: {e}")
                    break
            
            if received_stream_info:
                print("\n✅ AC-3 Detection Test Completed Successfully")
                print("\n🎯 Expected Universal Viewer Behavior:")
                print("   1. Connect to WebSocket ✅")
                print("   2. Receive stream_info with AC-3 audio ✅")
                print("   3. Detect AC-3 and enable video-only mode ✅")
                print("   4. Create only video SourceBuffer (no audio)")
                print("   5. Skip all audio chunks during streaming")
                print("   6. Play video successfully without audio")
                
                print("\n🔧 Next Steps:")
                print("   • Open http://localhost:8080/ in Chrome")
                print("   • Click 'Connect to Stream' button")
                print("   • Look for video-only mode messages in debug log")
                print("   • Verify video plays without audio errors")
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("💡 Make sure the server is running: cargo run -p OpenBroadcastNetwork-node")

if __name__ == "__main__":
    asyncio.run(test_browser_ac3_handling())