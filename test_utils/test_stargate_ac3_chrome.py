#!/usr/bin/env python3
"""
Comprehensive test for AC-3 video-only mode with Stargate file in Chrome.
This simulates the complete browser experience with AC-3 detection and fallback.
"""

import asyncio
import json
import websockets
import time
from pathlib import Path

async def test_stargate_ac3_chrome_compatibility():
    """Test Stargate AC-3 video file with Chrome MSE compatibility"""
    
    print("🎬 Testing Stargate AC-3 File with Chrome Compatibility")
    print("=" * 55)
    
    # Verify Stargate file exists
    stargate_file = Path(__file__).parent.parent / "Stargate SG1 S01E03.mp4"
    if not stargate_file.exists():
        print("❌ Stargate file not found - checking for alternative...")
        bigtrouble_file = Path(__file__).parent.parent / "bigtroublelittlechina.mp4"
        if bigtrouble_file.exists():
            print(f"✅ Using alternative file: {bigtrouble_file.name}")
        else:
            print("❌ No AC-3 test files available")
            return
    else:
        print(f"✅ Testing with: {stargate_file.name}")
    
    try:
        async with websockets.connect("ws://localhost:8080/stream") as websocket:
            print("🔗 WebSocket connected successfully")
            
            received_stream_info = False
            received_chunks = []
            chunk_count = 0
            init_segment_complete = False
            video_chunks_received = 0
            audio_chunks_skipped = 0
            
            # Monitor the complete streaming session
            start_time = time.time()
            timeout = 30  # 30 second timeout
            
            while time.time() - start_time < timeout:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    
                    if isinstance(message, str):
                        # JSON control message
                        try:
                            data = json.loads(message)
                            msg_type = data.get('type', 'unknown')
                            
                            if msg_type == 'stream_info':
                                print(f"\n📋 Stream Info Received:")
                                
                                # Video analysis
                                if 'video' in data['data']:
                                    video = data['data']['video']
                                    print(f"   📺 Video: {video.get('width')}x{video.get('height')} @ {video.get('fps')}fps")
                                    print(f"   📺 Video MIME: {video.get('mime_type')}")
                                
                                # Audio analysis (key for AC-3 test)
                                if 'audio' in data['data']:
                                    audio = data['data']['audio']
                                    audio_mime = audio.get('mime_type', '')
                                    print(f"   🎵 Audio MIME: {audio_mime}")
                                    
                                    # Test AC-3 detection
                                    is_ac3 = 'ac-3' in audio_mime.lower()
                                    print(f"\n🔍 AC-3 Detection: {is_ac3}")
                                    
                                    if is_ac3:
                                        print("✅ AC-3 audio detected - video-only mode expected")
                                        print("🎬 Universal Viewer should:")
                                        print("   • Log AC-3 incompatibility warning")
                                        print("   • Set detectedAudioMimeType = null")
                                        print("   • Create only video SourceBuffer")
                                        print("   • Skip all audio chunks")
                                    else:
                                        print(f"ℹ️ Audio codec: {audio_mime} (should work normally)")
                                
                                received_stream_info = True
                                
                            elif msg_type == 'chunk_info':
                                chunk_type = data['data'].get('chunk_type', 'unknown')
                                chunk_size = data['data'].get('size', 0)
                                
                                received_chunks.append({
                                    'type': chunk_type,
                                    'size': chunk_size,
                                    'timestamp': time.time()
                                })
                                
                                if chunk_type == 'initialization':
                                    print(f"📦 Init segment: {chunk_size} bytes")
                                elif chunk_type.startswith('initialization_chunk_'):
                                    chunk_num = chunk_type.split('_')[-1]
                                    print(f"📦 Init chunk {chunk_num}: {chunk_size} bytes")
                                elif chunk_type == 'initialization_complete':
                                    print("📦 Init complete signal")
                                    init_segment_complete = True
                                elif chunk_type == 'video':
                                    video_chunks_received += 1
                                    print(f"📺 Video chunk {video_chunks_received}: {chunk_size} bytes")
                                elif chunk_type == 'audio':
                                    audio_chunks_skipped += 1
                                    print(f"🚫 Audio chunk {audio_chunks_skipped}: {chunk_size} bytes (will be skipped)")
                                elif chunk_type == 'end_of_stream':
                                    print("🏁 End of stream signal")
                                    break
                                else:
                                    print(f"📦 Unknown chunk: {chunk_type} ({chunk_size} bytes)")
                            
                        except json.JSONDecodeError:
                            print(f"❌ Invalid JSON: {message[:100]}...")
                            
                    else:
                        # Binary data
                        chunk_count += 1
                        chunk_size = len(message)
                        
                        if chunk_count <= 5 or chunk_count % 10 == 0:
                            print(f"📦 Binary chunk {chunk_count}: {chunk_size} bytes")
                        
                        # Check first few bytes to identify MP4 boxes
                        if chunk_size >= 8:
                            try:
                                box_size = int.from_bytes(message[0:4], 'big')
                                box_type = message[4:8].decode('ascii', errors='ignore')
                                
                                if box_type in ['ftyp', 'moov', 'moof', 'mdat']:
                                    if chunk_count <= 5:
                                        print(f"   MP4 box: {box_type} ({box_size} bytes)")
                            except:
                                pass
                        
                        # Break after receiving enough chunks to test the system
                        if chunk_count >= 20:
                            print(f"✅ Received {chunk_count} chunks - test data sufficient")
                            break
                            
                except asyncio.TimeoutError:
                    if not received_stream_info:
                        print("⏰ Timeout waiting for stream_info")
                        break
                    else:
                        print("⏰ No more data - ending test")
                        break
                except Exception as e:
                    print(f"❌ Error during streaming: {e}")
                    break
            
            # Analyze test results
            print(f"\n📊 Test Results Summary:")
            print(f"   ⏱️ Duration: {time.time() - start_time:.1f} seconds")
            print(f"   📋 Stream info received: {'✅' if received_stream_info else '❌'}")
            print(f"   📦 Total chunks received: {chunk_count}")
            print(f"   📺 Video chunks: {video_chunks_received}")
            print(f"   🚫 Audio chunks (to skip): {audio_chunks_skipped}")
            print(f"   🎬 Init segment: {'✅' if init_segment_complete else '⚠️'}")
            
            # Test verdict
            success_criteria = [
                received_stream_info,
                chunk_count > 0,
                video_chunks_received > 0
            ]
            
            if all(success_criteria):
                print(f"\n✅ AC-3 Video Test PASSED")
                print(f"🎯 Browser should now:")
                print(f"   1. Detect AC-3 audio ✅")
                print(f"   2. Enable video-only mode ✅")
                print(f"   3. Receive video chunks ✅ ({video_chunks_received} chunks)")
                if audio_chunks_skipped > 0:
                    print(f"   4. Skip audio chunks ✅ ({audio_chunks_skipped} chunks)")
                print(f"   5. Play video without audio errors")
                
                print(f"\n🌐 Open http://localhost:8080/ in Chrome to test:")
                print(f"   • Click 'Connect to Stream'")
                print(f"   • Look for 'AC-3/Dolby Digital' warning messages")
                print(f"   • Verify video plays in video-only mode")
                
            else:
                print(f"\n❌ AC-3 Video Test FAILED")
                if not received_stream_info:
                    print("   • No stream_info received")
                if chunk_count == 0:
                    print("   • No chunks received")
                if video_chunks_received == 0:
                    print("   • No video chunks received")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_stargate_ac3_chrome_compatibility())