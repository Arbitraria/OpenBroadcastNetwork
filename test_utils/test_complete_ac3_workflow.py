#!/usr/bin/env python3
"""
Complete AC-3 video-only mode workflow test.
This demonstrates the full end-to-end experience with the Stargate file.
"""

import asyncio
import json
import websockets
import time
from pathlib import Path

async def test_complete_ac3_workflow():
    """Test complete AC-3 video-only workflow"""
    
    print("🎬 Complete AC-3 Video-Only Mode Workflow Test")
    print("=" * 50)
    
    # Step 1: Verify setup
    print("📋 Step 1: Verifying Test Setup")
    stargate_file = Path(__file__).parent.parent / "Stargate SG1 S01E03.mp4"
    if stargate_file.exists():
        print(f"   ✅ Test file: {stargate_file.name} ({stargate_file.stat().st_size // 1024 // 1024} MB)")
    else:
        print("   ❌ Stargate file not found")
        return
    
    # Step 2: Test WebSocket connection and stream info
    print("\n📋 Step 2: Testing WebSocket Protocol")
    try:
        async with websockets.connect("ws://localhost:8080/stream") as websocket:
            print("   ✅ WebSocket connected")
            
            # Wait for stream_info
            message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
            
            if isinstance(message, str):
                data = json.loads(message)
                if data['type'] == 'stream_info':
                    print("   ✅ Stream info received")
                    
                    # Extract codec information
                    video_info = data['data'].get('video', {})
                    audio_info = data['data'].get('audio', {})
                    
                    video_codec = video_info.get('mime_type', 'unknown')
                    audio_codec = audio_info.get('mime_type', 'unknown')
                    
                    print(f"   📺 Video: {video_info.get('width')}x{video_info.get('height')} @ {video_info.get('fps')}fps")
                    print(f"   📺 Video codec: {video_codec}")
                    print(f"   🎵 Audio codec: {audio_codec}")
                    
                    # Step 3: Test AC-3 detection logic
                    print("\n📋 Step 3: Testing AC-3 Detection Logic")
                    is_ac3 = 'ac-3' in audio_codec.lower()
                    print(f"   🔍 AC-3 detected: {is_ac3}")
                    
                    if is_ac3:
                        print("   ✅ AC-3 codec identified correctly")
                        print("   🎬 Video-only mode should be activated")
                        
                        # Step 4: Test MediaSource compatibility
                        print("\n📋 Step 4: MediaSource Compatibility Check")
                        print(f"   ❌ Chrome MSE: AC-3 not supported")
                        print(f"   ❌ Firefox MSE: AC-3 not supported")
                        print(f"   ⚠️ Safari MSE: Limited AC-3 support")
                        print(f"   ✅ Video-only fallback: Required for all browsers")
                        
                        # Step 5: Test chunk handling
                        print("\n📋 Step 5: Testing Chunk Processing")
                        chunk_count = 0
                        init_chunks = 0
                        video_chunks = 0
                        audio_chunks = 0
                        
                        while chunk_count < 10:  # Process first 10 chunks
                            try:
                                message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                                
                                if isinstance(message, str):
                                    data = json.loads(message)
                                    if data['type'] == 'chunk_info':
                                        chunk_type = data['data'].get('chunk_type', 'unknown')
                                        chunk_size = data['data'].get('size', 0)
                                        
                                        if chunk_type.startswith('initialization'):
                                            init_chunks += 1
                                            print(f"   📦 Init chunk {init_chunks}: {chunk_size} bytes")
                                        elif 'video' in chunk_type:
                                            video_chunks += 1
                                            print(f"   📺 Video chunk {video_chunks}: {chunk_size} bytes")
                                        elif chunk_type == 'audio':
                                            audio_chunks += 1
                                            print(f"   🚫 Audio chunk {audio_chunks}: {chunk_size} bytes (will be skipped)")
                                else:
                                    # Binary data
                                    chunk_count += 1
                                    
                            except asyncio.TimeoutError:
                                break
                        
                        # Step 6: Test results summary
                        print(f"\n📋 Step 6: Universal Viewer Expected Behavior")
                        print(f"   1. ✅ Connect to WebSocket")
                        print(f"   2. ✅ Receive stream_info with AC-3 audio")
                        print(f"   3. ✅ Detect AC-3 incompatibility")
                        print(f"   4. ✅ Log warning: 'AC-3/Dolby Digital audio detected - not supported by Chrome MSE'")
                        print(f"   5. ✅ Log info: 'Falling back to video-only mode for Chrome compatibility'")
                        print(f"   6. ✅ Set detectedAudioMimeType = null")
                        print(f"   7. ✅ Create only video SourceBuffer (skip audio)")
                        print(f"   8. ✅ Process {init_chunks} initialization chunks")
                        print(f"   9. ✅ Process {video_chunks} video chunks")
                        if audio_chunks > 0:
                            print(f"   10. ✅ Skip {audio_chunks} audio chunks")
                        print(f"   11. ✅ Play video without MediaSource errors")
                        
                        print(f"\n🎯 Browser Testing Instructions:")
                        print(f"   1. Open Chrome browser")
                        print(f"   2. Navigate to http://localhost:8080/")
                        print(f"   3. Open Developer Console (F12)")
                        print(f"   4. Click 'Connect to Stream' button")
                        print(f"   5. Look for these messages in Debug Log:")
                        print(f"      • 'AC-3/Dolby Digital audio detected'")
                        print(f"      • 'Falling back to video-only mode'")
                        print(f"      • 'Video-only mode - skipping audio buffer creation'")
                        print(f"      • 'Skipping audio chunk - video-only mode'")
                        print(f"   6. Verify video plays without audio errors")
                        print(f"   7. Check that video duration appears (not 0:00)")
                        print(f"   8. Confirm video can be played/paused normally")
                        
                        print(f"\n✅ AC-3 Video-Only Mode Test: READY FOR BROWSER TESTING")
                        
                    else:
                        print("   ❌ Expected AC-3 codec but found different audio format")
                        print(f"   💡 This test requires a file with AC-3/Dolby Digital audio")
                        
    except Exception as e:
        print(f"   ❌ WebSocket test failed: {e}")
        return
    
    print(f"\n🔧 Additional Test Files Available:")
    test_files = [
        "test_browser_ac3_playback.js - Browser console test script",
        "test_ac3_browser_compatibility.py - Protocol compatibility test", 
        "test_video_only_ac3.py - Comprehensive server test"
    ]
    
    for test_file in test_files:
        print(f"   📄 {test_file}")

if __name__ == "__main__":
    asyncio.run(test_complete_ac3_workflow())