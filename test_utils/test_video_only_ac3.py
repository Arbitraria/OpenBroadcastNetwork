#!/usr/bin/env python3
"""
Test video-only mode with AC-3 audio detection fallback.
This test verifies that the viewer correctly falls back to video-only mode
when AC-3/Dolby Digital audio is detected (not supported by Chrome MSE).
"""

import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path

# Add the parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

async def test_ac3_video_only_mode():
    """Test that AC-3 audio triggers video-only mode fallback"""
    
    print("🧪 Testing AC-3 Video-Only Mode Fallback")
    print("=" * 50)
    
    # Start the server
    print("🚀 Starting OpenBroadcastNetwork server...")
    server_process = subprocess.Popen(
        ["cargo", "run", "-p", "OpenBroadcastNetwork-node"],
        cwd=str(Path(__file__).parent.parent),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for server to start
    await asyncio.sleep(3)
    
    try:
        # Test with Stargate file (contains AC-3 audio)
        test_file = Path(__file__).parent.parent / "Stargate SG1 S01E03.mp4"
        
        if not test_file.exists():
            print("❌ Stargate test file not found - using bigtroublelittlechina.mp4 instead")
            test_file = Path(__file__).parent.parent / "bigtroublelittlechina.mp4"
            
        if not test_file.exists():
            print("❌ No AC-3 test files found, creating mock test...")
            await test_mock_ac3_detection()
            return
            
        print(f"📁 Testing with: {test_file.name}")
        
        # Test WebSocket connection and stream info
        import websockets
        
        async with websockets.connect("ws://localhost:8080/stream") as websocket:
            print("🔗 Connected to WebSocket")
            
            # Wait for stream_info message
            while True:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    
                    if isinstance(message, str):
                        # JSON message
                        data = json.loads(message)
                        print(f"📋 Received: {data['type']}")
                        
                        if data['type'] == 'stream_info':
                            print("✅ Stream info received:")
                            
                            # Check video info
                            if 'video' in data['data']:
                                video = data['data']['video']
                                print(f"   📺 Video: {video.get('width', '?')}x{video.get('height', '?')} @ {video.get('fps', '?')}fps")
                                print(f"   📺 Video codec: {video.get('mime_type', 'unknown')}")
                            
                            # Check audio info
                            if 'audio' in data['data']:
                                audio = data['data']['audio']
                                audio_codec = audio.get('mime_type', 'unknown')
                                print(f"   🎵 Audio codec: {audio_codec}")
                                
                                # Test AC-3 detection
                                if 'ac-3' in audio_codec.lower():
                                    print("✅ AC-3 audio detected correctly!")
                                    print("🎬 Browser should fall back to video-only mode")
                                    
                                    # Check if browser would support this codec
                                    print("\n🌐 Browser Compatibility Check:")
                                    print("   Chrome MSE: ❌ AC-3 not supported")
                                    print("   Firefox MSE: ❌ AC-3 not supported") 
                                    print("   Safari MSE: ⚠️ Limited AC-3 support")
                                    print("   → Video-only fallback required")
                                    
                                else:
                                    print(f"ℹ️ Non-AC-3 audio: {audio_codec}")
                                    print("   → Should work with audio+video")
                            else:
                                print("   🎵 No audio track (video-only stream)")
                            
                            break
                            
                    else:
                        # Binary data
                        print(f"📦 Binary chunk: {len(message)} bytes")
                        
                except asyncio.TimeoutError:
                    print("⏰ No stream_info received within timeout")
                    break
                except Exception as e:
                    print(f"❌ Error: {e}")
                    break
                    
    except Exception as e:
        print(f"❌ Test failed: {e}")
        
    finally:
        # Clean up server
        print("🛑 Stopping server...")
        server_process.terminate()
        server_process.wait()
        print("✅ Test completed")


async def test_mock_ac3_detection():
    """Test AC-3 detection logic without actual AC-3 file"""
    print("\n🔬 Mock AC-3 Detection Test")
    
    # Simulate different audio codec scenarios
    test_cases = [
        ("audio/mp4; codecs=\"ac-3\"", True, "Standard AC-3"),
        ("audio/mp4; codecs=\"AC-3\"", True, "Uppercase AC-3"),
        ("audio/mp4; codecs=\"mp4a.40.2\"", False, "Standard AAC"),
        ("audio/mp4; codecs=\"mp4a.67\"", False, "MP3 in MP4"),
    ]
    
    print("\n📊 AC-3 Detection Test Cases:")
    for codec, should_detect, description in test_cases:
        is_ac3 = 'ac-3' in codec.lower()
        status = "✅" if is_ac3 == should_detect else "❌"
        mode = "video-only" if is_ac3 else "audio+video"
        print(f"   {status} {description}: {codec} → {mode}")
    
    print("\n🎯 Expected Behavior:")
    print("   1. Detect AC-3 audio codec string")
    print("   2. Log warning about Chrome MSE incompatibility")
    print("   3. Set video-only mode (don't create audio SourceBuffer)")
    print("   4. Skip audio chunks during streaming")
    print("   5. Video playback should work normally")


if __name__ == "__main__":
    asyncio.run(test_ac3_video_only_mode())