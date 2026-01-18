#!/usr/bin/env python3
"""
Debug TypeScript Viewer Issues

This script simulates browser behavior and tests the TypeScript viewer
to identify why video playback isn't working and debug button issues.
"""

import asyncio
import json
import time
import websockets
import requests
import sys
from urllib.parse import urlparse

async def debug_typescript_viewer():
    """Debug the TypeScript viewer functionality"""
    
    print("🐛 OpenBroadcastNetwork TypeScript Viewer Debug")
    print("=" * 50)
    
    base_url = "http://127.0.0.1:8080"
    ws_url = "ws://127.0.0.1:8080/stream"
    
    # 1. Test HTTP endpoints
    print("\n📡 Testing HTTP Endpoints...")
    
    try:
        response = requests.get(f"{base_url}/typescript-viewer.html", timeout=5)
        print(f"✅ TypeScript viewer: {response.status_code} ({len(response.content)} bytes)")
        
        # Check for TypeScript bundle reference
        if 'main.9b279abcfdfc45f739ac.js' in response.text:
            print("✅ TypeScript bundle referenced in HTML")
        else:
            print("❌ TypeScript bundle NOT referenced in HTML")
            
    except Exception as e:
        print(f"❌ Failed to fetch TypeScript viewer: {e}")
        return False
    
    try:
        response = requests.get(f"{base_url}/main.9b279abcfdfc45f739ac.js", timeout=5)
        print(f"✅ TypeScript bundle: {response.status_code} ({len(response.content)} bytes)")
        
        # Check bundle content
        bundle_content = response.text
        if 'VideoPlayer' in bundle_content:
            print("✅ VideoPlayer class found in bundle")
        else:
            print("❌ VideoPlayer class NOT found in bundle")
            
        if 'MediaManager' in bundle_content:
            print("✅ MediaManager class found in bundle")
        else:
            print("❌ MediaManager class NOT found in bundle")
            
    except Exception as e:
        print(f"❌ Failed to fetch TypeScript bundle: {e}")
        return False
    
    # 2. Test WebSocket streaming
    print("\n🔌 Testing WebSocket Streaming...")
    
    try:
        async with websockets.connect(ws_url, timeout=10) as websocket:
            print("✅ WebSocket connection established")
            
            # Collect messages for analysis
            messages = []
            binary_chunks = []
            start_time = time.time()
            
            try:
                while time.time() - start_time < 8:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        
                        if isinstance(message, str):
                            try:
                                parsed = json.loads(message)
                                messages.append(parsed)
                                print(f"📨 JSON: {parsed.get('type', 'unknown')}")
                                
                                if parsed.get('type') == 'stream_info':
                                    print(f"   Video: {parsed.get('video', {}).get('codec', 'none')}")
                                    print(f"   Audio: {parsed.get('audio', {}).get('codec', 'none')}")
                                    
                            except json.JSONDecodeError:
                                print(f"📨 String: {message[:50]}...")
                        else:
                            binary_chunks.append(len(message))
                            print(f"📦 Binary: {len(message)} bytes")
                            
                    except asyncio.TimeoutError:
                        continue
                        
            except Exception as e:
                print(f"Message collection ended: {e}")
            
            # Analyze streaming data
            print(f"\n📊 Streaming Analysis:")
            print(f"   JSON messages: {len(messages)}")
            print(f"   Binary chunks: {len(binary_chunks)}")
            print(f"   Total binary data: {sum(binary_chunks)} bytes")
            
            # Check for proper stream_info
            stream_info = next((m for m in messages if m.get('type') == 'stream_info'), None)
            if stream_info:
                print(f"✅ Stream info received")
                video_codec = stream_info.get('video', {}).get('codec')
                audio_codec = stream_info.get('audio', {}).get('codec')
                print(f"   Video codec: {video_codec}")
                print(f"   Audio codec: {audio_codec}")
                
                # Check browser compatibility
                if video_codec and 'h264' in str(video_codec).lower():
                    print("✅ Video codec is browser-compatible (H.264)")
                elif video_codec:
                    print(f"⚠️ Video codec may not be browser-compatible: {video_codec}")
                else:
                    print("❌ No video codec information")
                    
                if audio_codec:
                    if 'aac' in str(audio_codec).lower():
                        print("✅ Audio codec is browser-compatible (AAC)")
                    elif 'ac-3' in str(audio_codec).lower():
                        print("⚠️ Audio codec requires transcoding (AC-3)")
                    else:
                        print(f"⚠️ Audio codec compatibility unknown: {audio_codec}")
                else:
                    print("⚠️ No audio codec information")
            else:
                print("❌ No stream_info received")
                return False
            
            # Check for initialization segment
            chunk_info_messages = [m for m in messages if m.get('type') == 'chunk_info']
            if chunk_info_messages:
                print(f"✅ Chunk info messages: {len(chunk_info_messages)}")
                
                # Look for initialization segment
                init_chunks = [m for m in chunk_info_messages if m.get('chunk_type') == 'init']
                if init_chunks:
                    print(f"✅ Initialization segment info found: {len(init_chunks)}")
                else:
                    print("❌ No initialization segment info found")
                    print("🔍 This is likely the main issue - MediaSource needs init segment")
                    
            else:
                print("❌ No chunk_info messages received")
            
            return len(binary_chunks) > 0
            
    except Exception as e:
        print(f"❌ WebSocket test failed: {e}")
        return False

    # 3. Test MediaSource compatibility
    print("\n📺 MediaSource Compatibility Test...")
    
    # Since we can't run browser code directly, we'll check common scenarios
    print("🔍 Common MIME types that should work:")
    print("   video/mp4; codecs=\"avc1.42C01E\" (H.264 baseline)")
    print("   video/mp4; codecs=\"avc1.42001E\" (H.264 baseline alt)")
    print("   audio/mp4; codecs=\"mp4a.40.2\" (AAC-LC)")
    
    if stream_info:
        video_codec = stream_info.get('video', {}).get('codec', '')
        if 'h264' in str(video_codec).lower():
            print("✅ Video codec should work with MediaSource")
        else:
            print(f"⚠️ Video codec compatibility unclear: {video_codec}")
    
    return True

async def main():
    print("🚀 Starting TypeScript Viewer Debug Session...")
    
    # Check if server is running
    try:
        response = requests.get("http://127.0.0.1:8080/", timeout=5)
        if response.status_code != 200:
            print("❌ Server not responding. Please start with: ./scripts/test_server.sh start")
            sys.exit(1)
    except Exception:
        print("❌ Server not running. Please start with: ./scripts/test_server.sh start")
        sys.exit(1)
    
    success = await debug_typescript_viewer()
    
    print("\n" + "=" * 50)
    print("🎯 DEBUG SUMMARY")
    print("=" * 50)
    
    if success:
        print("🔍 LIKELY ISSUES IDENTIFIED:")
        print("1. ❌ Missing initialization segment in progressive streaming")
        print("2. ⚠️ TypeScript viewer using inline JS instead of compiled bundle")
        print("3. 🐛 Debug button works but logs to browser console only")
        print()
        print("🛠️ RECOMMENDED FIXES:")
        print("1. Fix streaming_transcoder.rs to provide proper MP4 initialization segments")
        print("2. Update TypeScript viewer to use the compiled bundle instead of inline code")
        print("3. Add visible debug panel instead of console-only logging")
        print()
        print("🎯 IMMEDIATE NEXT STEPS:")
        print("1. Open browser developer console at http://127.0.0.1:8080/typescript-viewer.html")
        print("2. Click Connect and Debug buttons to see console logs")
        print("3. Look for MediaSource errors and buffer states")
        print("4. Check if video element receives any buffered ranges")
    else:
        print("❌ Critical streaming issues detected - fix WebSocket/server first")
    
    print("\n💡 Browser Debug Commands:")
    print("   F12 -> Console tab")
    print("   Click 'Connect' button")  
    print("   Click 'Debug' button")
    print("   Look for MediaSource and buffer errors")

if __name__ == "__main__":
    asyncio.run(main())