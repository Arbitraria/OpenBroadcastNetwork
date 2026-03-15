#!/usr/bin/env python3
"""
Quick test of the fixed TypeScript viewer
"""

import asyncio
import websockets
import json
import time

async def test_websocket_quickly():
    """Quick WebSocket test to verify server connectivity"""
    
    try:
        print("🔌 Testing WebSocket connection...")
        async with websockets.connect('ws://127.0.0.1:8080/stream') as ws:
            print("✅ WebSocket connected successfully")
            
            # Wait for initial messages
            start_time = time.time()
            messages_received = 0
            binary_received = 0
            
            while time.time() - start_time < 3 and messages_received < 5:
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    
                    if isinstance(message, str):
                        try:
                            parsed = json.loads(message)
                            print(f"📨 JSON: {parsed.get('type', 'unknown')}")
                            messages_received += 1
                            
                            if parsed.get('type') == 'stream_info':
                                print(f"   🎬 Video: {parsed.get('video', {}).get('codec', 'none')}")
                                print(f"   🔊 Audio: {parsed.get('audio', {}).get('codec', 'none')}")
                                
                        except json.JSONDecodeError:
                            print(f"📨 String: {message[:50]}...")
                    else:
                        print(f"📦 Binary: {len(message)} bytes")
                        binary_received += 1
                        
                except asyncio.TimeoutError:
                    break
            
            print(f"📊 Received {messages_received} JSON messages, {binary_received} binary chunks")
            
            if messages_received > 0:
                print("✅ Server is streaming data correctly")
                return True
            else:
                print("⚠️ No messages received from server")
                return False
                
    except Exception as e:
        print(f"❌ WebSocket test failed: {e}")
        return False

async def main():
    print("🚀 Quick test of fixed TypeScript viewer")
    print("=" * 40)
    
    success = await test_websocket_quickly()
    
    if success:
        print()
        print("🎯 READY TO TEST!")
        print("1. Open: http://127.0.0.1:8080/typescript-viewer-fixed.html")
        print("2. Click 'Connect' button")
        print("3. Click 'Debug' button to see real-time info")
        print("4. Video should start playing automatically")
        print()
        print("🔧 Shortcuts:")
        print("   Ctrl+C: Connect/Disconnect")
        print("   Ctrl+D: Toggle debug panel")
        print("   Space:  Play/Pause")
    else:
        print("❌ Server not ready - please check server status")

if __name__ == "__main__":
    asyncio.run(main())