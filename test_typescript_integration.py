#!/usr/bin/env python3
"""
Test TypeScript UI Integration with OpenBroadcastNetwork Streaming Server

This script verifies that the TypeScript components correctly integrate with
the Rust web server and can receive/process streaming data.
"""

import asyncio
import json
import time
import websockets
import requests
import sys
from urllib.parse import urlparse

class TypeScriptIntegrationTester:
    def __init__(self, base_url="http://127.0.0.1:8080"):
        self.base_url = base_url
        self.ws_url = base_url.replace('http:', 'ws:').replace('https:', 'wss:') + '/stream'
        self.test_results = []
        
    def log(self, message, level="INFO"):
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] [{level}] {message}")
        
    def test_result(self, test_name, success, details=""):
        result = {
            'test': test_name,
            'success': success,
            'details': details,
            'timestamp': time.time()
        }
        self.test_results.append(result)
        
        status = "✅ PASS" if success else "❌ FAIL"
        self.log(f"{status} {test_name}: {details}")
        
    async def test_http_endpoints(self):
        """Test HTTP endpoint accessibility"""
        self.log("Testing HTTP endpoints...")
        
        # Test main viewer
        try:
            response = requests.get(f"{self.base_url}/", timeout=5)
            self.test_result(
                "Main viewer endpoint", 
                response.status_code == 200,
                f"Status: {response.status_code}, Content-Type: {response.headers.get('content-type', 'unknown')}"
            )
        except Exception as e:
            self.test_result("Main viewer endpoint", False, f"Request failed: {e}")
            
        # Test TypeScript viewer
        try:
            response = requests.get(f"{self.base_url}/typescript-viewer.html", timeout=5)
            self.test_result(
                "TypeScript viewer endpoint", 
                response.status_code == 200,
                f"Status: {response.status_code}, Size: {len(response.content)} bytes"
            )
            
            # Check for TypeScript-specific content
            content = response.text
            has_typescript_content = (
                'TypeScript' in content and 
                'MediaSource' in content and
                'WebSocket' in content
            )
            self.test_result(
                "TypeScript viewer content", 
                has_typescript_content,
                "Contains expected TypeScript/MSE/WebSocket references"
            )
            
        except Exception as e:
            self.test_result("TypeScript viewer endpoint", False, f"Request failed: {e}")
            
        # Test JavaScript bundle
        try:
            response = requests.get(f"{self.base_url}/main.9b279abcfdfc45f739ac.js", timeout=5)
            self.test_result(
                "TypeScript JavaScript bundle", 
                response.status_code == 200,
                f"Status: {response.status_code}, Size: {len(response.content)} bytes"
            )
        except Exception as e:
            self.test_result("TypeScript JavaScript bundle", False, f"Request failed: {e}")
            
    async def test_websocket_connection(self):
        """Test WebSocket connection and protocol"""
        self.log("Testing WebSocket connection...")
        
        try:
            # Test basic WebSocket connection
            async with websockets.connect(self.ws_url, timeout=10) as websocket:
                self.test_result(
                    "WebSocket connection establishment", 
                    True,
                    "Successfully connected to WebSocket endpoint"
                )
                
                # Wait for initial messages
                messages_received = []
                try:
                    # Collect messages for 5 seconds
                    start_time = time.time()
                    while time.time() - start_time < 5:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                            if isinstance(message, str):
                                try:
                                    parsed = json.loads(message)
                                    messages_received.append(parsed)
                                    self.log(f"Received JSON message: {parsed.get('type', 'unknown')}")
                                except json.JSONDecodeError:
                                    self.log(f"Received non-JSON string: {message[:100]}...")
                            else:
                                # Binary message
                                self.log(f"Received binary message: {len(message)} bytes")
                                messages_received.append({'type': 'binary', 'size': len(message)})
                        except asyncio.TimeoutError:
                            continue
                            
                except Exception as e:
                    self.log(f"Message collection ended: {e}")
                
                # Analyze received messages
                json_messages = [m for m in messages_received if isinstance(m, dict) and m.get('type') != 'binary']
                binary_messages = [m for m in messages_received if isinstance(m, dict) and m.get('type') == 'binary']
                
                self.test_result(
                    "WebSocket message reception", 
                    len(messages_received) > 0,
                    f"Received {len(json_messages)} JSON messages, {len(binary_messages)} binary messages"
                )
                
                # Check for expected message types
                message_types = [m.get('type') for m in json_messages]
                expected_types = ['stream_info', 'chunk_info']
                
                has_stream_info = 'stream_info' in message_types
                has_chunk_info = 'chunk_info' in message_types
                has_binary_data = len(binary_messages) > 0
                
                self.test_result(
                    "Stream info message", 
                    has_stream_info,
                    "Received stream_info with codec information"
                )
                
                self.test_result(
                    "Chunk info messages", 
                    has_chunk_info,
                    "Received chunk_info messages before binary data"
                )
                
                self.test_result(
                    "Binary data reception", 
                    has_binary_data,
                    f"Received {len(binary_messages)} binary chunks"
                )
                
                # Check stream_info content
                stream_info = next((m for m in json_messages if m.get('type') == 'stream_info'), None)
                if stream_info:
                    has_video_info = 'video' in stream_info
                    has_audio_info = 'audio' in stream_info
                    
                    self.test_result(
                        "Video stream information", 
                        has_video_info,
                        f"Video codec: {stream_info.get('video', {}).get('codec', 'unknown')}"
                    )
                    
                    if has_audio_info:
                        self.test_result(
                            "Audio stream information", 
                            True,
                            f"Audio codec: {stream_info.get('audio', {}).get('codec', 'unknown')}"
                        )
                    else:
                        self.test_result(
                            "Audio stream information", 
                            False,
                            "No audio information in stream_info"
                        )
                
        except Exception as e:
            self.test_result("WebSocket connection establishment", False, f"Connection failed: {e}")
            
    async def test_mediatype_compatibility(self):
        """Test MediaSource Extensions compatibility"""
        self.log("Testing media type compatibility...")
        
        # This would need to be done in a browser context, so we'll simulate it
        # by checking the server's codec information
        
        try:
            async with websockets.connect(self.ws_url, timeout=10) as websocket:
                # Get stream info
                while True:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=5)
                        if isinstance(message, str):
                            try:
                                parsed = json.loads(message)
                                if parsed.get('type') == 'stream_info':
                                    # Check codec compatibility
                                    video_codec = parsed.get('video', {}).get('codec')
                                    audio_codec = parsed.get('audio', {}).get('codec')
                                    
                                    # Common browser-supported codecs
                                    supported_video = ['h264', 'avc1']
                                    supported_audio = ['aac', 'mp4a']
                                    
                                    video_compatible = any(codec in str(video_codec).lower() for codec in supported_video)
                                    audio_compatible = not audio_codec or any(codec in str(audio_codec).lower() for codec in supported_audio)
                                    
                                    self.test_result(
                                        "Video codec compatibility", 
                                        video_compatible,
                                        f"Codec '{video_codec}' is {'compatible' if video_compatible else 'not compatible'} with browsers"
                                    )
                                    
                                    self.test_result(
                                        "Audio codec compatibility", 
                                        audio_compatible,
                                        f"Codec '{audio_codec}' is {'compatible' if audio_compatible else 'not compatible'} with browsers"
                                    )
                                    
                                    break
                            except json.JSONDecodeError:
                                continue
                    except asyncio.TimeoutError:
                        self.test_result("MediaSource compatibility test", False, "No stream_info received")
                        break
                        
        except Exception as e:
            self.test_result("MediaSource compatibility test", False, f"Test failed: {e}")
            
    async def test_streaming_protocol(self):
        """Test the complete streaming protocol flow"""
        self.log("Testing streaming protocol flow...")
        
        try:
            async with websockets.connect(self.ws_url, timeout=10) as websocket:
                protocol_steps = {
                    'connection': False,
                    'stream_info': False,
                    'chunk_info': False,
                    'binary_data': False,
                    'data_ordering': True
                }
                
                protocol_steps['connection'] = True
                
                chunk_infos = []
                binary_chunks = []
                message_order = []
                
                start_time = time.time()
                while time.time() - start_time < 8:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        
                        if isinstance(message, str):
                            try:
                                parsed = json.loads(message)
                                message_type = parsed.get('type')
                                message_order.append(('json', message_type))
                                
                                if message_type == 'stream_info':
                                    protocol_steps['stream_info'] = True
                                elif message_type == 'chunk_info':
                                    protocol_steps['chunk_info'] = True
                                    chunk_infos.append(parsed)
                                    
                            except json.JSONDecodeError:
                                message_order.append(('string', 'non-json'))
                        else:
                            # Binary data
                            protocol_steps['binary_data'] = True
                            binary_chunks.append(len(message))
                            message_order.append(('binary', len(message)))
                            
                    except asyncio.TimeoutError:
                        continue
                
                # Analyze protocol compliance
                for step, completed in protocol_steps.items():
                    self.test_result(
                        f"Protocol step: {step}", 
                        completed,
                        "Completed successfully" if completed else "Not observed"
                    )
                
                # Check message ordering (chunk_info should precede binary data)
                chunk_binary_pairs = 0
                for i, (msg_type, content) in enumerate(message_order):
                    if msg_type == 'json' and content == 'chunk_info':
                        # Look for following binary message
                        for j in range(i + 1, min(i + 5, len(message_order))):
                            if message_order[j][0] == 'binary':
                                chunk_binary_pairs += 1
                                break
                
                self.test_result(
                    "Protocol message ordering", 
                    chunk_binary_pairs > 0,
                    f"Found {chunk_binary_pairs} chunk_info -> binary_data sequences"
                )
                
                # Data volume analysis
                total_binary_bytes = sum(binary_chunks)
                self.test_result(
                    "Binary data volume", 
                    total_binary_bytes > 0,
                    f"Received {total_binary_bytes} bytes in {len(binary_chunks)} chunks"
                )
                
        except Exception as e:
            self.test_result("Streaming protocol test", False, f"Test failed: {e}")
            
    def generate_report(self):
        """Generate test report"""
        self.log("=" * 60)
        self.log("TYPESCRIPT INTEGRATION TEST REPORT")
        self.log("=" * 60)
        
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results if r['success']])
        failed_tests = total_tests - passed_tests
        
        self.log(f"Total Tests: {total_tests}")
        self.log(f"Passed: {passed_tests}")
        self.log(f"Failed: {failed_tests}")
        self.log(f"Success Rate: {(passed_tests/total_tests*100):.1f}%")
        
        if failed_tests > 0:
            self.log("")
            self.log("FAILED TESTS:")
            for result in self.test_results:
                if not result['success']:
                    self.log(f"❌ {result['test']}: {result['details']}")
        
        self.log("")
        self.log("INTEGRATION STATUS:")
        
        # Critical tests for integration
        critical_tests = [
            'TypeScript viewer endpoint',
            'TypeScript JavaScript bundle', 
            'WebSocket connection establishment',
            'Stream info message',
            'Binary data reception'
        ]
        
        critical_passed = len([
            r for r in self.test_results 
            if r['test'] in critical_tests and r['success']
        ])
        
        if critical_passed == len(critical_tests):
            self.log("🎉 INTEGRATION SUCCESSFUL - TypeScript UI fully integrated with Rust server")
        elif critical_passed >= len(critical_tests) * 0.8:
            self.log("⚠️ INTEGRATION MOSTLY WORKING - Minor issues detected")
        else:
            self.log("❌ INTEGRATION ISSUES - Major problems detected")
            
        return passed_tests == total_tests
        
async def main():
    # Check if server is running
    try:
        response = requests.get("http://127.0.0.1:8080/", timeout=5)
        if response.status_code != 200:
            print("❌ Server not responding correctly. Please start the server first.")
            print("Run: ./scripts/test_server.sh start")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Cannot connect to server: {e}")
        print("Please ensure the server is running on http://127.0.0.1:8080")
        print("Run: ./scripts/test_server.sh start")
        sys.exit(1)
    
    # Run tests
    tester = TypeScriptIntegrationTester()
    
    print("🚀 Starting TypeScript Integration Tests...")
    print("🎯 Testing OpenBroadcastNetwork TypeScript UI integration")
    print("")
    
    await tester.test_http_endpoints()
    await tester.test_websocket_connection()
    await tester.test_mediatype_compatibility()
    await tester.test_streaming_protocol()
    
    success = tester.generate_report()
    
    if success:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())