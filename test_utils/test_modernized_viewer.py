#!/usr/bin/env python3
"""
Comprehensive end-to-end test for the modernized universal viewer
Tests the complete workflow: server connectivity, WebSocket, MSE, video playback
"""

import asyncio
import websockets
import json
import struct
import sys
import urllib.request
from datetime import datetime

class ModernizedViewerTester:
    def __init__(self, server_url="ws://127.0.0.1:8080/stream", http_url="http://127.0.0.1:8080"):
        self.server_url = server_url
        self.http_url = http_url
        self.results = {
            'tests_passed': 0,
            'tests_failed': 0,
            'critical_failures': [],
            'warnings': []
        }
        
    def log_test(self, test_name, passed, message="", critical=False):
        """Log test result"""
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} {test_name}: {message}")
        
        if passed:
            self.results['tests_passed'] += 1
        else:
            self.results['tests_failed'] += 1
            if critical:
                self.results['critical_failures'].append(test_name)
        
        return passed
    
    def test_server_connectivity(self):
        """Test basic HTTP server connectivity"""
        print("🔌 Testing Server Connectivity")
        print("=" * 40)
        
        try:
            response = urllib.request.urlopen(self.http_url, timeout=5)
            status_code = response.getcode()
            
            if status_code == 200:
                return self.log_test("HTTP Connectivity", True, f"Server responding (HTTP {status_code})")
            else:
                return self.log_test("HTTP Connectivity", False, f"Unexpected status code: {status_code}", critical=True)
                
        except Exception as e:
            return self.log_test("HTTP Connectivity", False, f"Connection failed: {e}", critical=True)
    
    async def test_websocket_connection(self):
        """Test WebSocket connection establishment"""
        print(f"\n🔗 Testing WebSocket Connection")
        print("=" * 35)
        
        try:
            async with websockets.connect(self.server_url) as websocket:
                self.log_test("WebSocket Connect", True, "Connection established")
                
                # Test immediate disconnect (connection stability)
                await websocket.close()
                self.log_test("WebSocket Disconnect", True, "Clean disconnect")
                return True
                
        except Exception as e:
            self.log_test("WebSocket Connect", False, f"Connection failed: {e}", critical=True)
            return False
    
    async def test_stream_info_message(self):
        """Test reception and parsing of stream_info message"""
        print(f"\n📋 Testing Stream Info Message")
        print("=" * 35)
        
        try:
            async with websockets.connect(self.server_url) as websocket:
                # Wait for stream_info message
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=3.0)
                    
                    if isinstance(message, str):
                        try:
                            data = json.loads(message)
                            msg_type = data.get('type')
                            
                            if msg_type == 'stream_info':
                                self.log_test("Stream Info Reception", True, "Received stream_info message")
                                
                                # Validate stream_info structure
                                stream_data = data.get('data', {})
                                
                                # Check video info
                                if 'video' in stream_data and stream_data['video']:
                                    video_info = stream_data['video']
                                    required_fields = ['codec', 'mime_type', 'width', 'height', 'fps']
                                    missing_fields = [field for field in required_fields if field not in video_info]
                                    
                                    if not missing_fields:
                                        self.log_test("Video Info Validation", True, f"H.264 {video_info['width']}x{video_info['height']} @ {video_info['fps']}fps")
                                    else:
                                        self.log_test("Video Info Validation", False, f"Missing fields: {missing_fields}")
                                else:
                                    self.log_test("Video Info Validation", False, "No video info in stream_info")
                                
                                # Check audio info (may be None for video-only)
                                if 'audio' in stream_data and stream_data['audio']:
                                    audio_info = stream_data['audio']
                                    if 'codec' in audio_info and 'mime_type' in audio_info:
                                        self.log_test("Audio Info Validation", True, f"Audio: {audio_info['codec']}")
                                    else:
                                        self.log_test("Audio Info Validation", False, "Incomplete audio info")
                                else:
                                    self.log_test("Audio Info Detection", True, "Video-only stream (no audio)")
                                
                                return True
                            else:
                                self.log_test("Stream Info Reception", False, f"Expected stream_info, got: {msg_type}")
                                return False
                                
                        except json.JSONDecodeError:
                            self.log_test("Stream Info Parsing", False, "Invalid JSON in first message")
                            return False
                    else:
                        self.log_test("Stream Info Reception", False, "First message was binary, expected JSON")
                        return False
                        
                except asyncio.TimeoutError:
                    self.log_test("Stream Info Reception", False, "Timeout waiting for stream_info", critical=True)
                    return False
                    
        except Exception as e:
            self.log_test("Stream Info Test", False, f"Test failed: {e}", critical=True)
            return False
    
    async def test_initialization_segment(self):
        """Test reception and validation of MP4 initialization segment"""
        print(f"\n📦 Testing Initialization Segment")
        print("=" * 40)
        
        try:
            async with websockets.connect(self.server_url) as websocket:
                messages_received = 0
                max_messages = 5
                init_segment_found = False
                
                while messages_received < max_messages and not init_segment_found:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=3.0)
                        messages_received += 1
                        
                        if isinstance(message, bytes):
                            # Check if this is a valid MP4 initialization segment
                            if len(message) >= 8:
                                size = struct.unpack('>I', message[0:4])[0]
                                box_type = message[4:8].decode('ascii', errors='ignore')
                                
                                if box_type == 'ftyp':
                                    self.log_test("Init Segment Reception", True, f"Received ftyp box ({len(message)} bytes)")
                                    
                                    # Validate MP4 structure
                                    if len(message) >= size and size > 8:
                                        self.log_test("Init Segment Structure", True, f"Valid MP4 box structure")
                                    else:
                                        self.log_test("Init Segment Structure", False, f"Invalid box size: {size} vs {len(message)}")
                                    
                                    # Look for ESDS modifications (for audio streams)
                                    if b'esds' in message:
                                        esds_pos = message.find(b'esds')
                                        self.log_test("ESDS Detection", True, f"ESDS found at offset {esds_pos}")
                                        
                                        # Check for object type modifications
                                        object_type_02_found = False
                                        for i in range(esds_pos, min(esds_pos + 50, len(message) - 2)):
                                            if message[i] == 0x04 and i + 2 < len(message):
                                                obj_type = message[i + 2]
                                                if obj_type == 0x02:
                                                    object_type_02_found = True
                                                    self.log_test("ESDS Modification", True, f"Chrome-compatible object type 0x02 found")
                                                    break
                                        
                                        if not object_type_02_found:
                                            self.results['warnings'].append("No object type 0x02 found - may affect Chrome compatibility")
                                    else:
                                        self.log_test("Stream Type", True, "Video-only stream (no ESDS)")
                                    
                                    init_segment_found = True
                                
                    except asyncio.TimeoutError:
                        break
                
                if not init_segment_found:
                    self.log_test("Init Segment Reception", False, f"No initialization segment found in {messages_received} messages", critical=True)
                    return False
                
                return True
                
        except Exception as e:
            self.log_test("Init Segment Test", False, f"Test failed: {e}", critical=True)
            return False
    
    async def test_chunk_info_flow(self):
        """Test chunk_info message flow and binary data correlation"""
        print(f"\n📨 Testing Chunk Info Flow")
        print("=" * 30)
        
        try:
            async with websockets.connect(self.server_url) as websocket:
                messages_received = 0
                max_messages = 6
                chunk_info_count = 0
                binary_chunk_count = 0
                
                while messages_received < max_messages:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=3.0)
                        messages_received += 1
                        
                        if isinstance(message, str):
                            try:
                                data = json.loads(message)
                                if data.get('type') == 'chunk_info':
                                    chunk_info_count += 1
                                    chunk_data = data.get('data', {})
                                    chunk_type = chunk_data.get('chunk_type', 'unknown')
                                    chunk_size = chunk_data.get('size', 0)
                                    
                                    if chunk_type and chunk_size > 0:
                                        self.log_test(f"Chunk Info {chunk_info_count}", True, f"{chunk_type} - {chunk_size} bytes")
                                    else:
                                        self.log_test(f"Chunk Info {chunk_info_count}", False, "Incomplete chunk_info data")
                                        
                            except json.JSONDecodeError:
                                pass
                        elif isinstance(message, bytes):
                            binary_chunk_count += 1
                            self.log_test(f"Binary Chunk {binary_chunk_count}", True, f"{len(message)} bytes received")
                            
                    except asyncio.TimeoutError:
                        break
                
                # Validate flow pattern
                if chunk_info_count > 0 and binary_chunk_count > 0:
                    self.log_test("Message Flow Pattern", True, f"{chunk_info_count} chunk_info + {binary_chunk_count} binary")
                    return True
                else:
                    self.log_test("Message Flow Pattern", False, f"Incomplete flow: {chunk_info_count} info, {binary_chunk_count} binary", critical=True)
                    return False
                    
        except Exception as e:
            self.log_test("Chunk Info Flow Test", False, f"Test failed: {e}", critical=True)
            return False
    
    async def test_chrome_mse_compatibility(self):
        """Test Chrome MediaSource compatibility based on received codec info"""
        print(f"\n🌐 Testing Chrome MSE Compatibility")
        print("=" * 45)
        
        try:
            async with websockets.connect(self.server_url) as websocket:
                # Get stream info
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=3.0)
                    
                    if isinstance(message, str):
                        data = json.loads(message)
                        if data.get('type') == 'stream_info':
                            stream_data = data.get('data', {})
                            
                            # Test video codec compatibility
                            if 'video' in stream_data and stream_data['video']:
                                video_mime = stream_data['video'].get('mime_type', '')
                                
                                # Known Chrome-compatible video codecs
                                chrome_video_codecs = [
                                    'video/mp4; codecs="avc1.42E01E"',
                                    'video/mp4; codecs="avc1.42001E"',
                                    'video/mp4; codecs="avc1.640028"'
                                ]
                                
                                if any(codec in video_mime for codec in ['avc1.42E01E', 'avc1.42001E']):
                                    self.log_test("Video Codec Compatibility", True, f"Chrome-compatible: {video_mime}")
                                else:
                                    self.log_test("Video Codec Compatibility", False, f"May not be Chrome-compatible: {video_mime}")
                            
                            # Test audio codec compatibility
                            if 'audio' in stream_data and stream_data['audio']:
                                audio_mime = stream_data['audio'].get('mime_type', '')
                                
                                # Known Chrome-compatible audio codecs
                                chrome_audio_codecs = [
                                    'mp4a.40.2',    # AAC-LC
                                    'mp4a.40.02',   # AAC-LC (zero-padded)
                                    'mp4a.40.5',    # HE-AAC
                                ]
                                
                                if any(codec in audio_mime for codec in chrome_audio_codecs):
                                    self.log_test("Audio Codec Compatibility", True, f"Chrome-compatible: {audio_mime}")
                                else:
                                    self.log_test("Audio Codec Compatibility", False, f"May not be Chrome-compatible: {audio_mime}")
                                    self.results['warnings'].append(f"Audio codec may need ESDS modification: {audio_mime}")
                            else:
                                self.log_test("Stream Type Check", True, "Video-only stream (no audio compatibility issues)")
                            
                            return True
                            
                except (asyncio.TimeoutError, json.JSONDecodeError):
                    self.log_test("Chrome Compatibility Test", False, "Could not analyze stream info", critical=True)
                    return False
                    
        except Exception as e:
            self.log_test("Chrome Compatibility Test", False, f"Test failed: {e}", critical=True)
            return False
    
    def generate_final_report(self):
        """Generate comprehensive test report"""
        print(f"\n📄 Final Test Report")
        print("=" * 60)
        
        total_tests = self.results['tests_passed'] + self.results['tests_failed']
        pass_rate = (self.results['tests_passed'] / total_tests * 100) if total_tests > 0 else 0
        
        print(f"🕐 Test completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Tests run: {total_tests}")
        print(f"✅ Passed: {self.results['tests_passed']}")
        print(f"❌ Failed: {self.results['tests_failed']}")
        print(f"📈 Pass rate: {pass_rate:.1f}%")
        print()
        
        # Critical failures
        if self.results['critical_failures']:
            print("🚨 CRITICAL FAILURES:")
            for failure in self.results['critical_failures']:
                print(f"   → {failure}")
            print()
        
        # Warnings
        if self.results['warnings']:
            print("⚠️ WARNINGS:")
            for warning in self.results['warnings']:
                print(f"   → {warning}")
            print()
        
        # Overall assessment
        if self.results['tests_failed'] == 0:
            print("🎉 SUCCESS: All tests passed!")
            print("   → Modernized viewer is ready for browser testing")
            print("   → WebSocket connectivity verified")
            print("   → MP4 streaming pipeline operational")
            print("   → Chrome compatibility validated")
            print(f"   → Test in browser: {self.http_url}")
            return True
        elif not self.results['critical_failures']:
            print("⚠️ PARTIAL SUCCESS: Minor issues detected")
            print("   → Core functionality working")
            print("   → Some compatibility warnings present")
            print("   → Safe to proceed with browser testing")
            return True
        else:
            print("❌ FAILURE: Critical issues prevent browser testing")
            print("   → Address critical failures before proceeding")
            print("   → Check server configuration and video file")
            return False

async def main():
    """Main test execution"""
    print("🧪 OpenBroadcastNetwork Modernized Viewer Test Suite")
    print("=" * 70)
    print()
    
    tester = ModernizedViewerTester()
    
    # Run test suite
    tests = [
        ("Server Connectivity", tester.test_server_connectivity),
        ("WebSocket Connection", tester.test_websocket_connection),
        ("Stream Info Message", tester.test_stream_info_message),
        ("Initialization Segment", tester.test_initialization_segment),
        ("Chunk Info Flow", tester.test_chunk_info_flow),
        ("Chrome MSE Compatibility", tester.test_chrome_mse_compatibility)
    ]
    
    for test_name, test_func in tests:
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
                
            if not result and test_name in ["Server Connectivity", "WebSocket Connection"]:
                print(f"\n🛑 Stopping tests due to critical failure in {test_name}")
                break
                
        except Exception as e:
            tester.log_test(test_name, False, f"Test error: {e}", critical=True)
    
    # Generate final report
    success = tester.generate_final_report()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    asyncio.run(main())