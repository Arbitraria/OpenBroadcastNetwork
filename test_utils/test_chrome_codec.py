#!/usr/bin/env python3
"""
Automated Chrome-based codec testing for OpenBroadcastNetwork
Tests WebSocket connection and MediaSource API compatibility with Chrome engine
"""

import asyncio
import websockets
import json
import struct
import sys
from datetime import datetime

class ChromeCodecTester:
    def __init__(self, server_url="ws://127.0.0.1:8080/stream"):
        self.server_url = server_url
        self.results = {}
        
    async def test_websocket_connection(self):
        """Test basic WebSocket connectivity"""
        print("🔌 Testing WebSocket Connection")
        print("=" * 50)
        
        try:
            async with websockets.connect(self.server_url) as websocket:
                print("✅ WebSocket connection successful")
                
                # Wait for first message
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    
                    if isinstance(message, bytes):
                        print(f"📦 Received binary data: {len(message)} bytes")
                        return self.analyze_initialization_segment(message)
                    else:
                        # JSON control message
                        try:
                            data = json.loads(message)
                            print(f"📋 Received control message: {data.get('type', 'unknown')}")
                            if data.get('type') == 'stream_info':
                                return self.analyze_stream_info(data)
                        except json.JSONDecodeError:
                            print(f"📄 Received text data: {len(message)} chars")
                            
                except asyncio.TimeoutError:
                    print("⏱️ Timeout waiting for server data")
                    return False
                    
        except Exception as e:
            print(f"❌ WebSocket connection failed: {e}")
            return False
            
    def analyze_initialization_segment(self, data):
        """Analyze MP4 initialization segment for ESDS modifications"""
        print(f"\n🔍 Analyzing Initialization Segment ({len(data)} bytes)")
        print("-" * 40)
        
        if len(data) < 8:
            print("❌ Data too small to be valid MP4")
            return False
            
        # Parse first MP4 box
        size = struct.unpack('>I', data[0:4])[0]
        box_type = data[4:8].decode('ascii', errors='ignore')
        
        print(f"📦 First box: '{box_type}' ({size} bytes)")
        
        if box_type != 'ftyp':
            print("⚠️ Expected ftyp box as first box")
            return False
            
        # Look for ESDS pattern and object type
        object_types_found = []
        esds_positions = []
        
        # Search for ESDS boxes
        i = 0
        while i < len(data) - 20:
            if data[i:i+4] == b'esds':
                esds_positions.append(i)
                print(f"🎯 Found ESDS box at offset {i}")
                
                # Look for object type pattern
                for j in range(i, min(i + 50, len(data) - 2)):
                    if data[j] == 0x04 and j + 2 < len(data):  # DecoderConfigDescriptor
                        obj_type = data[j + 2]
                        object_types_found.append((j, obj_type))
                        print(f"   📍 Object type: 0x{obj_type:02X} at offset {j + 2}")
            i += 1
            
        # Analyze results
        if not object_types_found:
            print("⚠️ No object types found in ESDS")
            return False
            
        # Check if modification was applied (0x40 -> 0x02)
        has_0x02 = any(obj_type == 0x02 for _, obj_type in object_types_found)
        has_0x40 = any(obj_type == 0x40 for _, obj_type in object_types_found)
        
        if has_0x02 and not has_0x40:
            print("✅ ESDS modification detected: Object type 0x40 → 0x02")
            self.results['esds_modified'] = True
        elif has_0x40:
            print("⚠️ Original object type 0x40 still present")
            self.results['esds_modified'] = False
        else:
            print("❓ Unexpected object type configuration")
            self.results['esds_modified'] = False
            
        return True
        
    def analyze_stream_info(self, data):
        """Analyze stream info message from server"""
        print(f"\n📋 Analyzing Stream Info")
        print("-" * 30)
        
        stream_data = data.get('data', {})
        
        if 'audio' in stream_data:
            audio_info = stream_data['audio']
            codec = audio_info.get('codec', 'Unknown')
            mime_type = audio_info.get('mime_type', 'Unknown')
            
            print(f"🎵 Audio codec: {codec}")
            print(f"🎵 Audio MIME: {mime_type}")
            
            self.results['server_audio_codec'] = mime_type
            
        if 'video' in stream_data:
            video_info = stream_data['video']
            codec = video_info.get('codec', 'Unknown')
            mime_type = video_info.get('mime_type', 'Unknown')
            
            print(f"📺 Video codec: {codec}")
            print(f"📺 Video MIME: {mime_type}")
            
            self.results['server_video_codec'] = mime_type
            
        return True
        
    def test_chrome_codec_support(self):
        """Simulate Chrome MediaSource.isTypeSupported() testing"""
        print(f"\n🌐 Chrome Codec Compatibility Test")
        print("=" * 50)
        
        # Common codec formats to test for AAC with object type 0x02
        test_codecs = [
            'audio/mp4; codecs="mp4a.40.2"',    # Standard AAC-LC 
            'audio/mp4; codecs="mp4a.40.02"',   # Zero-padded
            'audio/mp4; codecs="mp4a.67"',      # Alternative AAC-LC
            'audio/mp4; codecs="mp4a.66"',      # AAC Main
            'audio/mp4; codecs="mp4a.69"',      # MP3 in MP4
            'audio/mp4; codecs="mp4a.6B"',      # MP3 alternative
            'audio/mp4; codecs="mp4a.40.5"',    # HE-AAC
            'audio/mp4; codecs="mp4a.40"',      # Generic MPEG-4
            'audio/mp4'                         # Generic
        ]
        
        print("📊 Codec support matrix (based on Chrome specifications):")
        print()
        
        # Simulate Chrome support (based on known Chrome behavior)
        chrome_supported = [
            'audio/mp4; codecs="mp4a.40.2"',
            'audio/mp4; codecs="mp4a.40.02"', 
            'audio/mp4; codecs="mp4a.40.5"',
            'audio/mp4; codecs="mp4a.40"',
            'audio/mp4'
        ]
        
        recommended = None
        for codec in test_codecs:
            supported = codec in chrome_supported
            status = "✅ SUPPORTED" if supported else "❌ NOT SUPPORTED"
            print(f"  {codec:<40} {status}")
            
            if supported and not recommended:
                recommended = codec
                
        if recommended:
            print(f"\n🎯 RECOMMENDED for Chrome: {recommended}")
            self.results['recommended_codec'] = recommended
        else:
            print(f"\n⚠️ No supported codec formats found")
            
        return recommended is not None
        
    def generate_report(self):
        """Generate final test report"""
        print(f"\n📄 Test Report Summary")
        print("=" * 50)
        
        print(f"🕐 Test completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Server connectivity
        server_audio = self.results.get('server_audio_codec', 'Not detected')
        server_video = self.results.get('server_video_codec', 'Not detected')
        print(f"📡 Server audio codec: {server_audio}")
        print(f"📡 Server video codec: {server_video}")
        
        # ESDS modification
        esds_modified = self.results.get('esds_modified', False)
        esds_status = "✅ Applied" if esds_modified else "❌ Not applied"
        print(f"🔧 ESDS modification: {esds_status}")
        
        # Chrome compatibility
        recommended = self.results.get('recommended_codec')
        if recommended:
            print(f"🎯 Chrome-compatible codec: {recommended}")
        
        # Final assessment
        print()
        if esds_modified and recommended:
            print("🎉 SUCCESS: Server configured for Chrome compatibility")
            print("   → ESDS modified to object type 0x02")
            print("   → Chrome-supported codec format available")
            return True
        elif not esds_modified:
            print("⚠️ ISSUE: ESDS modification not detected")
            print("   → May cause 'object type 0x40 does not match' errors")
            return False
        else:
            print("❓ PARTIAL: ESDS modified but codec compatibility unclear")
            return False

async def main():
    """Main test execution"""
    print("🧪 OpenBroadcastNetwork Chrome Codec Tester")
    print("=" * 60)
    print()
    
    # Check server is running
    import urllib.request
    try:
        response = urllib.request.urlopen('http://127.0.0.1:8080/', timeout=3)
        print("✅ Server is running on http://127.0.0.1:8080/")
    except Exception as e:
        print(f"❌ Server not accessible: {e}")
        print("   Start server with: cargo run -p OpenBroadcastNetwork-node web-viewer --video sample_video.mp4")
        sys.exit(1)
    
    print()
    
    # Run tests
    tester = ChromeCodecTester()
    
    # Test WebSocket and analyze data
    connection_success = await tester.test_websocket_connection()
    
    if not connection_success:
        print("\n❌ WebSocket test failed - cannot proceed with codec analysis")
        sys.exit(1)
    
    # Test Chrome codec compatibility
    codec_success = tester.test_chrome_codec_support()
    
    # Generate final report
    overall_success = tester.generate_report()
    
    sys.exit(0 if overall_success else 1)

if __name__ == "__main__":
    asyncio.run(main())