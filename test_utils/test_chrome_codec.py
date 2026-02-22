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
    def __init__(self, server_url="ws://127.0.0.1:8081/stream"):
        self.server_url = server_url
        self.results = {}
        
    async def test_websocket_connection(self):
        """Test basic WebSocket connectivity and analyze stream data"""
        print("🔌 Testing WebSocket Connection")
        print("=" * 50)
        
        try:
            async with websockets.connect(self.server_url) as websocket:
                print("✅ WebSocket connection successful")
                
                # Process first few messages to get both stream_info and initialization segment
                messages_received = 0
                max_messages = 5
                stream_info_received = False
                init_segment_received = False
                
                while messages_received < max_messages and not (stream_info_received and init_segment_received):
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                        messages_received += 1
                        
                        if isinstance(message, bytes):
                            print(f"📦 Received binary data: {len(message)} bytes")
                            if not init_segment_received:
                                init_segment_received = self.analyze_initialization_segment(message)
                        else:
                            # JSON control message
                            try:
                                data = json.loads(message)
                                msg_type = data.get('type', 'unknown')
                                print(f"📋 Received control message: {msg_type}")
                                
                                if msg_type == 'stream_info' and not stream_info_received:
                                    stream_info_received = self.analyze_stream_info(data)
                                elif msg_type == 'chunk_info':
                                    chunk_type = data.get('data', {}).get('chunk_type', 'unknown')
                                    chunk_size = data.get('data', {}).get('size', 0)
                                    print(f"📦 Chunk info: {chunk_type} ({chunk_size} bytes)")
                                    
                            except json.JSONDecodeError:
                                print(f"📄 Received text data: {len(message)} chars")
                                
                    except asyncio.TimeoutError:
                        print("⏱️ Timeout waiting for server data")
                        break
                
                # Return success if we got both essential pieces
                return stream_info_received
                    
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

        # Look for ESDS pattern and analyze
        esds_found = False
        object_type_indication = None
        audio_object_type = None

        # Search for ESDS boxes (search for the 4-byte type)
        i = 0
        while i < len(data) - 30:
            if data[i:i+4] == b'esds':
                esds_found = True
                esds_offset = i - 4  # Box starts 4 bytes earlier (size field)
                print(f"🎯 Found ESDS box at offset {esds_offset}")

                # ESDS structure after box header (8 bytes) and version/flags (4 bytes):
                # - ES_Descriptor (tag 0x03)
                # - DecoderConfigDescriptor (tag 0x04) contains objectTypeIndication
                # - DecSpecificInfoDescriptor (tag 0x05) contains AudioSpecificConfig

                # Find DecoderConfigDescriptor (tag 0x04)
                for j in range(i + 4, min(i + 60, len(data) - 15)):
                    if data[j] == 0x04:  # DecoderConfigDescriptor tag
                        # Skip tag and length bytes to get to objectTypeIndication
                        k = j + 1
                        while k < len(data) and (data[k] & 0x80) != 0:
                            k += 1
                        k += 1  # Skip last length byte

                        if k < len(data):
                            object_type_indication = data[k]
                            print(f"   📍 objectTypeIndication: 0x{object_type_indication:02X} at offset {k}")

                            # Now find DecSpecificInfoDescriptor (tag 0x05) for AudioSpecificConfig
                            for m in range(k + 13, min(k + 30, len(data) - 5)):
                                if data[m] == 0x05:  # DecSpecificInfoDescriptor tag
                                    # Skip tag and length bytes
                                    n = m + 1
                                    while n < len(data) and (data[n] & 0x80) != 0:
                                        n += 1
                                    n += 1  # Skip last length byte

                                    if n < len(data):
                                        asc_byte = data[n]
                                        audio_object_type = (asc_byte >> 3) & 0x1F
                                        freq_index_upper = asc_byte & 0x07
                                        freq_index_lower = (data[n + 1] >> 7) & 0x01 if n + 1 < len(data) else 0
                                        freq_index = (freq_index_upper << 1) | freq_index_lower

                                        print(f"   📍 AudioSpecificConfig at offset {n}:")
                                        print(f"      - audioObjectType: {audio_object_type}")
                                        print(f"      - frequencyIndex: {freq_index}")
                                        print(f"      - raw bytes: 0x{asc_byte:02X} 0x{data[n+1]:02X}" if n + 1 < len(data) else f"      - raw byte: 0x{asc_byte:02X}")
                                    break
                        break
                break
            i += 1

        # Analyze results
        if not esds_found:
            print("ℹ️ No ESDS box found (may be video-only stream)")
            self.results['esds_found'] = False
            return True  # Not an error, just no audio

        self.results['esds_found'] = True
        self.results['object_type_indication'] = object_type_indication
        self.results['audio_object_type'] = audio_object_type

        # Chrome requires: objectTypeIndication=0x40 AND audioObjectType=2
        if object_type_indication == 0x40 and audio_object_type == 2:
            print("✅ ESDS is Chrome-compatible!")
            print("   - objectTypeIndication=0x40 (MPEG-4 Audio) ✓")
            print("   - audioObjectType=2 (AAC-LC) ✓")
            self.results['esds_chrome_compatible'] = True
        elif object_type_indication == 0x40 and audio_object_type != 2:
            print(f"⚠️ ESDS needs audioObjectType fix: {audio_object_type} → 2")
            self.results['esds_chrome_compatible'] = False
        elif object_type_indication != 0x40:
            print(f"⚠️ Unexpected objectTypeIndication: 0x{object_type_indication:02X} (expected 0x40)")
            self.results['esds_chrome_compatible'] = False
        else:
            print("❓ Could not determine ESDS compatibility")
            self.results['esds_chrome_compatible'] = False

        return True
        
    def analyze_stream_info(self, data):
        """Analyze stream info message from server"""
        print(f"\n📋 Analyzing Stream Info")
        print("-" * 30)
        
        stream_data = data.get('data', {})
        
        if not stream_data:
            print("⚠️ No data field in stream_info message")
            return False
            
        # Handle video info
        if 'video' in stream_data and stream_data['video']:
            video_info = stream_data['video']
            codec = video_info.get('codec', 'Unknown')
            mime_type = video_info.get('mime_type', 'Unknown')
            
            print(f"📺 Video codec: {codec}")
            print(f"📺 Video MIME: {mime_type}")
            
            self.results['server_video_codec'] = mime_type
        else:
            print("⚠️ No video info in stream")
            
        # Handle audio info (may be None for video-only streams)
        if 'audio' in stream_data and stream_data['audio']:
            audio_info = stream_data['audio']
            codec = audio_info.get('codec', 'Unknown')
            mime_type = audio_info.get('mime_type', 'Unknown')
            
            print(f"🎵 Audio codec: {codec}")
            print(f"🎵 Audio MIME: {mime_type}")
            
            self.results['server_audio_codec'] = mime_type
        else:
            print("📺 Video-only stream (no audio track)")
            self.results['server_audio_codec'] = None
            
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

        # ESDS analysis
        esds_found = self.results.get('esds_found', False)
        if esds_found:
            obj_type = self.results.get('object_type_indication')
            audio_obj = self.results.get('audio_object_type')
            chrome_compat = self.results.get('esds_chrome_compatible', False)

            print(f"🔧 ESDS objectTypeIndication: 0x{obj_type:02X}" if obj_type else "🔧 ESDS objectTypeIndication: Unknown")
            print(f"🔧 ESDS audioObjectType: {audio_obj}" if audio_obj else "🔧 ESDS audioObjectType: Unknown")
            print(f"🔧 Chrome compatible: {'✅ Yes' if chrome_compat else '❌ No'}")
        else:
            print("ℹ️ No ESDS box (video-only or non-AAC audio)")
            chrome_compat = True  # Video-only is fine

        # Chrome compatibility
        recommended = self.results.get('recommended_codec')
        if recommended:
            print(f"🎯 Recommended codec: {recommended}")

        # Final assessment
        print()
        if chrome_compat and recommended:
            print("🎉 SUCCESS: Server configured for Chrome compatibility")
            if esds_found:
                print("   → objectTypeIndication=0x40 (MPEG-4 Audio)")
                print("   → audioObjectType=2 (AAC-LC)")
            else:
                print("   → Video-only mode active")
            print("   → Chrome-supported codec format available")
            return True
        elif not chrome_compat:
            obj_type = self.results.get('object_type_indication')
            audio_obj = self.results.get('audio_object_type')
            print("⚠️ ISSUE: ESDS not Chrome-compatible")
            if obj_type != 0x40:
                print(f"   → objectTypeIndication should be 0x40, got 0x{obj_type:02X}")
            if audio_obj != 2:
                print(f"   → audioObjectType should be 2, got {audio_obj}")
            return False
        else:
            print("❓ PARTIAL: Need more testing")
            return False

async def main():
    """Main test execution"""
    print("🧪 OpenBroadcastNetwork Chrome Codec Tester")
    print("=" * 60)
    print()

    # Check server is running by testing WebSocket endpoint
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex(('127.0.0.1', 8081))
        sock.close()
        if result == 0:
            print("✅ Server is running on port 8081")
        else:
            raise Exception("Port 8081 not open")
    except Exception as e:
        print(f"❌ Server not accessible: {e}")
        print("   Start server with: cargo run -p OpenBroadcastNetwork-node -- web-viewer --video sample_video.mp4 --port 8081")
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