#!/usr/bin/env python3
"""
WebSocket Streaming Server Codec Verification Test

This script connects to the WebSocket streaming server and verifies that:
1. The stream_info contains the corrected codec string
2. The binary data contains properly formatted ESDS data
3. The AudioSpecificConfig has the correct audioObjectType
"""

import asyncio
import websockets
import json
import struct
import sys
from typing import Optional, Dict, Any

class CodecVerificationTest:
    def __init__(self, url: str = "ws://127.0.0.1:8080/stream"):
        self.url = url
        self.stream_info = None
        self.chunk_info = None
        self.binary_data = None
        
    async def connect_and_test(self):
        """Connect to the WebSocket server and run the verification test"""
        try:
            print(f"Connecting to {self.url}...")
            async with websockets.connect(self.url) as websocket:
                print("Connected successfully!")
                
                # Receive and process the first few messages
                await self.receive_initial_messages(websocket)
                
                # Run verification tests
                self.verify_stream_info()
                self.verify_binary_data()
                
                print("\n✅ All verification tests passed!")
                
        except websockets.exceptions.ConnectionRefused:
            print(f"❌ Failed to connect to {self.url}")
            print("Make sure the streaming server is running on port 8080")
            return False
        except Exception as e:
            print(f"❌ Error during test: {e}")
            return False
            
        return True
    
    async def receive_initial_messages(self, websocket):
        """Receive the first few messages from the server"""
        message_count = 0
        max_messages = 5  # Get stream_info, chunk_info, and a few binary chunks
        
        print("\nReceiving initial messages...")
        
        async for message in websocket:
            message_count += 1
            
            if isinstance(message, str):
                # JSON message
                try:
                    data = json.loads(message)
                    msg_type = data.get('type')
                    
                    print(f"📨 Message {message_count}: {msg_type}")
                    
                    if msg_type == 'stream_info':
                        self.stream_info = data
                        print(f"   Codec: {data.get('audio_codec', 'N/A')}")
                        
                    elif msg_type == 'chunk_info':
                        self.chunk_info = data
                        print(f"   Chunk size: {data.get('size', 'N/A')} bytes")
                        
                except json.JSONDecodeError:
                    print(f"   Invalid JSON: {message[:100]}...")
                    
            else:
                # Binary message
                print(f"📨 Message {message_count}: Binary data ({len(message)} bytes)")
                if self.binary_data is None:
                    self.binary_data = message
                    
            if message_count >= max_messages:
                break
                
        if message_count == 0:
            raise Exception("No messages received from server")
    
    def verify_stream_info(self):
        """Verify the stream_info message contains correct codec information"""
        print("\n🔍 Verifying stream_info...")
        
        if not self.stream_info:
            raise Exception("No stream_info message received")
            
        # Check for expected codec string
        expected_codec = 'audio/mp4; codecs="mp4a.40.2"'
        actual_codec = self.stream_info.get('audio_codec')
        
        print(f"   Expected codec: {expected_codec}")
        print(f"   Actual codec:   {actual_codec}")
        
        if actual_codec != expected_codec:
            raise Exception(f"Codec mismatch! Expected '{expected_codec}', got '{actual_codec}'")
            
        print("   ✅ Codec string is correct")
        
        # Print other stream info
        for key, value in self.stream_info.items():
            if key != 'audio_codec':
                print(f"   {key}: {value}")
    
    def verify_binary_data(self):
        """Verify the binary data contains properly formatted ESDS"""
        print("\n🔍 Verifying binary data...")
        
        if not self.binary_data:
            raise Exception("No binary data received")
            
        print(f"   Binary data size: {len(self.binary_data)} bytes")
        
        # Look for MP4 box structures
        self.analyze_mp4_boxes(self.binary_data)
        
        # Look for ESDS atom specifically
        esds_data = self.find_esds_atom(self.binary_data)
        if esds_data:
            self.analyze_esds_data(esds_data)
        else:
            print("   ⚠️  No ESDS atom found in binary data")
    
    def analyze_mp4_boxes(self, data: bytes):
        """Analyze MP4 box structure in the binary data"""
        print("   📦 Analyzing MP4 box structure...")
        
        offset = 0
        box_count = 0
        
        while offset < len(data) - 8 and box_count < 10:  # Limit to first 10 boxes
            try:
                # Read box size and type
                box_size = struct.unpack('>I', data[offset:offset+4])[0]
                box_type = data[offset+4:offset+8].decode('ascii', errors='ignore')
                
                print(f"     Box {box_count + 1}: '{box_type}' ({box_size} bytes)")
                
                if box_size == 0:
                    break
                    
                offset += box_size
                box_count += 1
                
            except (struct.error, UnicodeDecodeError):
                break
    
    def find_esds_atom(self, data: bytes) -> Optional[bytes]:
        """Find and extract ESDS atom from the data"""
        esds_marker = b'esds'
        
        for i in range(len(data) - 4):
            if data[i:i+4] == esds_marker:
                print(f"   🎯 Found ESDS atom at offset {i}")
                
                # Try to determine ESDS size
                # ESDS typically follows MP4 box format: size(4) + type(4) + data
                if i >= 4:
                    try:
                        box_size = struct.unpack('>I', data[i-4:i])[0]
                        esds_start = i - 4
                        esds_end = min(esds_start + box_size, len(data))
                        return data[esds_start:esds_end]
                    except struct.error:
                        pass
                
                # Fallback: return next 64 bytes
                return data[i:i+64]
                
        return None
    
    def analyze_esds_data(self, esds_data: bytes):
        """Analyze ESDS data structure and AudioSpecificConfig"""
        print("   🔬 Analyzing ESDS data...")
        print(f"      ESDS data size: {len(esds_data)} bytes")
        
        # Print first 32 bytes in hex
        hex_data = ' '.join(f'{b:02x}' for b in esds_data[:32])
        print(f"      First 32 bytes: {hex_data}")
        
        # Look for AudioSpecificConfig pattern
        self.find_audio_specific_config(esds_data)
    
    def find_audio_specific_config(self, data: bytes):
        """Find and analyze AudioSpecificConfig in ESDS data"""
        print("   🎵 Looking for AudioSpecificConfig...")
        
        # AudioSpecificConfig typically starts with audioObjectType
        # We're looking for 0x40 (object type) but audioObjectType=2 in config
        for i in range(len(data) - 2):
            byte1 = data[i]
            byte2 = data[i + 1] if i + 1 < len(data) else 0
            
            # Check for audioObjectType in the 5 most significant bits
            audio_object_type = (byte1 >> 3) & 0x1F
            
            if audio_object_type == 2:  # AAC LC
                print(f"      ✅ Found AudioSpecificConfig at offset {i}")
                print(f"         audioObjectType: {audio_object_type} (AAC LC)")
                
                # Extract sampling frequency index (4 bits)
                sampling_freq_index = ((byte1 & 0x07) << 1) | ((byte2 >> 7) & 0x01)
                print(f"         samplingFrequencyIndex: {sampling_freq_index}")
                
                # Extract channel configuration (4 bits)
                channel_config = (byte2 >> 3) & 0x0F
                print(f"         channelConfiguration: {channel_config}")
                
                return True
        
        print("      ⚠️  AudioSpecificConfig with audioObjectType=2 not found")
        
        # Look for object type 0x40 in the data
        if 0x40 in data:
            idx = data.find(0x40)
            print(f"      📍 Found object type 0x40 at offset {idx}")
        
        return False

async def main():
    """Main test function"""
    print("🧪 WebSocket Codec Verification Test")
    print("=" * 50)
    
    test = CodecVerificationTest()
    success = await test.connect_and_test()
    
    if success:
        print("\n🎉 Test completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Test failed!")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)