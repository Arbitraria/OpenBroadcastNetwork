#!/usr/bin/env python3
"""
Comprehensive WebSocket codec test with binary data analysis
"""

import websockets
import asyncio
import json
import struct
import sys

class CodecTester:
    def __init__(self):
        self.messages_received = 0
        self.stream_info = None
        self.first_binary = None
        
    async def test_websocket(self):
        uri = "ws://127.0.0.1:8080/stream"
        
        try:
            print(f"Connecting to {uri}...")
            
            websocket = await asyncio.wait_for(
                websockets.connect(uri), 
                timeout=10
            )
            
            print("Connected successfully!")
            print("Receiving messages...\n")
            
            # Receive several messages
            for i in range(5):  # Get first 5 messages
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5)
                    self.messages_received += 1
                    
                    if isinstance(message, str):
                        await self.handle_json_message(message, i + 1)
                    else:
                        await self.handle_binary_message(message, i + 1)
                        
                except asyncio.TimeoutError:
                    print(f"Timeout waiting for message {i + 1}")
                    break
                    
            await websocket.close()
            
            # Analysis
            print("\n" + "="*50)
            print("ANALYSIS RESULTS")
            print("="*50)
            
            self.analyze_codec_info()
            if self.first_binary:
                self.analyze_binary_data()
                
        except Exception as e:
            print(f"❌ Error: {e}")
            
    async def handle_json_message(self, message, msg_num):
        try:
            data = json.loads(message)
            msg_type = data.get('type', 'unknown')
            
            print(f"📨 Message {msg_num}: JSON - {msg_type}")
            
            if msg_type == 'stream_info':
                self.stream_info = data
                print(f"   Video codec: {data.get('data', {}).get('video', {}).get('mime_type', 'N/A')}")
                print(f"   Audio codec: {data.get('data', {}).get('audio', {}).get('mime_type', 'N/A')}")
                
            elif msg_type == 'chunk_info':
                print(f"   Chunk size: {data.get('size', 'N/A')} bytes")
                print(f"   Chunk type: {data.get('chunk_type', 'N/A')}")
                
            else:
                print(f"   Data: {json.dumps(data, indent=4)}")
                
        except json.JSONDecodeError as e:
            print(f"   ❌ JSON decode error: {e}")
            
    async def handle_binary_message(self, message, msg_num):
        print(f"📨 Message {msg_num}: Binary - {len(message)} bytes")
        
        if self.first_binary is None:
            self.first_binary = message
            
        # Show first 32 bytes in hex
        hex_preview = ' '.join(f'{b:02x}' for b in message[:32])
        print(f"   First 32 bytes: {hex_preview}")
        
        # Look for known patterns
        if b'esds' in message:
            esds_pos = message.find(b'esds')
            print(f"   📍 Found 'esds' at position {esds_pos}")
            
        if b'mp4a' in message:
            mp4a_pos = message.find(b'mp4a')
            print(f"   📍 Found 'mp4a' at position {mp4a_pos}")
            
    def analyze_codec_info(self):
        print("\n🔍 CODEC ANALYSIS")
        
        if not self.stream_info:
            print("❌ No stream_info received")
            return
            
        data = self.stream_info.get('data', {})
        audio = data.get('audio', {})
        video = data.get('video', {})
        
        print(f"Video codec: {video.get('mime_type', 'N/A')}")
        print(f"Audio codec: {audio.get('mime_type', 'N/A')}")
        
        # Check audio codec specifically
        audio_mime = audio.get('mime_type', '')
        expected_mime = 'audio/mp4; codecs="mp4a.40.2"'
        
        print(f"\nExpected audio codec: {expected_mime}")
        print(f"Actual audio codec:   {audio_mime}")
        
        if audio_mime == expected_mime:
            print("✅ Audio codec matches expected format!")
        else:
            print("❌ Audio codec does not match expected format")
            if 'mp4a.02.2' in audio_mime:
                print("   Note: Contains 'mp4a.02.2' instead of 'mp4a.40.2'")
                
    def analyze_binary_data(self):
        print("\n🔬 BINARY DATA ANALYSIS")
        
        data = self.first_binary
        print(f"Binary data size: {len(data)} bytes")
        
        # Look for MP4 atoms/boxes
        self.find_mp4_atoms(data)
        
        # Look for ESDS specifically
        if b'esds' in data:
            esds_pos = data.find(b'esds')
            print(f"\n📍 ESDS atom found at position {esds_pos}")
            
            # Extract some data around ESDS
            start = max(0, esds_pos - 8)
            end = min(len(data), esds_pos + 64)
            esds_region = data[start:end]
            
            print("ESDS region (hex):")
            for i in range(0, len(esds_region), 16):
                chunk = esds_region[i:i+16]
                hex_str = ' '.join(f'{b:02x}' for b in chunk)
                ascii_str = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in chunk)
                print(f"  {start+i:04x}: {hex_str:<48} {ascii_str}")
                
            # Look for AudioSpecificConfig pattern
            self.find_audio_specific_config(esds_region, esds_pos)
            
    def find_mp4_atoms(self, data):
        print("\n📦 MP4 Atoms/Boxes found:")
        
        common_atoms = [b'ftyp', b'moov', b'mdat', b'moof', b'trak', b'mdia', 
                       b'minf', b'stbl', b'stsd', b'mp4a', b'esds', b'free']
        
        for atom in common_atoms:
            pos = data.find(atom)
            if pos >= 0:
                print(f"   {atom.decode('ascii', errors='ignore')}: position {pos}")
                
    def find_audio_specific_config(self, data, esds_offset):
        print("\n🎵 Looking for AudioSpecificConfig...")
        
        # Look for potential AudioSpecificConfig patterns
        for i in range(len(data) - 1):
            byte1 = data[i]
            byte2 = data[i + 1] if i + 1 < len(data) else 0
            
            # Extract audioObjectType (5 bits)
            audio_object_type = (byte1 >> 3) & 0x1F
            
            if audio_object_type == 2:  # AAC LC
                # Extract sampling frequency index
                sampling_freq_index = ((byte1 & 0x07) << 1) | ((byte2 >> 7) & 0x01)
                
                # Extract channel configuration
                channel_config = (byte2 >> 3) & 0x0F
                
                print(f"   ✅ Found AudioSpecificConfig at offset {esds_offset + i}")
                print(f"      audioObjectType: {audio_object_type} (AAC LC)")
                print(f"      samplingFrequencyIndex: {sampling_freq_index}")
                print(f"      channelConfiguration: {channel_config}")
                return True
                
        print("   ❌ No valid AudioSpecificConfig found")
        return False

async def main():
    print("🧪 Comprehensive WebSocket Codec Test")
    print("=" * 50)
    
    tester = CodecTester()
    await tester.test_websocket()
    
    print(f"\nTest completed. Received {tester.messages_received} messages.")

if __name__ == "__main__":
    asyncio.run(main())