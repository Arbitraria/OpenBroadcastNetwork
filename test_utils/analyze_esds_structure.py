#!/usr/bin/env python3
"""
Detailed ESDS box structure analyzer
Check if the ESDS box itself has structural issues
"""

import asyncio
import websockets

def analyze_esds_structure(data, esds_offset):
    """Analyze the complete ESDS box structure"""
    print(f"\n📦 ESDS BOX STRUCTURE ANALYSIS")
    print("=" * 50)
    
    if esds_offset + 100 > len(data):
        print("❌ Not enough data for ESDS analysis")
        return
    
    # Go back to find the box header
    box_start = esds_offset - 4  # 'esds' is at offset 4 in the box
    if box_start < 0:
        print("❌ Cannot find ESDS box header")
        return
    
    # Read box header
    box_size = int.from_bytes(data[box_start-4:box_start], 'big')
    box_type = data[box_start:box_start+4]
    version_flags = data[box_start+4:box_start+8]
    
    print(f"📋 ESDS Box Header:")
    print(f"   Box Size: {box_size}")
    print(f"   Box Type: {box_type}")
    print(f"   Version: {version_flags[0]}")
    print(f"   Flags: {version_flags[1]:02x} {version_flags[2]:02x} {version_flags[3]:02x}")
    
    # Analyze ESDS descriptor structure
    esds_data_start = box_start + 8  # After version/flags
    esds_data = data[esds_data_start:box_start + box_size]
    
    print(f"\n📋 ESDS Data (first 60 bytes):")
    hex_data = ' '.join(f"{b:02x}" for b in esds_data[:60])
    print(f"   {hex_data}")
    
    # Parse descriptor hierarchy
    print(f"\n📋 ESDS Descriptor Hierarchy:")
    parse_descriptor(esds_data, 0, "Root")

def parse_descriptor(data, offset, name):
    """Parse ESDS descriptor with variable length encoding"""
    if offset >= len(data):
        return offset
    
    tag = data[offset]
    length_start = offset + 1
    
    # Parse variable length encoding
    length = 0
    length_bytes = 0
    pos = length_start
    
    while pos < len(data) and length_bytes < 4:
        byte = data[pos]
        length = (length << 7) | (byte & 0x7F)
        length_bytes += 1
        pos += 1
        if (byte & 0x80) == 0:
            break
    
    content_start = pos
    content_end = min(content_start + length, len(data))
    
    print(f"   {name}: Tag=0x{tag:02x}, Length={length}, Content={content_end-content_start} bytes")
    
    if tag == 0x03:  # ES_Descriptor
        print(f"      ES_ID: {int.from_bytes(data[content_start:content_start+2], 'big')}")
        print(f"      Flags: 0x{data[content_start+2]:02x}")
        # Look for DecoderConfigDescriptor
        next_offset = content_start + 3
        while next_offset < content_end:
            if data[next_offset] == 0x04:
                next_offset = parse_descriptor(data, next_offset, "DecoderConfigDescriptor")
                break
            next_offset += 1
            
    elif tag == 0x04:  # DecoderConfigDescriptor  
        if content_start + 13 <= len(data):
            object_type = data[content_start]
            stream_type = data[content_start + 1]
            buffer_size = int.from_bytes(data[content_start + 2:content_start + 5], 'big')
            max_bitrate = int.from_bytes(data[content_start + 5:content_start + 9], 'big') 
            avg_bitrate = int.from_bytes(data[content_start + 9:content_start + 13], 'big')
            
            print(f"      Object Type: 0x{object_type:02x}")
            print(f"      Stream Type: 0x{stream_type:02x}")
            print(f"      Buffer Size: {buffer_size}")
            print(f"      Max Bitrate: {max_bitrate}")
            print(f"      Avg Bitrate: {avg_bitrate}")
            
            # Look for DecSpecificInfo
            next_offset = content_start + 13
            while next_offset < content_end:
                if data[next_offset] == 0x05:
                    next_offset = parse_descriptor(data, next_offset, "DecSpecificInfo")
                    break
                next_offset += 1
                
    elif tag == 0x05:  # DecSpecificInfo
        config_data = data[content_start:content_end]
        print(f"      AudioSpecificConfig: {' '.join(f'{b:02x}' for b in config_data)}")
        return content_end
        
    elif tag == 0x06:  # SLConfigDescriptor
        print(f"      SL Config: {' '.join(f'{b:02x}' for b in data[content_start:content_end])}")
        return content_end
    
    return content_end

async def test_esds_structure():
    """Test the ESDS box structure"""
    print("🔍 ESDS Box Structure Analysis")
    print("=" * 50)
    
    try:
        async with websockets.connect("ws://127.0.0.1:8080/stream") as websocket:
            print("✅ WebSocket connected")
            
            message_count = 0
            while message_count < 5:
                message = await websocket.recv()
                message_count += 1
                
                if isinstance(message, bytes) and len(message) > 1000:
                    # Find ESDS
                    esds_pos = message.find(b'esds')
                    if esds_pos != -1:
                        analyze_esds_structure(message, esds_pos)
                        break
                        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_esds_structure())