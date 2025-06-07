#!/usr/bin/env python3
"""
Proper ESDS box structure analyzer - correctly parse MP4 box hierarchy
"""

import asyncio
import websockets
import struct

def parse_mp4_boxes(data, start_offset=0, max_depth=5, depth=0):
    """Parse MP4 box structure correctly"""
    boxes = []
    offset = start_offset
    
    while offset + 8 <= len(data):
        # Read box header
        if offset + 8 > len(data):
            break
            
        box_size = struct.unpack('>I', data[offset:offset+4])[0]
        box_type = data[offset+4:offset+8].decode('ascii', errors='ignore')
        
        if box_size < 8:
            print(f"Invalid box size {box_size} at offset {offset}")
            break
            
        if offset + box_size > len(data):
            print(f"Box extends beyond data: offset={offset}, size={box_size}, data_len={len(data)}")
            break
        
        box_info = {
            'type': box_type,
            'size': box_size,
            'offset': offset,
            'content_offset': offset + 8,
            'content_size': box_size - 8,
            'depth': depth
        }
        
        print(f"{'  ' * depth}📦 Box '{box_type}': size={box_size}, offset={offset}")
        
        # For containers, parse child boxes
        if depth < max_depth and box_type in ['moov', 'trak', 'mdia', 'minf', 'stbl']:
            print(f"{'  ' * depth}📂 Parsing children of {box_type}")
            child_boxes = parse_mp4_boxes(data, offset + 8, max_depth, depth + 1)
            box_info['children'] = child_boxes
            
            # Find if any children contain ESDS
            for child in child_boxes:
                if child['type'] == 'esds':
                    print(f"{'  ' * depth}🎯 Found ESDS in {box_type} hierarchy!")
                    analyze_esds_box(data, child)
        
        boxes.append(box_info)
        offset += box_size
        
        # Special handling for stsd (sample description)
        if box_type == 'stsd':
            print(f"{'  ' * depth}📋 Sample Description Box - looking for sample entries")
            stsd_content_start = offset - box_size + 8
            parse_stsd_entries(data, stsd_content_start, box_size - 8, depth + 1)
    
    return boxes

def parse_stsd_entries(data, stsd_start, stsd_size, depth):
    """Parse sample description entries where ESDS boxes live"""
    if stsd_size < 8:
        return
        
    # Skip version/flags (4 bytes) and entry count (4 bytes)
    entry_start = stsd_start + 8
    entry_count = struct.unpack('>I', data[stsd_start+4:stsd_start+8])[0]
    
    print(f"{'  ' * depth}📊 Sample description has {entry_count} entries")
    
    offset = entry_start
    for i in range(entry_count):
        if offset + 8 > len(data):
            break
            
        entry_size = struct.unpack('>I', data[offset:offset+4])[0]
        entry_format = data[offset+4:offset+8].decode('ascii', errors='ignore')
        
        print(f"{'  ' * depth}📄 Entry {i}: format='{entry_format}', size={entry_size}")
        
        if entry_format == 'mp4a':  # Audio sample entry
            print(f"{'  ' * depth}🎵 Audio sample entry - looking for ESDS")
            # Skip sample entry fields (36 bytes total)
            child_start = offset + 36
            child_end = offset + entry_size
            
            # Parse child boxes of the sample entry
            parse_sample_entry_children(data, child_start, child_end - child_start, depth + 1)
        
        offset += entry_size

def parse_sample_entry_children(data, start, size, depth):
    """Parse child boxes within a sample entry"""
    offset = start
    end = start + size
    
    while offset + 8 <= end and offset + 8 <= len(data):
        box_size = struct.unpack('>I', data[offset:offset+4])[0]
        box_type = data[offset+4:offset+8].decode('ascii', errors='ignore')
        
        if box_size < 8 or offset + box_size > end:
            break
            
        print(f"{'  ' * depth}📦 Sample entry child '{box_type}': size={box_size}, offset={offset}")
        
        if box_type == 'esds':
            print(f"{'  ' * depth}🎯 FOUND ESDS BOX!")
            box_info = {
                'type': box_type,
                'size': box_size,
                'offset': offset,
                'content_offset': offset + 8,
                'content_size': box_size - 8,
                'depth': depth
            }
            analyze_esds_box(data, box_info)
        
        offset += box_size

def analyze_esds_box(data, box_info):
    """Analyze the ESDS box structure in detail"""
    print(f"\n🔍 DETAILED ESDS ANALYSIS")
    print(f"   Box offset: {box_info['offset']}")
    print(f"   Box size: {box_info['size']}")
    print(f"   Content offset: {box_info['content_offset']}")
    print(f"   Content size: {box_info['content_size']}")
    
    # Validate box header
    box_start = box_info['offset']
    actual_size = struct.unpack('>I', data[box_start:box_start+4])[0]
    actual_type = data[box_start+4:box_start+8]
    
    print(f"\n📋 ESDS Box Header Validation:")
    print(f"   Size: {actual_size} (expected: {box_info['size']})")
    print(f"   Type: {actual_type} (expected: b'esds')")
    
    if actual_size != box_info['size']:
        print(f"   ❌ Size mismatch!")
    elif actual_type != b'esds':
        print(f"   ❌ Type mismatch!")
    else:
        print(f"   ✅ Box header is valid")
        
        # Parse ESDS content
        content_start = box_info['content_offset']
        content_end = content_start + box_info['content_size']
        
        if content_start + 4 <= len(data):
            version = data[content_start]
            flags = data[content_start+1:content_start+4]
            
            print(f"   Version: {version} (should be 0)")
            print(f"   Flags: {flags.hex()} (should be 000000)")
            
            # Parse ESDS descriptors
            payload_start = content_start + 4
            payload_end = content_end
            print(f"\n📋 ESDS Payload ({payload_end - payload_start} bytes):")
            
            payload = data[payload_start:payload_end]
            print(f"   Raw: {payload[:20].hex()}...")
            
            parse_esds_descriptors(payload)

def parse_esds_descriptors(payload):
    """Parse ESDS descriptor hierarchy"""
    offset = 0
    
    while offset < len(payload):
        if offset + 2 > len(payload):
            break
            
        tag = payload[offset]
        print(f"   📍 Descriptor tag: 0x{tag:02x}")
        
        if tag == 0x03:  # ES_Descriptor
            print(f"      ✅ ES_Descriptor found")
            break
        elif tag == 0x04:  # DecoderConfigDescriptor
            print(f"      ✅ DecoderConfigDescriptor found")
            # Parse object type
            if offset + 5 < len(payload):
                # Skip tag(1) + length(variable) - simplified assumption of 1-byte length
                obj_type_offset = offset + 2
                if payload[offset + 1] & 0x80 == 0:  # Single byte length
                    obj_type_offset = offset + 2
                else:
                    obj_type_offset = offset + 3  # Assume 2-byte length for now
                
                if obj_type_offset < len(payload):
                    object_type = payload[obj_type_offset]
                    print(f"      📍 Object Type: 0x{object_type:02x}")
            break
        else:
            offset += 1

async def proper_esds_analysis():
    """Properly analyze ESDS box structure"""
    print("🔍 PROPER ESDS Box Structure Analysis")
    print("=" * 60)
    
    try:
        async with websockets.connect("ws://127.0.0.1:8080/stream") as websocket:
            print("✅ WebSocket connected")
            
            message_count = 0
            while message_count < 5:
                message = await websocket.recv()
                message_count += 1
                
                if isinstance(message, bytes) and len(message) > 1000:
                    print(f"\n📦 Analyzing binary segment ({len(message)} bytes)")
                    print("=" * 50)
                    
                    # Parse the complete MP4 box structure
                    boxes = parse_mp4_boxes(message, 0, 6, 0)
                    
                    # Summary
                    print(f"\n📊 SUMMARY:")
                    print(f"   Total boxes found: {len(boxes)}")
                    box_types = [box['type'] for box in boxes]
                    print(f"   Box types: {', '.join(box_types)}")
                    
                    break
                    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(proper_esds_analysis())