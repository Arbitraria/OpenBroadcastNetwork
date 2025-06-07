#!/usr/bin/env python3
"""
Fixed ESDS analysis - properly locate box boundaries
"""

import asyncio
import websockets

def find_esds_box(data):
    """Find the complete ESDS box in the data"""
    esds_pos = data.find(b'esds')
    if esds_pos == -1:
        return None, None, None
    
    # ESDS is at offset 4 in the box (after 4-byte size)
    box_start = esds_pos - 4
    if box_start < 0:
        return None, None, None
    
    # Read box size (big-endian 32-bit)
    if box_start - 4 < 0:
        return None, None, None
        
    box_size_bytes = data[box_start-4:box_start]
    box_size = int.from_bytes(box_size_bytes, 'big')
    
    actual_box_start = box_start - 4
    box_end = actual_box_start + box_size
    
    print(f"📍 ESDS Box Found:")
    print(f"   Box starts at: {actual_box_start}")
    print(f"   Box size: {box_size}")
    print(f"   Box ends at: {box_end}")
    print(f"   'esds' at offset: {esds_pos}")
    
    return actual_box_start, box_size, data[actual_box_start:box_end]

def validate_esds_box(box_data):
    """Validate ESDS box structure"""
    if len(box_data) < 12:
        print("❌ ESDS box too small")
        return False
    
    # Parse box header
    size = int.from_bytes(box_data[0:4], 'big')
    box_type = box_data[4:8]
    version = box_data[8]
    flags = box_data[9:12]
    
    print(f"\n📋 ESDS Box Validation:")
    print(f"   Size: {size} (expected: {len(box_data)})")
    print(f"   Type: {box_type} (expected: b'esds')")
    print(f"   Version: {version} (should be 0)")
    print(f"   Flags: {flags[0]:02x} {flags[1]:02x} {flags[2]:02x} (should be 000000)")
    
    # Validate
    valid = True
    if size != len(box_data):
        print(f"❌ Size mismatch: header says {size}, actual {len(box_data)}")
        valid = False
    
    if box_type != b'esds':
        print(f"❌ Type mismatch: expected 'esds', got {box_type}")
        valid = False
        
    if version != 0:
        print(f"⚠️ Non-zero version: {version} (may cause issues)")
        
    if flags != b'\x00\x00\x00':
        print(f"⚠️ Non-zero flags: {flags.hex()} (may cause issues)")
    
    return valid

def analyze_raw_esds_data(box_data):
    """Analyze the raw ESDS data starting after version/flags"""
    esds_payload = box_data[12:]  # Skip size(4) + type(4) + version(1) + flags(3)
    
    print(f"\n📋 ESDS Payload Analysis:")
    print(f"   Payload size: {len(esds_payload)} bytes")
    print(f"   Raw data: {esds_payload[:40].hex()}")
    
    # Look for the descriptors
    if len(esds_payload) > 0:
        print(f"   First byte: 0x{esds_payload[0]:02x}")
        if esds_payload[0] == 0x03:
            print("   ✅ Starts with ES_Descriptor (0x03) - correct")
        else:
            print("   ❌ Does not start with ES_Descriptor (0x03)")
            
    # Find object type 0x40
    for i, byte in enumerate(esds_payload):
        if byte == 0x40:
            print(f"   📍 Found 0x40 at payload offset {i}")
            # Show context around 0x40
            start = max(0, i-5)
            end = min(len(esds_payload), i+10)
            context = esds_payload[start:end]
            print(f"   Context: {context.hex()}")
            break

async def comprehensive_esds_analysis():
    """Comprehensive ESDS analysis"""
    print("🔍 Comprehensive ESDS Analysis")
    print("=" * 50)
    
    try:
        async with websockets.connect("ws://127.0.0.1:8080/stream") as websocket:
            
            message_count = 0
            while message_count < 5:
                message = await websocket.recv()
                message_count += 1
                
                if isinstance(message, bytes) and len(message) > 1000:
                    print(f"📦 Analyzing binary segment ({len(message)} bytes)")
                    
                    box_start, box_size, box_data = find_esds_box(message)
                    
                    if box_data is not None:
                        # Validate box structure
                        is_valid = validate_esds_box(box_data)
                        
                        # Analyze raw data regardless
                        analyze_raw_esds_data(box_data)
                        
                        print(f"\n🎯 CONCLUSION:")
                        if is_valid:
                            print("   ✅ ESDS box structure appears valid")
                            print("   ❌ Structure is not the issue")
                            print("   🔄 Need to investigate browser compatibility")
                        else:
                            print("   ❌ ESDS box structure has issues!")
                            print("   🎯 This could be the root cause")
                        
                        break
                    else:
                        print("❌ Could not locate ESDS box properly")
                        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(comprehensive_esds_analysis())