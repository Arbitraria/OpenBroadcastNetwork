#!/usr/bin/env python3
"""
Detailed ESDS analysis of the binary initialization segment
"""

import asyncio
import websockets
import struct

async def analyze_esds_in_detail():
    """Detailed ESDS analysis"""
    print("🔍 Detailed ESDS Analysis")
    print("=" * 40)
    
    try:
        async with websockets.connect("ws://127.0.0.1:8080/stream") as websocket:
            print("✅ WebSocket connected, waiting for binary data...")
            
            while True:
                message = await websocket.recv()
                
                if isinstance(message, bytes) and len(message) > 1000:
                    print(f"\n📦 Analyzing binary segment ({len(message)} bytes)")
                    
                    # Find all ESDS boxes
                    esds_positions = []
                    i = 0
                    while i < len(message) - 4:
                        if message[i:i+4] == b'esds':
                            esds_positions.append(i)
                        i += 1
                    
                    print(f"🎯 Found {len(esds_positions)} ESDS box(es)")
                    
                    for pos in esds_positions:
                        print(f"\n📍 ESDS at offset {pos}:")
                        
                        # Show context around ESDS
                        start = max(0, pos - 20)
                        end = min(len(message), pos + 80)
                        context = message[start:end]
                        
                        print(f"   Context ({start}-{end}):")
                        hex_bytes = ' '.join(f"{b:02x}" for b in context)
                        print(f"   {hex_bytes}")
                        
                        # Look for DecoderConfigDescriptor (0x04) pattern
                        for j in range(max(0, pos - 10), min(len(message) - 10, pos + 60)):
                            if message[j] == 0x04:  # DecoderConfigDescriptor tag
                                print(f"   Found 0x04 at offset {j}")
                                
                                # Look for object type in next few bytes
                                for k in range(j + 1, min(len(message), j + 10)):
                                    obj_type = message[k]
                                    if obj_type in [0x02, 0x40, 0x67, 0x69]:  # Known audio object types
                                        print(f"   🎯 Object type 0x{obj_type:02X} at offset {k}")
                                        
                                        if obj_type == 0x02:
                                            print(f"   ✅ MODIFIED: Object type is 0x02 (AAC-LC)")
                                        elif obj_type == 0x40:
                                            print(f"   ❌ ORIGINAL: Object type is 0x40 (MPEG-4 Audio)")
                                        break
                    
                    break  # Only analyze first binary message
                    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(analyze_esds_in_detail())