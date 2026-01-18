#!/usr/bin/env python3
"""Dump initialization segment to analyze what's being sent to Chrome"""

import asyncio
import websockets
import json
import struct
import sys

async def dump_init_segment():
    uri = "ws://127.0.0.1:8080/stream"
    
    async with websockets.connect(uri) as websocket:
        print(f"Connected to {uri}")
        
        while True:
            message = await websocket.recv()
            
            if isinstance(message, str):
                # JSON message
                data = json.loads(message)
                msg_type = data.get('type')
                print(f"\nReceived message type: {msg_type}")
                
                if msg_type == 'stream_info':
                    print(f"Stream info: {json.dumps(data['data'], indent=2)}")
                elif msg_type == 'chunk_info':
                    chunk_data = data['data']
                    if chunk_data.get('chunk_type') == 'initialization':
                        print(f"\nINITIALIZATION SEGMENT INFO:")
                        print(f"  Size: {chunk_data['size']} bytes")
                        print(f"  Waiting for binary data...")
                        
            else:
                # Binary data
                print(f"\nReceived binary data: {len(message)} bytes")
                
                # Save initialization segment
                with open('/tmp/init_segment.mp4', 'wb') as f:
                    f.write(message)
                print("Saved initialization segment to /tmp/init_segment.mp4")
                
                # Parse box structure
                print("\nBox structure:")
                pos = 0
                while pos < len(message) - 8:
                    size = struct.unpack('>I', message[pos:pos+4])[0]
                    box_type = message[pos+4:pos+8].decode('ascii', errors='ignore')
                    print(f"  {box_type}: {size} bytes at offset {pos}")
                    
                    if box_type == 'moov':
                        # Look for ESDS in moov
                        print("  Analyzing moov box for ESDS...")
                        moov_data = message[pos:pos+size]
                        find_esds_in_data(moov_data, 8, "    ")
                    
                    pos += size
                
                # Close connection after getting init segment
                break
                
def find_esds_in_data(data, start_offset, indent):
    """Recursively find ESDS boxes"""
    pos = start_offset
    while pos < len(data) - 8:
        size = struct.unpack('>I', data[pos:pos+4])[0]
        if size == 0 or size > len(data) - pos:
            break
            
        box_type = data[pos+4:pos+8].decode('ascii', errors='ignore')
        
        if box_type == 'esds':
            print(f"{indent}FOUND ESDS at offset {pos}, size {size}")
            # Dump ESDS content
            esds_data = data[pos+8:pos+size]
            print(f"{indent}ESDS header: {' '.join(f'{b:02x}' for b in esds_data[:min(32, len(esds_data))])}")
            
        elif box_type in ['trak', 'mdia', 'minf', 'stbl', 'stsd', 'mp4a']:
            # Recurse into container boxes
            child_offset = pos + 8
            if box_type == 'stsd':
                child_offset += 8  # Skip version and entry count
            elif box_type == 'mp4a':
                child_offset += 28  # Skip sample entry fields
                
            find_esds_in_data(data, child_offset, indent + "  ")
            
        pos += size

if __name__ == "__main__":
    asyncio.run(dump_init_segment())