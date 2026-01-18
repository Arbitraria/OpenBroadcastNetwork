#!/usr/bin/env python3
"""Analyze ESDS box in transcoded MP4 file to debug AudioSpecificConfig issues"""

import sys
import struct
import subprocess
import json
from pathlib import Path

def find_box(data, box_type, start=0):
    """Find a box of given type in MP4 data"""
    pos = start
    while pos < len(data) - 8:
        size = struct.unpack('>I', data[pos:pos+4])[0]
        btype = data[pos+4:pos+8].decode('ascii', errors='ignore')
        
        if size == 0:
            break
        elif size == 1:
            # 64-bit size
            if pos + 16 > len(data):
                break
            size = struct.unpack('>Q', data[pos+8:pos+16])[0]
            
        if btype == box_type:
            return pos, size
            
        pos += size
    return None, None

def parse_esds(data):
    """Parse ESDS box and extract AudioSpecificConfig"""
    print("\nESD Box Analysis:")
    print(f"Total ESDS data length: {len(data)} bytes")
    
    # Skip box header (already handled)
    pos = 0
    
    # Version and flags
    version = data[pos]
    flags = struct.unpack('>I', b'\x00' + data[pos+1:pos+4])[0]
    print(f"Version: {version}, Flags: {flags:06x}")
    pos += 4
    
    # Parse descriptors
    while pos < len(data):
        if pos + 2 > len(data):
            break
            
        tag = data[pos]
        pos += 1
        
        # Parse length (can be multi-byte)
        length = 0
        for i in range(4):
            if pos >= len(data):
                break
            byte = data[pos]
            pos += 1
            length = (length << 7) | (byte & 0x7f)
            if not (byte & 0x80):
                break
        
        print(f"\nDescriptor Tag: 0x{tag:02x}, Length: {length}")
        
        if pos + length > len(data):
            print(f"ERROR: Descriptor length {length} exceeds remaining data (pos={pos}, data_len={len(data)})")
            break
            
        desc_data = data[pos:pos+length]
        print(f"  Descriptor data: {' '.join(f'{b:02x}' for b in desc_data[:min(16, len(desc_data))])}{' ...' if len(desc_data) > 16 else ''}")
        
        if tag == 0x03:  # ES_Descriptor
            print("  ES_Descriptor found")
            es_id = struct.unpack('>H', desc_data[0:2])[0]
            print(f"    ES_ID: {es_id}")
            
        elif tag == 0x04:  # DecoderConfigDescriptor
            print("  DecoderConfigDescriptor found")
            if len(desc_data) < 2:
                print("    ERROR: DecoderConfigDescriptor too short")
                continue
            object_type = desc_data[0]
            stream_type = desc_data[1]
            print(f"    objectTypeIndication: 0x{object_type:02x} ({object_type})")
            print(f"    streamType: 0x{stream_type:02x}")
            
            # Object type 0x40 = MPEG-4 Audio
            if object_type == 0x40:
                print("    -> MPEG-4 Audio (AAC)")
            elif object_type == 0x6b:
                print("    -> MPEG-1 Audio Layer 3 (MP3)")
            elif object_type == 0x66:
                print("    -> MPEG-2 AAC Main")
            elif object_type == 0x67:
                print("    -> MPEG-2 AAC LC")
                
        elif tag == 0x05:  # DecoderSpecificInfo
            print("  DecoderSpecificInfo (AudioSpecificConfig) found")
            print(f"    Raw data: {' '.join(f'{b:02x}' for b in desc_data)}")
            
            if len(desc_data) >= 2:
                # Parse AudioSpecificConfig
                byte1 = desc_data[0]
                byte2 = desc_data[1]
                
                # audioObjectType is 5 bits
                audio_object_type = (byte1 >> 3) & 0x1f
                
                # samplingFrequencyIndex is 4 bits
                sampling_freq_index = ((byte1 & 0x07) << 1) | ((byte2 >> 7) & 0x01)
                
                # channelConfiguration is 4 bits
                channel_config = (byte2 >> 3) & 0x0f
                
                print(f"    audioObjectType: {audio_object_type}")
                print(f"    samplingFrequencyIndex: {sampling_freq_index}")
                print(f"    channelConfiguration: {channel_config}")
                
                # Construct codec string
                if audio_object_type > 0:
                    codec_string = f"mp4a.40.{audio_object_type}"
                    print(f"    -> Codec string should be: {codec_string}")
                
                # Sampling rates
                sampling_rates = [96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000]
                if sampling_freq_index < len(sampling_rates):
                    print(f"    -> Sampling rate: {sampling_rates[sampling_freq_index]} Hz")
                    
        elif tag == 0x06:  # SLConfigDescriptor
            print("  SLConfigDescriptor found")
            
        pos += length
    
    return None

def analyze_transcoded_file(file_path):
    """Analyze the transcoded MP4 file"""
    print(f"\nAnalyzing transcoded file: {file_path}")
    
    # First check with ffprobe
    try:
        cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', str(file_path)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            for stream in data.get('streams', []):
                if stream.get('codec_type') == 'audio':
                    print(f"\nFFprobe audio stream info:")
                    print(f"  Codec: {stream.get('codec_name')}")
                    print(f"  Profile: {stream.get('profile')}")
                    print(f"  Sample rate: {stream.get('sample_rate')} Hz")
                    print(f"  Channels: {stream.get('channels')}")
    except Exception as e:
        print(f"FFprobe error: {e}")
    
    # Read file and find ESDS
    with open(file_path, 'rb') as f:
        data = f.read()
    
    # Find moov box
    moov_pos, moov_size = find_box(data, 'moov')
    if moov_pos is None:
        print("ERROR: No moov box found")
        return
        
    moov_data = data[moov_pos:moov_pos+moov_size]
    
    # Find trak boxes in moov
    pos = 8  # Skip moov header
    track_num = 0
    
    while pos < moov_size - 8:
        trak_pos, trak_size = find_box(moov_data, 'trak', pos)
        if trak_pos is None:
            break
            
        track_num += 1
        print(f"\n=== Track {track_num} ===")
        
        trak_data = moov_data[trak_pos:trak_pos+trak_size]
        
        # Find mdia box in trak
        mdia_pos, mdia_size = find_box(trak_data, 'mdia', 8)
        if mdia_pos is None:
            pos = trak_pos + trak_size
            continue
            
        mdia_data = trak_data[mdia_pos:mdia_pos+mdia_size]
        
        # Find hdlr to check if audio track
        hdlr_pos, hdlr_size = find_box(mdia_data, 'hdlr', 8)
        if hdlr_pos is not None:
            hdlr_data = mdia_data[hdlr_pos+8:hdlr_pos+hdlr_size]
            handler_type = hdlr_data[8:12].decode('ascii', errors='ignore')
            print(f"Handler type: {handler_type}")
            
            if handler_type != 'soun':
                pos = trak_pos + trak_size
                continue
        
        # Find minf->stbl->stsd
        minf_pos, minf_size = find_box(mdia_data, 'minf', 8)
        if minf_pos is None:
            pos = trak_pos + trak_size
            continue
            
        minf_data = mdia_data[minf_pos:minf_pos+minf_size]
        
        stbl_pos, stbl_size = find_box(minf_data, 'stbl', 8)
        if stbl_pos is None:
            pos = trak_pos + trak_size
            continue
            
        stbl_data = minf_data[stbl_pos:stbl_pos+stbl_size]
        
        stsd_pos, stsd_size = find_box(stbl_data, 'stsd', 8)
        if stsd_pos is None:
            pos = trak_pos + trak_size
            continue
            
        stsd_data = stbl_data[stsd_pos+8:stsd_pos+stsd_size]
        
        # Parse stsd
        version = stsd_data[0]
        entry_count = struct.unpack('>I', stsd_data[4:8])[0]
        print(f"STSD entries: {entry_count}")
        
        # Parse first entry (should be mp4a for AAC)
        if len(stsd_data) >= 16:
            entry_size = struct.unpack('>I', stsd_data[8:12])[0]
            entry_type = stsd_data[12:16].decode('ascii', errors='ignore')
            print(f"Audio format: {entry_type}")
            
            if entry_type == 'mp4a':
                # Find esds box within mp4a
                mp4a_data = stsd_data[8:8+entry_size]
                esds_pos, esds_size = find_box(mp4a_data, 'esds', 36)  # mp4a header is 36 bytes
                
                if esds_pos is not None:
                    esds_data = mp4a_data[esds_pos+8:esds_pos+esds_size]
                    parse_esds(esds_data)
        
        pos = trak_pos + trak_size

# Main execution
if __name__ == "__main__":
    if len(sys.argv) > 1:
        analyze_transcoded_file(sys.argv[1])
    else:
        # Look for the most recent transcoded file
        import glob
        import os
        
        # Check for temp files
        temp_files = glob.glob("/tmp/.tmp*.mp4")
        if temp_files:
            latest = max(temp_files, key=os.path.getmtime)
            print(f"Analyzing most recent temp file: {latest}")
            analyze_transcoded_file(latest)
        else:
            print("Usage: python analyze_transcoded_esds.py <mp4_file>")
            print("No temp files found in /tmp/")