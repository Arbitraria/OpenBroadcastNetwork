#!/usr/bin/env python3
"""
Detailed AudioSpecificConfig analyzer for ESDS debugging
This will decode the exact AudioSpecificConfig and determine the correct codec string
"""

import asyncio
import websockets
import struct

def decode_audiospecificconfig(config_bytes):
    """
    Decode AudioSpecificConfig according to ISO/IEC 14496-3
    Returns tuple: (aac_object_type, sampling_frequency_index, channel_configuration)
    """
    if len(config_bytes) < 2:
        return None, None, None
    
    # First 16 bits contain the main config
    config_word = (config_bytes[0] << 8) | config_bytes[1]
    
    # Bits 15-11: audioObjectType (5 bits)
    aac_object_type = (config_word >> 11) & 0x1F
    
    # Bits 10-7: samplingFrequencyIndex (4 bits)  
    sampling_freq_index = (config_word >> 7) & 0x0F
    
    # Bits 6-3: channelConfiguration (4 bits)
    channel_config = (config_word >> 3) & 0x0F
    
    return aac_object_type, sampling_freq_index, channel_config

def get_sampling_frequency(index):
    """Convert sampling frequency index to actual frequency"""
    frequencies = [
        96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050,
        16000, 12000, 11025, 8000, 7350, None, None, None
    ]
    return frequencies[index] if index < len(frequencies) else None

def get_codec_string_from_config(esds_object_type, aac_object_type, sampling_freq_index, channel_config):
    """
    Determine the correct codec string based on ESDS object type and AudioSpecificConfig
    """
    print(f"\n🎯 CODEC STRING ANALYSIS:")
    print(f"   ESDS Object Type: 0x{esds_object_type:02X}")
    print(f"   AAC Object Type: {aac_object_type}")
    print(f"   Sampling Freq Index: {sampling_freq_index}")
    print(f"   Channel Config: {channel_config}")
    
    # The codec string format is: mp4a.ESDS_OT.AAC_OT
    # where ESDS_OT is from ESDS DecoderConfigDescriptor
    # and AAC_OT is from AudioSpecificConfig
    
    if esds_object_type == 0x40:  # MPEG-4 Audio
        if aac_object_type == 1:    # AAC Main
            codec_string = "mp4a.40.1"
        elif aac_object_type == 2:  # AAC-LC (most common)
            codec_string = "mp4a.40.2" 
        elif aac_object_type == 3:  # AAC-SSR
            codec_string = "mp4a.40.3"
        elif aac_object_type == 4:  # AAC-LTP
            codec_string = "mp4a.40.4"
        elif aac_object_type == 5:  # SBR (HE-AAC)
            codec_string = "mp4a.40.5"
        elif aac_object_type == 29: # HE-AAC v2
            codec_string = "mp4a.40.29"
        else:
            codec_string = f"mp4a.40.{aac_object_type}"
            
    elif esds_object_type == 0x02:  # AAC-LC direct
        codec_string = "mp4a.40.02"  # Zero-padded format
        
    else:
        # Other object types
        codec_string = f"mp4a.{esds_object_type:02X}.{aac_object_type}"
    
    return codec_string

async def analyze_audiospecificconfig():
    """Analyze the AudioSpecificConfig in detail"""
    print("🔍 AudioSpecificConfig Detailed Analysis")
    print("=" * 60)
    
    try:
        async with websockets.connect("ws://127.0.0.1:8080/stream") as websocket:
            print("✅ WebSocket connected, waiting for initialization segment...")
            
            message_count = 0
            while message_count < 10:  # Look at first few messages
                message = await websocket.recv()
                message_count += 1
                
                if isinstance(message, bytes) and len(message) > 1000:
                    print(f"\n📦 Analyzing binary segment ({len(message)} bytes)")
                    
                    # Find ESDS box
                    esds_pos = message.find(b'esds')
                    if esds_pos == -1:
                        print("❌ No ESDS box found")
                        continue
                        
                    print(f"🎯 ESDS found at offset {esds_pos}")
                    
                    # Analyze ESDS structure starting from 'esds'
                    esds_start = esds_pos
                    esds_data = message[esds_start:]
                    
                    if len(esds_data) < 20:
                        print("❌ ESDS data too short")
                        continue
                    
                    print(f"\n📋 ESDS Raw Data (first 40 bytes):")
                    hex_bytes = ' '.join(f"{b:02x}" for b in esds_data[:40])
                    print(f"   {hex_bytes}")
                    
                    # Find DecoderConfigDescriptor (0x04)
                    esds_object_type = None
                    audiospecificconfig = None
                    
                    for i in range(len(esds_data) - 10):
                        if esds_data[i] == 0x04:  # DecoderConfigDescriptor
                            print(f"\n🔍 Found DecoderConfigDescriptor at offset {esds_start + i}")
                            
                            # Skip tag and length bytes to find object type
                            j = i + 1
                            
                            # Skip variable length encoding
                            while j < len(esds_data) and (esds_data[j] & 0x80) != 0:
                                j += 1
                            if j < len(esds_data):
                                j += 1  # Skip last length byte
                            
                            if j < len(esds_data):
                                esds_object_type = esds_data[j]
                                print(f"   📍 ESDS Object Type: 0x{esds_object_type:02X}")
                                
                                # Look for DecSpecificInfo (0x05) which contains AudioSpecificConfig
                                for k in range(j, min(len(esds_data) - 5, j + 50)):
                                    if esds_data[k] == 0x05:  # DecSpecificInfo
                                        print(f"   📍 Found DecSpecificInfo at offset {esds_start + k}")
                                        
                                        # Skip tag and length to get AudioSpecificConfig
                                        m = k + 1
                                        
                                        # Skip variable length encoding
                                        while m < len(esds_data) and (esds_data[m] & 0x80) != 0:
                                            m += 1
                                        if m < len(esds_data):
                                            m += 1  # Skip last length byte
                                        
                                        if m + 4 < len(esds_data):
                                            # Extract AudioSpecificConfig (typically 2-4 bytes)
                                            config_length = min(4, len(esds_data) - m)
                                            audiospecificconfig = esds_data[m:m + config_length]
                                            
                                            print(f"   📍 AudioSpecificConfig: {' '.join(f'{b:02x}' for b in audiospecificconfig)}")
                                            break
                                break
                    
                    if esds_object_type is not None and audiospecificconfig is not None:
                        print(f"\n🎯 DETAILED ANALYSIS:")
                        print(f"   ESDS Object Type: 0x{esds_object_type:02X}")
                        
                        # Decode AudioSpecificConfig
                        aac_ot, sf_idx, ch_cfg = decode_audiospecificconfig(audiospecificconfig)
                        
                        if aac_ot is not None:
                            print(f"   AAC Object Type: {aac_ot}")
                            print(f"   Sampling Frequency Index: {sf_idx}")
                            print(f"   Channel Configuration: {ch_cfg}")
                            
                            # Get actual sampling frequency
                            freq = get_sampling_frequency(sf_idx)
                            if freq:
                                print(f"   Sampling Frequency: {freq} Hz")
                            
                            # Determine correct codec string
                            correct_codec = get_codec_string_from_config(esds_object_type, aac_ot, sf_idx, ch_cfg)
                            
                            print(f"\n✅ CORRECT CODEC STRING: {correct_codec}")
                            print(f"🔄 Current server sends: mp4a.40.2")
                            
                            if correct_codec != "mp4a.40.2":
                                print(f"❌ MISMATCH DETECTED!")
                                print(f"   Browser expects: {correct_codec}")
                                print(f"   Server sends: mp4a.40.2")
                                print(f"   This explains the codec validation error!")
                            else:
                                print(f"✅ Codec strings match - issue must be elsewhere")
                        
                        else:
                            print("❌ Failed to decode AudioSpecificConfig")
                    
                    else:
                        print("❌ Could not find ESDS object type or AudioSpecificConfig")
                    
                    break  # Only analyze first binary message
                    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(analyze_audiospecificconfig())