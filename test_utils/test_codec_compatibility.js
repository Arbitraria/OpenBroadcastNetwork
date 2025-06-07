#!/usr/bin/env node

/**
 * Automated codec compatibility tester
 * Tests different AAC codec string formats to find what works
 */

const WebSocket = require('ws');
const http = require('http');

// Test configuration
const SERVER_URL = 'ws://127.0.0.1:8080/stream';
const INIT_SEGMENT_TIMEOUT = 5000;

// Codec formats to test
const CODEC_FORMATS = [
    'mp4a.40.02',   // Zero-padded (RFC 6381 compliant)
    'mp4a.40.2',    // Non-padded (common but may fail)  
    'mp4a.64.2',    // Decimal 100 representation
    'mp4a.40.05',   // HE-AAC with zero-padding
    'mp4a.40.5',    // HE-AAC
    'mp4a.40.29',   // HE-AAC v2
    'mp4a.40',      // Generic MPEG-4 audio
    'mp4a.67',      // Alternative AAC-LC representation
    'mp4a.102.2',   // Another decimal variant
];

console.log('🧪 AAC Codec Compatibility Tester');
console.log('================================\n');

// Function to parse MP4 box structure
function parseMP4Box(buffer, offset = 0) {
    if (buffer.length < offset + 8) return null;
    
    const size = buffer.readUInt32BE(offset);
    const type = buffer.toString('ascii', offset + 4, offset + 8);
    
    return { size, type, offset };
}

// Function to find ESDS box and extract object type
function findObjectType(buffer) {
    let offset = 0;
    
    while (offset < buffer.length - 8) {
        const box = parseMP4Box(buffer, offset);
        if (!box) break;
        
        if (box.type === 'moov') {
            // Search within moov box
            const moovEnd = offset + box.size;
            let moovOffset = offset + 8;
            
            while (moovOffset < moovEnd) {
                const innerBox = parseMP4Box(buffer, moovOffset);
                if (!innerBox) break;
                
                // Look for esds box pattern
                if (buffer.length > moovOffset + 20) {
                    for (let i = moovOffset; i < Math.min(moovOffset + innerBox.size, buffer.length - 4); i++) {
                        // Look for ESDS box signature
                        if (buffer.toString('ascii', i, i + 4) === 'esds') {
                            // Found ESDS, look for object type (usually around offset +12 to +20)
                            for (let j = i + 8; j < Math.min(i + 30, buffer.length); j++) {
                                if (buffer[j] === 0x04) { // DecoderConfigDescriptor tag
                                    // Object type is typically 1-2 bytes after this tag
                                    if (j + 2 < buffer.length) {
                                        const objectType = buffer[j + 2];
                                        console.log(`  📦 Found object type: 0x${objectType.toString(16).toUpperCase()}`);
                                        return objectType;
                                    }
                                }
                            }
                        }
                    }
                }
                
                moovOffset += innerBox.size;
            }
        }
        
        offset += box.size;
    }
    
    return null;
}

// Test WebSocket connection and codec
async function testCodec() {
    return new Promise((resolve) => {
        console.log('📡 Connecting to WebSocket server...');
        
        const ws = new WebSocket(SERVER_URL);
        let initSegmentReceived = false;
        let objectType = null;
        
        const timeout = setTimeout(() => {
            if (!initSegmentReceived) {
                console.log('❌ Timeout: No initialization segment received');
            }
            ws.close();
            resolve({ success: false, objectType });
        }, INIT_SEGMENT_TIMEOUT);
        
        ws.on('open', () => {
            console.log('✅ WebSocket connected');
        });
        
        ws.on('message', (data) => {
            if (!initSegmentReceived) {
                console.log(`📥 Received first message: ${data.length} bytes`);
                
                // Check if it's binary data (initialization segment)
                if (Buffer.isBuffer(data) && data.length > 100) {
                    initSegmentReceived = true;
                    
                    // Parse MP4 structure
                    const firstBox = parseMP4Box(data, 0);
                    if (firstBox) {
                        console.log(`  🎬 First box: ${firstBox.type} (${firstBox.size} bytes)`);
                        
                        if (firstBox.type === 'ftyp') {
                            console.log('  ✅ Valid MP4 initialization segment detected');
                            
                            // Try to find object type
                            objectType = findObjectType(data);
                        }
                    }
                    
                    clearTimeout(timeout);
                    ws.close();
                    resolve({ success: true, objectType });
                } else {
                    // Might be a JSON control message
                    try {
                        const msg = JSON.parse(data.toString());
                        console.log(`  📋 Control message: ${msg.type}`);
                        
                        if (msg.type === 'stream_info' && msg.data.audio) {
                            console.log(`  🎵 Audio codec from server: ${msg.data.audio.mime_type}`);
                        }
                    } catch (e) {
                        // Not JSON, ignore
                    }
                }
            }
        });
        
        ws.on('error', (err) => {
            console.log(`❌ WebSocket error: ${err.message}`);
            clearTimeout(timeout);
            resolve({ success: false, objectType });
        });
        
        ws.on('close', () => {
            console.log('🔌 WebSocket closed');
            clearTimeout(timeout);
            resolve({ success: initSegmentReceived, objectType });
        });
    });
}

// Function to test browser codec support simulation
function simulateBrowserSupport(codecString, objectType) {
    // Simulate browser MediaSource.isTypeSupported() logic
    const fullMimeType = `audio/mp4; codecs="${codecString}"`;
    
    // Known working combinations based on browser behavior
    const supportMatrix = {
        0x40: ['mp4a.40.2', 'mp4a.40.02', 'mp4a.64.2'], // MPEG-4 Audio
        0x67: ['mp4a.67'],     // AAC-LC alternative
        0x66: ['mp4a.66'],     // AAC Main
        0x02: ['mp4a.40.2'],   // Direct AAC-LC
    };
    
    const supported = supportMatrix[objectType]?.includes(codecString) ?? false;
    
    console.log(`  ${supported ? '✅' : '❌'} ${fullMimeType} - ${supported ? 'Would be supported' : 'Would fail'}`);
    
    return supported;
}

// Main test function
async function runTests() {
    console.log('🔍 Testing server connection and codec detection...\n');
    
    const result = await testCodec();
    
    if (result.success) {
        console.log('\n✅ Successfully received initialization segment');
        
        if (result.objectType !== null) {
            console.log(`\n🎯 Detected AAC Object Type: 0x${result.objectType.toString(16).toUpperCase()}`);
            console.log('\n📊 Testing codec string compatibility:\n');
            
            let foundWorking = false;
            for (const codec of CODEC_FORMATS) {
                const wouldWork = simulateBrowserSupport(codec, result.objectType);
                if (wouldWork && !foundWorking) {
                    foundWorking = true;
                    console.log(`\n🎉 RECOMMENDED: Use "${codec}" for object type 0x${result.objectType.toString(16).toUpperCase()}`);
                }
            }
            
            if (!foundWorking) {
                console.log('\n⚠️  No standard codec string found for this object type');
                console.log('    You may need to use a custom codec string or re-encode the audio');
            }
        } else {
            console.log('\n⚠️  Could not detect object type from initialization segment');
        }
    } else {
        console.log('\n❌ Failed to receive initialization segment');
        console.log('   Make sure the server is running on http://127.0.0.1:8080');
    }
}

// Run the tests
runTests().catch(console.error);