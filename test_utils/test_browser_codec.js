#!/usr/bin/env node

/**
 * Browser-based codec compatibility tester using Puppeteer
 * This simulates actual browser behavior for testing codec strings
 */

const puppeteer = require('puppeteer');

const CODEC_FORMATS = [
    'mp4a.40.02',   // Zero-padded (might work)
    'mp4a.40.2',    // Non-padded (current default)
    'mp4a.64.2',    // Decimal 100 representation  
    'mp4a.40.5',    // HE-AAC
    'mp4a.40.29',   // HE-AAC v2
    'mp4a.40',      // Generic MPEG-4 audio
    'mp4a.67',      // Alternative AAC-LC
    'mp4a.66',      // AAC Main
    'mp4a.69',      // MP3 in MP4
    'mp4a.6B',      // MP3 in MP4 (alt)
];

async function testCodecs() {
    console.log('🌐 Browser Codec Compatibility Test');
    console.log('===================================\n');
    
    const browser = await puppeteer.launch({ 
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    });
    
    const page = await browser.newPage();
    
    // Enable console logging
    page.on('console', msg => {
        const text = msg.text();
        if (text.includes('CODEC_TEST')) {
            console.log(text.replace('CODEC_TEST:', '').trim());
        }
    });
    
    // Navigate to a blank page and test MediaSource API
    await page.goto('about:blank');
    
    // Inject test code
    const results = await page.evaluate((codecs) => {
        const testResults = [];
        
        console.log('CODEC_TEST: 🔍 Testing MediaSource codec support in browser...\n');
        
        if (!window.MediaSource) {
            console.log('CODEC_TEST: ❌ MediaSource API not available');
            return testResults;
        }
        
        // Test each codec
        for (const codec of codecs) {
            const mimeType = `audio/mp4; codecs="${codec}"`;
            const supported = MediaSource.isTypeSupported(mimeType);
            
            testResults.push({
                codec,
                mimeType,
                supported
            });
            
            console.log(`CODEC_TEST: ${supported ? '✅' : '❌'} ${mimeType}`);
        }
        
        // Also test video codec for comparison
        const videoMime = 'video/mp4; codecs="avc1.42E01E"';
        const videoSupported = MediaSource.isTypeSupported(videoMime);
        console.log(`CODEC_TEST: \n📹 Video codec test: ${videoSupported ? '✅' : '❌'} ${videoMime}`);
        
        return testResults;
    }, CODEC_FORMATS);
    
    await browser.close();
    
    // Analyze results
    console.log('\n📊 Summary:');
    console.log('===========\n');
    
    const supported = results.filter(r => r.supported);
    const unsupported = results.filter(r => !r.supported);
    
    if (supported.length > 0) {
        console.log('✅ Supported audio codecs:');
        supported.forEach(r => {
            console.log(`   • ${r.mimeType}`);
        });
        
        console.log(`\n🎯 RECOMMENDED: Use "${supported[0].codec}" for best compatibility`);
    } else {
        console.log('❌ No supported AAC codec formats found!');
        console.log('   This browser may not support AAC in MP4 containers');
    }
    
    if (unsupported.length > 0) {
        console.log('\n❌ Unsupported codecs:');
        unsupported.forEach(r => {
            console.log(`   • ${r.codec}`);
        });
    }
    
    // Return the first working codec
    return supported.length > 0 ? supported[0].codec : null;
}

// Test with actual browser and then test with server
async function runFullTest() {
    try {
        console.log('🚀 Starting automated codec testing...\n');
        
        // First test browser support
        const workingCodec = await testCodecs();
        
        if (workingCodec) {
            console.log(`\n✨ Found working codec: ${workingCodec}`);
            console.log('\n💡 Next steps:');
            console.log('1. Update the MP4 parser to use this codec format');
            console.log('2. Ensure the server sends this exact format');
            console.log('3. Test with the actual video stream');
        } else {
            console.log('\n⚠️  No working codec found - may need to re-encode audio');
        }
        
    } catch (error) {
        console.error('❌ Error during testing:', error.message);
        console.log('\n💡 Make sure you have Puppeteer installed:');
        console.log('   npm install puppeteer');
    }
}

// Check if we're running directly
if (require.main === module) {
    runFullTest();
}

module.exports = { testCodecs };