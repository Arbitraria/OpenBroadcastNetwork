/**
 * Browser test for AC-3 video-only mode with Stargate file.
 * This script can be run in the browser console to test the complete experience.
 */

async function testAC3VideoOnlyMode() {
    console.log('🎬 Testing AC-3 Video-Only Mode in Browser');
    console.log('==========================================');
    
    // Check if we're on the right page
    if (!window.location.href.includes('localhost:8080')) {
        console.error('❌ Please run this test on http://localhost:8080/');
        return;
    }
    
    // Check for required elements
    const connectBtn = document.getElementById('connectBtn');
    const video = document.getElementById('video');
    const log = document.getElementById('log');
    
    if (!connectBtn || !video || !log) {
        console.error('❌ Required elements not found. Make sure you\'re on the universal viewer page.');
        return;
    }
    
    console.log('✅ Page elements found');
    
    // Monitor log messages for AC-3 detection
    let ac3Detected = false;
    let videoOnlyModeEnabled = false;
    let audioChunksSkipped = 0;
    let videoChunksReceived = 0;
    
    // Override logMessage to monitor for our specific messages
    const originalLogMessage = window.logMessage;
    window.logMessage = function(message, type) {
        // Call original function
        if (originalLogMessage) {
            originalLogMessage(message, type);
        }
        
        // Monitor for AC-3 detection
        if (message.includes('AC-3/Dolby Digital audio detected')) {
            ac3Detected = true;
            console.log('✅ AC-3 Detection: Found AC-3 audio warning');
        }
        
        if (message.includes('Falling back to video-only mode')) {
            videoOnlyModeEnabled = true;
            console.log('✅ Video-Only Mode: Enabled due to AC-3');
        }
        
        if (message.includes('Video-only mode - skipping audio buffer creation')) {
            console.log('✅ Audio Buffer: Skipped creation in video-only mode');
        }
        
        if (message.includes('Skipping audio chunk') && message.includes('video-only mode')) {
            audioChunksSkipped++;
            if (audioChunksSkipped <= 3) {
                console.log(`✅ Audio Chunk Skip ${audioChunksSkipped}: Correctly skipped audio chunk`);
            }
        }
        
        if (message.includes('Appended') && message.includes('to video buffer')) {
            videoChunksReceived++;
            if (videoChunksReceived <= 3) {
                console.log(`✅ Video Chunk ${videoChunksReceived}: Successfully appended video data`);
            }
        }
    };
    
    // Monitor video events
    video.addEventListener('loadstart', () => {
        console.log('📺 Video: Loading started');
    });
    
    video.addEventListener('loadedmetadata', () => {
        console.log('📺 Video: Metadata loaded');
        console.log(`📺 Video: Duration = ${video.duration.toFixed(2)}s`);
    });
    
    video.addEventListener('canplay', () => {
        console.log('📺 Video: Can start playing');
    });
    
    video.addEventListener('playing', () => {
        console.log('✅ Video: Successfully playing in video-only mode!');
    });
    
    video.addEventListener('error', (e) => {
        console.error('❌ Video Error:', e);
        if (video.error) {
            console.error(`❌ Video Error Details: Code ${video.error.code}, Message: ${video.error.message}`);
        }
    });
    
    // Start the test
    console.log('🔗 Connecting to stream...');
    connectBtn.click();
    
    // Wait for results and provide summary
    setTimeout(() => {
        console.log('\n📊 Test Results Summary:');
        console.log('========================');
        console.log(`AC-3 Detection: ${ac3Detected ? '✅ PASS' : '❌ FAIL'}`);
        console.log(`Video-Only Mode: ${videoOnlyModeEnabled ? '✅ PASS' : '❌ FAIL'}`);
        console.log(`Audio Chunks Skipped: ${audioChunksSkipped > 0 ? '✅ PASS' : '⚠️ NONE'} (${audioChunksSkipped} skipped)`);
        console.log(`Video Chunks Received: ${videoChunksReceived > 0 ? '✅ PASS' : '❌ FAIL'} (${videoChunksReceived} received)`);
        console.log(`Video Duration: ${video.duration > 0 ? '✅ PASS' : '❌ FAIL'} (${video.duration.toFixed(2)}s)`);
        console.log(`Video Ready State: ${video.readyState >= 3 ? '✅ READY' : '⚠️ LOADING'} (state: ${video.readyState})`);
        
        const allCriteriaMet = ac3Detected && videoOnlyModeEnabled && videoChunksReceived > 0;
        
        if (allCriteriaMet) {
            console.log('\n🎉 AC-3 VIDEO-ONLY MODE TEST: PASSED!');
            console.log('✅ The universal viewer successfully:');
            console.log('   • Detected AC-3 audio incompatibility');
            console.log('   • Enabled video-only mode fallback');
            console.log('   • Received and processed video chunks');
            console.log('   • Avoided MediaSource audio errors');
            
            if (video.paused) {
                console.log('\n▶️ Video is paused - click play to verify playback works');
            } else {
                console.log('\n▶️ Video is playing successfully!');
            }
        } else {
            console.log('\n❌ AC-3 VIDEO-ONLY MODE TEST: FAILED');
            console.log('Some criteria were not met. Check the debug log for details.');
        }
        
    }, 15000); // 15 second summary
}

// Instructions for manual testing
console.log('🧪 AC-3 Video-Only Mode Browser Test');
console.log('===================================');
console.log('1. Make sure you\'re on http://localhost:8080/');
console.log('2. Open browser developer console (F12)');
console.log('3. Run: testAC3VideoOnlyMode()');
console.log('4. Watch for AC-3 detection and video-only mode messages');
console.log('5. Verify video plays without audio errors');
console.log('');
console.log('To run the test now, execute: testAC3VideoOnlyMode()');

// Export for global access
window.testAC3VideoOnlyMode = testAC3VideoOnlyMode;