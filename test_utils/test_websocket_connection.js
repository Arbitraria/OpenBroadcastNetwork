const WebSocket = require('ws');

const ws = new WebSocket('ws://127.0.0.1:8080/stream');

ws.on('open', function open() {
    console.log('WebSocket connected');
});

ws.on('message', function message(data) {
    if (data instanceof Buffer) {
        console.log(`Received binary data: ${data.length} bytes`);
        console.log(`First 16 bytes: ${Array.from(data.slice(0, 16)).map(b => b.toString(16).padStart(2, '0')).join(' ')}`);
        
        // Check if this looks like MP4 data
        if (data.length >= 8) {
            const boxSize = data.readUInt32BE(0);
            const boxType = data.slice(4, 8).toString('ascii');
            console.log(`MP4 box: type='${boxType}', size=${boxSize}, actual_size=${data.length}`);
        }
    } else {
        console.log(`Received text data: ${data}`);
    }
});

ws.on('error', function error(err) {
    console.error('WebSocket error:', err);
});

ws.on('close', function close() {
    console.log('WebSocket closed');
});

// Keep the script running for 10 seconds to receive data
setTimeout(() => {
    ws.close();
    process.exit(0);
}, 10000);