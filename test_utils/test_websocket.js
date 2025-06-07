// Simple Node.js script to test WebSocket connection
const WebSocket = require('ws');

const ws = new WebSocket('ws://127.0.0.1:8080/stream');

ws.on('open', function open() {
  console.log('✅ WebSocket connected successfully!');
});

ws.on('message', function message(data) {
  console.log('📦 Received message:', data.length, 'bytes');
  
  try {
    // Try to parse as JSON first
    const jsonData = JSON.parse(data);
    console.log('📄 JSON Message:', JSON.stringify(jsonData, null, 2));
  } catch (e) {
    // Binary data
    console.log('🎥 Binary data received:', data.length, 'bytes');
  }
});

ws.on('error', function error(err) {
  console.error('❌ WebSocket error:', err.message);
});

ws.on('close', function close() {
  console.log('🔌 WebSocket connection closed');
});

// Keep alive for 10 seconds
setTimeout(() => {
  console.log('⏰ Test complete, closing connection');
  ws.close();
  process.exit(0);
}, 10000);