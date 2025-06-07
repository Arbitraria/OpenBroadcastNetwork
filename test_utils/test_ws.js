const WebSocket = require('ws');

const ws = new WebSocket('ws://127.0.0.1:8080/stream');

ws.on('open', function open() {
  console.log('Connected to WebSocket');
});

ws.on('message', function message(data) {
  if (typeof data === 'string') {
    console.log('Received text:', data);
  } else {
    console.log('Received binary data:', data.length, 'bytes');
  }
});

ws.on('error', function error(err) {
  console.error('WebSocket error:', err);
});

ws.on('close', function close() {
  console.log('WebSocket closed');
});

// Keep the script running
setTimeout(() => {
  console.log('Test complete');
  process.exit(0);
}, 10000);
