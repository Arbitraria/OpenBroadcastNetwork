/**
 * Tests for WebSocketClient - WebSocket communication handling
 */

import { WebSocketClient } from '../../core/WebSocketClient';
import { 
  StreamInfoMessage, 
  ChunkInfoMessage, 
  ErrorMessage,
  BinaryChunk 
} from '../../types';

// Mock WebSocket
class MockWebSocket extends EventTarget {
  url: string;
  readyState: number = WebSocket.CONNECTING;
  binaryType: string = 'arraybuffer';
  protocol: string = '';
  extensions: string = '';
  bufferedAmount: number = 0;

  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  onopen: ((event: Event) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;

  constructor(url: string) {
    super();
    this.url = url;
  }

  send(data: string | ArrayBuffer | Blob): void {
    // Mock implementation
  }

  close(code?: number, reason?: string): void {
    this.readyState = WebSocket.CLOSED;
    const closeEvent = new CloseEvent('close', { 
      code: code ?? 1000, 
      reason: reason ?? '', 
      wasClean: true 
    });
    this.dispatchEvent(closeEvent);
    this.onclose?.(closeEvent);
  }

  // Helper methods for testing
  mockOpen(): void {
    this.readyState = WebSocket.OPEN;
    const openEvent = new Event('open');
    this.dispatchEvent(openEvent);
    this.onopen?.(openEvent);
  }

  mockMessage(data: string | ArrayBuffer): void {
    const messageEvent = new MessageEvent('message', { data });
    this.dispatchEvent(messageEvent);
    this.onmessage?.(messageEvent);
  }

  mockError(): void {
    const errorEvent = new Event('error');
    this.dispatchEvent(errorEvent);
    this.onerror?.(errorEvent);
  }
}

// Mock global WebSocket
(global as any).WebSocket = MockWebSocket;

describe('WebSocketClient', () => {
  let client: WebSocketClient;
  let mockSocket: MockWebSocket;

  beforeEach(() => {
    client = new WebSocketClient('ws://localhost:8080');
    // Mock socket will be created during connect()
    mockSocket = null as any;
  });

  afterEach(() => {
    client.disconnect();
  });

  describe('connection management', () => {
    test('should create WebSocket with correct URL', async () => {
      const connectPromise = client.connect();
      
      // Get the socket after connect starts
      mockSocket = (client as any).socket as MockWebSocket;
      expect(mockSocket.url).toBe('ws://localhost:8080');
      expect(mockSocket.binaryType).toBe('arraybuffer');
      
      // Mock successful connection to complete the test
      mockSocket.mockOpen();
      await connectPromise;
    });

    test('should connect and emit connected event', async () => {
      const connectPromise = client.connect();
      
      // Get the socket after connect starts
      mockSocket = (client as any).socket as MockWebSocket;
      
      // Mock successful connection
      mockSocket.mockOpen();
      
      const result = await connectPromise;
      expect(result).toBe(true);
      expect(client.isConnected()).toBe(true);
    });

    test('should handle connection timeout', async () => {
      const connectPromise = client.connect(100); // 100ms timeout
      
      // Don't mock connection - let it timeout
      await expect(connectPromise).rejects.toThrow('Connection timeout');
    });

    test('should handle connection error', async () => {
      const connectPromise = client.connect();
      
      // Get the socket after connect starts
      mockSocket = (client as any).socket as MockWebSocket;
      
      // Mock connection error
      mockSocket.mockError();
      
      await expect(connectPromise).rejects.toThrow('WebSocket connection failed');
    });

    test('should disconnect and emit disconnected event', async () => {
      const onDisconnected = jest.fn();
      client.on('disconnected', onDisconnected);
      
      // First connect
      const connectPromise = client.connect();
      mockSocket = (client as any).socket as MockWebSocket;
      mockSocket.mockOpen();
      await connectPromise;
      
      // Then disconnect
      client.disconnect();
      
      expect(mockSocket.readyState).toBe(WebSocket.CLOSED);
      expect(client.isConnected()).toBe(false);
      expect(onDisconnected).toHaveBeenCalled();
    });

    test('should auto-reconnect on connection loss', async () => {
      const onReconnecting = jest.fn();
      const onReconnected = jest.fn();
      
      client.on('reconnecting', onReconnecting);
      client.on('connected', onReconnected);
      
      // Enable auto-reconnect
      client.setAutoReconnect(true, 50, 2);
      
      // Initial connection
      const connectPromise = client.connect();
      mockSocket = (client as any).socket as MockWebSocket;
      mockSocket.mockOpen();
      await connectPromise;
      
      // Simulate connection loss
      mockSocket.close(1006, 'Connection lost');
      
      // Should start reconnecting
      expect(onReconnecting).toHaveBeenCalled();
      
      // Mock successful reconnection
      setTimeout(() => {
        const newSocket = (client as any).socket as MockWebSocket;
        newSocket.mockOpen();
      }, 60);
      
      // Wait for reconnection
      await new Promise(resolve => {
        client.on('connected', resolve);
      });
      
      expect(onReconnected).toHaveBeenCalled();
    });
  });

  describe('message handling', () => {
    beforeEach(async () => {
      const connectPromise = client.connect();
      mockSocket = (client as any).socket as MockWebSocket;
      mockSocket.mockOpen();
      await connectPromise;
    });

    test('should parse and emit stream info messages', () => {
      const onStreamInfo = jest.fn();
      client.on('stream_info', (event: CustomEvent) => onStreamInfo(event.detail));
      
      const streamInfo: StreamInfoMessage = {
        type: 'stream_info',
        video: {
          codec: 'h264',
          width: 1920,
          height: 1080,
          bitrate: 5000000,
        },
        audio: {
          codec: 'aac',
          channels: 2,
          sample_rate: 44100,
        },
      };
      
      mockSocket.mockMessage(JSON.stringify(streamInfo));
      
      expect(onStreamInfo).toHaveBeenCalledWith(streamInfo);
    });

    test('should parse and emit chunk info messages', () => {
      const onChunkInfo = jest.fn();
      client.on('chunk_info', (event: CustomEvent) => onChunkInfo(event.detail));
      
      const chunkInfo: ChunkInfoMessage = {
        type: 'chunk_info',
        chunk_type: 'init',
        size: 1024,
        sequence: 1,
        total_chunks: 3,
      };
      
      mockSocket.mockMessage(JSON.stringify(chunkInfo));
      
      expect(onChunkInfo).toHaveBeenCalledWith(chunkInfo);
    });

    test('should handle error messages', () => {
      const onError = jest.fn();
      client.on('error', (event: CustomEvent) => onError(event.detail));
      
      const errorMsg: ErrorMessage = {
        type: 'error',
        code: 'CODEC_NOT_SUPPORTED',
        message: 'Video codec not supported',
      };
      
      mockSocket.mockMessage(JSON.stringify(errorMsg));
      
      expect(onError).toHaveBeenCalledWith(errorMsg);
    });

    test('should handle binary data chunks', () => {
      const onBinaryChunk = jest.fn();
      client.on('binary_chunk', (event: CustomEvent) => onBinaryChunk(event.detail));
      
      const binaryData = new ArrayBuffer(1024);
      mockSocket.mockMessage(binaryData);
      
      expect(onBinaryChunk).toHaveBeenCalledWith(
        expect.objectContaining({
          data: binaryData,
        })
      );
    });

    test('should handle malformed JSON gracefully', () => {
      const onParseError = jest.fn();
      client.on('parse_error', (event: CustomEvent) => onParseError(event.detail));
      
      mockSocket.mockMessage('{ invalid json }');
      
      expect(onParseError).toHaveBeenCalledWith(
        expect.objectContaining({
          error: expect.any(Error),
          rawData: '{ invalid json }',
        })
      );
    });
  });

  describe('chunked data reassembly', () => {
    beforeEach(async () => {
      const connectPromise = client.connect();
      mockSocket = (client as any).socket as MockWebSocket;
      mockSocket.mockOpen();
      await connectPromise;
    });

    test('should reassemble chunked initialization segment', () => {
      const onInitSegment = jest.fn();
      client.on('init_segment', (event: CustomEvent) => onInitSegment(event.detail));
      
      // Send chunk info first
      const chunkInfo: ChunkInfoMessage = {
        type: 'chunk_info',
        chunk_type: 'init',
        size: 2048,
        sequence: 1,
        total_chunks: 2,
      };
      mockSocket.mockMessage(JSON.stringify(chunkInfo));
      
      // Send first chunk
      const chunk1 = new ArrayBuffer(1024);
      mockSocket.mockMessage(chunk1);
      
      // Should not emit yet
      expect(onInitSegment).not.toHaveBeenCalled();
      
      // Send second chunk info
      const chunkInfo2: ChunkInfoMessage = {
        type: 'chunk_info',
        chunk_type: 'init',
        size: 1024,
        sequence: 2,
        total_chunks: 2,
      };
      mockSocket.mockMessage(JSON.stringify(chunkInfo2));
      
      // Send second chunk
      const chunk2 = new ArrayBuffer(1024);
      mockSocket.mockMessage(chunk2);
      
      // Should emit complete segment
      expect(onInitSegment).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.any(ArrayBuffer),
          totalSize: 2048,
        })
      );
    });

    test('should handle chunk timeout', async () => {
      const onChunkTimeout = jest.fn();
      client.on('chunk_timeout', onChunkTimeout);
      
      // Set short timeout for testing
      client.setChunkTimeout(100);
      
      // Send chunk info but no data
      const chunkInfo: ChunkInfoMessage = {
        type: 'chunk_info',
        chunk_type: 'init',
        size: 1024,
        sequence: 1,
        total_chunks: 2,
      };
      mockSocket.mockMessage(JSON.stringify(chunkInfo));
      
      // Wait for timeout
      await new Promise(resolve => setTimeout(resolve, 150));
      
      expect(onChunkTimeout).toHaveBeenCalled();
    });

    test('should reset chunk state on new stream', () => {
      const onInitSegment = jest.fn();
      client.on('init_segment', onInitSegment);
      
      // Start first chunked sequence
      const chunkInfo1: ChunkInfoMessage = {
        type: 'chunk_info',
        chunk_type: 'init',
        size: 1024,
        sequence: 1,
        total_chunks: 2,
      };
      mockSocket.mockMessage(JSON.stringify(chunkInfo1));
      
      // Reset chunks (simulating new stream)
      client.resetChunks();
      
      // Start new sequence
      const chunkInfo2: ChunkInfoMessage = {
        type: 'chunk_info',
        chunk_type: 'init',
        size: 2048,
        sequence: 1,
        total_chunks: 1,
      };
      mockSocket.mockMessage(JSON.stringify(chunkInfo2));
      
      const chunk = new ArrayBuffer(2048);
      mockSocket.mockMessage(chunk);
      
      // Should emit only the new complete segment
      expect(onInitSegment).toHaveBeenCalledTimes(1);
    });
  });

  describe('message sending', () => {
    beforeEach(async () => {
      const connectPromise = client.connect();
      mockSocket = (client as any).socket as MockWebSocket;
      mockSocket.mockOpen();
      await connectPromise;
    });

    test('should send JSON messages', () => {
      const sendSpy = jest.spyOn(mockSocket, 'send');
      
      const message = { type: 'ping', timestamp: Date.now() };
      client.send(message);
      
      expect(sendSpy).toHaveBeenCalledWith(JSON.stringify(message));
    });

    test('should send binary data', () => {
      const sendSpy = jest.spyOn(mockSocket, 'send');
      
      const data = new ArrayBuffer(1024);
      client.sendBinary(data);
      
      expect(sendSpy).toHaveBeenCalledWith(data);
    });

    test('should queue messages when disconnected', () => {
      // Disconnect first
      client.disconnect();
      
      const message = { type: 'ping' };
      client.send(message);
      
      // Should be queued, not sent immediately
      const sendSpy = jest.spyOn(MockWebSocket.prototype, 'send');
      expect(sendSpy).not.toHaveBeenCalled();
      
      // Reconnect and check if queued message is sent
      const connectPromise = client.connect();
      mockSocket = (client as any).socket as MockWebSocket;
      mockSocket.mockOpen();
      
      return connectPromise.then(() => {
        expect(sendSpy).toHaveBeenCalledWith(JSON.stringify(message));
      });
    });
  });

  describe('connection state', () => {
    test('should track connection state correctly', async () => {
      expect(client.getState()).toBe('disconnected');
      
      const connectPromise = client.connect();
      expect(client.getState()).toBe('connecting');
      
      mockSocket = (client as any).socket as MockWebSocket;
      
      mockSocket.mockOpen();
      await connectPromise;
      
      expect(client.getState()).toBe('connected');
      expect(client.isConnected()).toBe(true);
      
      client.disconnect();
      expect(client.getState()).toBe('disconnected');
      expect(client.isConnected()).toBe(false);
    });

    test('should provide connection statistics', async () => {
      const connectPromise = client.connect();
      mockSocket = (client as any).socket as MockWebSocket;
      mockSocket.mockOpen();
      await connectPromise;
      
      // Send some messages
      client.send({ type: 'test1' });
      client.send({ type: 'test2' });
      
      const stats = client.getStatistics();
      expect(stats.messagesSent).toBe(2);
      expect(stats.messagesReceived).toBe(0);
      expect(stats.bytesReceived).toBe(0);
      expect(stats.connectionTime).toBeGreaterThanOrEqual(0);
    });

    test('should update statistics on received messages', async () => {
      const connectPromise = client.connect();
      mockSocket = (client as any).socket as MockWebSocket;
      mockSocket.mockOpen();
      await connectPromise;
      
      // Receive JSON message
      mockSocket.mockMessage('{"type":"test"}');
      
      // Receive binary message
      const binaryData = new ArrayBuffer(1024);
      mockSocket.mockMessage(binaryData);
      
      const stats = client.getStatistics();
      expect(stats.messagesReceived).toBe(2);
      expect(stats.bytesReceived).toBe(1024 + '{"type":"test"}'.length);
    });
  });

  describe('error handling', () => {
    test('should handle WebSocket errors gracefully', async () => {
      const onError = jest.fn();
      client.on('connection_error', (event: CustomEvent) => onError(event.detail));
      
      const connectPromise = client.connect();
      mockSocket = (client as any).socket as MockWebSocket;
      mockSocket.mockError();
      
      try {
        await connectPromise;
      } catch (error) {
        // Expected to fail
      }
      
      expect(onError).toHaveBeenCalled();
    });

    test('should handle unexpected disconnections', async () => {
      const onDisconnected = jest.fn();
      client.on('disconnected', (event: CustomEvent) => onDisconnected(event.detail));
      
      const connectPromise = client.connect();
      mockSocket = (client as any).socket as MockWebSocket;
      mockSocket.mockOpen();
      await connectPromise;
      
      // Simulate unexpected disconnection
      mockSocket.close(1006, 'Connection lost');
      
      expect(onDisconnected).toHaveBeenCalledWith(
        expect.objectContaining({
          code: 1006,
          reason: 'Connection lost',
          wasClean: true, // Our mock always sets wasClean to true
        })
      );
    });

    test('should respect maximum message size', async () => {
      const onError = jest.fn();
      client.on('message_error', (event: CustomEvent) => onError(event.detail));
      client.setMaxMessageSize(100);
      
      const connectPromise = client.connect();
      mockSocket = (client as any).socket as MockWebSocket;
      mockSocket.mockOpen();
      await connectPromise;
      
      // Send oversized message
      const largeMessage = 'x'.repeat(200);
      mockSocket.mockMessage(largeMessage);
      
      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({
          error: expect.stringContaining('Message too large'),
          size: 200,
          maxSize: 100,
        })
      );
    });
  });
});