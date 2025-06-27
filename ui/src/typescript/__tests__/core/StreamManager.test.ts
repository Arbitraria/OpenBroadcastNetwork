/**
 * Tests for StreamManager - Coordinates MediaManager and WebSocketClient
 */

import { StreamManager } from '../../core/StreamManager';
import { MediaManager } from '../../core/MediaManager';
import { WebSocketClient } from '../../core/WebSocketClient';
import { 
  StreamInfoMessage, 
  ChunkInfoMessage, 
  ApplicationState,
  ConnectionState 
} from '../../types';

// Mock dependencies
jest.mock('../../core/MediaManager');
jest.mock('../../core/WebSocketClient');

// Mock the prototype methods before they're used
(WebSocketClient.prototype as any).on = jest.fn();
(WebSocketClient.prototype as any).emit = jest.fn();
(WebSocketClient.prototype as any).addEventListener = jest.fn();
(WebSocketClient.prototype as any).removeEventListener = jest.fn();
(WebSocketClient.prototype as any).dispatchEvent = jest.fn();
(WebSocketClient.prototype as any).connect = jest.fn();
(WebSocketClient.prototype as any).disconnect = jest.fn();
(WebSocketClient.prototype as any).isConnected = jest.fn();
(WebSocketClient.prototype as any).getState = jest.fn();
(WebSocketClient.prototype as any).resetChunks = jest.fn();
(WebSocketClient.prototype as any).getStatistics = jest.fn();

(MediaManager.prototype as any).initialize = jest.fn();
(MediaManager.prototype as any).getState = jest.fn();
(MediaManager.prototype as any).checkCodecSupport = jest.fn();
(MediaManager.prototype as any).createVideoBuffer = jest.fn();
(MediaManager.prototype as any).createAudioBuffer = jest.fn();
(MediaManager.prototype as any).appendVideoData = jest.fn();
(MediaManager.prototype as any).appendAudioData = jest.fn();
(MediaManager.prototype as any).getVideoBufferedRanges = jest.fn();
(MediaManager.prototype as any).cleanup = jest.fn();

const MockMediaManager = MediaManager as jest.MockedClass<typeof MediaManager>;
const MockWebSocketClient = WebSocketClient as jest.MockedClass<typeof WebSocketClient>;

describe('StreamManager', () => {
  let streamManager: StreamManager;
  let mockMediaManager: jest.Mocked<MediaManager>;
  let mockWebSocketClient: jest.Mocked<WebSocketClient>;
  let mockVideoElement: HTMLVideoElement;

  beforeEach(() => {
    // Create mock video element
    mockVideoElement = document.createElement('video');
    
    // Reset mocks
    jest.clearAllMocks();
    
    // Setup default prototype mock behaviors
    (WebSocketClient.prototype.isConnected as jest.Mock).mockReturnValue(false);
    (WebSocketClient.prototype.getState as jest.Mock).mockReturnValue('disconnected');
    (WebSocketClient.prototype.connect as jest.Mock).mockResolvedValue(true);
    (WebSocketClient.prototype.getStatistics as jest.Mock).mockReturnValue({
      messagesSent: 0,
      messagesReceived: 0,
      bytesReceived: 0,
      connectionTime: 0,
      reconnectAttempts: 0,
      lastError: null,
    });
    
    (MediaManager.prototype.initialize as jest.Mock).mockResolvedValue('blob:mock-url');
    (MediaManager.prototype.getState as jest.Mock).mockReturnValue({
      mediaSource: null,
      objectURL: null,
      readyState: 'closed',
      duration: 0,
      videoBuffer: null,
      audioBuffer: null,
    });
    (MediaManager.prototype.checkCodecSupport as jest.Mock).mockReturnValue({ supported: true });
    (MediaManager.prototype.createVideoBuffer as jest.Mock).mockResolvedValue({
      buffer: {} as SourceBuffer,
      mimeType: 'video/mp4; codecs="avc1.42C01E"',
      codec: 'avc1.42C01E',
      updating: false,
      appendQueue: [],
      initialized: false,
      lastAppendTime: 0,
    });
    (MediaManager.prototype.createAudioBuffer as jest.Mock).mockResolvedValue({
      buffer: {} as SourceBuffer,
      mimeType: 'audio/mp4; codecs="mp4a.40.2"',
      codec: 'mp4a.40.2',
      updating: false,
      appendQueue: [],
      initialized: false,
      lastAppendTime: 0,
    });
    (MediaManager.prototype.appendVideoData as jest.Mock).mockResolvedValue(undefined);
    (MediaManager.prototype.appendAudioData as jest.Mock).mockResolvedValue(undefined);
    (MediaManager.prototype.getVideoBufferedRanges as jest.Mock).mockReturnValue([]);
    
    // Create stream manager
    streamManager = new StreamManager('ws://localhost:8080', mockVideoElement);
    
    // Get references to the created instances for easier access
    mockMediaManager = streamManager['mediaManager'] as jest.Mocked<MediaManager>;
    mockWebSocketClient = streamManager['webSocketClient'] as jest.Mocked<WebSocketClient>;
  });

  afterEach(() => {
    if (streamManager) {
      streamManager.cleanup();
    }
  });

  describe('initialization', () => {
    test('should create MediaManager and WebSocketClient', () => {
      expect(MockMediaManager).toHaveBeenCalledWith();
      expect(MockWebSocketClient).toHaveBeenCalledWith('ws://localhost:8080', 
        expect.objectContaining({
          autoReconnect: true,
          maxReconnectAttempts: 5,
        })
      );
    });

    test('should setup event listeners on WebSocketClient', () => {
      expect(WebSocketClient.prototype.on).toHaveBeenCalledWith('connected', expect.any(Function));
      expect(WebSocketClient.prototype.on).toHaveBeenCalledWith('disconnected', expect.any(Function));
      expect(WebSocketClient.prototype.on).toHaveBeenCalledWith('stream_info', expect.any(Function));
      expect(WebSocketClient.prototype.on).toHaveBeenCalledWith('chunk_info', expect.any(Function));
      expect(WebSocketClient.prototype.on).toHaveBeenCalledWith('init_segment', expect.any(Function));
      expect(WebSocketClient.prototype.on).toHaveBeenCalledWith('binary_chunk', expect.any(Function));
    });

    test('should initialize MediaManager on connection', async () => {
      await streamManager.connect();
      
      expect(MediaManager.prototype.initialize).toHaveBeenCalled();
      expect(mockVideoElement.src).toBe('blob:mock-url');
    });
  });

  describe('connection management', () => {
    test('should connect to WebSocket server', async () => {
      const result = await streamManager.connect();
      
      expect(result).toBe(true);
      expect(WebSocketClient.prototype.connect).toHaveBeenCalled();
      expect(MediaManager.prototype.initialize).toHaveBeenCalled();
    });

    test('should handle connection failure', async () => {
      mockWebSocketClient.connect.mockRejectedValue(new Error('Connection failed'));
      
      await expect(streamManager.connect()).rejects.toThrow('Connection failed');
    });

    test('should disconnect cleanly', () => {
      streamManager.disconnect();
      
      expect(mockWebSocketClient.disconnect).toHaveBeenCalled();
      expect(mockMediaManager.cleanup).toHaveBeenCalled();
    });

    test('should track connection state', () => {
      mockWebSocketClient.getState.mockReturnValue('connected');
      
      const state = streamManager.getConnectionState();
      expect(state).toBe('connected');
    });
  });

  describe('stream handling', () => {
    beforeEach(async () => {
      await streamManager.connect();
    });

    test('should handle stream info message', () => {
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

      // Get the stream_info event handler
      const streamInfoHandler = mockWebSocketClient.on.mock.calls
        .find(call => call[0] === 'stream_info')?.[1];
      
      expect(streamInfoHandler).toBeDefined();
      
      // Simulate receiving stream info
      streamInfoHandler?.({ detail: streamInfo } as CustomEvent);
      
      const state = streamManager.getState();
      expect(state.stream.streamInfo).toEqual(streamInfo);
      expect(state.stream.hasVideo).toBe(true);
      expect(state.stream.hasAudio).toBe(true);
    });

    test('should create video buffer on stream info with H.264', async () => {
      mockMediaManager.checkCodecSupport.mockReturnValue({ 
        supported: true, 
        mimeType: 'video/mp4; codecs="avc1.42C01E"' 
      });
      mockMediaManager.createVideoBuffer.mockResolvedValue({
        buffer: {} as SourceBuffer,
        mimeType: 'video/mp4; codecs="avc1.42C01E"',
        codec: 'avc1.42C01E',
        updating: false,
        appendQueue: [],
        initialized: false,
        lastAppendTime: 0,
      });

      const streamInfo: StreamInfoMessage = {
        type: 'stream_info',
        video: { codec: 'h264', width: 1920, height: 1080 },
      };

      const streamInfoHandler = mockWebSocketClient.on.mock.calls
        .find(call => call[0] === 'stream_info')?.[1];
      
      streamInfoHandler?.({ detail: streamInfo } as CustomEvent);
      
      // Wait for async operations
      await new Promise(resolve => setTimeout(resolve, 0));
      
      expect(mockMediaManager.createVideoBuffer).toHaveBeenCalledWith(
        'video/mp4; codecs="avc1.42C01E"'
      );
    });

    test('should handle codec fallback for AC-3 in Chrome', async () => {
      mockMediaManager.checkCodecSupport
        .mockReturnValueOnce({ 
          supported: false, 
          fallback: 'video/mp4; codecs="avc1.42C01E"',
          reason: 'Chrome does not support AC-3'
        })
        .mockReturnValueOnce({ 
          supported: true, 
          mimeType: 'video/mp4; codecs="avc1.42C01E"'
        });

      const streamInfo: StreamInfoMessage = {
        type: 'stream_info',
        video: { codec: 'ac-3', width: 1920, height: 1080 },
      };

      const streamInfoHandler = mockWebSocketClient.on.mock.calls
        .find(call => call[0] === 'stream_info')?.[1];
      
      streamInfoHandler?.({ detail: streamInfo } as CustomEvent);
      
      await new Promise(resolve => setTimeout(resolve, 0));
      
      expect(mockMediaManager.checkCodecSupport).toHaveBeenCalledTimes(2);
      expect(mockMediaManager.createVideoBuffer).toHaveBeenCalledWith(
        'video/mp4; codecs="avc1.42C01E"'
      );
    });

    test('should handle initialization segment', async () => {
      const initData = new ArrayBuffer(2048);
      
      const initSegmentHandler = mockWebSocketClient.on.mock.calls
        .find(call => call[0] === 'init_segment')?.[1];
      
      initSegmentHandler?.({ 
        detail: { 
          data: initData, 
          totalSize: 2048, 
          chunkCount: 2 
        } 
      } as CustomEvent);
      
      expect(mockMediaManager.appendVideoData).toHaveBeenCalledWith(initData);
    });

    test('should handle media segments', async () => {
      const mediaData = new ArrayBuffer(1024);
      
      const binaryChunkHandler = mockWebSocketClient.on.mock.calls
        .find(call => call[0] === 'binary_chunk')?.[1];
      
      binaryChunkHandler?.({ 
        detail: { 
          data: mediaData, 
          isInit: false,
          sequence: 1
        } 
      } as CustomEvent);
      
      expect(mockMediaManager.appendVideoData).toHaveBeenCalledWith(mediaData);
    });
  });

  describe('state management', () => {
    test('should provide complete application state', () => {
      const state = streamManager.getState();
      
      expect(state).toHaveProperty('connection');
      expect(state).toHaveProperty('stream');
      expect(state).toHaveProperty('media');
      expect(state).toHaveProperty('playback');
      expect(state).toHaveProperty('ui');
      expect(state).toHaveProperty('error');
      expect(state).toHaveProperty('statistics');
    });

    test('should update connection state on WebSocket events', () => {
      const connectedHandler = mockWebSocketClient.on.mock.calls
        .find(call => call[0] === 'connected')?.[1];
      
      connectedHandler?.({} as CustomEvent);
      
      const state = streamManager.getState();
      expect(state.connection.state).toBe('connected');
    });

    test('should update error state on failures', () => {
      const errorHandler = mockWebSocketClient.on.mock.calls
        .find(call => call[0] === 'connection_error')?.[1];
      
      const error = new Error('Connection failed');
      errorHandler?.({ detail: error } as CustomEvent);
      
      const state = streamManager.getState();
      expect(state.error.lastError).toEqual(expect.objectContaining({
        message: 'Connection failed',
        component: 'websocket',
      }));
    });

    test('should track statistics', () => {
      mockWebSocketClient.getStatistics.mockReturnValue({
        messagesSent: 5,
        messagesReceived: 10,
        bytesReceived: 1024000,
        connectionTime: 1500,
        reconnectAttempts: 0,
        lastError: null,
      });

      const state = streamManager.getState();
      expect(state.statistics.messagesSent).toBe(5);
      expect(state.statistics.messagesReceived).toBe(10);
      expect(state.statistics.bytesReceived).toBe(1024000);
    });
  });

  describe('playback control', () => {
    beforeEach(async () => {
      await streamManager.connect();
    });

    test('should play video', async () => {
      const playSpy = jest.spyOn(mockVideoElement, 'play').mockResolvedValue();
      
      await streamManager.play();
      
      expect(playSpy).toHaveBeenCalled();
    });

    test('should pause video', () => {
      const pauseSpy = jest.spyOn(mockVideoElement, 'pause');
      
      streamManager.pause();
      
      expect(pauseSpy).toHaveBeenCalled();
    });

    test('should seek to position', () => {
      streamManager.seek(30);
      
      expect(mockVideoElement.currentTime).toBe(30);
    });

    test('should set volume', () => {
      streamManager.setVolume(0.7);
      
      expect(mockVideoElement.volume).toBe(0.7);
    });

    test('should mute/unmute', () => {
      streamManager.setMuted(true);
      expect(mockVideoElement.muted).toBe(true);
      
      streamManager.setMuted(false);
      expect(mockVideoElement.muted).toBe(false);
    });
  });

  describe('buffer management', () => {
    beforeEach(async () => {
      await streamManager.connect();
    });

    test('should get buffered ranges', () => {
      mockMediaManager.getVideoBufferedRanges.mockReturnValue([
        { start: 0, end: 10, duration: 10 },
        { start: 15, end: 25, duration: 10 },
      ]);

      const ranges = streamManager.getBufferedRanges();
      
      expect(ranges).toHaveLength(2);
      expect(ranges[0]).toEqual({ start: 0, end: 10, duration: 10 });
    });

    test('should clear buffers on reset', async () => {
      streamManager.resetStream();
      
      expect(mockMediaManager.cleanup).toHaveBeenCalled();
      expect(mockWebSocketClient.resetChunks).toHaveBeenCalled();
    });
  });

  describe('error handling', () => {
    test('should handle MediaManager errors', async () => {
      mockMediaManager.initialize.mockRejectedValue(new Error('MSE not supported'));
      
      await expect(streamManager.connect()).rejects.toThrow('MSE not supported');
      
      const state = streamManager.getState();
      expect(state.error.lastError?.component).toBe('media');
    });

    test('should handle codec errors with fallback', async () => {
      mockMediaManager.checkCodecSupport.mockReturnValue({ 
        supported: false, 
        reason: 'Codec not supported'
      });

      const streamInfo: StreamInfoMessage = {
        type: 'stream_info',
        video: { codec: 'unknown', width: 1920, height: 1080 },
      };

      const streamInfoHandler = mockWebSocketClient.on.mock.calls
        .find(call => call[0] === 'stream_info')?.[1];
      
      streamInfoHandler?.({ detail: streamInfo } as CustomEvent);
      
      await new Promise(resolve => setTimeout(resolve, 0));
      
      const state = streamManager.getState();
      expect(state.error.lastError?.component).toBe('codec');
    });

    test('should retry on recoverable errors', async () => {
      let attempts = 0;
      mockWebSocketClient.connect.mockImplementation(() => {
        attempts++;
        if (attempts < 3) {
          return Promise.reject(new Error('Temporary failure'));
        }
        return Promise.resolve(true);
      });

      streamManager.setRetryOptions({ maxAttempts: 3, interval: 10 });
      
      const result = await streamManager.connect();
      
      expect(result).toBe(true);
      expect(attempts).toBe(3);
    });
  });

  describe('event emission', () => {
    test('should emit state changes', () => {
      const stateListener = jest.fn();
      streamManager.on('state_change', stateListener);
      
      // Trigger a state change
      const connectedHandler = mockWebSocketClient.on.mock.calls
        .find(call => call[0] === 'connected')?.[1];
      
      connectedHandler?.({} as CustomEvent);
      
      expect(stateListener).toHaveBeenCalledWith(
        expect.objectContaining({
          detail: expect.objectContaining({
            type: 'connection',
            payload: expect.objectContaining({
              state: 'connected'
            })
          })
        })
      );
    });

    test('should emit buffering events', () => {
      const bufferingListener = jest.fn();
      streamManager.on('buffering', bufferingListener);
      
      // Simulate buffer update
      Object.defineProperty(mockVideoElement, 'buffered', {
        value: {
          length: 1,
          start: () => 0,
          end: () => 10,
        },
        configurable: true,
      });
      
      streamManager.updateBufferState();
      
      expect(bufferingListener).toHaveBeenCalled();
    });

    test('should emit playback events', () => {
      const playListener = jest.fn();
      const pauseListener = jest.fn();
      
      streamManager.on('play', playListener);
      streamManager.on('pause', pauseListener);
      
      // Simulate video events
      mockVideoElement.dispatchEvent(new Event('play'));
      mockVideoElement.dispatchEvent(new Event('pause'));
      
      expect(playListener).toHaveBeenCalled();
      expect(pauseListener).toHaveBeenCalled();
    });
  });

  describe('cleanup', () => {
    test('should cleanup all resources', () => {
      streamManager.cleanup();
      
      expect(mockWebSocketClient.disconnect).toHaveBeenCalled();
      expect(mockMediaManager.cleanup).toHaveBeenCalled();
    });

    test('should remove video element source', () => {
      mockVideoElement.src = 'blob:test-url';
      
      streamManager.cleanup();
      
      expect(mockVideoElement.src).toBe('');
    });
  });
});