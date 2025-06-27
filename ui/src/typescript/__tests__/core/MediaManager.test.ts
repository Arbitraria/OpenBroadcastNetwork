/**
 * Tests for MediaManager - Media Source Extensions handling
 */

import { MediaManager } from '../../core/MediaManager';
import { MediaConfig, CodecSupportResult, BufferedRange } from '../../types';

// Mock MediaSource API
const mockMediaSource = {
  addEventListener: jest.fn(),
  removeEventListener: jest.fn(),
  addSourceBuffer: jest.fn(),
  endOfStream: jest.fn(),
  duration: 0,
  readyState: 'open' as const,
};

const mockSourceBuffer = {
  addEventListener: jest.fn(),
  removeEventListener: jest.fn(),
  appendBuffer: jest.fn((data: ArrayBuffer) => {
    // Simulate async buffer append
    (mockSourceBuffer as any).updating = true;
    setTimeout(() => {
      (mockSourceBuffer as any).updating = false;
      // Trigger all updateend listeners
      const updateEndListeners = mockSourceBuffer.addEventListener.mock.calls
        .filter(call => call[0] === 'updateend')
        .map(call => call[1]);
      updateEndListeners.forEach(listener => listener?.());
    }, 10);
  }),
  remove: jest.fn((start: number, end: number) => {
    // Simulate async buffer remove
    (mockSourceBuffer as any).updating = true;
    setTimeout(() => {
      (mockSourceBuffer as any).updating = false;
      // Trigger all updateend listeners
      const updateEndListeners = mockSourceBuffer.addEventListener.mock.calls
        .filter(call => call[0] === 'updateend')
        .map(call => call[1]);
      updateEndListeners.forEach(listener => listener?.());
    }, 10);
  }),
  abort: jest.fn(),
  updating: false,
  buffered: {
    length: 0,
    start: jest.fn(),
    end: jest.fn(),
  } as TimeRanges,
};

// Mock window.MediaSource
(global as any).MediaSource = jest.fn(() => mockMediaSource);
(global as any).MediaSource.isTypeSupported = jest.fn((type: string) => {
  // Simulate Chrome codec support
  const supported = [
    'video/mp4; codecs="avc1.42C01E"',
    'audio/mp4; codecs="mp4a.40.2"',
    'video/webm; codecs="vp9"',
    'audio/webm; codecs="opus"',
  ];
  return supported.includes(type);
});

(global as any).URL = {
  createObjectURL: jest.fn((obj: any) => 'blob:mock-url'),
  revokeObjectURL: jest.fn(),
};

describe('MediaManager', () => {
  let manager: MediaManager;

  beforeEach(() => {
    jest.clearAllMocks();
    manager = new MediaManager();
    mockMediaSource.addSourceBuffer.mockReturnValue(mockSourceBuffer);
  });

  afterEach(() => {
    manager.cleanup();
  });

  describe('initialization', () => {
    test('should create MediaSource and return blob URL', async () => {
      const initPromise = manager.initialize();
      
      // Simulate sourceopen event immediately
      setTimeout(() => {
        const openHandler = mockMediaSource.addEventListener.mock.calls.find(
          call => call[0] === 'sourceopen'
        )?.[1];
        openHandler?.();
      }, 0);
      
      const url = await initPromise;
      expect(url).toBe('blob:mock-url');
      expect(MediaSource).toHaveBeenCalledTimes(1);
      expect(URL.createObjectURL).toHaveBeenCalledWith(mockMediaSource);
    });

    test('should wait for sourceopen event', async () => {
      const initPromise = manager.initialize();
      
      // Simulate sourceopen event
      setTimeout(() => {
        const openHandler = mockMediaSource.addEventListener.mock.calls.find(
          call => call[0] === 'sourceopen'
        )?.[1];
        openHandler?.();
      }, 0);
      
      const url = await initPromise;
      expect(url).toBe('blob:mock-url');
    });

    test('should handle initialization timeout', async () => {
      // Don't trigger sourceopen event
      await expect(manager.initialize(100)).rejects.toThrow('MediaSource initialization timeout');
    });
  });

  describe('source buffer creation', () => {
    beforeEach(async () => {
      const initPromise = manager.initialize();
      
      // Trigger sourceopen event
      setTimeout(() => {
        const openHandler = mockMediaSource.addEventListener.mock.calls.find(
          call => call[0] === 'sourceopen'
        )?.[1];
        openHandler?.();
      }, 0);
      
      await initPromise;
    });

    test('should create video source buffer with supported codec', async () => {
      const buffer = await manager.createVideoBuffer('video/mp4; codecs="avc1.42C01E"');
      
      expect(buffer).toBeDefined();
      expect(mockMediaSource.addSourceBuffer).toHaveBeenCalledWith('video/mp4; codecs="avc1.42C01E"');
    });

    test('should create audio source buffer with supported codec', async () => {
      const buffer = await manager.createAudioBuffer('audio/mp4; codecs="mp4a.40.2"');
      
      expect(buffer).toBeDefined();
      expect(mockMediaSource.addSourceBuffer).toHaveBeenCalledWith('audio/mp4; codecs="mp4a.40.2"');
    });

    test('should throw error for unsupported codec', async () => {
      await expect(
        manager.createVideoBuffer('video/mp4; codecs="hev1.1.6.L93.90"')
      ).rejects.toThrow('Codec not supported');
    });

    test('should handle codec fallback', async () => {
      const config: MediaConfig = {
        videoCodec: 'ac-3',
        videoMimeType: 'video/mp4; codecs="ac-3"',
      };
      
      const buffer = await manager.createVideoBuffer(
        'video/mp4; codecs="ac-3"',
        { fallback: 'video/mp4; codecs="avc1.42C01E"' }
      );
      
      expect(buffer).toBeDefined();
      expect(mockMediaSource.addSourceBuffer).toHaveBeenCalledWith('video/mp4; codecs="avc1.42C01E"');
    });

    test('should prevent duplicate video buffer creation', async () => {
      await manager.createVideoBuffer('video/mp4; codecs="avc1.42C01E"');
      
      await expect(
        manager.createVideoBuffer('video/mp4; codecs="avc1.42C01E"')
      ).rejects.toThrow('Video buffer already exists');
    });
  });

  describe('data appending', () => {
    beforeEach(async () => {
      const initPromise = manager.initialize();
      
      // Trigger sourceopen event
      setTimeout(() => {
        const openHandler = mockMediaSource.addEventListener.mock.calls.find(
          call => call[0] === 'sourceopen'
        )?.[1];
        openHandler?.();
      }, 0);
      
      await initPromise;
    });

    test('should append data to video buffer', async () => {
      await manager.createVideoBuffer('video/mp4; codecs="avc1.42C01E"');
      
      const data = new ArrayBuffer(1024);
      await manager.appendVideoData(data);
      
      expect(mockSourceBuffer.appendBuffer).toHaveBeenCalledWith(data);
    });

    test('should queue data when buffer is updating', async () => {
      await manager.createVideoBuffer('video/mp4; codecs="avc1.42C01E"');
      
      // First append - will set updating to true
      const data1 = new ArrayBuffer(1024);
      const append1Promise = manager.appendVideoData(data1);
      
      // Second append while first is updating - should be queued
      const data2 = new ArrayBuffer(2048);
      const append2Promise = manager.appendVideoData(data2);
      
      // Wait for both to complete
      await Promise.all([append1Promise, append2Promise]);
      
      // Verify both were appended
      expect(mockSourceBuffer.appendBuffer).toHaveBeenCalledTimes(2);
      expect(mockSourceBuffer.appendBuffer).toHaveBeenCalledWith(data1);
      expect(mockSourceBuffer.appendBuffer).toHaveBeenCalledWith(data2);
    });

    test('should throw error when appending without buffer', async () => {
      const data = new ArrayBuffer(1024);
      await expect(manager.appendVideoData(data)).rejects.toThrow('Video buffer not initialized');
    });
  });

  describe('buffer management', () => {
    beforeEach(async () => {
      const initPromise = manager.initialize();
      
      // Trigger sourceopen event
      setTimeout(() => {
        const openHandler = mockMediaSource.addEventListener.mock.calls.find(
          call => call[0] === 'sourceopen'
        )?.[1];
        openHandler?.();
      }, 0);
      
      await initPromise;
    });

    test('should get buffered ranges', async () => {
      await manager.createVideoBuffer('video/mp4; codecs="avc1.42C01E"');
      
      // Mock buffered ranges
      (mockSourceBuffer.buffered as any).length = 2;
      mockSourceBuffer.buffered.start.mockImplementation((i: number) => i === 0 ? 0 : 10);
      mockSourceBuffer.buffered.end.mockImplementation((i: number) => i === 0 ? 5 : 15);
      
      const ranges = manager.getVideoBufferedRanges();
      
      expect(ranges).toHaveLength(2);
      expect(ranges[0]).toEqual({ start: 0, end: 5, duration: 5 });
      expect(ranges[1]).toEqual({ start: 10, end: 15, duration: 5 });
    });

    test('should remove buffer range', async () => {
      await manager.createVideoBuffer('video/mp4; codecs="avc1.42C01E"');
      
      await manager.removeVideoBuffer(0, 10);
      
      expect(mockSourceBuffer.remove).toHaveBeenCalledWith(0, 10);
    });

    test('should handle remove with updating buffer', async () => {
      await manager.createVideoBuffer('video/mp4; codecs="avc1.42C01E"');
      
      // Clear previous calls
      jest.clearAllMocks();
      
      // Set buffer to updating
      (mockSourceBuffer as any).updating = true;
      
      const removePromise = manager.removeVideoBuffer(0, 10);
      
      // Should wait for updateend
      expect(mockSourceBuffer.remove).not.toHaveBeenCalled();
      
      // Trigger updateend to allow remove
      (mockSourceBuffer as any).updating = false;
      const updateEndListeners = mockSourceBuffer.addEventListener.mock.calls
        .filter(call => call[0] === 'updateend')
        .map(call => call[1]);
      updateEndListeners.forEach(listener => listener?.());
      
      await removePromise;
      expect(mockSourceBuffer.remove).toHaveBeenCalledWith(0, 10);
    });
  });

  describe('codec detection', () => {
    test('should check codec support', () => {
      const result = manager.checkCodecSupport('video/mp4; codecs="avc1.42C01E"');
      
      expect(result.supported).toBe(true);
      expect(result.mimeType).toBe('video/mp4; codecs="avc1.42C01E"');
    });

    test('should detect unsupported codec', () => {
      const result = manager.checkCodecSupport('video/mp4; codecs="hev1.1.6.L93.90"');
      
      expect(result.supported).toBe(false);
      expect(result.reason).toContain('not supported');
    });

    test('should suggest fallback for AC-3 in Chrome', () => {
      // Mock Chrome user agent
      Object.defineProperty(navigator, 'userAgent', {
        value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        configurable: true,
      });
      
      const result = manager.checkCodecSupport('video/mp4; codecs="ac-3"');
      
      expect(result.supported).toBe(false);
      expect(result.fallback).toBe('video/mp4; codecs="avc1.42C01E"');
      expect(result.reason).toContain('Chrome');
    });
  });

  describe('state management', () => {
    test('should track media source state', async () => {
      expect(manager.getState()).toMatchObject({
        mediaSource: null,
        readyState: 'closed',
        videoBuffer: null,
        audioBuffer: null,
      });
      
      const initPromise = manager.initialize();
      
      // Trigger sourceopen event
      setTimeout(() => {
        const openHandler = mockMediaSource.addEventListener.mock.calls.find(
          call => call[0] === 'sourceopen'
        )?.[1];
        openHandler?.();
      }, 0);
      
      await initPromise;
      
      expect(manager.getState()).toMatchObject({
        mediaSource: mockMediaSource,
        readyState: 'open',
        objectURL: 'blob:mock-url',
      });
    });

    test('should update duration', async () => {
      const initPromise = manager.initialize();
      
      // Trigger sourceopen event
      setTimeout(() => {
        const openHandler = mockMediaSource.addEventListener.mock.calls.find(
          call => call[0] === 'sourceopen'
        )?.[1];
        openHandler?.();
      }, 0);
      
      await initPromise;
      
      manager.setDuration(120);
      
      expect(mockMediaSource.duration).toBe(120);
    });

    test('should end stream', async () => {
      const initPromise = manager.initialize();
      
      // Trigger sourceopen event
      setTimeout(() => {
        const openHandler = mockMediaSource.addEventListener.mock.calls.find(
          call => call[0] === 'sourceopen'
        )?.[1];
        openHandler?.();
      }, 0);
      
      await initPromise;
      
      manager.endOfStream();
      
      expect(mockMediaSource.endOfStream).toHaveBeenCalled();
    });
  });

  describe('cleanup', () => {
    test('should cleanup resources', async () => {
      const initPromise = manager.initialize();
      
      // Trigger sourceopen event
      setTimeout(() => {
        const openHandler = mockMediaSource.addEventListener.mock.calls.find(
          call => call[0] === 'sourceopen'
        )?.[1];
        openHandler?.();
      }, 0);
      
      const url = await initPromise;
      
      await manager.createVideoBuffer('video/mp4; codecs="avc1.42C01E"');
      
      manager.cleanup();
      
      expect(URL.revokeObjectURL).toHaveBeenCalledWith(url);
      expect(mockSourceBuffer.abort).toHaveBeenCalled();
      expect(mockSourceBuffer.removeEventListener).toHaveBeenCalled();
    });
  });

  describe('error handling', () => {
    test('should handle source buffer errors', async () => {
      const initPromise = manager.initialize();
      
      // Trigger sourceopen event
      setTimeout(() => {
        const openHandler = mockMediaSource.addEventListener.mock.calls.find(
          call => call[0] === 'sourceopen'
        )?.[1];
        openHandler?.();
      }, 0);
      
      await initPromise;
      
      await manager.createVideoBuffer('video/mp4; codecs="avc1.42C01E"');
      
      // Simulate error event
      const errorHandler = mockSourceBuffer.addEventListener.mock.calls.find(
        call => call[0] === 'error'
      )?.[1];
      
      const errorEvent = new Event('error');
      errorHandler?.(errorEvent);
      
      // Should still be able to get state
      expect(manager.getState().videoBuffer).toBeDefined();
    });

    test('should handle MediaSource errors', async () => {
      const initPromise = manager.initialize();
      
      // Simulate error event instead of sourceopen
      const errorHandler = mockMediaSource.addEventListener.mock.calls.find(
        call => call[0] === 'error'
      )?.[1];
      errorHandler?.(new Event('error'));
      
      await expect(initPromise).rejects.toThrow('MediaSource error');
    });
  });
});