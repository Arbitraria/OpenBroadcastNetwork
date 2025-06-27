/**
 * Tests for VideoPlayer - Core video element wrapper
 */

import { VideoPlayer } from '../../components/VideoPlayer';
import { StreamManager } from '../../core/StreamManager';

// Mock dependencies
jest.mock('../../core/StreamManager');

// Store original createElement
const originalCreateElement = document.createElement.bind(document);

// Mock browser APIs
const createMockVideoElement = () => {
  // Create a real video element first
  const video = originalCreateElement('video');
  
  // Add mock methods
  (video as any).play = jest.fn().mockResolvedValue(undefined);
  (video as any).pause = jest.fn();
  
  // Override addEventListener to capture calls
  const addEventListener = jest.fn();
  const removeEventListener = jest.fn();
  
  Object.defineProperty(video, 'addEventListener', {
    value: addEventListener,
    writable: true,
  });
  
  Object.defineProperty(video, 'removeEventListener', {
    value: removeEventListener,
    writable: true,
  });
  
  // Set up default properties with getters and setters
  let currentTime = 0;
  let duration = 100;
  let volume = 1;
  let muted = false;
  let paused = true;
  let ended = false;
  let width = 640;
  let height = 480;
  let poster = '';
  let playbackRate = 1;
  let autoplay = false;
  let controls = false;
  let loop = false;
  let preload = 'metadata';
  let crossOrigin = null;
  let error = null;
  
  Object.defineProperties(video, {
    currentTime: { 
      get: () => currentTime, 
      set: (value) => { currentTime = value; },
      configurable: true 
    },
    duration: { 
      get: () => duration, 
      set: (value) => { duration = value; },
      configurable: true 
    },
    volume: { 
      get: () => volume, 
      set: (value) => { volume = value; },
      configurable: true 
    },
    muted: { 
      get: () => muted, 
      set: (value) => { muted = value; },
      configurable: true 
    },
    paused: { 
      get: () => paused, 
      set: (value) => { paused = value; },
      configurable: true 
    },
    ended: { 
      get: () => ended, 
      set: (value) => { ended = value; },
      configurable: true 
    },
    width: { 
      get: () => width, 
      set: (value) => { width = value; },
      configurable: true 
    },
    height: { 
      get: () => height, 
      set: (value) => { height = value; },
      configurable: true 
    },
    poster: { 
      get: () => poster, 
      set: (value) => { poster = value; },
      configurable: true 
    },
    playbackRate: { 
      get: () => playbackRate, 
      set: (value) => { playbackRate = value; },
      configurable: true 
    },
    autoplay: { 
      get: () => autoplay, 
      set: (value) => { autoplay = value; },
      configurable: true 
    },
    controls: { 
      get: () => controls, 
      set: (value) => { controls = value; },
      configurable: true 
    },
    loop: { 
      get: () => loop, 
      set: (value) => { loop = value; },
      configurable: true 
    },
    preload: { 
      get: () => preload, 
      set: (value) => { preload = value; },
      configurable: true 
    },
    crossOrigin: { 
      get: () => crossOrigin, 
      set: (value) => { crossOrigin = value; },
      configurable: true 
    },
    error: { 
      get: () => error, 
      set: (value) => { error = value; },
      configurable: true 
    },
    buffered: {
      value: {
        length: 0,
        start: jest.fn(),
        end: jest.fn(),
      },
      writable: true,
    },
  });
  
  // Mock parentNode
  Object.defineProperty(video, 'parentNode', {
    value: {
      removeChild: jest.fn(),
    },
    writable: true,
    configurable: true,
  });
  
  return video;
};

const mockResizeObserver = {
  observe: jest.fn(),
  disconnect: jest.fn(),
};

global.ResizeObserver = jest.fn(() => mockResizeObserver);

// Mock fullscreen API
const mockDocument = {
  fullscreenElement: null,
  exitFullscreen: jest.fn(),
};

Object.defineProperty(document, 'fullscreenElement', {
  get: () => mockDocument.fullscreenElement,
  configurable: true,
});

Object.defineProperty(document, 'exitFullscreen', {
  value: mockDocument.exitFullscreen,
  configurable: true,
});

const MockStreamManager = StreamManager as jest.MockedClass<typeof StreamManager>;

describe('VideoPlayer', () => {
  let container: HTMLElement;
  let videoPlayer: VideoPlayer;
  let mockMediaElement: any;

  beforeEach(() => {    
    // Create container
    container = originalCreateElement('div');
    document.body.appendChild(container);

    // Setup StreamManager prototype mocks
    MockStreamManager.prototype.connect = jest.fn().mockResolvedValue(true);
    MockStreamManager.prototype.disconnect = jest.fn();
    MockStreamManager.prototype.cleanup = jest.fn();
    MockStreamManager.prototype.on = jest.fn();
    MockStreamManager.prototype.play = jest.fn().mockResolvedValue(undefined);
    MockStreamManager.prototype.pause = jest.fn();
    MockStreamManager.prototype.seek = jest.fn();
    MockStreamManager.prototype.setVolume = jest.fn();
    MockStreamManager.prototype.setMuted = jest.fn();
    MockStreamManager.prototype.getBufferedRanges = jest.fn().mockReturnValue([]);

    // Mock createElement to return our mock video element
    jest.spyOn(document, 'createElement').mockImplementation((tagName: string) => {
      if (tagName === 'video') {
        // Create fresh mock video element for each test
        mockMediaElement = createMockVideoElement();
        return mockMediaElement as any;
      }
      return originalCreateElement(tagName);
    });

    // Reset mocks
    jest.clearAllMocks();
    
    // Create video player
    videoPlayer = new VideoPlayer(container);
  });

  afterEach(() => {
    if (videoPlayer) {
      videoPlayer.cleanup();
    }
    if (container && container.parentNode) {
      document.body.removeChild(container);
    }
    jest.restoreAllMocks();
  });

  describe('initialization', () => {
    test('should create video element with default options', () => {
      expect(document.createElement).toHaveBeenCalledWith('video');
      expect(container.children.length).toBe(1);
      expect(container.children[0]).toBe(mockMediaElement);
    });

    test('should create video element with custom options', () => {
      const customContainer = document.createElement('div');
      const customPlayer = new VideoPlayer(customContainer, {
        autoplay: true,
        muted: true,
        controls: true,
        width: 1920,
        height: 1080,
        className: 'custom-video',
      });

      expect(mockMediaElement.autoplay).toBe(true);
      expect(mockMediaElement.muted).toBe(true);
      expect(mockMediaElement.controls).toBe(true);
      expect(mockMediaElement.width).toBe(1920);
      expect(mockMediaElement.height).toBe(1080);
      expect(mockMediaElement.className).toBe('custom-video');

      customPlayer.cleanup();
    });

    test('should setup resize observer', () => {
      expect(ResizeObserver).toHaveBeenCalled();
      expect(mockResizeObserver.observe).toHaveBeenCalledWith(container);
    });

    test('should setup event listeners', () => {
      expect(mockMediaElement.addEventListener).toHaveBeenCalledWith('play', expect.any(Function));
      expect(mockMediaElement.addEventListener).toHaveBeenCalledWith('pause', expect.any(Function));
      expect(mockMediaElement.addEventListener).toHaveBeenCalledWith('timeupdate', expect.any(Function));
      expect(mockMediaElement.addEventListener).toHaveBeenCalledWith('error', expect.any(Function));
    });
  });

  describe('stream connection', () => {
    test('should create StreamManager on connection', async () => {
      MockStreamManager.prototype.connect = jest.fn().mockResolvedValue(true);
      MockStreamManager.prototype.on = jest.fn();

      await videoPlayer.connectToStream('ws://localhost:8080');

      expect(MockStreamManager).toHaveBeenCalledWith('ws://localhost:8080', mockMediaElement);
      expect(MockStreamManager.prototype.connect).toHaveBeenCalled();
    });

    test('should setup StreamManager event listeners', async () => {
      MockStreamManager.prototype.connect = jest.fn().mockResolvedValue(true);
      MockStreamManager.prototype.on = jest.fn();

      await videoPlayer.connectToStream('ws://localhost:8080');

      expect(MockStreamManager.prototype.on).toHaveBeenCalledWith('state_change', expect.any(Function));
      expect(MockStreamManager.prototype.on).toHaveBeenCalledWith('buffering', expect.any(Function));
      expect(MockStreamManager.prototype.on).toHaveBeenCalledWith('error', expect.any(Function));
    });

    test('should cleanup previous StreamManager on new connection', async () => {
      MockStreamManager.prototype.connect = jest.fn().mockResolvedValue(true);
      MockStreamManager.prototype.cleanup = jest.fn();
      MockStreamManager.prototype.on = jest.fn();

      // First connection
      await videoPlayer.connectToStream('ws://localhost:8080');
      const firstManager = videoPlayer.getStreamManager();

      // Second connection
      await videoPlayer.connectToStream('ws://localhost:8081');

      expect(MockStreamManager.prototype.cleanup).toHaveBeenCalled();
    });

    test('should disconnect from stream', () => {
      MockStreamManager.prototype.disconnect = jest.fn();
      MockStreamManager.prototype.on = jest.fn();

      // First need to connect
      videoPlayer['streamManager'] = new MockStreamManager('ws://test', mockMediaElement as any);

      videoPlayer.disconnect();

      expect(MockStreamManager.prototype.disconnect).toHaveBeenCalled();
      expect(videoPlayer.getStreamManager()).toBeNull();
    });
  });

  describe('playback control', () => {
    test('should play video without StreamManager', async () => {
      mockMediaElement.play.mockResolvedValue();

      await videoPlayer.play();

      expect(mockMediaElement.play).toHaveBeenCalled();
    });

    test('should play video with StreamManager', async () => {
      MockStreamManager.prototype.play = jest.fn().mockResolvedValue();
      MockStreamManager.prototype.on = jest.fn();
      
      videoPlayer['streamManager'] = new MockStreamManager('ws://test', mockMediaElement as any);

      await videoPlayer.play();

      expect(MockStreamManager.prototype.play).toHaveBeenCalled();
    });

    test('should pause video without StreamManager', () => {
      videoPlayer.pause();

      expect(mockMediaElement.pause).toHaveBeenCalled();
    });

    test('should pause video with StreamManager', () => {
      MockStreamManager.prototype.pause = jest.fn();
      MockStreamManager.prototype.on = jest.fn();
      
      videoPlayer['streamManager'] = new MockStreamManager('ws://test', mockMediaElement as any);

      videoPlayer.pause();

      expect(MockStreamManager.prototype.pause).toHaveBeenCalled();
    });

    test('should seek video', () => {
      videoPlayer.seek(30);

      expect(mockMediaElement.currentTime).toBe(30);
    });

    test('should set volume with clamping', () => {
      videoPlayer.setVolume(0.5);
      expect(mockMediaElement.volume).toBe(0.5);

      videoPlayer.setVolume(-0.1);
      expect(mockMediaElement.volume).toBe(0);

      videoPlayer.setVolume(1.5);
      expect(mockMediaElement.volume).toBe(1);
    });

    test('should set muted state', () => {
      videoPlayer.setMuted(true);
      expect(mockMediaElement.muted).toBe(true);

      videoPlayer.setMuted(false);
      expect(mockMediaElement.muted).toBe(false);
    });
  });

  describe('fullscreen functionality', () => {
    test('should enter fullscreen', async () => {
      const mockRequestFullscreen = jest.fn().mockResolvedValue();
      container.requestFullscreen = mockRequestFullscreen;

      await videoPlayer.enterFullscreen();

      expect(mockRequestFullscreen).toHaveBeenCalled();
    });

    test('should handle webkit fullscreen', async () => {
      const mockWebkitRequestFullscreen = jest.fn().mockResolvedValue();
      (container as any).webkitRequestFullscreen = mockWebkitRequestFullscreen;

      await videoPlayer.enterFullscreen();

      expect(mockWebkitRequestFullscreen).toHaveBeenCalled();
    });

    test('should exit fullscreen', async () => {
      await videoPlayer.exitFullscreen();

      expect(mockDocument.exitFullscreen).toHaveBeenCalled();
    });

    test('should detect fullscreen state', () => {
      mockDocument.fullscreenElement = container;
      expect(videoPlayer.isFullscreen()).toBe(true);

      mockDocument.fullscreenElement = null;
      expect(videoPlayer.isFullscreen()).toBe(false);
    });

    test('should handle fullscreen errors', async () => {
      const mockRequestFullscreen = jest.fn().mockRejectedValue(new Error('Fullscreen failed'));
      container.requestFullscreen = mockRequestFullscreen;

      const errorListener = jest.fn();
      videoPlayer.addEventListener('error', errorListener);

      await videoPlayer.enterFullscreen();

      expect(errorListener).toHaveBeenCalledWith(
        expect.objectContaining({
          detail: expect.objectContaining({
            message: expect.stringContaining('Fullscreen failed')
          })
        })
      );
    });
  });

  describe('state getters', () => {
    test('should get playback state', () => {
      // Get the actual video element from the player
      const videoElement = videoPlayer.getVideoElement();
      
      // Update the properties directly on the video element
      videoElement.currentTime = 30;
      videoElement.duration = 120;
      videoElement.paused = false;
      videoElement.ended = false;

      // Trigger updatePlaybackState by accessing the private method
      (videoPlayer as any).updatePlaybackState();

      const state = videoPlayer.getPlaybackState();

      expect(state.currentTime).toBe(30);
      expect(state.duration).toBe(120);
      expect(state.playing).toBe(true);
      expect(state.ended).toBe(false);
    });

    test('should get buffered ranges without StreamManager', () => {
      mockMediaElement.buffered = {
        length: 2,
        start: jest.fn().mockImplementation((i) => i === 0 ? 0 : 20),
        end: jest.fn().mockImplementation((i) => i === 0 ? 10 : 30),
      };

      const ranges = videoPlayer.getBufferedRanges();

      expect(ranges).toHaveLength(2);
      expect(ranges[0]).toEqual({ start: 0, end: 10, duration: 10 });
      expect(ranges[1]).toEqual({ start: 20, end: 30, duration: 10 });
    });

    test('should get buffered ranges with StreamManager', () => {
      MockStreamManager.prototype.getBufferedRanges = jest.fn().mockReturnValue([
        { start: 0, end: 15, duration: 15 }
      ]);
      MockStreamManager.prototype.on = jest.fn();
      
      videoPlayer['streamManager'] = new MockStreamManager('ws://test', mockMediaElement as any);

      const ranges = videoPlayer.getBufferedRanges();

      expect(MockStreamManager.prototype.getBufferedRanges).toHaveBeenCalled();
      expect(ranges).toEqual([{ start: 0, end: 15, duration: 15 }]);
    });

    test('should get video element properties', () => {
      mockMediaElement.currentTime = 45;
      mockMediaElement.duration = 100;
      mockMediaElement.volume = 0.8;
      mockMediaElement.muted = true;
      mockMediaElement.paused = false;
      mockMediaElement.ended = true;

      expect(videoPlayer.getCurrentTime()).toBe(45);
      expect(videoPlayer.getDuration()).toBe(100);
      expect(videoPlayer.getVolume()).toBe(0.8);
      expect(videoPlayer.isMuted()).toBe(true);
      expect(videoPlayer.isPaused()).toBe(false);
      expect(videoPlayer.isEnded()).toBe(true);
    });
  });

  describe('utility methods', () => {
    test('should set video size', () => {
      videoPlayer.setSize(1920, 1080);

      expect(mockMediaElement.width).toBe(1920);
      expect(mockMediaElement.height).toBe(1080);
      expect(container.style.width).toBe('1920px');
      expect(container.style.height).toBe('1080px');
    });

    test('should set poster', () => {
      videoPlayer.setPoster('https://example.com/poster.jpg');

      expect(mockMediaElement.poster).toBe('https://example.com/poster.jpg');
    });

    test('should set playback rate with clamping', () => {
      videoPlayer.setPlaybackRate(1.5);
      expect(mockMediaElement.playbackRate).toBe(1.5);

      videoPlayer.setPlaybackRate(0.1);
      expect(mockMediaElement.playbackRate).toBe(0.25);

      videoPlayer.setPlaybackRate(5.0);
      expect(mockMediaElement.playbackRate).toBe(4.0);
    });

    test('should get playback rate', () => {
      mockMediaElement.playbackRate = 2.0;
      expect(videoPlayer.getPlaybackRate()).toBe(2.0);
    });
  });

  describe('event handling', () => {
    test('should emit play event', () => {
      const playListener = jest.fn();
      videoPlayer.addEventListener('play', playListener);

      // Get the play event handler and trigger it
      const playHandler = mockMediaElement.addEventListener.mock.calls
        .find(call => call[0] === 'play')?.[1];
      
      expect(playHandler).toBeDefined();
      
      mockMediaElement.paused = false;
      playHandler?.({ type: 'play' });

      expect(playListener).toHaveBeenCalledWith(
        expect.objectContaining({
          detail: expect.objectContaining({
            playing: true
          })
        })
      );
    });

    test('should emit pause event', () => {
      const pauseListener = jest.fn();
      videoPlayer.addEventListener('pause', pauseListener);

      const pauseHandler = mockMediaElement.addEventListener.mock.calls
        .find(call => call[0] === 'pause')?.[1];
      
      mockMediaElement.paused = true;
      pauseHandler?.({ type: 'pause' });

      expect(pauseListener).toHaveBeenCalledWith(
        expect.objectContaining({
          detail: expect.objectContaining({
            playing: false
          })
        })
      );
    });

    test('should emit timeupdate event', () => {
      const timeListener = jest.fn();
      videoPlayer.addEventListener('timeupdate', timeListener);

      const timeHandler = mockMediaElement.addEventListener.mock.calls
        .find(call => call[0] === 'timeupdate')?.[1];
      
      mockMediaElement.currentTime = 25;
      timeHandler?.({ type: 'timeupdate' });

      expect(timeListener).toHaveBeenCalledWith(
        expect.objectContaining({
          detail: expect.objectContaining({
            currentTime: 25
          })
        })
      );
    });

    test('should emit resize event', () => {
      const resizeListener = jest.fn();
      videoPlayer.addEventListener('resize', resizeListener);

      // Trigger resize observer callback
      const resizeCallback = (ResizeObserver as jest.Mock).mock.calls[0][0];
      resizeCallback();

      expect(resizeListener).toHaveBeenCalledWith(
        expect.objectContaining({
          detail: expect.objectContaining({
            width: expect.any(Number),
            height: expect.any(Number)
          })
        })
      );
    });

    test('should emit error event on video error', () => {
      const errorListener = jest.fn();
      videoPlayer.addEventListener('error', errorListener);

      const errorHandler = mockMediaElement.addEventListener.mock.calls
        .find(call => call[0] === 'error')?.[1];
      
      mockMediaElement.error = {
        code: 4,
        message: 'Media format not supported'
      };
      
      errorHandler?.({ type: 'error' });

      expect(errorListener).toHaveBeenCalledWith(
        expect.objectContaining({
          detail: expect.objectContaining({
            message: expect.stringContaining('Video error')
          })
        })
      );
    });
  });

  describe('cleanup', () => {
    test('should cleanup all resources', () => {
      MockStreamManager.prototype.cleanup = jest.fn();
      MockStreamManager.prototype.on = jest.fn();
      
      videoPlayer['streamManager'] = new MockStreamManager('ws://test', mockMediaElement as any);

      videoPlayer.cleanup();

      expect(MockStreamManager.prototype.cleanup).toHaveBeenCalled();
      expect(mockResizeObserver.disconnect).toHaveBeenCalled();
      expect(mockMediaElement.parentNode.removeChild).toHaveBeenCalledWith(mockMediaElement);
      expect(videoPlayer.getStreamManager()).toBeNull();
    });

    test('should remove event listeners on cleanup', () => {
      videoPlayer.cleanup();

      expect(mockMediaElement.removeEventListener).toHaveBeenCalledWith('play', expect.any(Function));
      expect(mockMediaElement.removeEventListener).toHaveBeenCalledWith('pause', expect.any(Function));
      expect(mockMediaElement.removeEventListener).toHaveBeenCalledWith('timeupdate', expect.any(Function));
    });
  });

  describe('container selection', () => {
    test('should accept container by ID', () => {
      const testContainer = document.createElement('div');
      testContainer.id = 'test-container';
      document.body.appendChild(testContainer);

      const player = new VideoPlayer('test-container');

      expect(player.getContainer()).toBe(testContainer);

      player.cleanup();
      document.body.removeChild(testContainer);
    });

    test('should create new container if ID not found', () => {
      const player = new VideoPlayer('non-existent-id');

      expect(player.getContainer()).toBeInstanceOf(HTMLElement);

      player.cleanup();
    });
  });
});