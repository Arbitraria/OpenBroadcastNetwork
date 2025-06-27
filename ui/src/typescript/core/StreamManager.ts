/**
 * StreamManager - Coordinates MediaManager and WebSocketClient for streaming
 */

import { MediaManager } from './MediaManager';
import { WebSocketClient } from './WebSocketClient';
import {
  ApplicationState,
  StreamState,
  ConnectionState,
  MediaSourceState,
  PlaybackState,
  UIState,
  ErrorState,
  StreamStatistics,
  AdaptiveState,
  StreamInfoMessage,
  ChunkInfoMessage,
  ApplicationError,
  StateUpdate,
  BufferedRange,
  CodecSupportResult,
} from '../types';

export interface StreamManagerOptions {
  autoReconnect?: boolean;
  maxReconnectAttempts?: number;
  reconnectInterval?: number;
  chunkTimeout?: number;
  maxMessageSize?: number;
}

export interface RetryOptions {
  maxAttempts: number;
  interval: number;
  backoffMultiplier?: number;
}

export class StreamManager extends EventTarget {
  private mediaManager: MediaManager;
  private webSocketClient: WebSocketClient;
  private videoElement: HTMLVideoElement;
  
  // State
  private state: ApplicationState;
  private retryOptions: RetryOptions = {
    maxAttempts: 3,
    interval: 1000,
    backoffMultiplier: 2,
  };
  
  // Event listeners cleanup
  private videoEventListeners: Array<{ event: string; listener: EventListener }> = [];

  constructor(
    wsUrl: string, 
    videoElement: HTMLVideoElement, 
    options: StreamManagerOptions = {}
  ) {
    super();
    
    this.videoElement = videoElement;
    this.mediaManager = new MediaManager();
    this.webSocketClient = new WebSocketClient(wsUrl, {
      autoReconnect: options.autoReconnect ?? true,
      maxReconnectAttempts: options.maxReconnectAttempts ?? 5,
      reconnectInterval: options.reconnectInterval ?? 1000,
      chunkTimeout: options.chunkTimeout ?? 10000,
      maxMessageSize: options.maxMessageSize ?? 50 * 1024 * 1024,
    });

    // Initialize state
    this.state = this.createInitialState();
    
    // Setup event handlers
    this.setupWebSocketEventHandlers();
    this.setupVideoEventHandlers();
  }

  /**
   * Connect to streaming server
   */
  async connect(): Promise<boolean> {
    try {
      this.updateConnectionState('connecting');
      
      // Initialize MediaSource
      const objectURL = await this.mediaManager.initialize();
      this.videoElement.src = objectURL;
      
      // Connect WebSocket
      const connected = await this.retryOperation(
        () => this.webSocketClient.connect(),
        this.retryOptions
      );
      
      if (connected) {
        this.updateConnectionState('connected');
        this.emitStateChange('connection', { state: 'connected' });
      }
      
      return connected;
    } catch (error) {
      this.handleError(error as Error, 'network');
      throw error;
    }
  }

  /**
   * Disconnect from streaming server
   */
  disconnect(): void {
    this.webSocketClient.disconnect();
    this.updateConnectionState('disconnected');
    this.emitStateChange('connection', { state: 'disconnected' });
  }

  /**
   * Get current connection state
   */
  getConnectionState(): ConnectionState {
    return this.webSocketClient.getState();
  }

  /**
   * Get complete application state
   */
  getState(): ApplicationState {
    // Update dynamic state
    this.updatePlaybackState();
    this.updateStatistics();
    this.updateMediaState();
    
    return { ...this.state };
  }

  /**
   * Playback controls
   */
  async play(): Promise<void> {
    try {
      await this.videoElement.play();
      this.state.playback.playing = true;
      this.emitStateChange('playback', { playing: true });
    } catch (error) {
      this.handleError(error as Error, 'media');
      throw error;
    }
  }

  pause(): void {
    this.videoElement.pause();
    this.state.playback.playing = false;
    this.emitStateChange('playback', { playing: false });
  }

  seek(time: number): void {
    this.videoElement.currentTime = time;
    this.state.playback.currentTime = time;
    this.emitStateChange('playback', { currentTime: time });
  }

  setVolume(volume: number): void {
    this.videoElement.volume = Math.max(0, Math.min(1, volume));
    this.state.ui.volume = this.videoElement.volume;
    this.emitStateChange('ui', { volume: this.videoElement.volume });
  }

  setMuted(muted: boolean): void {
    this.videoElement.muted = muted;
    this.state.ui.muted = muted;
    this.emitStateChange('ui', { muted });
  }

  /**
   * Buffer management
   */
  getBufferedRanges(): BufferedRange[] {
    return this.mediaManager.getVideoBufferedRanges();
  }

  resetStream(): void {
    this.mediaManager.cleanup();
    this.webSocketClient.resetChunks();
    this.state.stream = this.createInitialStreamState();
    this.emitStateChange('stream', this.state.stream);
  }

  updateBufferState(): void {
    const buffered = this.getVideoBufferedRanges();
    this.emit('buffering', { ranges: buffered });
  }

  /**
   * Configuration
   */
  setRetryOptions(options: Partial<RetryOptions>): void {
    this.retryOptions = { ...this.retryOptions, ...options };
  }

  /**
   * EventEmitter-style methods
   */
  on(event: string, listener: EventListener): void {
    this.addEventListener(event, listener);
  }

  off(event: string, listener: EventListener): void {
    this.removeEventListener(event, listener);
  }

  emit(event: string, detail?: any): void {
    this.dispatchEvent(new CustomEvent(event, { detail }));
  }

  /**
   * Cleanup
   */
  cleanup(): void {
    // Disconnect
    this.webSocketClient.disconnect();
    
    // Cleanup media
    this.mediaManager.cleanup();
    
    // Clear video source
    this.videoElement.src = '';
    
    // Remove video event listeners
    this.cleanupVideoEventListeners();
    
    // Reset state
    this.state = this.createInitialState();
  }

  /**
   * Private helper methods
   */

  private createInitialState(): ApplicationState {
    return {
      connection: {
        url: '',
        socket: null,
        state: 'disconnected',
        reconnectAttempts: 0,
        lastError: null,
        lastConnectTime: 0,
      },
      stream: this.createInitialStreamState(),
      media: {
        mediaSource: null,
        objectURL: null,
        readyState: 'closed',
        duration: 0,
        videoBuffer: null,
        audioBuffer: null,
      },
      playback: {
        playing: false,
        currentTime: 0,
        duration: 0,
        buffered: [],
        seeking: false,
        ended: false,
        error: null,
      },
      ui: {
        controlsVisible: true,
        fullscreen: false,
        volume: 1,
        muted: false,
        playbackRate: 1,
        debugMode: false,
        statusMessage: '',
      },
      error: {
        lastError: null,
        errorCount: 0,
        recoverable: true,
        retryCount: 0,
      },
      statistics: {
        bytesReceived: 0,
        chunksReceived: 0,
        initSegmentsReceived: 0,
        mediaSegmentsReceived: 0,
        droppedFrames: 0,
        decodedFrames: 0,
        bufferUnderruns: 0,
        connectionTime: 0,
        firstFrameTime: 0,
      },
      adaptive: {
        availableQualities: [],
        currentQuality: null,
        autoSwitch: true,
        bandwidth: 0,
      },
    };
  }

  private createInitialStreamState(): StreamState {
    return {
      streamInfo: null,
      hasVideo: false,
      hasAudio: false,
      initialized: false,
      ready: false,
      chunkedData: new Map(),
    };
  }

  private setupWebSocketEventHandlers(): void {
    this.webSocketClient.on('connected', () => {
      this.updateConnectionState('connected');
      this.emitStateChange('connection', { state: 'connected' });
    });

    this.webSocketClient.on('disconnected', () => {
      this.updateConnectionState('disconnected');
      this.emitStateChange('connection', { state: 'disconnected' });
    });

    this.webSocketClient.on('connection_error', (event: CustomEvent) => {
      this.handleError(event.detail, 'websocket');
    });

    this.webSocketClient.on('stream_info', (event: CustomEvent) => {
      this.handleStreamInfo(event.detail);
    });

    this.webSocketClient.on('chunk_info', (event: CustomEvent) => {
      this.handleChunkInfo(event.detail);
    });

    this.webSocketClient.on('init_segment', (event: CustomEvent) => {
      this.handleInitSegment(event.detail.data);
    });

    this.webSocketClient.on('binary_chunk', (event: CustomEvent) => {
      if (!event.detail.isInit) {
        this.handleMediaSegment(event.detail.data);
      }
    });
  }

  private setupVideoEventHandlers(): void {
    const events = [
      'play', 'pause', 'seeking', 'seeked', 'ended', 'error',
      'loadstart', 'loadedmetadata', 'loadeddata', 'canplay', 'canplaythrough',
      'timeupdate', 'progress', 'durationchange'
    ];

    events.forEach(eventName => {
      const listener = (event: Event) => {
        this.handleVideoEvent(eventName, event);
      };
      
      this.videoElement.addEventListener(eventName, listener);
      this.videoEventListeners.push({ event: eventName, listener });
    });
  }

  private cleanupVideoEventListeners(): void {
    this.videoEventListeners.forEach(({ event, listener }) => {
      this.videoElement.removeEventListener(event, listener);
    });
    this.videoEventListeners = [];
  }

  private handleVideoEvent(eventName: string, event: Event): void {
    this.updatePlaybackState();
    
    // Emit specific events
    switch (eventName) {
      case 'play':
        this.emit('play');
        break;
      case 'pause':
        this.emit('pause');
        break;
      case 'seeking':
        this.emit('seeking', { time: this.videoElement.currentTime });
        break;
      case 'seeked':
        this.emit('seeked', { time: this.videoElement.currentTime });
        break;
      case 'ended':
        this.emit('ended');
        break;
      case 'error':
        const error = this.videoElement.error;
        if (error) {
          this.handleError(new Error(`Video error: ${error.message}`), 'media');
        }
        break;
      case 'timeupdate':
        this.updateBufferState();
        break;
    }
  }

  private async handleStreamInfo(streamInfo: StreamInfoMessage): Promise<void> {
    try {
      this.state.stream.streamInfo = streamInfo;
      this.state.stream.hasVideo = !!streamInfo.video;
      this.state.stream.hasAudio = !!streamInfo.audio;
      
      // Setup video buffer if needed
      if (streamInfo.video) {
        await this.setupVideoBuffer(streamInfo.video.codec);
      }
      
      // Setup audio buffer if needed
      if (streamInfo.audio) {
        await this.setupAudioBuffer(streamInfo.audio.codec);
      }
      
      this.state.stream.initialized = true;
      this.emitStateChange('stream', this.state.stream);
      
    } catch (error) {
      this.handleError(error as Error, 'codec');
    }
  }

  private async setupVideoBuffer(codec: string): Promise<void> {
    let mimeType = this.getVideoMimeType(codec);
    let codecSupport = this.mediaManager.checkCodecSupport(mimeType);
    
    // Try fallback if not supported
    if (!codecSupport.supported && codecSupport.fallback) {
      mimeType = codecSupport.fallback;
      codecSupport = this.mediaManager.checkCodecSupport(mimeType);
    }
    
    if (!codecSupport.supported) {
      throw new Error(`Video codec not supported: ${codec}`);
    }
    
    await this.mediaManager.createVideoBuffer(mimeType);
  }

  private async setupAudioBuffer(codec: string): Promise<void> {
    const mimeType = this.getAudioMimeType(codec);
    const codecSupport = this.mediaManager.checkCodecSupport(mimeType);
    
    if (!codecSupport.supported) {
      throw new Error(`Audio codec not supported: ${codec}`);
    }
    
    await this.mediaManager.createAudioBuffer(mimeType);
  }

  private getVideoMimeType(codec: string): string {
    switch (codec.toLowerCase()) {
      case 'h264':
      case 'avc':
        return 'video/mp4; codecs="avc1.42C01E"';
      case 'hevc':
      case 'h265':
        return 'video/mp4; codecs="hev1.1.6.L93.90"';
      case 'vp9':
        return 'video/webm; codecs="vp09.00.10.08"';
      case 'av1':
        return 'video/mp4; codecs="av01.0.08M.08"';
      case 'ac-3':
        return 'video/mp4; codecs="ac-3"';
      default:
        return `video/mp4; codecs="${codec}"`;
    }
  }

  private getAudioMimeType(codec: string): string {
    switch (codec.toLowerCase()) {
      case 'aac':
        return 'audio/mp4; codecs="mp4a.40.2"';
      case 'opus':
        return 'audio/webm; codecs="opus"';
      case 'vorbis':
        return 'audio/webm; codecs="vorbis"';
      case 'mp3':
        return 'audio/mpeg';
      case 'ac-3':
        return 'audio/mp4; codecs="ac-3"';
      default:
        return `audio/mp4; codecs="${codec}"`;
    }
  }

  private handleChunkInfo(chunkInfo: ChunkInfoMessage): void {
    this.state.statistics.chunksReceived++;
    
    if (chunkInfo.chunk_type === 'init') {
      this.state.statistics.initSegmentsReceived++;
    } else {
      this.state.statistics.mediaSegmentsReceived++;
    }
  }

  private async handleInitSegment(data: ArrayBuffer): Promise<void> {
    try {
      await this.mediaManager.appendVideoData(data);
      this.state.statistics.bytesReceived += data.byteLength;
      this.state.stream.ready = true;
      this.emitStateChange('stream', { ready: true });
    } catch (error) {
      this.handleError(error as Error, 'media');
    }
  }

  private async handleMediaSegment(data: ArrayBuffer): Promise<void> {
    try {
      await this.mediaManager.appendVideoData(data);
      this.state.statistics.bytesReceived += data.byteLength;
      this.updateBufferState();
    } catch (error) {
      this.handleError(error as Error, 'media');
    }
  }

  private updateConnectionState(state: ConnectionState): void {
    this.state.connection.state = state;
    this.state.connection.lastConnectTime = Date.now();
  }

  private updatePlaybackState(): void {
    this.state.playback = {
      playing: !this.videoElement.paused,
      currentTime: this.videoElement.currentTime,
      duration: this.videoElement.duration || 0,
      buffered: this.getVideoBufferedRanges(),
      seeking: this.videoElement.seeking,
      ended: this.videoElement.ended,
      error: this.videoElement.error,
    };
  }

  private updateStatistics(): void {
    const wsStats = this.webSocketClient.getStatistics();
    this.state.statistics = {
      ...this.state.statistics,
      ...wsStats,
    };
  }

  private updateMediaState(): void {
    this.state.media = this.mediaManager.getState();
  }

  private getVideoBufferedRanges(): BufferedRange[] {
    const buffered = this.videoElement.buffered;
    const ranges: BufferedRange[] = [];
    
    for (let i = 0; i < buffered.length; i++) {
      const start = buffered.start(i);
      const end = buffered.end(i);
      ranges.push({
        start,
        end,
        duration: end - start,
      });
    }
    
    return ranges;
  }

  private handleError(error: Error, component: ApplicationError['component']): void {
    const appError: ApplicationError = {
      code: error.name || 'UNKNOWN_ERROR',
      message: error.message,
      timestamp: Date.now(),
      component,
      details: error,
      stack: error.stack,
    };

    this.state.error.lastError = appError;
    this.state.error.errorCount++;
    
    this.emitStateChange('error', {
      lastError: appError,
      errorCount: this.state.error.errorCount,
    });
    
    this.emit('error', appError);
  }

  private emitStateChange<K extends keyof ApplicationState>(
    type: K, 
    payload: Partial<ApplicationState[K]>
  ): void {
    const stateUpdate: StateUpdate<K> = {
      type,
      payload,
      timestamp: Date.now(),
    };
    
    this.emit('state_change', stateUpdate);
  }

  private async retryOperation<T>(
    operation: () => Promise<T>,
    options: RetryOptions
  ): Promise<T> {
    let lastError: Error;
    let delay = options.interval;
    
    for (let attempt = 1; attempt <= options.maxAttempts; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error as Error;
        this.state.error.retryCount = attempt;
        
        if (attempt === options.maxAttempts) {
          break;
        }
        
        // Wait before retry
        await new Promise(resolve => setTimeout(resolve, delay));
        
        // Exponential backoff
        if (options.backoffMultiplier) {
          delay *= options.backoffMultiplier;
        }
      }
    }
    
    throw lastError!;
  }
}