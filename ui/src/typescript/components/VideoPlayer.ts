/**
 * VideoPlayer - Core video element wrapper with enhanced functionality
 */

import { StreamManager } from '../core/StreamManager';
import { PlaybackState, BufferedRange } from '../types';

export interface VideoPlayerOptions {
  autoplay?: boolean;
  muted?: boolean;
  controls?: boolean;
  loop?: boolean;
  preload?: 'auto' | 'metadata' | 'none';
  crossOrigin?: 'anonymous' | 'use-credentials' | null;
  className?: string;
  width?: number;
  height?: number;
}

export interface VideoPlayerEvents {
  play: Event;
  pause: Event;
  ended: Event;
  timeupdate: Event;
  loadstart: Event;
  loadedmetadata: Event;
  loadeddata: Event;
  canplay: Event;
  canplaythrough: Event;
  waiting: Event;
  seeking: Event;
  seeked: Event;
  volumechange: Event;
  error: ErrorEvent;
  resize: Event;
  fullscreenchange: Event;
}

export class VideoPlayer extends EventTarget {
  private videoElement: HTMLVideoElement;
  private container: HTMLElement;
  private streamManager: StreamManager | null = null;
  private resizeObserver: ResizeObserver | null = null;
  private playbackState: PlaybackState;

  constructor(container: HTMLElement | string, options: VideoPlayerOptions = {}) {
    super();
    
    // Get container element
    this.container = typeof container === 'string' 
      ? document.getElementById(container) ?? document.createElement('div')
      : container;

    // Create video element
    this.videoElement = this.createVideoElement(options);
    this.container.appendChild(this.videoElement);

    // Initialize playback state
    this.playbackState = {
      playing: false,
      currentTime: 0,
      duration: 0,
      buffered: [],
      seeking: false,
      ended: false,
      error: null,
    };

    // Setup event listeners
    this.setupEventListeners();
    this.setupResizeObserver();
  }

  /**
   * Connect to streaming server
   */
  async connectToStream(websocketUrl: string): Promise<void> {
    if (this.streamManager) {
      this.streamManager.cleanup();
    }

    this.streamManager = new StreamManager(websocketUrl, this.videoElement);
    
    // Forward StreamManager events
    this.streamManager.on('state_change', (event) => {
      this.dispatchEvent(new CustomEvent('stream_state_change', { detail: event.detail }));
    });

    this.streamManager.on('buffering', (event) => {
      this.dispatchEvent(new CustomEvent('buffering', { detail: event.detail }));
    });

    this.streamManager.on('error', (event) => {
      this.playbackState.error = event.detail;
      this.dispatchEvent(new CustomEvent('error', { detail: event.detail }));
    });

    await this.streamManager.connect();
  }

  /**
   * Disconnect from streaming server
   */
  disconnect(): void {
    if (this.streamManager) {
      this.streamManager.disconnect();
      this.streamManager = null;
    }
  }

  /**
   * Playback control methods
   */
  async play(): Promise<void> {
    try {
      if (this.streamManager) {
        await this.streamManager.play();
      } else {
        await this.videoElement.play();
      }
    } catch (error) {
      this.handleError(error as Error);
    }
  }

  pause(): void {
    if (this.streamManager) {
      this.streamManager.pause();
    } else {
      this.videoElement.pause();
    }
  }

  seek(time: number): void {
    if (this.streamManager) {
      this.streamManager.seek(time);
    } else {
      this.videoElement.currentTime = time;
    }
  }

  setVolume(volume: number): void {
    const clampedVolume = Math.max(0, Math.min(1, volume));
    if (this.streamManager) {
      this.streamManager.setVolume(clampedVolume);
    } else {
      this.videoElement.volume = clampedVolume;
    }
  }

  setMuted(muted: boolean): void {
    if (this.streamManager) {
      this.streamManager.setMuted(muted);
    } else {
      this.videoElement.muted = muted;
    }
  }

  /**
   * Fullscreen methods
   */
  async enterFullscreen(): Promise<void> {
    try {
      if (this.container.requestFullscreen) {
        await this.container.requestFullscreen();
      } else if ((this.container as any).webkitRequestFullscreen) {
        await (this.container as any).webkitRequestFullscreen();
      } else if ((this.container as any).mozRequestFullScreen) {
        await (this.container as any).mozRequestFullScreen();
      } else if ((this.container as any).msRequestFullscreen) {
        await (this.container as any).msRequestFullscreen();
      }
    } catch (error) {
      this.handleError(new Error(`Fullscreen failed: ${(error as Error).message}`));
    }
  }

  async exitFullscreen(): Promise<void> {
    try {
      if (document.exitFullscreen) {
        await document.exitFullscreen();
      } else if ((document as any).webkitExitFullscreen) {
        await (document as any).webkitExitFullscreen();
      } else if ((document as any).mozCancelFullScreen) {
        await (document as any).mozCancelFullScreen();
      } else if ((document as any).msExitFullscreen) {
        await (document as any).msExitFullscreen();
      }
    } catch (error) {
      this.handleError(new Error(`Exit fullscreen failed: ${(error as Error).message}`));
    }
  }

  isFullscreen(): boolean {
    return !!(
      document.fullscreenElement ||
      (document as any).webkitFullscreenElement ||
      (document as any).mozFullScreenElement ||
      (document as any).msFullscreenElement
    );
  }

  /**
   * State getters
   */
  getPlaybackState(): PlaybackState {
    return { ...this.playbackState };
  }

  getBufferedRanges(): BufferedRange[] {
    if (this.streamManager) {
      return this.streamManager.getBufferedRanges();
    }

    const ranges: BufferedRange[] = [];
    const buffered = this.videoElement.buffered;
    for (let i = 0; i < buffered.length; i++) {
      const start = buffered.start(i);
      const end = buffered.end(i);
      ranges.push({ start, end, duration: end - start });
    }
    return ranges;
  }

  getCurrentTime(): number {
    return this.videoElement.currentTime;
  }

  getDuration(): number {
    return this.videoElement.duration || 0;
  }

  getVolume(): number {
    return this.videoElement.volume;
  }

  isMuted(): boolean {
    return this.videoElement.muted;
  }

  isPaused(): boolean {
    return this.videoElement.paused;
  }

  isEnded(): boolean {
    return this.videoElement.ended;
  }

  /**
   * Video element getters
   */
  getVideoElement(): HTMLVideoElement {
    return this.videoElement;
  }

  getContainer(): HTMLElement {
    return this.container;
  }

  getStreamManager(): StreamManager | null {
    return this.streamManager;
  }

  /**
   * Utility methods
   */
  setSize(width: number, height: number): void {
    this.videoElement.width = width;
    this.videoElement.height = height;
    this.container.style.width = `${width}px`;
    this.container.style.height = `${height}px`;
  }

  setPoster(url: string): void {
    this.videoElement.poster = url;
  }

  setPlaybackRate(rate: number): void {
    this.videoElement.playbackRate = Math.max(0.25, Math.min(4.0, rate));
  }

  getPlaybackRate(): number {
    return this.videoElement.playbackRate;
  }

  /**
   * Cleanup
   */
  cleanup(): void {
    if (this.streamManager) {
      this.streamManager.cleanup();
      this.streamManager = null;
    }

    if (this.resizeObserver) {
      this.resizeObserver.disconnect();
      this.resizeObserver = null;
    }

    this.removeEventListeners();
    
    if (this.videoElement.parentNode) {
      this.videoElement.parentNode.removeChild(this.videoElement);
    }
  }

  /**
   * Private methods
   */
  private createVideoElement(options: VideoPlayerOptions): HTMLVideoElement {
    const video = document.createElement('video');
    
    video.autoplay = options.autoplay ?? false;
    video.muted = options.muted ?? false;
    video.controls = options.controls ?? false;
    video.loop = options.loop ?? false;
    video.preload = options.preload ?? 'metadata';
    video.crossOrigin = options.crossOrigin ?? null;
    
    if (options.className) {
      video.className = options.className;
    }
    
    if (options.width) {
      video.width = options.width;
    }
    
    if (options.height) {
      video.height = options.height;
    }

    // Default styles
    video.style.width = '100%';
    video.style.height = '100%';
    video.style.display = 'block';

    return video;
  }

  private setupEventListeners(): void {
    // Playback events
    this.videoElement.addEventListener('play', this.handlePlay.bind(this));
    this.videoElement.addEventListener('pause', this.handlePause.bind(this));
    this.videoElement.addEventListener('ended', this.handleEnded.bind(this));
    this.videoElement.addEventListener('timeupdate', this.handleTimeUpdate.bind(this));
    
    // Loading events
    this.videoElement.addEventListener('loadstart', this.handleLoadStart.bind(this));
    this.videoElement.addEventListener('loadedmetadata', this.handleLoadedMetadata.bind(this));
    this.videoElement.addEventListener('loadeddata', this.handleLoadedData.bind(this));
    this.videoElement.addEventListener('canplay', this.handleCanPlay.bind(this));
    this.videoElement.addEventListener('canplaythrough', this.handleCanPlayThrough.bind(this));
    
    // Buffer events
    this.videoElement.addEventListener('waiting', this.handleWaiting.bind(this));
    this.videoElement.addEventListener('seeking', this.handleSeeking.bind(this));
    this.videoElement.addEventListener('seeked', this.handleSeeked.bind(this));
    
    // Volume events
    this.videoElement.addEventListener('volumechange', this.handleVolumeChange.bind(this));
    
    // Error events
    this.videoElement.addEventListener('error', this.handleVideoError.bind(this));
    
    // Fullscreen events
    document.addEventListener('fullscreenchange', this.handleFullscreenChange.bind(this));
    document.addEventListener('webkitfullscreenchange', this.handleFullscreenChange.bind(this));
    document.addEventListener('mozfullscreenchange', this.handleFullscreenChange.bind(this));
    document.addEventListener('MSFullscreenChange', this.handleFullscreenChange.bind(this));
  }

  private removeEventListeners(): void {
    // Remove all event listeners
    this.videoElement.removeEventListener('play', this.handlePlay);
    this.videoElement.removeEventListener('pause', this.handlePause);
    this.videoElement.removeEventListener('ended', this.handleEnded);
    this.videoElement.removeEventListener('timeupdate', this.handleTimeUpdate);
    this.videoElement.removeEventListener('loadstart', this.handleLoadStart);
    this.videoElement.removeEventListener('loadedmetadata', this.handleLoadedMetadata);
    this.videoElement.removeEventListener('loadeddata', this.handleLoadedData);
    this.videoElement.removeEventListener('canplay', this.handleCanPlay);
    this.videoElement.removeEventListener('canplaythrough', this.handleCanPlayThrough);
    this.videoElement.removeEventListener('waiting', this.handleWaiting);
    this.videoElement.removeEventListener('seeking', this.handleSeeking);
    this.videoElement.removeEventListener('seeked', this.handleSeeked);
    this.videoElement.removeEventListener('volumechange', this.handleVolumeChange);
    this.videoElement.removeEventListener('error', this.handleVideoError);
    
    document.removeEventListener('fullscreenchange', this.handleFullscreenChange);
    document.removeEventListener('webkitfullscreenchange', this.handleFullscreenChange);
    document.removeEventListener('mozfullscreenchange', this.handleFullscreenChange);
    document.removeEventListener('MSFullscreenChange', this.handleFullscreenChange);
  }

  private setupResizeObserver(): void {
    if (typeof ResizeObserver !== 'undefined') {
      this.resizeObserver = new ResizeObserver(() => {
        this.dispatchEvent(new CustomEvent('resize', {
          detail: {
            width: this.container.clientWidth,
            height: this.container.clientHeight,
          },
        }));
      });
      
      this.resizeObserver.observe(this.container);
    }
  }

  private updatePlaybackState(): void {
    this.playbackState = {
      playing: !this.videoElement.paused && !this.videoElement.ended,
      currentTime: this.videoElement.currentTime,
      duration: this.videoElement.duration || 0,
      buffered: this.getBufferedRanges(),
      seeking: this.playbackState.seeking,
      ended: this.videoElement.ended,
      error: this.playbackState.error,
    };
  }

  /**
   * Event handlers
   */
  private handlePlay(event: Event): void {
    this.playbackState.playing = true;
    this.updatePlaybackState();
    this.dispatchEvent(new CustomEvent('play', { detail: this.playbackState }));
  }

  private handlePause(event: Event): void {
    this.playbackState.playing = false;
    this.updatePlaybackState();
    this.dispatchEvent(new CustomEvent('pause', { detail: this.playbackState }));
  }

  private handleEnded(event: Event): void {
    this.playbackState.playing = false;
    this.playbackState.ended = true;
    this.updatePlaybackState();
    this.dispatchEvent(new CustomEvent('ended', { detail: this.playbackState }));
  }

  private handleTimeUpdate(event: Event): void {
    this.updatePlaybackState();
    this.dispatchEvent(new CustomEvent('timeupdate', { detail: this.playbackState }));
  }

  private handleLoadStart(event: Event): void {
    this.dispatchEvent(new CustomEvent('loadstart', { detail: event }));
  }

  private handleLoadedMetadata(event: Event): void {
    this.updatePlaybackState();
    this.dispatchEvent(new CustomEvent('loadedmetadata', { detail: this.playbackState }));
  }

  private handleLoadedData(event: Event): void {
    this.updatePlaybackState();
    this.dispatchEvent(new CustomEvent('loadeddata', { detail: this.playbackState }));
  }

  private handleCanPlay(event: Event): void {
    this.dispatchEvent(new CustomEvent('canplay', { detail: this.playbackState }));
  }

  private handleCanPlayThrough(event: Event): void {
    this.dispatchEvent(new CustomEvent('canplaythrough', { detail: this.playbackState }));
  }

  private handleWaiting(event: Event): void {
    this.dispatchEvent(new CustomEvent('waiting', { detail: this.playbackState }));
  }

  private handleSeeking(event: Event): void {
    this.playbackState.seeking = true;
    this.updatePlaybackState();
    this.dispatchEvent(new CustomEvent('seeking', { detail: this.playbackState }));
  }

  private handleSeeked(event: Event): void {
    this.playbackState.seeking = false;
    this.updatePlaybackState();
    this.dispatchEvent(new CustomEvent('seeked', { detail: this.playbackState }));
  }

  private handleVolumeChange(event: Event): void {
    this.dispatchEvent(new CustomEvent('volumechange', { 
      detail: { 
        volume: this.videoElement.volume, 
        muted: this.videoElement.muted 
      } 
    }));
  }

  private handleVideoError(event: Event): void {
    const error = this.videoElement.error;
    if (error) {
      this.handleError(new Error(`Video error: ${error.message} (code: ${error.code})`));
    }
  }

  private handleFullscreenChange(): void {
    this.dispatchEvent(new CustomEvent('fullscreenchange', { 
      detail: { 
        fullscreen: this.isFullscreen() 
      } 
    }));
  }

  private handleError(error: Error): void {
    this.playbackState.error = error as MediaError;
    this.dispatchEvent(new CustomEvent('error', { detail: error }));
  }
}