/**
 * MediaManager - Handles Media Source Extensions (MSE) operations
 */

import {
  MediaSourceState,
  SourceBufferState,
  CodecSupportResult,
  BufferedRange,
} from '../types';

export interface MediaManagerOptions {
  fallback?: string;
}

export class MediaManager {
  private mediaSource: MediaSource | null = null;
  private objectURL: string | null = null;
  private videoBuffer: SourceBufferState | null = null;
  private audioBuffer: SourceBufferState | null = null;
  private initializePromise: Promise<string> | null = null;

  constructor() {}

  /**
   * Initialize MediaSource and return blob URL
   */
  async initialize(timeout: number = 5000): Promise<string> {
    if (this.initializePromise) {
      return this.initializePromise;
    }

    this.initializePromise = new Promise<string>((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        reject(new Error('MediaSource initialization timeout'));
      }, timeout);

      try {
        this.mediaSource = new MediaSource();
        this.objectURL = URL.createObjectURL(this.mediaSource);

        const handleOpen = (): void => {
          clearTimeout(timeoutId);
          this.mediaSource?.removeEventListener('sourceopen', handleOpen);
          this.mediaSource?.removeEventListener('error', handleError);
          resolve(this.objectURL!);
        };

        const handleError = (): void => {
          clearTimeout(timeoutId);
          this.mediaSource?.removeEventListener('sourceopen', handleOpen);
          this.mediaSource?.removeEventListener('error', handleError);
          reject(new Error('MediaSource error'));
        };

        this.mediaSource.addEventListener('sourceopen', handleOpen);
        this.mediaSource.addEventListener('error', handleError);
      } catch (error) {
        clearTimeout(timeoutId);
        reject(error);
      }
    });

    return this.initializePromise;
  }

  /**
   * Create video source buffer
   */
  async createVideoBuffer(
    mimeType: string,
    options?: MediaManagerOptions
  ): Promise<SourceBufferState> {
    if (!this.mediaSource) {
      throw new Error('MediaSource not initialized');
    }

    if (this.videoBuffer) {
      throw new Error('Video buffer already exists');
    }

    let finalMimeType = mimeType;

    // Check codec support
    const support = this.checkCodecSupport(mimeType);
    if (!support.supported) {
      if (options?.fallback && MediaSource.isTypeSupported(options.fallback)) {
        finalMimeType = options.fallback;
      } else if (support.fallback && MediaSource.isTypeSupported(support.fallback)) {
        finalMimeType = support.fallback;
      } else {
        throw new Error(`Codec not supported: ${mimeType}`);
      }
    }

    try {
      const buffer = this.mediaSource.addSourceBuffer(finalMimeType);
      
      this.videoBuffer = {
        buffer,
        mimeType: finalMimeType,
        codec: this.extractCodec(finalMimeType),
        updating: false,
        appendQueue: [],
        initialized: false,
        lastAppendTime: 0,
      };

      this.setupBufferEventHandlers(buffer, 'video');
      
      return this.videoBuffer;
    } catch (error) {
      throw new Error(`Failed to create video buffer: ${error}`);
    }
  }

  /**
   * Create audio source buffer
   */
  async createAudioBuffer(
    mimeType: string,
    options?: MediaManagerOptions
  ): Promise<SourceBufferState> {
    if (!this.mediaSource) {
      throw new Error('MediaSource not initialized');
    }

    if (this.audioBuffer) {
      throw new Error('Audio buffer already exists');
    }

    let finalMimeType = mimeType;

    // Check codec support
    const support = this.checkCodecSupport(mimeType);
    if (!support.supported) {
      if (options?.fallback && MediaSource.isTypeSupported(options.fallback)) {
        finalMimeType = options.fallback;
      } else {
        throw new Error(`Codec not supported: ${mimeType}`);
      }
    }

    try {
      const buffer = this.mediaSource.addSourceBuffer(finalMimeType);
      
      this.audioBuffer = {
        buffer,
        mimeType: finalMimeType,
        codec: this.extractCodec(finalMimeType),
        updating: false,
        appendQueue: [],
        initialized: false,
        lastAppendTime: 0,
      };

      this.setupBufferEventHandlers(buffer, 'audio');
      
      return this.audioBuffer;
    } catch (error) {
      throw new Error(`Failed to create audio buffer: ${error}`);
    }
  }

  /**
   * Append data to video buffer
   */
  async appendVideoData(data: ArrayBuffer): Promise<void> {
    if (!this.videoBuffer?.buffer) {
      throw new Error('Video buffer not initialized');
    }

    return this.appendToBuffer(this.videoBuffer, data);
  }

  /**
   * Append data to audio buffer
   */
  async appendAudioData(data: ArrayBuffer): Promise<void> {
    if (!this.audioBuffer?.buffer) {
      throw new Error('Audio buffer not initialized');
    }

    return this.appendToBuffer(this.audioBuffer, data);
  }

  /**
   * Get video buffered ranges
   */
  getVideoBufferedRanges(): BufferedRange[] {
    if (!this.videoBuffer?.buffer) {
      return [];
    }

    return this.getBufferedRanges(this.videoBuffer.buffer.buffered);
  }

  /**
   * Get audio buffered ranges
   */
  getAudioBufferedRanges(): BufferedRange[] {
    if (!this.audioBuffer?.buffer) {
      return [];
    }

    return this.getBufferedRanges(this.audioBuffer.buffer.buffered);
  }

  /**
   * Remove video buffer range
   */
  async removeVideoBuffer(start: number, end: number): Promise<void> {
    if (!this.videoBuffer?.buffer) {
      throw new Error('Video buffer not initialized');
    }

    return this.removeFromBuffer(this.videoBuffer, start, end);
  }

  /**
   * Remove audio buffer range
   */
  async removeAudioBuffer(start: number, end: number): Promise<void> {
    if (!this.audioBuffer?.buffer) {
      throw new Error('Audio buffer not initialized');
    }

    return this.removeFromBuffer(this.audioBuffer, start, end);
  }

  /**
   * Check codec support
   */
  checkCodecSupport(mimeType: string): CodecSupportResult {
    if (MediaSource.isTypeSupported(mimeType)) {
      return {
        supported: true,
        mimeType,
      };
    }

    // Check for Chrome AC-3 limitation
    if (mimeType.includes('ac-3') && this.isChrome()) {
      return {
        supported: false,
        reason: 'Chrome does not support AC-3 codec in MSE',
        fallback: 'video/mp4; codecs="avc1.42C01E"',
      };
    }

    return {
      supported: false,
      reason: `Codec not supported: ${mimeType}`,
    };
  }

  /**
   * Get current state
   */
  getState(): MediaSourceState {
    return {
      mediaSource: this.mediaSource,
      objectURL: this.objectURL,
      readyState: this.mediaSource?.readyState ?? 'closed',
      duration: this.mediaSource?.duration ?? 0,
      videoBuffer: this.videoBuffer,
      audioBuffer: this.audioBuffer,
    };
  }

  /**
   * Set duration
   */
  setDuration(duration: number): void {
    if (this.mediaSource && this.mediaSource.readyState === 'open') {
      this.mediaSource.duration = duration;
    }
  }

  /**
   * End of stream
   */
  endOfStream(): void {
    if (this.mediaSource && this.mediaSource.readyState === 'open') {
      this.mediaSource.endOfStream();
    }
  }

  /**
   * Cleanup resources
   */
  cleanup(): void {
    if (this.videoBuffer?.buffer) {
      this.cleanupBuffer(this.videoBuffer);
    }

    if (this.audioBuffer?.buffer) {
      this.cleanupBuffer(this.audioBuffer);
    }

    if (this.objectURL) {
      URL.revokeObjectURL(this.objectURL);
      this.objectURL = null;
    }

    this.mediaSource = null;
    this.videoBuffer = null;
    this.audioBuffer = null;
    this.initializePromise = null;
  }

  /**
   * Private helper methods
   */

  private setupBufferEventHandlers(buffer: SourceBuffer, type: 'video' | 'audio'): void {
    const state = type === 'video' ? this.videoBuffer : this.audioBuffer;
    if (!state) return;

    buffer.addEventListener('updateend', () => {
      state.updating = false;
      this.processAppendQueue(state);
    });

    buffer.addEventListener('error', () => {
      console.error(`${type} buffer error`);
    });

    buffer.addEventListener('abort', () => {
      console.warn(`${type} buffer aborted`);
    });
  }

  private async appendToBuffer(state: SourceBufferState, data: ArrayBuffer): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!state.buffer) {
        reject(new Error('Buffer not initialized'));
        return;
      }

      if (state.buffer.updating || state.appendQueue.length > 0) {
        // Queue the data
        state.appendQueue.push(data);
        
        // Set up resolution when this data is processed
        const checkQueue = (): void => {
          if (!state.appendQueue.includes(data)) {
            state.buffer?.removeEventListener('updateend', checkQueue);
            resolve();
          }
        };
        
        state.buffer.addEventListener('updateend', checkQueue);
      } else {
        // Append immediately
        try {
          state.updating = true;
          state.buffer.appendBuffer(data);
          state.lastAppendTime = Date.now();
          
          const handleUpdateEnd = (): void => {
            state.buffer?.removeEventListener('updateend', handleUpdateEnd);
            state.buffer?.removeEventListener('error', handleError);
            resolve();
          };
          
          const handleError = (): void => {
            state.buffer?.removeEventListener('updateend', handleUpdateEnd);
            state.buffer?.removeEventListener('error', handleError);
            reject(new Error('Buffer append error'));
          };
          
          state.buffer.addEventListener('updateend', handleUpdateEnd);
          state.buffer.addEventListener('error', handleError);
        } catch (error) {
          state.updating = false;
          reject(error);
        }
      }
    });
  }

  private processAppendQueue(state: SourceBufferState): void {
    if (!state.buffer || state.updating || state.appendQueue.length === 0) {
      return;
    }

    const data = state.appendQueue.shift();
    if (data) {
      try {
        state.updating = true;
        state.buffer.appendBuffer(data);
        state.lastAppendTime = Date.now();
      } catch (error) {
        state.updating = false;
        console.error('Error processing append queue:', error);
      }
    }
  }

  private async removeFromBuffer(
    state: SourceBufferState,
    start: number,
    end: number
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!state.buffer) {
        reject(new Error('Buffer not initialized'));
        return;
      }

      const performRemove = (): void => {
        try {
          state.buffer!.remove(start, end);
          
          const handleUpdateEnd = (): void => {
            state.buffer?.removeEventListener('updateend', handleUpdateEnd);
            state.buffer?.removeEventListener('error', handleError);
            resolve();
          };
          
          const handleError = (): void => {
            state.buffer?.removeEventListener('updateend', handleUpdateEnd);
            state.buffer?.removeEventListener('error', handleError);
            reject(new Error('Buffer remove error'));
          };
          
          state.buffer!.addEventListener('updateend', handleUpdateEnd);
          state.buffer!.addEventListener('error', handleError);
        } catch (error) {
          reject(error);
        }
      };

      if (state.buffer.updating) {
        const waitForUpdate = (): void => {
          state.buffer?.removeEventListener('updateend', waitForUpdate);
          performRemove();
        };
        state.buffer.addEventListener('updateend', waitForUpdate);
      } else {
        performRemove();
      }
    });
  }

  private getBufferedRanges(buffered: TimeRanges): BufferedRange[] {
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

  private cleanupBuffer(state: SourceBufferState): void {
    if (state.buffer) {
      try {
        if (!state.buffer.updating) {
          state.buffer.abort();
        }
      } catch (error) {
        // Ignore abort errors during cleanup
      }
      
      // Remove all event listeners
      state.buffer.removeEventListener('updateend', () => {});
      state.buffer.removeEventListener('error', () => {});
      state.buffer.removeEventListener('abort', () => {});
    }
  }

  private extractCodec(mimeType: string): string {
    const match = mimeType.match(/codecs="([^"]+)"/);
    return match?.[1] ?? '';
  }

  private isChrome(): boolean {
    return /Chrome/.test(navigator.userAgent) && !/Edge/.test(navigator.userAgent);
  }
}