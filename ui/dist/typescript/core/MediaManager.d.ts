/**
 * MediaManager - Handles Media Source Extensions (MSE) operations
 */
import { MediaSourceState, SourceBufferState, CodecSupportResult, BufferedRange } from '../types';
export interface MediaManagerOptions {
    fallback?: string;
}
export declare class MediaManager {
    private mediaSource;
    private objectURL;
    private videoBuffer;
    private audioBuffer;
    private initializePromise;
    constructor();
    /**
     * Initialize MediaSource and return blob URL
     */
    initialize(timeout?: number): Promise<string>;
    /**
     * Create video source buffer
     */
    createVideoBuffer(mimeType: string, options?: MediaManagerOptions): Promise<SourceBufferState>;
    /**
     * Create audio source buffer
     */
    createAudioBuffer(mimeType: string, options?: MediaManagerOptions): Promise<SourceBufferState>;
    /**
     * Append data to video buffer
     */
    appendVideoData(data: ArrayBuffer): Promise<void>;
    /**
     * Append data to audio buffer
     */
    appendAudioData(data: ArrayBuffer): Promise<void>;
    /**
     * Get video buffered ranges
     */
    getVideoBufferedRanges(): BufferedRange[];
    /**
     * Get audio buffered ranges
     */
    getAudioBufferedRanges(): BufferedRange[];
    /**
     * Remove video buffer range
     */
    removeVideoBuffer(start: number, end: number): Promise<void>;
    /**
     * Remove audio buffer range
     */
    removeAudioBuffer(start: number, end: number): Promise<void>;
    /**
     * Check codec support
     */
    checkCodecSupport(mimeType: string): CodecSupportResult;
    /**
     * Get current state
     */
    getState(): MediaSourceState;
    /**
     * Set duration
     */
    setDuration(duration: number): void;
    /**
     * End of stream
     */
    endOfStream(): void;
    /**
     * Cleanup resources
     */
    cleanup(): void;
    /**
     * Private helper methods
     */
    private setupBufferEventHandlers;
    private appendToBuffer;
    private processAppendQueue;
    private removeFromBuffer;
    private getBufferedRanges;
    private cleanupBuffer;
    private extractCodec;
    private isChrome;
}
//# sourceMappingURL=MediaManager.d.ts.map