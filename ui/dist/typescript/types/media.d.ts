/**
 * Media-related types for MediaSource Extensions (MSE) handling
 */
export interface MediaConfig {
    videoCodec?: string;
    audioCodec?: string;
    videoMimeType?: string;
    audioMimeType?: string;
    videoProfile?: string;
    audioProfile?: string;
}
export interface SourceBufferState {
    buffer: SourceBuffer | null;
    mimeType: string;
    codec: string;
    updating: boolean;
    appendQueue: ArrayBuffer[];
    initialized: boolean;
    lastAppendTime: number;
}
export interface MediaSourceState {
    mediaSource: MediaSource | null;
    objectURL: string | null;
    readyState: 'closed' | 'open' | 'ended';
    duration: number;
    videoBuffer: SourceBufferState | null;
    audioBuffer: SourceBufferState | null;
}
export interface BufferedRange {
    start: number;
    end: number;
    duration: number;
}
export interface CodecSupportResult {
    supported: boolean;
    mimeType?: string;
    fallback?: string;
    reason?: string;
}
export interface BrowserInfo {
    name: 'chrome' | 'firefox' | 'safari' | 'edge' | 'unknown';
    version: number;
    userAgent: string;
}
export interface PlaybackState {
    playing: boolean;
    currentTime: number;
    duration: number;
    buffered: BufferedRange[];
    seeking: boolean;
    ended: boolean;
    error: MediaError | null;
}
export interface StreamQuality {
    bitrate: number;
    width?: number;
    height?: number;
    frameRate?: number;
}
export interface AdaptiveState {
    availableQualities: StreamQuality[];
    currentQuality: StreamQuality | null;
    autoSwitch: boolean;
    bandwidth: number;
}
export interface StreamStatistics {
    bytesReceived: number;
    chunksReceived: number;
    initSegmentsReceived: number;
    mediaSegmentsReceived: number;
    droppedFrames: number;
    decodedFrames: number;
    bufferUnderruns: number;
    connectionTime: number;
    firstFrameTime: number;
}
export interface MediaEvent<T = unknown> {
    type: string;
    timestamp: number;
    data?: T;
}
export interface BufferUpdateEvent extends MediaEvent {
    type: 'buffer_update';
    data: {
        buffered: BufferedRange[];
        level: number;
    };
}
export interface QualityChangeEvent extends MediaEvent {
    type: 'quality_change';
    data: {
        from: StreamQuality | null;
        to: StreamQuality;
        reason: 'manual' | 'auto' | 'initial';
    };
}
export type VideoCodecString = `avc1.${string}` | `hev1.${string}` | `hvc1.${string}` | `vp09.${string}` | `av01.${string}` | 'vp8';
export type AudioCodecString = `mp4a.${string}` | 'opus' | 'vorbis' | 'flac' | 'mp3' | 'ac-3' | 'ec-3';
export interface MimeTypeInfo {
    container: 'mp4' | 'webm' | 'ogg';
    videoCodec?: VideoCodecString;
    audioCodec?: AudioCodecString;
    full: string;
}
//# sourceMappingURL=media.d.ts.map