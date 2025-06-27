/**
 * Media-related types for MediaSource Extensions (MSE) handling
 */

// Media configuration
export interface MediaConfig {
  videoCodec?: string;
  audioCodec?: string;
  videoMimeType?: string;
  audioMimeType?: string;
  videoProfile?: string;
  audioProfile?: string;
}

// Source buffer state
export interface SourceBufferState {
  buffer: SourceBuffer | null;
  mimeType: string;
  codec: string;
  updating: boolean;
  appendQueue: ArrayBuffer[];
  initialized: boolean;
  lastAppendTime: number;
}

// Media source state
export interface MediaSourceState {
  mediaSource: MediaSource | null;
  objectURL: string | null;
  readyState: 'closed' | 'open' | 'ended';
  duration: number;
  videoBuffer: SourceBufferState | null;
  audioBuffer: SourceBufferState | null;
}

// Buffered ranges helper
export interface BufferedRange {
  start: number;
  end: number;
  duration: number;
}

// Codec support result
export interface CodecSupportResult {
  supported: boolean;
  mimeType?: string;
  fallback?: string;
  reason?: string;
}

// Browser detection
export interface BrowserInfo {
  name: 'chrome' | 'firefox' | 'safari' | 'edge' | 'unknown';
  version: number;
  userAgent: string;
}

// Playback state
export interface PlaybackState {
  playing: boolean;
  currentTime: number;
  duration: number;
  buffered: BufferedRange[];
  seeking: boolean;
  ended: boolean;
  error: MediaError | null;
}

// Stream quality
export interface StreamQuality {
  bitrate: number;
  width?: number;
  height?: number;
  frameRate?: number;
}

// Adaptive bitrate state
export interface AdaptiveState {
  availableQualities: StreamQuality[];
  currentQuality: StreamQuality | null;
  autoSwitch: boolean;
  bandwidth: number;
}

// Statistics
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

// Events
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

// Type for MSE codec strings
export type VideoCodecString = 
  | `avc1.${string}` // H.264
  | `hev1.${string}` // HEVC
  | `hvc1.${string}` // HEVC
  | `vp09.${string}` // VP9
  | `av01.${string}` // AV1
  | 'vp8';

export type AudioCodecString = 
  | `mp4a.${string}` // AAC
  | 'opus'
  | 'vorbis'
  | 'flac'
  | 'mp3'
  | 'ac-3'
  | 'ec-3';

// MIME type helpers
export interface MimeTypeInfo {
  container: 'mp4' | 'webm' | 'ogg';
  videoCodec?: VideoCodecString;
  audioCodec?: AudioCodecString;
  video?: string;
  audio?: string;
  full: string;
}