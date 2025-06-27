/**
 * CodecDetector - Browser codec compatibility detection and recommendations
 */

import { 
  BrowserInfo, 
  CodecSupportResult, 
  MimeTypeInfo, 
  VideoCodecString,
  AudioCodecString 
} from '../types';

export interface CodecDetectorOptions {
  fallbacks?: string[];
}

export interface CodecRecommendation {
  video: string[];
  audio: string[];
  container: 'mp4' | 'webm' | 'ogg';
}

export interface SupportedFeatures {
  mse: boolean;
  eme: boolean;
  webrtc: boolean;
  webassembly: boolean;
}

export interface CodecDetails {
  container: string;
  videoCodec?: VideoCodecString;
  audioCodec?: AudioCodecString;
}

export interface CodecQuality {
  compression: 'excellent' | 'good' | 'fair' | 'poor';
  compatibility: 'excellent' | 'good' | 'fair' | 'poor';
  performance: 'excellent' | 'good' | 'fair' | 'poor';
}

export class CodecDetector {
  private browserInfo: BrowserInfo | null = null;
  private codecCache = new Map<string, CodecSupportResult>();
  private featuresCache: SupportedFeatures | null = null;

  constructor() {
    this.browserInfo = this.detectBrowser();
  }

  /**
   * Detect browser type and version
   */
  detectBrowser(): BrowserInfo {
    const userAgent = navigator.userAgent;

    // Edge (must check before Chrome)
    const edgeMatch = userAgent.match(/Edg\/(\d+)/);
    if (edgeMatch) {
      const version = parseInt(edgeMatch[1], 10);
      return { name: 'edge', version, userAgent };
    }

    // Chrome
    const chromeMatch = userAgent.match(/Chrome\/(\d+)/);
    if (chromeMatch && !userAgent.includes('Edg')) {
      const version = parseInt(chromeMatch[1], 10);
      return { name: 'chrome', version, userAgent };
    }

    // Firefox
    const firefoxMatch = userAgent.match(/Firefox\/(\d+)/);
    if (firefoxMatch) {
      const version = parseInt(firefoxMatch[1], 10);
      return { name: 'firefox', version, userAgent };
    }

    // Safari
    const safariMatch = userAgent.match(/Version\/(\d+).*Safari/);
    if (safariMatch && !userAgent.includes('Chrome')) {
      const version = parseInt(safariMatch[1], 10);
      return { name: 'safari', version, userAgent };
    }

    return { name: 'unknown', version: 0, userAgent };
  }

  /**
   * Check if a specific codec/MIME type is supported
   */
  checkCodecSupport(
    mimeType: string, 
    options: CodecDetectorOptions = {}
  ): CodecSupportResult {
    // Check cache first
    const cacheKey = `${mimeType}_${JSON.stringify(options)}`;
    if (this.codecCache.has(cacheKey)) {
      return this.codecCache.get(cacheKey)!;
    }

    let result: CodecSupportResult;

    try {
      // Validate MIME type format
      if (!this.isValidMimeType(mimeType)) {
        result = {
          supported: false,
          reason: 'Invalid MIME type format',
        };
      } else {
        result = this.performCodecCheck(mimeType, options);
      }
    } catch (error) {
      result = {
        supported: false,
        reason: `Codec detection error: ${(error as Error).message}`,
      };
    }

    // Cache result
    this.codecCache.set(cacheKey, result);
    return result;
  }

  /**
   * Generate MIME type strings for given codecs
   */
  getMimeType(videoCodec?: string, audioCodec?: string): MimeTypeInfo {
    let container: 'mp4' | 'webm' | 'ogg';
    let videoMime: string | undefined;
    let audioMime: string | undefined;

    // Determine container based on codecs
    if (this.isWebMCodec(videoCodec) || this.isWebMCodec(audioCodec)) {
      container = 'webm';
      videoMime = videoCodec ? `video/webm; codecs="${this.getVideoCodecString(videoCodec)}"` : undefined;
      audioMime = audioCodec ? `audio/webm; codecs="${this.getAudioCodecString(audioCodec)}"` : undefined;
    } else {
      container = 'mp4';
      videoMime = videoCodec ? `video/mp4; codecs="${this.getVideoCodecString(videoCodec)}"` : undefined;
      audioMime = audioCodec ? `audio/mp4; codecs="${this.getAudioCodecString(audioCodec)}"` : undefined;
    }

    // Generate combined MIME type
    const codecs = [
      videoCodec && this.getVideoCodecString(videoCodec),
      audioCodec && this.getAudioCodecString(audioCodec),
    ].filter(Boolean).join(',');

    let full: string;
    if (videoCodec && audioCodec) {
      full = `video/${container}; codecs="${codecs}"`;
    } else if (videoCodec) {
      full = `video/${container}; codecs="${codecs}"`;
    } else {
      full = `audio/${container}; codecs="${codecs}"`;
    }

    return {
      container,
      videoCodec: videoCodec as VideoCodecString,
      audioCodec: audioCodec as AudioCodecString,
      video: videoMime,
      audio: audioMime,
      full,
    };
  }

  /**
   * Get recommended codecs for current browser
   */
  getRecommendedCodecs(): CodecRecommendation {
    const browserInfo = this.detectBrowser();

    switch (browserInfo.name) {
      case 'chrome':
      case 'edge':
        return {
          video: ['h264', 'vp9', 'av1'],
          audio: ['aac', 'opus'],
          container: 'mp4',
        };

      case 'firefox':
        return {
          video: ['vp9', 'h264', 'av1'],
          audio: ['opus', 'aac'],
          container: 'webm',
        };

      case 'safari':
        return {
          video: ['h264', 'hevc'],
          audio: ['aac'],
          container: 'mp4',
        };

      default:
        return {
          video: ['h264', 'vp9'],
          audio: ['aac', 'opus'],
          container: 'mp4',
        };
    }
  }

  /**
   * Get fallback codecs for a given codec
   */
  getCodecFallbacks(codec: string): string[] {
    const fallbacks: Record<string, string[]> = {
      'hevc': ['h264', 'vp9'],
      'h265': ['h264', 'vp9'],
      'av1': ['vp9', 'h264'],
      'vp9': ['h264'],
      'vp8': ['h264'],
      'ac-3': ['aac'],
      'ec-3': ['aac'],
      'opus': ['aac'],
      'vorbis': ['aac'],
    };

    return fallbacks[codec.toLowerCase()] || ['h264', 'aac'];
  }

  /**
   * Check MediaSource Extensions support
   */
  supportsMediaSourceExtensions(): boolean {
    return typeof MediaSource !== 'undefined' && !!MediaSource.isTypeSupported;
  }

  /**
   * Check Encrypted Media Extensions support
   */
  supportsEncryptedMedia(): boolean {
    return typeof navigator !== 'undefined' && 
           'requestMediaKeySystemAccess' in navigator;
  }

  /**
   * Get comprehensive feature support information
   */
  getSupportedFeatures(): SupportedFeatures {
    if (this.featuresCache) {
      return this.featuresCache;
    }

    this.featuresCache = {
      mse: this.supportsMediaSourceExtensions(),
      eme: this.supportsEncryptedMedia(),
      webrtc: this.supportsWebRTC(),
      webassembly: this.supportsWebAssembly(),
    };

    return this.featuresCache;
  }

  /**
   * Parse codec information from MIME type string
   */
  parseCodecString(mimeType: string): CodecDetails {
    const match = mimeType.match(/^(\w+)\/(\w+)(?:;\s*codecs="([^"]+)")?/);
    if (!match) {
      throw new Error('Invalid MIME type format');
    }

    const [, , container, codecsString] = match;
    const codecs = codecsString ? codecsString.split(',').map(c => c.trim()) : [];

    const videoCodec = codecs.find(c => this.isVideoCodec(c)) as VideoCodecString;
    const audioCodec = codecs.find(c => this.isAudioCodec(c)) as AudioCodecString;

    return { container, videoCodec, audioCodec };
  }

  /**
   * Get codec family name from codec string
   */
  getCodecFamily(codecString: string): string {
    if (codecString.startsWith('avc1')) return 'h264';
    if (codecString.startsWith('hev1') || codecString.startsWith('hvc1')) return 'hevc';
    if (codecString.startsWith('vp09')) return 'vp9';
    if (codecString.startsWith('vp08')) return 'vp8';
    if (codecString.startsWith('av01')) return 'av1';
    if (codecString.startsWith('mp4a')) return 'aac';
    if (codecString === 'opus') return 'opus';
    if (codecString === 'vorbis') return 'vorbis';
    if (codecString === 'ac-3') return 'ac3';
    if (codecString === 'ec-3') return 'eac3';
    
    return codecString;
  }

  /**
   * Get quality assessment for codec
   */
  getCodecQuality(codecString: string): CodecQuality {
    const family = this.getCodecFamily(codecString);

    const qualities: Record<string, CodecQuality> = {
      'av1': { compression: 'excellent', compatibility: 'fair', performance: 'good' },
      'hevc': { compression: 'excellent', compatibility: 'fair', performance: 'good' },
      'vp9': { compression: 'excellent', compatibility: 'good', performance: 'good' },
      'h264': { compression: 'good', compatibility: 'excellent', performance: 'excellent' },
      'vp8': { compression: 'good', compatibility: 'good', performance: 'good' },
      'aac': { compression: 'excellent', compatibility: 'excellent', performance: 'excellent' },
      'opus': { compression: 'excellent', compatibility: 'good', performance: 'excellent' },
      'ac3': { compression: 'fair', compatibility: 'fair', performance: 'good' },
    };

    return qualities[family] || { 
      compression: 'fair', 
      compatibility: 'fair', 
      performance: 'fair' 
    };
  }

  /**
   * Clear codec support cache
   */
  clearCache(): void {
    this.codecCache.clear();
    this.featuresCache = null;
  }

  /**
   * Private helper methods
   */

  private performCodecCheck(mimeType: string, options: CodecDetectorOptions): CodecSupportResult {
    // First try the requested MIME type
    if (typeof MediaSource !== 'undefined' && MediaSource.isTypeSupported && MediaSource.isTypeSupported(mimeType)) {
      return {
        supported: true,
        mimeType,
      };
    }

    // Check browser-specific fallbacks
    const browserFallback = this.getBrowserSpecificFallback(mimeType);
    if (browserFallback && typeof MediaSource !== 'undefined' && MediaSource.isTypeSupported && MediaSource.isTypeSupported(browserFallback)) {
      return {
        supported: true,
        mimeType: browserFallback,
        fallback: browserFallback,
      };
    }

    // Try provided fallbacks
    if (options.fallbacks) {
      for (const fallback of options.fallbacks) {
        if (typeof MediaSource !== 'undefined' && MediaSource.isTypeSupported && MediaSource.isTypeSupported(fallback)) {
          return {
            supported: true,
            mimeType: fallback,
            fallback,
          };
        }
      }
    }

    // Not supported
    return {
      supported: false,
      reason: `${mimeType} not supported in ${this.browserInfo?.name}`,
      fallback: browserFallback,
    };
  }

  private getBrowserSpecificFallback(mimeType: string): string | undefined {
    // Always get fresh browser info for tests
    const browserInfo = this.detectBrowser();

    // Chrome-specific fallbacks
    if (browserInfo.name === 'chrome') {
      if (mimeType.includes('ac-3')) {
        return 'video/mp4; codecs="avc1.42C01E"';
      }
      if (mimeType.includes('hevc') || mimeType.includes('hev1')) {
        return 'video/mp4; codecs="avc1.42C01E"';
      }
    }

    // Firefox-specific fallbacks
    if (browserInfo.name === 'firefox') {
      if (mimeType.includes('hevc') || mimeType.includes('hev1')) {
        return 'video/webm; codecs="vp09.00.10.08"';
      }
      if (mimeType.includes('avc1') && mimeType.includes('video/mp4')) {
        return 'video/webm; codecs="vp09.00.10.08"';
      }
    }

    // Safari-specific fallbacks
    if (browserInfo.name === 'safari') {
      if (mimeType.includes('vp9') || mimeType.includes('webm')) {
        return 'video/mp4; codecs="avc1.42C01E"';
      }
    }

    return undefined;
  }

  private isValidMimeType(mimeType: string): boolean {
    return /^(audio|video)\/\w+/.test(mimeType);
  }

  private getVideoMimeType(codec: string): string {
    return `video/${this.getContainer(codec)}; codecs="${this.getVideoCodecString(codec)}"`;
  }

  private getAudioMimeType(codec: string): string {
    return `audio/${this.getContainer(codec)}; codecs="${this.getAudioCodecString(codec)}"`;
  }

  private getContainer(codec: string): string {
    const webmCodecs = ['vp8', 'vp9', 'av1', 'opus', 'vorbis'];
    return webmCodecs.includes(codec.toLowerCase()) ? 'webm' : 'mp4';
  }

  private getVideoCodecString(codec: string): string {
    const codecMap: Record<string, string> = {
      'h264': 'avc1.42C01E',
      'avc': 'avc1.42C01E',
      'hevc': 'hev1.1.6.L93.90',
      'h265': 'hev1.1.6.L93.90',
      'vp9': 'vp09.00.10.08',
      'vp8': 'vp08',
      'av1': 'av01.0.08M.08',
    };

    return codecMap[codec.toLowerCase()] || codec;
  }

  private getAudioCodecString(codec: string): string {
    const codecMap: Record<string, string> = {
      'aac': 'mp4a.40.2',
      'mp3': 'mp3',
      'opus': 'opus',
      'vorbis': 'vorbis',
      'ac-3': 'ac-3',
      'ec-3': 'ec-3',
    };

    return codecMap[codec.toLowerCase()] || codec;
  }

  private isWebMCodec(codec?: string): boolean {
    if (!codec) return false;
    const webmCodecs = ['vp8', 'vp9', 'av1', 'opus', 'vorbis'];
    return webmCodecs.includes(codec.toLowerCase());
  }

  private getWebMVideoCodec(codec: string): string {
    return this.getVideoCodecString(codec);
  }

  private getWebMAudioCodec(codec: string): string {
    return this.getAudioCodecString(codec);
  }

  private getMP4VideoCodec(codec: string): string {
    return this.getVideoCodecString(codec);
  }

  private getMP4AudioCodec(codec: string): string {
    return this.getAudioCodecString(codec);
  }

  private isVideoCodec(codec: string): boolean {
    const videoCodecs = ['avc1', 'hev1', 'hvc1', 'vp08', 'vp09', 'av01'];
    return videoCodecs.some(v => codec.startsWith(v));
  }

  private isAudioCodec(codec: string): boolean {
    const audioCodecs = ['mp4a', 'opus', 'vorbis', 'ac-3', 'ec-3', 'mp3'];
    return audioCodecs.some(a => codec.startsWith(a)) || audioCodecs.includes(codec);
  }

  private supportsWebRTC(): boolean {
    return typeof RTCPeerConnection !== 'undefined' ||
           typeof (window as any).webkitRTCPeerConnection !== 'undefined' ||
           typeof (window as any).mozRTCPeerConnection !== 'undefined';
  }

  private supportsWebAssembly(): boolean {
    return typeof WebAssembly !== 'undefined';
  }
}