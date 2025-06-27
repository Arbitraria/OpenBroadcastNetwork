/**
 * Tests for CodecDetector - Browser codec compatibility detection
 */

import { CodecDetector } from '../../utils/CodecDetector';
import { BrowserInfo, CodecSupportResult, MimeTypeInfo } from '../../types';

// Mock MediaSource for testing
const mockMediaSource = {
  isTypeSupported: jest.fn(),
};

(global as any).MediaSource = mockMediaSource;

describe('CodecDetector', () => {
  let detector: CodecDetector;

  beforeEach(() => {
    detector = new CodecDetector();
    jest.clearAllMocks();
  });

  describe('browser detection', () => {
    test('should detect Chrome browser', () => {
      Object.defineProperty(navigator, 'userAgent', {
        value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        configurable: true,
      });

      const browser = detector.detectBrowser();
      
      expect(browser.name).toBe('chrome');
      expect(browser.version).toBe(91);
      expect(browser.userAgent).toContain('Chrome');
    });

    test('should detect Firefox browser', () => {
      Object.defineProperty(navigator, 'userAgent', {
        value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        configurable: true,
      });

      const browser = detector.detectBrowser();
      
      expect(browser.name).toBe('firefox');
      expect(browser.version).toBe(89);
    });

    test('should detect Safari browser', () => {
      Object.defineProperty(navigator, 'userAgent', {
        value: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
        configurable: true,
      });

      const browser = detector.detectBrowser();
      
      expect(browser.name).toBe('safari');
      expect(browser.version).toBe(14);
    });

    test('should detect Edge browser', () => {
      Object.defineProperty(navigator, 'userAgent', {
        value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
        configurable: true,
      });

      const browser = detector.detectBrowser();
      
      expect(browser.name).toBe('edge');
      expect(browser.version).toBe(91);
    });

    test('should detect unknown browser', () => {
      Object.defineProperty(navigator, 'userAgent', {
        value: 'Some Unknown Browser/1.0',
        configurable: true,
      });

      const browser = detector.detectBrowser();
      
      expect(browser.name).toBe('unknown');
      expect(browser.version).toBe(0);
    });
  });

  describe('codec support detection', () => {
    test('should detect H.264 support', () => {
      mockMediaSource.isTypeSupported.mockReturnValue(true);

      const result = detector.checkCodecSupport('video/mp4; codecs="avc1.42C01E"');
      
      expect(result.supported).toBe(true);
      expect(result.mimeType).toBe('video/mp4; codecs="avc1.42C01E"');
      expect(mockMediaSource.isTypeSupported).toHaveBeenCalledWith('video/mp4; codecs="avc1.42C01E"');
    });

    test('should detect unsupported codec', () => {
      mockMediaSource.isTypeSupported.mockReturnValue(false);

      const result = detector.checkCodecSupport('video/mp4; codecs="hev1.1.6.L93.90"');
      
      expect(result.supported).toBe(false);
      expect(result.reason).toContain('not supported');
    });

    test('should suggest H.264 fallback for AC-3 in Chrome', () => {
      Object.defineProperty(navigator, 'userAgent', {
        value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124 Safari/537.36',
        configurable: true,
      });

      mockMediaSource.isTypeSupported.mockReturnValue(false);

      const result = detector.checkCodecSupport('video/mp4; codecs="ac-3"');
      
      expect(result.supported).toBe(false);
      expect(result.fallback).toBe('video/mp4; codecs="avc1.42C01E"');
      expect(result.reason).toContain('Chrome');
    });

    test('should suggest VP9 fallback for HEVC in Firefox', () => {
      Object.defineProperty(navigator, 'userAgent', {
        value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        configurable: true,
      });

      mockMediaSource.isTypeSupported.mockReturnValue(false);

      const result = detector.checkCodecSupport('video/mp4; codecs="hev1.1.6.L93.90"');
      
      expect(result.supported).toBe(false);
      expect(result.fallback).toBe('video/webm; codecs="vp09.00.10.08"');
      expect(result.reason).toContain('Firefox');
    });

    test('should check multiple codec formats', () => {
      mockMediaSource.isTypeSupported
        .mockReturnValueOnce(false) // First format fails
        .mockReturnValueOnce(true);  // Second format succeeds

      const result = detector.checkCodecSupport('video/mp4; codecs="hev1.1.6.L93.90"', {
        fallbacks: ['video/mp4; codecs="avc1.42C01E"'],
      });
      
      expect(result.supported).toBe(true);
      expect(result.mimeType).toBe('video/mp4; codecs="avc1.42C01E"');
    });
  });

  describe('MIME type generation', () => {
    test('should generate H.264 MIME type', () => {
      const mimeType = detector.getMimeType('h264', 'aac');
      
      expect(mimeType.video).toBe('video/mp4; codecs="avc1.42C01E"');
      expect(mimeType.audio).toBe('audio/mp4; codecs="mp4a.40.2"');
      expect(mimeType.full).toBe('video/mp4; codecs="avc1.42C01E,mp4a.40.2"');
    });

    test('should generate VP9/Opus MIME type', () => {
      const mimeType = detector.getMimeType('vp9', 'opus');
      
      expect(mimeType.video).toBe('video/webm; codecs="vp09.00.10.08"');
      expect(mimeType.audio).toBe('audio/webm; codecs="opus"');
      expect(mimeType.full).toBe('video/webm; codecs="vp09.00.10.08,opus"');
    });

    test('should handle video-only MIME type', () => {
      const mimeType = detector.getMimeType('h264');
      
      expect(mimeType.video).toBe('video/mp4; codecs="avc1.42C01E"');
      expect(mimeType.audio).toBeUndefined();
      expect(mimeType.full).toBe('video/mp4; codecs="avc1.42C01E"');
    });

    test('should handle audio-only MIME type', () => {
      const mimeType = detector.getMimeType(undefined, 'aac');
      
      expect(mimeType.video).toBeUndefined();
      expect(mimeType.audio).toBe('audio/mp4; codecs="mp4a.40.2"');
      expect(mimeType.full).toBe('audio/mp4; codecs="mp4a.40.2"');
    });

    test('should handle unknown codecs gracefully', () => {
      const mimeType = detector.getMimeType('unknown-codec', 'unknown-audio');
      
      expect(mimeType.video).toBe('video/mp4; codecs="unknown-codec"');
      expect(mimeType.audio).toBe('audio/mp4; codecs="unknown-audio"');
    });
  });

  describe('compatibility recommendations', () => {
    test('should recommend best codec for Chrome', () => {
      Object.defineProperty(navigator, 'userAgent', {
        value: 'Mozilla/5.0 Chrome/91.0.4472.124 Safari/537.36',
        configurable: true,
      });

      const recommendation = detector.getRecommendedCodecs();
      
      expect(recommendation.video).toContain('h264');
      expect(recommendation.audio).toContain('aac');
      expect(recommendation.container).toBe('mp4');
    });

    test('should recommend best codec for Firefox', () => {
      Object.defineProperty(navigator, 'userAgent', {
        value: 'Mozilla/5.0 Firefox/89.0',
        configurable: true,
      });

      const recommendation = detector.getRecommendedCodecs();
      
      expect(recommendation.video).toContain('vp9');
      expect(recommendation.audio).toContain('opus');
      expect(recommendation.container).toBe('webm');
    });

    test('should provide fallback recommendations', () => {
      const recommendations = detector.getCodecFallbacks('h265');
      
      expect(recommendations).toContain('h264');
      expect(recommendations).toContain('vp9');
    });
  });

  describe('feature detection', () => {
    test('should detect MediaSource Extensions support', () => {
      (global as any).MediaSource = mockMediaSource;
      
      const supported = detector.supportsMediaSourceExtensions();
      
      expect(supported).toBe(true);
    });

    test('should detect lack of MSE support', () => {
      (global as any).MediaSource = undefined;
      
      const supported = detector.supportsMediaSourceExtensions();
      
      expect(supported).toBe(false);
    });

    test('should detect encrypted media support', () => {
      (global as any).navigator.requestMediaKeySystemAccess = jest.fn();
      
      const supported = detector.supportsEncryptedMedia();
      
      expect(supported).toBe(true);
    });

    test('should check for specific features', () => {
      const features = detector.getSupportedFeatures();
      
      expect(features).toHaveProperty('mse');
      expect(features).toHaveProperty('eme');
      expect(features).toHaveProperty('webrtc');
      expect(features).toHaveProperty('webassembly');
    });
  });

  describe('performance optimization', () => {
    test('should cache codec support results', () => {
      mockMediaSource.isTypeSupported.mockReturnValue(true);

      // First call
      detector.checkCodecSupport('video/mp4; codecs="avc1.42C01E"');
      // Second call should use cache
      detector.checkCodecSupport('video/mp4; codecs="avc1.42C01E"');
      
      expect(mockMediaSource.isTypeSupported).toHaveBeenCalledTimes(1);
    });

    test('should clear cache when requested', () => {
      mockMediaSource.isTypeSupported.mockReturnValue(true);

      detector.checkCodecSupport('video/mp4; codecs="avc1.42C01E"');
      detector.clearCache();
      detector.checkCodecSupport('video/mp4; codecs="avc1.42C01E"');
      
      expect(mockMediaSource.isTypeSupported).toHaveBeenCalledTimes(2);
    });
  });

  describe('error handling', () => {
    test('should handle MediaSource.isTypeSupported throwing error', () => {
      mockMediaSource.isTypeSupported.mockImplementation(() => {
        throw new Error('API not available');
      });

      const result = detector.checkCodecSupport('video/mp4; codecs="avc1.42C01E"');
      
      expect(result.supported).toBe(false);
      expect(result.reason).toContain('Codec detection error');
    });

    test('should handle invalid MIME types gracefully', () => {
      const result = detector.checkCodecSupport('invalid-mime-type');
      
      expect(result.supported).toBe(false);
      expect(result.reason).toContain('Invalid MIME type');
    });
  });

  describe('detailed codec information', () => {
    test('should extract codec details from MIME type', () => {
      const details = detector.parseCodecString('video/mp4; codecs="avc1.42C01E,mp4a.40.2"');
      
      expect(details.container).toBe('mp4');
      expect(details.videoCodec).toBe('avc1.42C01E');
      expect(details.audioCodec).toBe('mp4a.40.2');
    });

    test('should identify codec families', () => {
      expect(detector.getCodecFamily('avc1.42C01E')).toBe('h264');
      expect(detector.getCodecFamily('hev1.1.6.L93.90')).toBe('hevc');
      expect(detector.getCodecFamily('vp09.00.10.08')).toBe('vp9');
      expect(detector.getCodecFamily('mp4a.40.2')).toBe('aac');
    });

    test('should provide codec quality estimates', () => {
      const quality = detector.getCodecQuality('avc1.42C01E');
      
      expect(quality).toHaveProperty('compression');
      expect(quality).toHaveProperty('compatibility');
      expect(quality).toHaveProperty('performance');
    });
  });
});