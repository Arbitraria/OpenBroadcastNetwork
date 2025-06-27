/**
 * Example usage of OpenBroadcastNetwork TypeScript components
 */

import { VideoPlayer, Controls, StatusDisplay } from './components';
import { MediaManager } from './core/MediaManager';
import { WebSocketClient } from './core/WebSocketClient';
import { StreamManager } from './core/StreamManager';

// Example: Complete video player setup
export function createVideoPlayer(containerId: string, websocketUrl: string) {
  const container = document.getElementById(containerId);
  if (!container) {
    throw new Error(`Container with id '${containerId}' not found`);
  }

  // Create video player
  const videoPlayer = new VideoPlayer(container, {
    autoplay: false,
    muted: false,
    controls: false, // We'll use custom controls
    width: 1280,
    height: 720,
  });

  // Create custom controls
  const controls = new Controls(videoPlayer, container, {
    showPlayButton: true,
    showSeekBar: true,
    showVolumeControl: true,
    showFullscreenButton: true,
    autoHide: true,
  });

  // Create status display
  const statusDisplay = new StatusDisplay(videoPlayer, container, {
    showConnectionStatus: true,
    showBufferLevel: true,
    showBandwidth: true,
    showErrors: true,
    position: 'top-right',
  });

  // Setup event listeners
  videoPlayer.addEventListener('error', (event) => {
    console.error('Video player error:', event.detail);
  });

  videoPlayer.addEventListener('play', () => {
    console.log('Video started playing');
  });

  videoPlayer.addEventListener('pause', () => {
    console.log('Video paused');
  });

  controls.addEventListener('controls_shown', () => {
    console.log('Controls shown');
  });

  controls.addEventListener('controls_hidden', () => {
    console.log('Controls hidden');
  });

  // Connect to stream
  videoPlayer.connectToStream(websocketUrl)
    .then(() => {
      console.log('Connected to stream successfully');
    })
    .catch((error) => {
      console.error('Failed to connect to stream:', error);
    });

  return {
    videoPlayer,
    controls,
    statusDisplay,
    cleanup: () => {
      videoPlayer.cleanup();
      controls.cleanup();
      statusDisplay.cleanup();
    },
  };
}

// Example: Simple MediaManager usage
export function createSimplePlayer() {
  const mediaManager = new MediaManager();
  const videoElement = document.createElement('video');
  
  return mediaManager.initialize()
    .then(url => {
      videoElement.src = url;
      return {
        videoElement,
        mediaManager,
        cleanup: () => mediaManager.cleanup(),
      };
    });
}

// Example: WebSocket client usage
export function createWebSocketConnection(url: string) {
  const client = new WebSocketClient(url, {
    autoReconnect: true,
    maxReconnectAttempts: 5,
    reconnectInterval: 1000,
  });

  client.addEventListener('connected', () => {
    console.log('WebSocket connected');
  });

  client.addEventListener('disconnected', () => {
    console.log('WebSocket disconnected');
  });

  client.addEventListener('stream_info', (event) => {
    console.log('Stream info received:', event.detail);
  });

  client.addEventListener('init_segment', (event) => {
    console.log('Init segment received:', event.detail);
  });

  client.addEventListener('binary_chunk', (event) => {
    console.log('Binary chunk received:', event.detail);
  });

  return {
    client,
    connect: () => client.connect(),
    disconnect: () => client.disconnect(),
    cleanup: () => client.disconnect(),
  };
}

// Example: Full application setup
export class StreamingApp {
  private videoPlayer: VideoPlayer | null = null;
  private controls: Controls | null = null;
  private statusDisplay: StatusDisplay | null = null;
  private streamManager: StreamManager | null = null;

  constructor(private containerId: string, private websocketUrl: string) {}

  async initialize(): Promise<void> {
    const container = document.getElementById(this.containerId);
    if (!container) {
      throw new Error(`Container with id '${this.containerId}' not found`);
    }

    // Create video player
    this.videoPlayer = new VideoPlayer(container, {
      autoplay: false,
      muted: false,
      controls: false,
    });

    // Create controls
    this.controls = new Controls(this.videoPlayer, container, {
      autoHide: true,
      autoHideDelay: 3000,
    });

    // Create status display
    this.statusDisplay = new StatusDisplay(this.videoPlayer, container, {
      position: 'top-right',
      autoHide: false,
    });

    // Setup application event listeners
    this.setupEventListeners();

    // Connect to stream
    await this.videoPlayer.connectToStream(this.websocketUrl);
  }

  private setupEventListeners(): void {
    if (!this.videoPlayer || !this.controls || !this.statusDisplay) return;

    // Video player events
    this.videoPlayer.addEventListener('play', () => {
      this.onPlaybackStateChange('playing');
    });

    this.videoPlayer.addEventListener('pause', () => {
      this.onPlaybackStateChange('paused');
    });

    this.videoPlayer.addEventListener('error', (event) => {
      this.onError(event.detail);
    });

    this.videoPlayer.addEventListener('stream_state_change', (event) => {
      this.statusDisplay?.updateFromApplicationState(event.detail);
    });

    // Keyboard shortcuts
    document.addEventListener('keydown', (event) => {
      if (!this.videoPlayer) return;

      switch (event.code) {
        case 'Space':
          event.preventDefault();
          if (this.videoPlayer.isPaused()) {
            this.videoPlayer.play();
          } else {
            this.videoPlayer.pause();
          }
          break;
        case 'KeyF':
          event.preventDefault();
          if (this.videoPlayer.isFullscreen()) {
            this.videoPlayer.exitFullscreen();
          } else {
            this.videoPlayer.enterFullscreen();
          }
          break;
        case 'KeyM':
          event.preventDefault();
          this.videoPlayer.setMuted(!this.videoPlayer.isMuted());
          break;
        case 'ArrowLeft':
          event.preventDefault();
          const currentTime = this.videoPlayer.getCurrentTime();
          this.videoPlayer.seek(Math.max(0, currentTime - 10));
          break;
        case 'ArrowRight':
          event.preventDefault();
          const time = this.videoPlayer.getCurrentTime();
          this.videoPlayer.seek(time + 10);
          break;
      }
    });
  }

  private onPlaybackStateChange(state: 'playing' | 'paused'): void {
    console.log(`Playback state changed: ${state}`);
    
    // Update controls based on state
    if (this.controls) {
      const playbackState = this.videoPlayer!.getPlaybackState();
      this.controls.updateFromPlaybackState(playbackState);
    }
  }

  private onError(error: Error): void {
    console.error('Streaming error:', error);
    
    // Show error to user
    if (this.statusDisplay) {
      this.statusDisplay.addError({
        message: error.message,
        component: 'app',
        severity: 'error',
      });
    }
  }

  play(): Promise<void> {
    if (!this.videoPlayer) {
      throw new Error('Video player not initialized');
    }
    return this.videoPlayer.play();
  }

  pause(): void {
    if (!this.videoPlayer) {
      throw new Error('Video player not initialized');
    }
    this.videoPlayer.pause();
  }

  seek(time: number): void {
    if (!this.videoPlayer) {
      throw new Error('Video player not initialized');
    }
    this.videoPlayer.seek(time);
  }

  setVolume(volume: number): void {
    if (!this.videoPlayer) {
      throw new Error('Video player not initialized');
    }
    this.videoPlayer.setVolume(volume);
  }

  getState() {
    if (!this.videoPlayer) {
      throw new Error('Video player not initialized');
    }
    return {
      playback: this.videoPlayer.getPlaybackState(),
      buffered: this.videoPlayer.getBufferedRanges(),
      volume: this.videoPlayer.getVolume(),
      muted: this.videoPlayer.isMuted(),
      fullscreen: this.videoPlayer.isFullscreen(),
    };
  }

  cleanup(): void {
    if (this.videoPlayer) {
      this.videoPlayer.cleanup();
      this.videoPlayer = null;
    }
    if (this.controls) {
      this.controls.cleanup();
      this.controls = null;
    }
    if (this.statusDisplay) {
      this.statusDisplay.cleanup();
      this.statusDisplay = null;
    }
  }
}

// Usage examples for documentation
export const examples = {
  // Basic usage
  basic: () => {
    const player = createVideoPlayer('video-container', 'ws://localhost:8080');
    return player;
  },

  // Advanced usage
  advanced: async () => {
    const app = new StreamingApp('video-container', 'ws://localhost:8080');
    await app.initialize();
    
    // Example API calls
    await app.play();
    app.setVolume(0.8);
    
    return app;
  },

  // Component-by-component usage
  components: () => {
    const container = document.getElementById('video-container')!;
    const videoPlayer = new VideoPlayer(container);
    const controls = new Controls(videoPlayer, container);
    const statusDisplay = new StatusDisplay(videoPlayer, container);
    
    return { videoPlayer, controls, statusDisplay };
  },
};