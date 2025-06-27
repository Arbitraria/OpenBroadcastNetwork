/**
 * Controls - Video player control interface
 */

import { VideoPlayer } from './VideoPlayer';
import { PlaybackState, BufferedRange } from '../types';

export interface ControlsOptions {
  className?: string;
  showPlayButton?: boolean;
  showPauseButton?: boolean;
  showSeekBar?: boolean;
  showVolumeControl?: boolean;
  showFullscreenButton?: boolean;
  showTimeDisplay?: boolean;
  showBufferIndicator?: boolean;
  autoHide?: boolean;
  autoHideDelay?: number;
}

export interface ControlsTheme {
  backgroundColor?: string;
  buttonColor?: string;
  buttonHoverColor?: string;
  progressBarColor?: string;
  progressBufferColor?: string;
  progressHandleColor?: string;
  textColor?: string;
  fontSize?: string;
}

export class Controls extends EventTarget {
  private container: HTMLElement;
  private videoPlayer: VideoPlayer;
  private controlsElement: HTMLElement;
  private options: Required<ControlsOptions>;
  private theme: ControlsTheme;
  private isVisible = true;
  private autoHideTimer: NodeJS.Timeout | null = null;
  
  // Control elements
  private playButton: HTMLButtonElement | null = null;
  private pauseButton: HTMLButtonElement | null = null;
  private seekBar: HTMLInputElement | null = null;
  private seekBarContainer: HTMLElement | null = null;
  private volumeSlider: HTMLInputElement | null = null;
  private volumeButton: HTMLButtonElement | null = null;
  private fullscreenButton: HTMLButtonElement | null = null;
  private timeDisplay: HTMLElement | null = null;
  private bufferIndicator: HTMLElement | null = null;
  private progressBar: HTMLElement | null = null;
  private progressHandle: HTMLElement | null = null;

  // State
  private isDragging = false;
  private lastInteraction = Date.now();

  constructor(
    videoPlayer: VideoPlayer, 
    container: HTMLElement, 
    options: ControlsOptions = {},
    theme: ControlsTheme = {}
  ) {
    super();
    
    this.videoPlayer = videoPlayer;
    this.container = container;
    this.options = {
      className: options.className ?? 'video-controls',
      showPlayButton: options.showPlayButton ?? true,
      showPauseButton: options.showPauseButton ?? true,
      showSeekBar: options.showSeekBar ?? true,
      showVolumeControl: options.showVolumeControl ?? true,
      showFullscreenButton: options.showFullscreenButton ?? true,
      showTimeDisplay: options.showTimeDisplay ?? true,
      showBufferIndicator: options.showBufferIndicator ?? true,
      autoHide: options.autoHide ?? true,
      autoHideDelay: options.autoHideDelay ?? 3000,
    };
    
    this.theme = {
      backgroundColor: theme.backgroundColor ?? 'rgba(0, 0, 0, 0.7)',
      buttonColor: theme.buttonColor ?? '#ffffff',
      buttonHoverColor: theme.buttonHoverColor ?? '#cccccc',
      progressBarColor: theme.progressBarColor ?? '#0078d4',
      progressBufferColor: theme.progressBufferColor ?? 'rgba(255, 255, 255, 0.3)',
      progressHandleColor: theme.progressHandleColor ?? '#ffffff',
      textColor: theme.textColor ?? '#ffffff',
      fontSize: theme.fontSize ?? '14px',
    };

    this.createControls();
    this.setupEventListeners();
    this.setupAutoHide();
  }

  /**
   * Show controls
   */
  show(): void {
    this.isVisible = true;
    this.controlsElement.style.opacity = '1';
    this.controlsElement.style.pointerEvents = 'auto';
    this.lastInteraction = Date.now();
    this.dispatchEvent(new CustomEvent('controls_shown'));
  }

  /**
   * Hide controls
   */
  hide(): void {
    this.isVisible = false;
    this.controlsElement.style.opacity = '0';
    this.controlsElement.style.pointerEvents = 'none';
    this.dispatchEvent(new CustomEvent('controls_hidden'));
  }

  /**
   * Toggle controls visibility
   */
  toggle(): void {
    if (this.isVisible) {
      this.hide();
    } else {
      this.show();
    }
  }

  /**
   * Update controls based on player state
   */
  updateFromPlaybackState(state: PlaybackState): void {
    // Update play/pause buttons
    if (this.playButton && this.pauseButton) {
      if (state.playing) {
        this.playButton.style.display = 'none';
        this.pauseButton.style.display = 'inline-block';
      } else {
        this.playButton.style.display = 'inline-block';
        this.pauseButton.style.display = 'none';
      }
    }

    // Update seek bar
    if (this.seekBar && !this.isDragging) {
      const progress = state.duration > 0 ? (state.currentTime / state.duration) * 100 : 0;
      this.seekBar.value = progress.toString();
      this.updateProgressBar(progress);
    }

    // Update time display
    if (this.timeDisplay) {
      const current = this.formatTime(state.currentTime);
      const duration = this.formatTime(state.duration);
      this.timeDisplay.textContent = `${current} / ${duration}`;
    }

    // Update buffer indicator
    if (this.bufferIndicator) {
      this.updateBufferIndicator(state.buffered, state.duration);
    }
  }

  /**
   * Update volume controls
   */
  updateVolumeControls(volume: number, muted: boolean): void {
    if (this.volumeSlider) {
      this.volumeSlider.value = (volume * 100).toString();
    }

    if (this.volumeButton) {
      this.volumeButton.innerHTML = this.getVolumeIcon(muted ? 0 : volume);
      this.volumeButton.setAttribute('aria-label', muted ? 'Unmute' : 'Mute');
    }
  }

  /**
   * Enable/disable controls
   */
  setEnabled(enabled: boolean): void {
    const elements = [
      this.playButton,
      this.pauseButton,
      this.seekBar,
      this.volumeSlider,
      this.volumeButton,
      this.fullscreenButton,
    ].filter(Boolean);

    elements.forEach(element => {
      (element as HTMLElement).disabled = !enabled;
    });
  }

  /**
   * Apply custom theme
   */
  applyTheme(newTheme: Partial<ControlsTheme>): void {
    this.theme = { ...this.theme, ...newTheme };
    this.updateStyles();
  }

  /**
   * Cleanup
   */
  cleanup(): void {
    if (this.autoHideTimer) {
      clearTimeout(this.autoHideTimer);
      this.autoHideTimer = null;
    }

    this.removeEventListeners();
    
    if (this.controlsElement && this.controlsElement.parentNode) {
      this.controlsElement.parentNode.removeChild(this.controlsElement);
    }
  }

  /**
   * Private methods
   */
  private createControls(): void {
    this.controlsElement = document.createElement('div');
    this.controlsElement.className = this.options.className;
    
    // Create control groups
    const leftGroup = document.createElement('div');
    leftGroup.className = 'controls-left';
    
    const centerGroup = document.createElement('div');
    centerGroup.className = 'controls-center';
    
    const rightGroup = document.createElement('div');
    rightGroup.className = 'controls-right';

    // Create play/pause buttons
    if (this.options.showPlayButton || this.options.showPauseButton) {
      this.createPlayPauseButtons(leftGroup);
    }

    // Create volume controls
    if (this.options.showVolumeControl) {
      this.createVolumeControls(leftGroup);
    }

    // Create time display
    if (this.options.showTimeDisplay) {
      this.createTimeDisplay(leftGroup);
    }

    // Create seek bar
    if (this.options.showSeekBar) {
      this.createSeekBar(centerGroup);
    }

    // Create fullscreen button
    if (this.options.showFullscreenButton) {
      this.createFullscreenButton(rightGroup);
    }

    // Assemble controls
    this.controlsElement.appendChild(leftGroup);
    this.controlsElement.appendChild(centerGroup);
    this.controlsElement.appendChild(rightGroup);
    
    this.container.appendChild(this.controlsElement);
    this.updateStyles();
  }

  private createPlayPauseButtons(parent: HTMLElement): void {
    if (this.options.showPlayButton) {
      this.playButton = document.createElement('button');
      this.playButton.className = 'control-button play-button';
      this.playButton.innerHTML = this.getPlayIcon();
      this.playButton.setAttribute('aria-label', 'Play');
      this.playButton.addEventListener('click', () => this.videoPlayer.play());
      parent.appendChild(this.playButton);
    }

    if (this.options.showPauseButton) {
      this.pauseButton = document.createElement('button');
      this.pauseButton.className = 'control-button pause-button';
      this.pauseButton.innerHTML = this.getPauseIcon();
      this.pauseButton.setAttribute('aria-label', 'Pause');
      this.pauseButton.style.display = 'none';
      this.pauseButton.addEventListener('click', () => this.videoPlayer.pause());
      parent.appendChild(this.pauseButton);
    }
  }

  private createVolumeControls(parent: HTMLElement): void {
    const volumeContainer = document.createElement('div');
    volumeContainer.className = 'volume-controls';

    this.volumeButton = document.createElement('button');
    this.volumeButton.className = 'control-button volume-button';
    this.volumeButton.innerHTML = this.getVolumeIcon(1);
    this.volumeButton.setAttribute('aria-label', 'Mute');
    this.volumeButton.addEventListener('click', () => {
      this.videoPlayer.setMuted(!this.videoPlayer.isMuted());
    });

    this.volumeSlider = document.createElement('input');
    this.volumeSlider.type = 'range';
    this.volumeSlider.className = 'volume-slider';
    this.volumeSlider.min = '0';
    this.volumeSlider.max = '100';
    this.volumeSlider.value = '100';
    this.volumeSlider.addEventListener('input', (e) => {
      const volume = parseInt((e.target as HTMLInputElement).value) / 100;
      this.videoPlayer.setVolume(volume);
    });

    volumeContainer.appendChild(this.volumeButton);
    volumeContainer.appendChild(this.volumeSlider);
    parent.appendChild(volumeContainer);
  }

  private createTimeDisplay(parent: HTMLElement): void {
    this.timeDisplay = document.createElement('div');
    this.timeDisplay.className = 'time-display';
    this.timeDisplay.textContent = '00:00 / 00:00';
    parent.appendChild(this.timeDisplay);
  }

  private createSeekBar(parent: HTMLElement): void {
    this.seekBarContainer = document.createElement('div');
    this.seekBarContainer.className = 'seek-bar-container';

    this.seekBar = document.createElement('input');
    this.seekBar.type = 'range';
    this.seekBar.className = 'seek-bar';
    this.seekBar.min = '0';
    this.seekBar.max = '100';
    this.seekBar.value = '0';

    if (this.options.showBufferIndicator) {
      this.bufferIndicator = document.createElement('div');
      this.bufferIndicator.className = 'buffer-indicator';
      this.seekBarContainer.appendChild(this.bufferIndicator);
    }

    this.progressBar = document.createElement('div');
    this.progressBar.className = 'progress-bar';
    
    this.progressHandle = document.createElement('div');
    this.progressHandle.className = 'progress-handle';
    this.progressBar.appendChild(this.progressHandle);

    this.seekBarContainer.appendChild(this.progressBar);
    this.seekBarContainer.appendChild(this.seekBar);

    // Seek bar event listeners
    this.seekBar.addEventListener('mousedown', () => {
      this.isDragging = true;
    });

    this.seekBar.addEventListener('mouseup', () => {
      this.isDragging = false;
    });

    this.seekBar.addEventListener('input', (e) => {
      const progress = parseInt((e.target as HTMLInputElement).value);
      const duration = this.videoPlayer.getDuration();
      const time = (progress / 100) * duration;
      
      if (this.isDragging) {
        this.updateProgressBar(progress);
      }
    });

    this.seekBar.addEventListener('change', (e) => {
      const progress = parseInt((e.target as HTMLInputElement).value);
      const duration = this.videoPlayer.getDuration();
      const time = (progress / 100) * duration;
      this.videoPlayer.seek(time);
      this.isDragging = false;
    });

    parent.appendChild(this.seekBarContainer);
  }

  private createFullscreenButton(parent: HTMLElement): void {
    this.fullscreenButton = document.createElement('button');
    this.fullscreenButton.className = 'control-button fullscreen-button';
    this.fullscreenButton.innerHTML = this.getFullscreenIcon();
    this.fullscreenButton.setAttribute('aria-label', 'Fullscreen');
    this.fullscreenButton.addEventListener('click', () => {
      if (this.videoPlayer.isFullscreen()) {
        this.videoPlayer.exitFullscreen();
      } else {
        this.videoPlayer.enterFullscreen();
      }
    });
    parent.appendChild(this.fullscreenButton);
  }

  private updateProgressBar(progress: number): void {
    if (this.progressBar) {
      this.progressBar.style.width = `${progress}%`;
    }
  }

  private updateBufferIndicator(bufferedRanges: BufferedRange[], duration: number): void {
    if (!this.bufferIndicator || duration === 0) return;

    this.bufferIndicator.innerHTML = '';
    
    bufferedRanges.forEach(range => {
      const startPercent = (range.start / duration) * 100;
      const endPercent = (range.end / duration) * 100;
      
      const bufferSegment = document.createElement('div');
      bufferSegment.className = 'buffer-segment';
      bufferSegment.style.left = `${startPercent}%`;
      bufferSegment.style.width = `${endPercent - startPercent}%`;
      
      this.bufferIndicator.appendChild(bufferSegment);
    });
  }

  private setupEventListeners(): void {
    // Video player events
    this.videoPlayer.addEventListener('play', this.handlePlaybackChange.bind(this));
    this.videoPlayer.addEventListener('pause', this.handlePlaybackChange.bind(this));
    this.videoPlayer.addEventListener('timeupdate', this.handleTimeUpdate.bind(this));
    this.videoPlayer.addEventListener('volumechange', this.handleVolumeChange.bind(this));
    this.videoPlayer.addEventListener('fullscreenchange', this.handleFullscreenChange.bind(this));
    this.videoPlayer.addEventListener('buffering', this.handleBuffering.bind(this));

    // Container events for auto-hide
    if (this.options.autoHide) {
      this.container.addEventListener('mousemove', this.handleMouseMove.bind(this));
      this.container.addEventListener('mouseleave', this.handleMouseLeave.bind(this));
      this.container.addEventListener('click', this.handleClick.bind(this));
    }
  }

  private removeEventListeners(): void {
    this.videoPlayer.removeEventListener('play', this.handlePlaybackChange);
    this.videoPlayer.removeEventListener('pause', this.handlePlaybackChange);
    this.videoPlayer.removeEventListener('timeupdate', this.handleTimeUpdate);
    this.videoPlayer.removeEventListener('volumechange', this.handleVolumeChange);
    this.videoPlayer.removeEventListener('fullscreenchange', this.handleFullscreenChange);
    this.videoPlayer.removeEventListener('buffering', this.handleBuffering);

    if (this.options.autoHide) {
      this.container.removeEventListener('mousemove', this.handleMouseMove);
      this.container.removeEventListener('mouseleave', this.handleMouseLeave);
      this.container.removeEventListener('click', this.handleClick);
    }
  }

  private setupAutoHide(): void {
    if (this.options.autoHide) {
      this.resetAutoHideTimer();
    }
  }

  private resetAutoHideTimer(): void {
    if (this.autoHideTimer) {
      clearTimeout(this.autoHideTimer);
    }
    
    this.show();
    
    this.autoHideTimer = setTimeout(() => {
      if (!this.isDragging && this.videoPlayer.getPlaybackState().playing) {
        this.hide();
      }
    }, this.options.autoHideDelay);
  }

  private updateStyles(): void {
    const styles = `
      .${this.options.className} {
        position: absolute;
        bottom: 0;
        left: 0;
        right: 0;
        display: flex;
        align-items: center;
        padding: 10px;
        background: ${this.theme.backgroundColor};
        color: ${this.theme.textColor};
        font-size: ${this.theme.fontSize};
        transition: opacity 0.3s ease;
        z-index: 1000;
      }
      
      .controls-left, .controls-right {
        display: flex;
        align-items: center;
        gap: 10px;
      }
      
      .controls-center {
        flex: 1;
        display: flex;
        align-items: center;
        margin: 0 15px;
      }
      
      .control-button {
        background: none;
        border: none;
        color: ${this.theme.buttonColor};
        cursor: pointer;
        padding: 5px;
        border-radius: 3px;
        transition: color 0.2s ease;
      }
      
      .control-button:hover {
        color: ${this.theme.buttonHoverColor};
      }
      
      .control-button:disabled {
        opacity: 0.5;
        cursor: not-allowed;
      }
      
      .volume-controls {
        display: flex;
        align-items: center;
        gap: 5px;
      }
      
      .volume-slider {
        width: 60px;
      }
      
      .seek-bar-container {
        position: relative;
        flex: 1;
        height: 20px;
        display: flex;
        align-items: center;
      }
      
      .seek-bar {
        width: 100%;
        height: 4px;
        background: transparent;
        outline: none;
        -webkit-appearance: none;
        cursor: pointer;
      }
      
      .progress-bar {
        position: absolute;
        left: 0;
        top: 50%;
        transform: translateY(-50%);
        height: 4px;
        background: ${this.theme.progressBarColor};
        border-radius: 2px;
        pointer-events: none;
      }
      
      .buffer-indicator {
        position: absolute;
        left: 0;
        top: 50%;
        transform: translateY(-50%);
        height: 4px;
        width: 100%;
        pointer-events: none;
      }
      
      .buffer-segment {
        position: absolute;
        height: 100%;
        background: ${this.theme.progressBufferColor};
        border-radius: 2px;
      }
      
      .time-display {
        white-space: nowrap;
        font-family: monospace;
      }
    `;

    let styleElement = document.getElementById('video-controls-styles');
    if (!styleElement) {
      styleElement = document.createElement('style');
      styleElement.id = 'video-controls-styles';
      document.head.appendChild(styleElement);
    }
    styleElement.textContent = styles;
  }

  private formatTime(seconds: number): string {
    if (!isFinite(seconds)) return '00:00';
    
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    if (hours > 0) {
      return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    }
    
    return `${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
  }

  /**
   * Event handlers
   */
  private handlePlaybackChange(event: CustomEvent): void {
    this.updateFromPlaybackState(event.detail);
  }

  private handleTimeUpdate(event: CustomEvent): void {
    this.updateFromPlaybackState(event.detail);
  }

  private handleVolumeChange(event: CustomEvent): void {
    this.updateVolumeControls(event.detail.volume, event.detail.muted);
  }

  private handleFullscreenChange(event: CustomEvent): void {
    if (this.fullscreenButton) {
      this.fullscreenButton.innerHTML = event.detail.fullscreen 
        ? this.getExitFullscreenIcon() 
        : this.getFullscreenIcon();
      this.fullscreenButton.setAttribute('aria-label', 
        event.detail.fullscreen ? 'Exit Fullscreen' : 'Fullscreen');
    }
  }

  private handleBuffering(event: CustomEvent): void {
    const bufferedRanges = this.videoPlayer.getBufferedRanges();
    const duration = this.videoPlayer.getDuration();
    this.updateBufferIndicator(bufferedRanges, duration);
  }

  private handleMouseMove(): void {
    this.lastInteraction = Date.now();
    if (this.options.autoHide) {
      this.resetAutoHideTimer();
    }
  }

  private handleMouseLeave(): void {
    if (this.options.autoHide && !this.isDragging) {
      this.autoHideTimer = setTimeout(() => {
        this.hide();
      }, 1000);
    }
  }

  private handleClick(): void {
    this.lastInteraction = Date.now();
    if (this.options.autoHide) {
      this.resetAutoHideTimer();
    }
  }

  /**
   * Icon methods (using simple SVG icons)
   */
  private getPlayIcon(): string {
    return `<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
      <path d="M3 2v12l10-6z"/>
    </svg>`;
  }

  private getPauseIcon(): string {
    return `<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
      <path d="M2 2h4v12H2zM10 2h4v12h-4z"/>
    </svg>`;
  }

  private getVolumeIcon(volume: number): string {
    if (volume === 0) {
      return `<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
        <path d="M8 2L4 6H1v4h3l4 4V2z"/>
        <path d="M11 6l2 2-2 2M13 4l4 4-4 4" stroke="currentColor" stroke-width="1" fill="none"/>
      </svg>`;
    } else if (volume < 0.5) {
      return `<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
        <path d="M8 2L4 6H1v4h3l4 4V2z"/>
        <path d="M11 6l2 2-2 2" stroke="currentColor" stroke-width="1" fill="none"/>
      </svg>`;
    } else {
      return `<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
        <path d="M8 2L4 6H1v4h3l4 4V2z"/>
        <path d="M11 6l2 2-2 2M13 4l4 4-4 4" stroke="currentColor" stroke-width="1" fill="none"/>
      </svg>`;
    }
  }

  private getFullscreenIcon(): string {
    return `<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
      <path d="M1 1v4h2V3h2V1zM11 1v2h2v2h2V1zM13 13v-2h2v4h-4v-2zM3 11v2h2v2H1v-4z"/>
    </svg>`;
  }

  private getExitFullscreenIcon(): string {
    return `<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
      <path d="M3 3v2h2v2H3V5H1V3zM11 7V5h2V3h-2v2h-2v2zM9 9v2h2v2h2v-2h-2V9zM5 11v2H3v-2h2V9H3v2z"/>
    </svg>`;
  }
}