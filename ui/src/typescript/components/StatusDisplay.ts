/**
 * StatusDisplay - Shows connection status, buffering, and error information
 */

import { VideoPlayer } from './VideoPlayer';
import { ApplicationState, ConnectionState, StreamStatistics } from '../types';

export interface StatusDisplayOptions {
  className?: string;
  showConnectionStatus?: boolean;
  showBufferLevel?: boolean;
  showStatistics?: boolean;
  showErrors?: boolean;
  showBandwidth?: boolean;
  autoHide?: boolean;
  autoHideDelay?: number;
  position?: 'top-left' | 'top-right' | 'bottom-left' | 'bottom-right';
}

export interface StatusDisplayTheme {
  backgroundColor?: string;
  textColor?: string;
  errorColor?: string;
  warningColor?: string;
  successColor?: string;
  fontSize?: string;
  borderRadius?: string;
  opacity?: number;
}

interface StatusInfo {
  connection: {
    state: ConnectionState;
    quality: 'excellent' | 'good' | 'fair' | 'poor';
    latency?: number;
  };
  buffer: {
    level: number;
    health: 'healthy' | 'low' | 'critical';
    duration: number;
  };
  stream: {
    bitrate?: number;
    fps?: number;
    resolution?: string;
    codec?: string;
  };
  statistics: StreamStatistics;
  errors: Array<{
    message: string;
    timestamp: number;
    component: string;
    severity: 'error' | 'warning' | 'info';
  }>;
}

export class StatusDisplay extends EventTarget {
  private container: HTMLElement;
  private videoPlayer: VideoPlayer;
  private statusElement: HTMLElement;
  private options: Required<StatusDisplayOptions>;
  private theme: StatusDisplayTheme;
  private isVisible = true;
  private autoHideTimer: NodeJS.Timeout | null = null;
  private updateTimer: NodeJS.Timeout | null = null;
  
  // Status elements
  private connectionStatusElement: HTMLElement | null = null;
  private bufferLevelElement: HTMLElement | null = null;
  private statisticsElement: HTMLElement | null = null;
  private errorElement: HTMLElement | null = null;
  private bandwidthElement: HTMLElement | null = null;

  // State
  private statusInfo: StatusInfo;
  private lastUpdate = Date.now();

  constructor(
    videoPlayer: VideoPlayer,
    container: HTMLElement,
    options: StatusDisplayOptions = {},
    theme: StatusDisplayTheme = {}
  ) {
    super();
    
    this.videoPlayer = videoPlayer;
    this.container = container;
    this.options = {
      className: options.className ?? 'status-display',
      showConnectionStatus: options.showConnectionStatus ?? true,
      showBufferLevel: options.showBufferLevel ?? true,
      showStatistics: options.showStatistics ?? false,
      showErrors: options.showErrors ?? true,
      showBandwidth: options.showBandwidth ?? true,
      autoHide: options.autoHide ?? false,
      autoHideDelay: options.autoHideDelay ?? 5000,
      position: options.position ?? 'top-right',
    };
    
    this.theme = {
      backgroundColor: theme.backgroundColor ?? 'rgba(0, 0, 0, 0.8)',
      textColor: theme.textColor ?? '#ffffff',
      errorColor: theme.errorColor ?? '#ff4444',
      warningColor: theme.warningColor ?? '#ffaa00',
      successColor: theme.successColor ?? '#44ff44',
      fontSize: theme.fontSize ?? '12px',
      borderRadius: theme.borderRadius ?? '4px',
      opacity: theme.opacity ?? 0.9,
    };

    // Initialize status info
    this.statusInfo = {
      connection: { state: 'disconnected', quality: 'poor' },
      buffer: { level: 0, health: 'critical', duration: 0 },
      stream: {},
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
      errors: [],
    };

    this.createStatusDisplay();
    this.setupEventListeners();
    this.startUpdateTimer();
  }

  /**
   * Show status display
   */
  show(): void {
    this.isVisible = true;
    this.statusElement.style.display = 'block';
    this.dispatchEvent(new CustomEvent('status_shown'));
  }

  /**
   * Hide status display
   */
  hide(): void {
    this.isVisible = false;
    this.statusElement.style.display = 'none';
    this.dispatchEvent(new CustomEvent('status_hidden'));
  }

  /**
   * Toggle status display visibility
   */
  toggle(): void {
    if (this.isVisible) {
      this.hide();
    } else {
      this.show();
    }
  }

  /**
   * Update status from application state
   */
  updateFromApplicationState(state: ApplicationState): void {
    // Update connection status
    this.statusInfo.connection = {
      state: state.connection.state,
      quality: this.getConnectionQuality(state.connection),
      latency: state.connection.latency,
    };

    // Update buffer info
    const bufferLevel = this.calculateBufferLevel(state.playback.buffered, state.playback.currentTime);
    this.statusInfo.buffer = {
      level: bufferLevel,
      health: this.getBufferHealth(bufferLevel),
      duration: this.calculateBufferDuration(state.playback.buffered, state.playback.currentTime),
    };

    // Update stream info
    if (state.stream.streamInfo) {
      this.statusInfo.stream = {
        bitrate: state.stream.streamInfo.video?.bitrate,
        fps: state.stream.streamInfo.video?.frameRate,
        resolution: state.stream.streamInfo.video ? 
          `${state.stream.streamInfo.video.width}x${state.stream.streamInfo.video.height}` : undefined,
        codec: state.stream.streamInfo.video?.codec,
      };
    }

    // Update statistics
    this.statusInfo.statistics = { ...state.statistics };

    // Update errors
    if (state.error.lastError) {
      this.addError({
        message: state.error.lastError.message,
        timestamp: Date.now(),
        component: state.error.lastError.component,
        severity: 'error',
      });
    }

    this.updateDisplay();
  }

  /**
   * Add error message
   */
  addError(error: {
    message: string;
    component: string;
    severity: 'error' | 'warning' | 'info';
  }): void {
    this.statusInfo.errors.unshift({
      ...error,
      timestamp: Date.now(),
    });

    // Keep only last 5 errors
    if (this.statusInfo.errors.length > 5) {
      this.statusInfo.errors = this.statusInfo.errors.slice(0, 5);
    }

    this.updateDisplay();
    
    if (this.options.autoHide && error.severity === 'error') {
      this.show();
      this.resetAutoHideTimer();
    }
  }

  /**
   * Clear all errors
   */
  clearErrors(): void {
    this.statusInfo.errors = [];
    this.updateDisplay();
  }

  /**
   * Apply custom theme
   */
  applyTheme(newTheme: Partial<StatusDisplayTheme>): void {
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

    if (this.updateTimer) {
      clearInterval(this.updateTimer);
      this.updateTimer = null;
    }

    this.removeEventListeners();
    
    if (this.statusElement && this.statusElement.parentNode) {
      this.statusElement.parentNode.removeChild(this.statusElement);
    }
  }

  /**
   * Private methods
   */
  private createStatusDisplay(): void {
    this.statusElement = document.createElement('div');
    this.statusElement.className = this.options.className;
    
    // Create status sections
    if (this.options.showConnectionStatus) {
      this.connectionStatusElement = this.createConnectionStatus();
      this.statusElement.appendChild(this.connectionStatusElement);
    }

    if (this.options.showBufferLevel) {
      this.bufferLevelElement = this.createBufferLevel();
      this.statusElement.appendChild(this.bufferLevelElement);
    }

    if (this.options.showBandwidth) {
      this.bandwidthElement = this.createBandwidthDisplay();
      this.statusElement.appendChild(this.bandwidthElement);
    }

    if (this.options.showStatistics) {
      this.statisticsElement = this.createStatistics();
      this.statusElement.appendChild(this.statisticsElement);
    }

    if (this.options.showErrors) {
      this.errorElement = this.createErrorDisplay();
      this.statusElement.appendChild(this.errorElement);
    }

    this.container.appendChild(this.statusElement);
    this.updateStyles();
  }

  private createConnectionStatus(): HTMLElement {
    const element = document.createElement('div');
    element.className = 'connection-status';
    
    const label = document.createElement('span');
    label.className = 'status-label';
    label.textContent = 'Connection:';
    
    const status = document.createElement('span');
    status.className = 'status-value';
    
    const indicator = document.createElement('span');
    indicator.className = 'status-indicator';
    
    element.appendChild(label);
    element.appendChild(status);
    element.appendChild(indicator);
    
    return element;
  }

  private createBufferLevel(): HTMLElement {
    const element = document.createElement('div');
    element.className = 'buffer-level';
    
    const label = document.createElement('span');
    label.className = 'status-label';
    label.textContent = 'Buffer:';
    
    const progressContainer = document.createElement('div');
    progressContainer.className = 'buffer-progress-container';
    
    const progressBar = document.createElement('div');
    progressBar.className = 'buffer-progress-bar';
    
    const progressFill = document.createElement('div');
    progressFill.className = 'buffer-progress-fill';
    
    const value = document.createElement('span');
    value.className = 'status-value';
    
    progressBar.appendChild(progressFill);
    progressContainer.appendChild(progressBar);
    
    element.appendChild(label);
    element.appendChild(progressContainer);
    element.appendChild(value);
    
    return element;
  }

  private createBandwidthDisplay(): HTMLElement {
    const element = document.createElement('div');
    element.className = 'bandwidth-display';
    
    const label = document.createElement('span');
    label.className = 'status-label';
    label.textContent = 'Bandwidth:';
    
    const value = document.createElement('span');
    value.className = 'status-value';
    
    element.appendChild(label);
    element.appendChild(value);
    
    return element;
  }

  private createStatistics(): HTMLElement {
    const element = document.createElement('div');
    element.className = 'statistics-display';
    
    const title = document.createElement('div');
    title.className = 'status-title';
    title.textContent = 'Statistics';
    
    const content = document.createElement('div');
    content.className = 'statistics-content';
    
    element.appendChild(title);
    element.appendChild(content);
    
    return element;
  }

  private createErrorDisplay(): HTMLElement {
    const element = document.createElement('div');
    element.className = 'error-display';
    
    const content = document.createElement('div');
    content.className = 'error-content';
    
    element.appendChild(content);
    
    return element;
  }

  private updateDisplay(): void {
    this.updateConnectionStatus();
    this.updateBufferLevel();
    this.updateBandwidthDisplay();
    this.updateStatistics();
    this.updateErrorDisplay();
    this.lastUpdate = Date.now();
  }

  private updateConnectionStatus(): void {
    if (!this.connectionStatusElement) return;

    const statusValue = this.connectionStatusElement.querySelector('.status-value') as HTMLElement;
    const indicator = this.connectionStatusElement.querySelector('.status-indicator') as HTMLElement;

    if (statusValue && indicator) {
      const { state, quality, latency } = this.statusInfo.connection;
      
      statusValue.textContent = `${state.toUpperCase()} (${quality})`;
      if (latency !== undefined) {
        statusValue.textContent += ` - ${latency}ms`;
      }

      // Update indicator color
      indicator.className = `status-indicator ${state} ${quality}`;
    }
  }

  private updateBufferLevel(): void {
    if (!this.bufferLevelElement) return;

    const progressFill = this.bufferLevelElement.querySelector('.buffer-progress-fill') as HTMLElement;
    const statusValue = this.bufferLevelElement.querySelector('.status-value') as HTMLElement;

    if (progressFill && statusValue) {
      const { level, health, duration } = this.statusInfo.buffer;
      
      progressFill.style.width = `${level}%`;
      progressFill.className = `buffer-progress-fill ${health}`;
      
      statusValue.textContent = `${duration.toFixed(1)}s (${health})`;
    }
  }

  private updateBandwidthDisplay(): void {
    if (!this.bandwidthElement) return;

    const statusValue = this.bandwidthElement.querySelector('.status-value') as HTMLElement;
    
    if (statusValue) {
      const bandwidth = this.calculateBandwidth();
      statusValue.textContent = this.formatBandwidth(bandwidth);
    }
  }

  private updateStatistics(): void {
    if (!this.statisticsElement) return;

    const content = this.statisticsElement.querySelector('.statistics-content') as HTMLElement;
    
    if (content) {
      const stats = this.statusInfo.statistics;
      const streamInfo = this.statusInfo.stream;
      
      content.innerHTML = `
        <div class="stat-item">Bytes: ${this.formatBytes(stats.bytesReceived)}</div>
        <div class="stat-item">Chunks: ${stats.chunksReceived}</div>
        <div class="stat-item">Dropped: ${stats.droppedFrames}</div>
        ${streamInfo.resolution ? `<div class="stat-item">Resolution: ${streamInfo.resolution}</div>` : ''}
        ${streamInfo.fps ? `<div class="stat-item">FPS: ${streamInfo.fps}</div>` : ''}
        ${streamInfo.codec ? `<div class="stat-item">Codec: ${streamInfo.codec}</div>` : ''}
      `;
    }
  }

  private updateErrorDisplay(): void {
    if (!this.errorElement) return;

    const content = this.errorElement.querySelector('.error-content') as HTMLElement;
    
    if (content) {
      if (this.statusInfo.errors.length === 0) {
        content.innerHTML = '';
        return;
      }

      const errorsList = this.statusInfo.errors.map(error => {
        const timeAgo = this.getTimeAgo(error.timestamp);
        return `
          <div class="error-item ${error.severity}">
            <span class="error-component">[${error.component}]</span>
            <span class="error-message">${error.message}</span>
            <span class="error-time">${timeAgo}</span>
          </div>
        `;
      }).join('');

      content.innerHTML = errorsList;
    }
  }

  private setupEventListeners(): void {
    // Video player events
    this.videoPlayer.addEventListener('stream_state_change', this.handleStateChange.bind(this));
    this.videoPlayer.addEventListener('error', this.handleError.bind(this));
    this.videoPlayer.addEventListener('buffering', this.handleBuffering.bind(this));

    // Auto-hide setup
    if (this.options.autoHide) {
      this.container.addEventListener('mouseenter', this.handleMouseEnter.bind(this));
      this.container.addEventListener('mouseleave', this.handleMouseLeave.bind(this));
    }
  }

  private removeEventListeners(): void {
    this.videoPlayer.removeEventListener('stream_state_change', this.handleStateChange);
    this.videoPlayer.removeEventListener('error', this.handleError);
    this.videoPlayer.removeEventListener('buffering', this.handleBuffering);

    if (this.options.autoHide) {
      this.container.removeEventListener('mouseenter', this.handleMouseEnter);
      this.container.removeEventListener('mouseleave', this.handleMouseLeave);
    }
  }

  private startUpdateTimer(): void {
    this.updateTimer = setInterval(() => {
      if (this.videoPlayer.getStreamManager()) {
        const state = this.videoPlayer.getStreamManager()!.getState();
        this.updateFromApplicationState(state);
      }
    }, 1000);
  }

  private resetAutoHideTimer(): void {
    if (this.autoHideTimer) {
      clearTimeout(this.autoHideTimer);
    }
    
    this.autoHideTimer = setTimeout(() => {
      this.hide();
    }, this.options.autoHideDelay);
  }

  private updateStyles(): void {
    const positionStyles = this.getPositionStyles();
    
    const styles = `
      .${this.options.className} {
        position: absolute;
        ${positionStyles}
        background: ${this.theme.backgroundColor};
        color: ${this.theme.textColor};
        font-size: ${this.theme.fontSize};
        border-radius: ${this.theme.borderRadius};
        padding: 8px;
        z-index: 1001;
        opacity: ${this.theme.opacity};
        font-family: monospace;
        min-width: 200px;
        max-width: 300px;
      }
      
      .status-label {
        font-weight: bold;
        margin-right: 5px;
      }
      
      .status-value {
        margin-left: 5px;
      }
      
      .status-indicator {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-left: 5px;
      }
      
      .status-indicator.connected.excellent { background: ${this.theme.successColor}; }
      .status-indicator.connected.good { background: #88ff88; }
      .status-indicator.connecting { background: ${this.theme.warningColor}; }
      .status-indicator.disconnected { background: ${this.theme.errorColor}; }
      
      .buffer-progress-container {
        display: inline-block;
        width: 60px;
        height: 4px;
        background: rgba(255, 255, 255, 0.3);
        border-radius: 2px;
        margin: 0 5px;
        vertical-align: middle;
      }
      
      .buffer-progress-fill {
        height: 100%;
        border-radius: 2px;
        transition: width 0.3s ease;
      }
      
      .buffer-progress-fill.healthy { background: ${this.theme.successColor}; }
      .buffer-progress-fill.low { background: ${this.theme.warningColor}; }
      .buffer-progress-fill.critical { background: ${this.theme.errorColor}; }
      
      .statistics-content .stat-item {
        font-size: 10px;
        margin: 2px 0;
      }
      
      .error-item {
        margin: 2px 0;
        padding: 2px 4px;
        border-radius: 2px;
        font-size: 10px;
      }
      
      .error-item.error { background: rgba(255, 68, 68, 0.2); }
      .error-item.warning { background: rgba(255, 170, 0, 0.2); }
      .error-item.info { background: rgba(68, 68, 255, 0.2); }
      
      .error-component {
        font-weight: bold;
        margin-right: 5px;
      }
      
      .error-time {
        float: right;
        opacity: 0.7;
      }
      
      .status-title {
        font-weight: bold;
        border-bottom: 1px solid rgba(255, 255, 255, 0.3);
        margin-bottom: 5px;
        padding-bottom: 2px;
      }
    `;

    let styleElement = document.getElementById('status-display-styles');
    if (!styleElement) {
      styleElement = document.createElement('style');
      styleElement.id = 'status-display-styles';
      document.head.appendChild(styleElement);
    }
    styleElement.textContent = styles;
  }

  private getPositionStyles(): string {
    switch (this.options.position) {
      case 'top-left':
        return 'top: 10px; left: 10px;';
      case 'top-right':
        return 'top: 10px; right: 10px;';
      case 'bottom-left':
        return 'bottom: 50px; left: 10px;';
      case 'bottom-right':
        return 'bottom: 50px; right: 10px;';
      default:
        return 'top: 10px; right: 10px;';
    }
  }

  /**
   * Utility methods
   */
  private getConnectionQuality(connection: any): 'excellent' | 'good' | 'fair' | 'poor' {
    if (connection.state !== 'connected') return 'poor';
    
    const latency = connection.latency || 0;
    if (latency < 50) return 'excellent';
    if (latency < 100) return 'good';
    if (latency < 200) return 'fair';
    return 'poor';
  }

  private calculateBufferLevel(buffered: any[], currentTime: number): number {
    if (!buffered || buffered.length === 0) return 0;
    
    const currentBuffer = buffered.find(range => 
      currentTime >= range.start && currentTime <= range.end
    );
    
    if (!currentBuffer) return 0;
    
    const bufferAhead = currentBuffer.end - currentTime;
    return Math.min(100, (bufferAhead / 30) * 100); // 30 seconds = 100%
  }

  private getBufferHealth(level: number): 'healthy' | 'low' | 'critical' {
    if (level > 50) return 'healthy';
    if (level > 20) return 'low';
    return 'critical';
  }

  private calculateBufferDuration(buffered: any[], currentTime: number): number {
    if (!buffered || buffered.length === 0) return 0;
    
    const currentBuffer = buffered.find(range => 
      currentTime >= range.start && currentTime <= range.end
    );
    
    return currentBuffer ? currentBuffer.end - currentTime : 0;
  }

  private calculateBandwidth(): number {
    const now = Date.now();
    const timeDiff = now - this.lastUpdate;
    const bytesDiff = this.statusInfo.statistics.bytesReceived;
    
    if (timeDiff === 0) return 0;
    
    return (bytesDiff * 8) / (timeDiff / 1000); // bits per second
  }

  private formatBandwidth(bps: number): string {
    if (bps >= 1000000) {
      return `${(bps / 1000000).toFixed(1)} Mbps`;
    } else if (bps >= 1000) {
      return `${(bps / 1000).toFixed(0)} Kbps`;
    } else {
      return `${bps.toFixed(0)} bps`;
    }
  }

  private formatBytes(bytes: number): string {
    if (bytes >= 1048576) {
      return `${(bytes / 1048576).toFixed(1)} MB`;
    } else if (bytes >= 1024) {
      return `${(bytes / 1024).toFixed(0)} KB`;
    } else {
      return `${bytes} B`;
    }
  }

  private getTimeAgo(timestamp: number): string {
    const diff = Date.now() - timestamp;
    const seconds = Math.floor(diff / 1000);
    
    if (seconds < 60) return `${seconds}s ago`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    return `${hours}h ago`;
  }

  /**
   * Event handlers
   */
  private handleStateChange(event: CustomEvent): void {
    this.updateFromApplicationState(event.detail.payload);
  }

  private handleError(event: CustomEvent): void {
    this.addError({
      message: event.detail.message,
      component: 'player',
      severity: 'error',
    });
  }

  private handleBuffering(event: CustomEvent): void {
    // Update buffer status
    this.updateDisplay();
  }

  private handleMouseEnter(): void {
    if (this.options.autoHide) {
      this.show();
    }
  }

  private handleMouseLeave(): void {
    if (this.options.autoHide) {
      this.resetAutoHideTimer();
    }
  }
}