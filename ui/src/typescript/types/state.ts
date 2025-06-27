/**
 * Application state types
 */

import { MediaSourceState, PlaybackState, StreamStatistics, AdaptiveState } from './media';
import { StreamInfoMessage, ChunkedDataState } from './messages';

// Connection states
export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting' | 'error';

// WebSocket connection state
export interface WebSocketState {
  url: string;
  socket: WebSocket | null;
  state: ConnectionState;
  reconnectAttempts: number;
  lastError: Error | null;
  lastConnectTime: number;
}

// Stream state
export interface StreamState {
  streamInfo: StreamInfoMessage | null;
  hasVideo: boolean;
  hasAudio: boolean;
  initialized: boolean;
  ready: boolean;
  chunkedData: Map<string, ChunkedDataState>;
}

// UI state
export interface UIState {
  controlsVisible: boolean;
  fullscreen: boolean;
  volume: number;
  muted: boolean;
  playbackRate: number;
  debugMode: boolean;
  statusMessage: string;
}

// Error state
export interface ErrorState {
  lastError: ApplicationError | null;
  errorCount: number;
  recoverable: boolean;
  retryCount: number;
}

export interface ApplicationError {
  code: string;
  message: string;
  timestamp: number;
  component: 'websocket' | 'media' | 'codec' | 'network' | 'unknown';
  details?: unknown;
  stack?: string;
}

// Complete application state
export interface ApplicationState {
  connection: WebSocketState;
  stream: StreamState;
  media: MediaSourceState;
  playback: PlaybackState;
  ui: UIState;
  error: ErrorState;
  statistics: StreamStatistics;
  adaptive: AdaptiveState;
}

// State update actions
export interface StateUpdate<K extends keyof ApplicationState> {
  type: K;
  payload: Partial<ApplicationState[K]>;
  timestamp: number;
}

// Settings that persist
export interface UserSettings {
  volume: number;
  muted: boolean;
  debugMode: boolean;
  autoplay: boolean;
  preferredQuality: 'auto' | 'high' | 'medium' | 'low';
  bufferTarget: number; // seconds
}

// Feature flags
export interface FeatureFlags {
  enableAdaptiveBitrate: boolean;
  enableP2P: boolean;
  enableWebRTC: boolean;
  enableDebugOverlay: boolean;
  enableKeyboardShortcuts: boolean;
}