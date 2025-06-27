/**
 * Application state types
 */
import { MediaSourceState, PlaybackState, StreamStatistics, AdaptiveState } from './media';
import { StreamInfoMessage, ChunkedDataState } from './messages';
export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting' | 'error';
export interface WebSocketState {
    url: string;
    socket: WebSocket | null;
    state: ConnectionState;
    reconnectAttempts: number;
    lastError: Error | null;
    lastConnectTime: number;
}
export interface StreamState {
    streamInfo: StreamInfoMessage | null;
    hasVideo: boolean;
    hasAudio: boolean;
    initialized: boolean;
    ready: boolean;
    chunkedData: Map<string, ChunkedDataState>;
}
export interface UIState {
    controlsVisible: boolean;
    fullscreen: boolean;
    volume: number;
    muted: boolean;
    playbackRate: number;
    debugMode: boolean;
    statusMessage: string;
}
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
export interface StateUpdate<K extends keyof ApplicationState> {
    type: K;
    payload: Partial<ApplicationState[K]>;
    timestamp: number;
}
export interface UserSettings {
    volume: number;
    muted: boolean;
    debugMode: boolean;
    autoplay: boolean;
    preferredQuality: 'auto' | 'high' | 'medium' | 'low';
    bufferTarget: number;
}
export interface FeatureFlags {
    enableAdaptiveBitrate: boolean;
    enableP2P: boolean;
    enableWebRTC: boolean;
    enableDebugOverlay: boolean;
    enableKeyboardShortcuts: boolean;
}
//# sourceMappingURL=state.d.ts.map