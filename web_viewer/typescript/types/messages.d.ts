/**
 * WebSocket message types for OpenBroadcastNetwork streaming protocol
 */
export interface BaseMessage {
    type: string;
}
export interface StreamInfoMessage extends BaseMessage {
    type: 'stream_info';
    video?: VideoTrackInfo;
    audio?: AudioTrackInfo;
}
export interface VideoTrackInfo {
    codec: string;
    width: number;
    height: number;
    frame_rate?: number;
    bitrate?: number;
    profile?: string;
    level?: string;
}
export interface AudioTrackInfo {
    codec: string;
    channels: number;
    sample_rate: number;
    bitrate?: number;
}
export interface ChunkInfoMessage extends BaseMessage {
    type: 'chunk_info';
    chunk_type: 'init' | 'media';
    size: number;
    sequence?: number;
    total_chunks?: number;
    timestamp?: number;
}
export interface ErrorMessage extends BaseMessage {
    type: 'error';
    code: string;
    message: string;
    details?: unknown;
}
export interface PlaybackControlMessage extends BaseMessage {
    type: 'playback_control';
    action: 'play' | 'pause' | 'seek' | 'stop';
    position?: number;
}
export interface BufferStatusMessage extends BaseMessage {
    type: 'buffer_status';
    video_buffered?: number;
    audio_buffered?: number;
    target_buffer?: number;
}
export type WebSocketMessage = StreamInfoMessage | ChunkInfoMessage | ErrorMessage | PlaybackControlMessage | BufferStatusMessage;
export declare function isStreamInfoMessage(msg: WebSocketMessage): msg is StreamInfoMessage;
export declare function isChunkInfoMessage(msg: WebSocketMessage): msg is ChunkInfoMessage;
export declare function isErrorMessage(msg: WebSocketMessage): msg is ErrorMessage;
export interface BinaryChunk {
    sequence: number;
    data: ArrayBuffer;
    isInit: boolean;
    timestamp?: number;
}
export interface ChunkedDataState {
    expectedChunks: number;
    receivedChunks: Map<number, ArrayBuffer>;
    totalSize: number;
    startTime: number;
}
//# sourceMappingURL=messages.d.ts.map