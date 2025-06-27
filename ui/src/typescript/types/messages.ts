/**
 * WebSocket message types for OpenBroadcastNetwork streaming protocol
 */

// Base message type
export interface BaseMessage {
  type: string;
}

// Stream information message
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

// Chunk information message
export interface ChunkInfoMessage extends BaseMessage {
  type: 'chunk_info';
  chunk_type: 'init' | 'media';
  size: number;
  sequence?: number;
  total_chunks?: number;
  timestamp?: number;
}

// Error message
export interface ErrorMessage extends BaseMessage {
  type: 'error';
  code: string;
  message: string;
  details?: unknown;
}

// Control messages
export interface PlaybackControlMessage extends BaseMessage {
  type: 'playback_control';
  action: 'play' | 'pause' | 'seek' | 'stop';
  position?: number; // For seek operations
}

export interface BufferStatusMessage extends BaseMessage {
  type: 'buffer_status';
  video_buffered?: number;
  audio_buffered?: number;
  target_buffer?: number;
}

// Union type for all message types
export type WebSocketMessage = 
  | StreamInfoMessage 
  | ChunkInfoMessage 
  | ErrorMessage 
  | PlaybackControlMessage
  | BufferStatusMessage;

// Type guards
export function isStreamInfoMessage(msg: WebSocketMessage): msg is StreamInfoMessage {
  return msg.type === 'stream_info';
}

export function isChunkInfoMessage(msg: WebSocketMessage): msg is ChunkInfoMessage {
  return msg.type === 'chunk_info';
}

export function isErrorMessage(msg: WebSocketMessage): msg is ErrorMessage {
  return msg.type === 'error';
}

// Binary data handling
export interface BinaryChunk {
  sequence: number;
  data: ArrayBuffer;
  isInit: boolean;
  timestamp?: number;
}

// Chunked data reassembly
export interface ChunkedDataState {
  expectedChunks: number;
  receivedChunks: Map<number, ArrayBuffer>;
  totalSize: number;
  startTime: number;
}