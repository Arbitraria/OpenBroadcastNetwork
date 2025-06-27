/**
 * WebSocketClient - Handles WebSocket communication for streaming
 */

import { 
  WebSocketMessage, 
  ChunkInfoMessage, 
  BinaryChunk,
  ChunkedDataState,
  isStreamInfoMessage,
  isChunkInfoMessage,
  isErrorMessage 
} from '../types';

export interface WebSocketStatistics {
  messagesSent: number;
  messagesReceived: number;
  bytesReceived: number;
  connectionTime: number;
  reconnectAttempts: number;
  lastError: Error | null;
}

export interface WebSocketOptions {
  autoReconnect?: boolean;
  reconnectInterval?: number;
  maxReconnectAttempts?: number;
  chunkTimeout?: number;
  maxMessageSize?: number;
}

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting' | 'error';

export interface ChunkTimeoutEvent {
  chunkType: string;
  sequence: number;
  elapsedTime: number;
}

export interface ParseErrorEvent {
  error: Error;
  rawData: string;
}

export interface MessageErrorEvent {
  error: string;
  size: number;
  maxSize: number;
}

export interface DisconnectedEvent {
  code: number;
  reason: string;
  wasClean: boolean;
}

export interface InitSegmentEvent {
  data: ArrayBuffer;
  totalSize: number;
  chunkCount: number;
}

export class WebSocketClient extends EventTarget {
  private socket: WebSocket | null = null;
  private url: string;
  private state: ConnectionState = 'disconnected';
  private messageQueue: string[] = [];
  private binaryQueue: ArrayBuffer[] = [];
  
  // Auto-reconnect settings
  private autoReconnect = false;
  private reconnectInterval = 1000;
  private maxReconnectAttempts = 5;
  private reconnectAttempts = 0;
  private reconnectTimeoutId: number | null = null;
  
  // Chunked data handling
  private chunkedData = new Map<string, ChunkedDataState>();
  private chunkTimeout = 10000; // 10 seconds
  private chunkTimeouts = new Map<string, number>();
  
  // Message handling
  private maxMessageSize = 50 * 1024 * 1024; // 50MB default
  
  // Statistics
  private statistics: WebSocketStatistics = {
    messagesSent: 0,
    messagesReceived: 0,
    bytesReceived: 0,
    connectionTime: 0,
    reconnectAttempts: 0,
    lastError: null,
  };
  private connectTime = 0;

  constructor(url: string, options: WebSocketOptions = {}) {
    super();
    this.url = url;
    
    // Create socket immediately but don't connect
    this.createSocket();
    
    if (options.autoReconnect !== undefined) {
      this.autoReconnect = options.autoReconnect;
    }
    if (options.reconnectInterval !== undefined) {
      this.reconnectInterval = options.reconnectInterval;
    }
    if (options.maxReconnectAttempts !== undefined) {
      this.maxReconnectAttempts = options.maxReconnectAttempts;
    }
    if (options.chunkTimeout !== undefined) {
      this.chunkTimeout = options.chunkTimeout;
    }
    if (options.maxMessageSize !== undefined) {
      this.maxMessageSize = options.maxMessageSize;
    }
  }

  /**
   * EventEmitter-style convenience methods
   */
  on(event: string, listener: EventListener): void {
    this.addEventListener(event, listener);
  }

  off(event: string, listener: EventListener): void {
    this.removeEventListener(event, listener);
  }

  emit(event: string, detail?: any): void {
    this.dispatchEvent(new CustomEvent(event, { detail }));
  }

  /**
   * Create WebSocket instance
   */
  private createSocket(): void {
    // Don't create the actual WebSocket here in constructor
    // Will be created in connect()
  }

  /**
   * Connect to WebSocket server
   */
  async connect(timeout: number = 5000): Promise<boolean> {
    if (this.state === 'connected') {
      return true;
    }

    this.state = 'connecting';
    this.connectTime = Date.now();

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        if (this.socket) {
          this.socket.close();
        }
        this.state = 'error';
        reject(new Error('Connection timeout'));
      }, timeout);

      try {
        this.socket = new WebSocket(this.url);
        this.socket.binaryType = 'arraybuffer';

        this.socket.onopen = () => {
          clearTimeout(timeoutId);
          this.state = 'connected';
          this.statistics.connectionTime = Date.now() - this.connectTime;
          this.reconnectAttempts = 0;
          
          // Send queued messages
          this.flushMessageQueue();
          
          this.dispatchEvent(new CustomEvent('connected'));
          resolve(true);
        };

        this.socket.onclose = (event) => {
          clearTimeout(timeoutId);
          this.handleDisconnection(event);
          
          if (this.state === 'connecting') {
            reject(new Error('WebSocket connection failed'));
          }
        };

        this.socket.onerror = () => {
          clearTimeout(timeoutId);
          this.state = 'error';
          this.statistics.lastError = new Error('WebSocket connection failed');
          
          this.dispatchEvent(new CustomEvent('connection_error', {
            detail: this.statistics.lastError,
          }));
          
          reject(this.statistics.lastError);
        };

        this.socket.onmessage = (event) => {
          this.handleMessage(event);
        };

      } catch (error) {
        clearTimeout(timeoutId);
        this.state = 'error';
        this.statistics.lastError = error as Error;
        reject(error);
      }
    });
  }

  /**
   * Disconnect from WebSocket server
   */
  disconnect(): void {
    if (this.reconnectTimeoutId) {
      clearTimeout(this.reconnectTimeoutId);
      this.reconnectTimeoutId = null;
    }

    if (this.socket) {
      this.socket.close(1000, 'Client disconnect');
    }

    this.state = 'disconnected';
    this.socket = null;
    this.clearChunkTimeouts();
    
    this.dispatchEvent(new CustomEvent('disconnected', {
      detail: { code: 1000, reason: 'Client disconnect', wasClean: true },
    }));
  }

  /**
   * Send JSON message
   */
  send(message: object): void {
    const jsonStr = JSON.stringify(message);
    
    if (this.state === 'connected' && this.socket) {
      this.socket.send(jsonStr);
      this.statistics.messagesSent++;
    } else {
      // Queue message for later
      this.messageQueue.push(jsonStr);
    }
  }

  /**
   * Send binary data
   */
  sendBinary(data: ArrayBuffer): void {
    if (this.state === 'connected' && this.socket) {
      this.socket.send(data);
      this.statistics.messagesSent++;
    } else {
      // Queue binary data for later
      this.binaryQueue.push(data);
    }
  }

  /**
   * Get current connection state
   */
  getState(): ConnectionState {
    return this.state;
  }

  /**
   * Check if currently connected
   */
  isConnected(): boolean {
    return this.state === 'connected';
  }

  /**
   * Get connection statistics
   */
  getStatistics(): WebSocketStatistics {
    return { ...this.statistics };
  }

  /**
   * Configure auto-reconnect
   */
  setAutoReconnect(enabled: boolean, interval?: number, maxAttempts?: number): void {
    this.autoReconnect = enabled;
    if (interval !== undefined) {
      this.reconnectInterval = interval;
    }
    if (maxAttempts !== undefined) {
      this.maxReconnectAttempts = maxAttempts;
    }
  }

  /**
   * Set chunk timeout
   */
  setChunkTimeout(timeout: number): void {
    this.chunkTimeout = timeout;
  }

  /**
   * Set maximum message size
   */
  setMaxMessageSize(size: number): void {
    this.maxMessageSize = size;
  }

  /**
   * Reset chunked data state
   */
  resetChunks(): void {
    this.chunkedData.clear();
    this.clearChunkTimeouts();
  }

  /**
   * Private helper methods
   */

  private handleMessage(event: MessageEvent): void {
    try {
      const data = event.data;
      
      if (typeof data === 'string') {
        // Check message size
        if (data.length > this.maxMessageSize) {
          this.dispatchEvent(new CustomEvent('message_error', {
            detail: {
              error: `Message too large: ${data.length} bytes`,
              size: data.length,
              maxSize: this.maxMessageSize,
            } as MessageErrorEvent,
          }));
          return;
        }

        this.statistics.messagesReceived++;
        this.statistics.bytesReceived += data.length;
        
        this.handleJSONMessage(data);
      } else if (data instanceof ArrayBuffer) {
        // Check binary data size
        if (data.byteLength > this.maxMessageSize) {
          this.dispatchEvent(new CustomEvent('message_error', {
            detail: {
              error: `Binary message too large: ${data.byteLength} bytes`,
              size: data.byteLength,
              maxSize: this.maxMessageSize,
            } as MessageErrorEvent,
          }));
          return;
        }

        this.statistics.messagesReceived++;
        this.statistics.bytesReceived += data.byteLength;
        
        this.handleBinaryMessage(data);
      }
    } catch (error) {
      this.statistics.lastError = error as Error;
      this.dispatchEvent(new CustomEvent('message_error', {
        detail: { error: (error as Error).message },
      }));
    }
  }

  private handleJSONMessage(data: string): void {
    try {
      const message = JSON.parse(data) as WebSocketMessage;
      
      if (isStreamInfoMessage(message)) {
        this.dispatchEvent(new CustomEvent('stream_info', {
          detail: message,
        }));
      } else if (isChunkInfoMessage(message)) {
        this.handleChunkInfo(message);
        this.dispatchEvent(new CustomEvent('chunk_info', {
          detail: message,
        }));
      } else if (isErrorMessage(message)) {
        this.dispatchEvent(new CustomEvent('error', {
          detail: message,
        }));
      } else {
        // Generic message
        this.dispatchEvent(new CustomEvent('message', {
          detail: message,
        }));
      }
    } catch (error) {
      this.dispatchEvent(new CustomEvent('parse_error', {
        detail: {
          error: error as Error,
          rawData: data,
        } as ParseErrorEvent,
      }));
    }
  }

  private handleBinaryMessage(data: ArrayBuffer): void {
    const chunk: BinaryChunk = {
      sequence: this.getCurrentChunkSequence(),
      data,
      isInit: this.isCurrentChunkInit(),
      timestamp: Date.now(),
    };

    this.dispatchEvent(new CustomEvent('binary_chunk', {
      detail: chunk,
    }));

    // Handle chunked data reassembly
    this.handleChunkData(data);
  }

  private handleChunkInfo(chunkInfo: ChunkInfoMessage): void {
    const key = `${chunkInfo.chunk_type}_${chunkInfo.sequence}`;
    
    if (!this.chunkedData.has(key)) {
      const state: ChunkedDataState = {
        expectedChunks: chunkInfo.total_chunks ?? 1,
        receivedChunks: new Map(),
        totalSize: 0,
        startTime: Date.now(),
      };
      this.chunkedData.set(key, state);
      
      // Set timeout for this chunk sequence
      const timeoutId = window.setTimeout(() => {
        this.handleChunkTimeout(key, chunkInfo);
      }, this.chunkTimeout);
      
      this.chunkTimeouts.set(key, timeoutId);
    }
  }

  private handleChunkData(data: ArrayBuffer): void {
    // Find the corresponding chunk info
    for (const [key, state] of this.chunkedData.entries()) {
      if (state.receivedChunks.size < state.expectedChunks) {
        const sequence = state.receivedChunks.size + 1;
        state.receivedChunks.set(sequence, data);
        state.totalSize += data.byteLength;
        
        // Check if all chunks received
        if (state.receivedChunks.size === state.expectedChunks) {
          this.assembleChunks(key, state);
          this.chunkedData.delete(key);
          
          // Clear timeout
          const timeoutId = this.chunkTimeouts.get(key);
          if (timeoutId) {
            clearTimeout(timeoutId);
            this.chunkTimeouts.delete(key);
          }
        }
        break;
      }
    }
  }

  private assembleChunks(key: string, state: ChunkedDataState): void {
    // Combine all chunks in order
    const combined = new ArrayBuffer(state.totalSize);
    const view = new Uint8Array(combined);
    let offset = 0;
    
    for (let i = 1; i <= state.expectedChunks; i++) {
      const chunk = state.receivedChunks.get(i);
      if (chunk) {
        const chunkView = new Uint8Array(chunk);
        view.set(chunkView, offset);
        offset += chunk.byteLength;
      }
    }
    
    const [chunkType] = key.split('_');
    
    if (chunkType === 'init') {
      this.dispatchEvent(new CustomEvent('init_segment', {
        detail: {
          data: combined,
          totalSize: state.totalSize,
          chunkCount: state.expectedChunks,
        } as InitSegmentEvent,
      }));
    } else {
      this.dispatchEvent(new CustomEvent('media_segment', {
        detail: {
          data: combined,
          totalSize: state.totalSize,
          chunkCount: state.expectedChunks,
        },
      }));
    }
  }

  private handleChunkTimeout(key: string, chunkInfo: ChunkInfoMessage): void {
    const state = this.chunkedData.get(key);
    if (state) {
      this.dispatchEvent(new CustomEvent('chunk_timeout', {
        detail: {
          chunkType: chunkInfo.chunk_type,
          sequence: chunkInfo.sequence ?? 0,
          elapsedTime: Date.now() - state.startTime,
        } as ChunkTimeoutEvent,
      }));
      
      this.chunkedData.delete(key);
      this.chunkTimeouts.delete(key);
    }
  }

  private handleDisconnection(event: CloseEvent): void {
    this.state = 'disconnected';
    this.socket = null;
    this.clearChunkTimeouts();
    
    const disconnectedEvent: DisconnectedEvent = {
      code: event.code,
      reason: event.reason,
      wasClean: event.wasClean,
    };
    
    this.dispatchEvent(new CustomEvent('disconnected', {
      detail: disconnectedEvent,
    }));
    
    // Handle auto-reconnect
    if (this.autoReconnect && 
        event.code !== 1000 && // Not a normal closure
        this.reconnectAttempts < this.maxReconnectAttempts) {
      
      this.state = 'reconnecting';
      this.reconnectAttempts++;
      this.statistics.reconnectAttempts = this.reconnectAttempts;
      
      this.dispatchEvent(new CustomEvent('reconnecting', {
        detail: { attempt: this.reconnectAttempts },
      }));
      
      this.reconnectTimeoutId = window.setTimeout(() => {
        this.connect().catch(() => {
          // Connection failed, will trigger another reconnect attempt
        });
      }, this.reconnectInterval);
    }
  }

  private flushMessageQueue(): void {
    if (this.socket && this.state === 'connected') {
      // Send queued JSON messages
      while (this.messageQueue.length > 0) {
        const message = this.messageQueue.shift();
        if (message) {
          this.socket.send(message);
          this.statistics.messagesSent++;
        }
      }
      
      // Send queued binary messages
      while (this.binaryQueue.length > 0) {
        const data = this.binaryQueue.shift();
        if (data) {
          this.socket.send(data);
          this.statistics.messagesSent++;
        }
      }
    }
  }

  private clearChunkTimeouts(): void {
    for (const timeoutId of this.chunkTimeouts.values()) {
      clearTimeout(timeoutId);
    }
    this.chunkTimeouts.clear();
  }

  private getCurrentChunkSequence(): number {
    // Simple implementation - would be enhanced based on actual protocol
    return 1;
  }

  private isCurrentChunkInit(): boolean {
    // Simple implementation - would be enhanced based on actual protocol
    return true;
  }
}