/**
 * WebSocketClient - Handles WebSocket communication for streaming
 */
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
export declare class WebSocketClient extends EventTarget {
    private socket;
    private url;
    private state;
    private messageQueue;
    private binaryQueue;
    private autoReconnect;
    private reconnectInterval;
    private maxReconnectAttempts;
    private reconnectAttempts;
    private reconnectTimeoutId;
    private chunkedData;
    private chunkTimeout;
    private chunkTimeouts;
    private maxMessageSize;
    private statistics;
    private connectTime;
    constructor(url: string, options?: WebSocketOptions);
    /**
     * EventEmitter-style convenience methods
     */
    on(event: string, listener: EventListener): void;
    off(event: string, listener: EventListener): void;
    emit(event: string, detail?: any): void;
    /**
     * Create WebSocket instance
     */
    private createSocket;
    /**
     * Connect to WebSocket server
     */
    connect(timeout?: number): Promise<boolean>;
    /**
     * Disconnect from WebSocket server
     */
    disconnect(): void;
    /**
     * Send JSON message
     */
    send(message: object): void;
    /**
     * Send binary data
     */
    sendBinary(data: ArrayBuffer): void;
    /**
     * Get current connection state
     */
    getState(): ConnectionState;
    /**
     * Check if currently connected
     */
    isConnected(): boolean;
    /**
     * Get connection statistics
     */
    getStatistics(): WebSocketStatistics;
    /**
     * Configure auto-reconnect
     */
    setAutoReconnect(enabled: boolean, interval?: number, maxAttempts?: number): void;
    /**
     * Set chunk timeout
     */
    setChunkTimeout(timeout: number): void;
    /**
     * Set maximum message size
     */
    setMaxMessageSize(size: number): void;
    /**
     * Reset chunked data state
     */
    resetChunks(): void;
    /**
     * Private helper methods
     */
    private handleMessage;
    private handleJSONMessage;
    private handleBinaryMessage;
    private handleChunkInfo;
    private handleChunkData;
    private assembleChunks;
    private handleChunkTimeout;
    private handleDisconnection;
    private flushMessageQueue;
    private clearChunkTimeouts;
    private getCurrentChunkSequence;
    private isCurrentChunkInit;
}
//# sourceMappingURL=WebSocketClient.d.ts.map