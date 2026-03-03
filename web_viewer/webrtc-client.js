/**
 * WebRTC Stream Client for OpenBroadcastNetwork
 *
 * Provides browser-to-browser P2P streaming using WebRTC data channels.
 * Uses the same fMP4/MSE pipeline as WebSocket, but with WebRTC as transport.
 *
 * Architecture:
 * - Signaling via WebSocket to /webrtc/signal endpoint
 * - Data transfer via WebRTC DataChannels (ordered, reliable)
 * - Falls back to WebSocket if WebRTC fails
 */

class WebRtcStreamClient {
    /**
     * Create a new WebRTC stream client
     * @param {string} signalingUrl - WebSocket URL for signaling (e.g., ws://localhost:8080/webrtc/signal)
     * @param {string} streamId - ID of the stream to join
     * @param {boolean} isPublisher - Whether this client is a publisher (sends data) or subscriber (receives data)
     */
    constructor(signalingUrl, streamId, isPublisher = false) {
        this.signalingUrl = signalingUrl;
        this.streamId = streamId;
        this.isPublisher = isPublisher;
        this.localPeerId = this.generatePeerId();

        // WebRTC connections: peerId -> RTCPeerConnection
        this.peerConnections = new Map();
        // Data channels: peerId -> RTCDataChannel
        this.dataChannels = new Map();

        // Signaling WebSocket
        this.signalingWs = null;
        this.signalingConnected = false;

        // Callbacks
        this.onStreamData = null;       // Called when stream data is received
        this.onPeerConnected = null;    // Called when a peer connects
        this.onPeerDisconnected = null; // Called when a peer disconnects
        this.onError = null;            // Called on error
        this.onStateChange = null;      // Called on connection state changes

        // ICE servers configuration
        this.iceServers = [
            { urls: 'stun:stun.l.google.com:19302' },
            { urls: 'stun:stun1.l.google.com:19302' },
        ];

        // Pending ICE candidates (before remote description is set)
        this.pendingIceCandidates = new Map(); // peerId -> candidate[]

        // Reconnection state
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 3;
        this.reconnectDelay = 1000;

        // Message chunking for large segments (WebRTC ~16KB limit)
        this.MAX_CHUNK_SIZE = 16000;
        this.incomingChunks = new Map(); // peerId -> { chunks: [], totalSize: 0, expectedSize: 0 }

        this.log('info', `WebRTC client created for stream ${streamId} (${isPublisher ? 'publisher' : 'subscriber'})`);
    }

    /**
     * Generate a unique peer ID
     */
    generatePeerId() {
        return 'peer-' + crypto.randomUUID();
    }

    /**
     * Connect to the signaling server and join the stream
     */
    async connect() {
        return new Promise((resolve, reject) => {
            try {
                this.log('info', `Connecting to signaling server: ${this.signalingUrl}`);
                this.signalingWs = new WebSocket(this.signalingUrl);

                this.signalingWs.onopen = () => {
                    this.signalingConnected = true;
                    this.reconnectAttempts = 0;
                    this.log('info', 'Signaling WebSocket connected');
                    this.announceJoin();
                    this.notifyStateChange('connected');
                    resolve();
                };

                this.signalingWs.onmessage = (event) => {
                    try {
                        const message = JSON.parse(event.data);
                        this.handleSignalingMessage(message);
                    } catch (e) {
                        this.log('error', `Failed to parse signaling message: ${e.message}`);
                    }
                };

                this.signalingWs.onerror = (error) => {
                    this.log('error', `Signaling WebSocket error: ${error}`);
                    this.notifyError(new Error('Signaling connection failed'));
                };

                this.signalingWs.onclose = () => {
                    this.signalingConnected = false;
                    this.log('info', 'Signaling WebSocket closed');
                    this.notifyStateChange('disconnected');

                    // Attempt reconnection
                    if (this.reconnectAttempts < this.maxReconnectAttempts) {
                        this.reconnectAttempts++;
                        this.log('info', `Attempting reconnection ${this.reconnectAttempts}/${this.maxReconnectAttempts}`);
                        setTimeout(() => this.connect(), this.reconnectDelay * this.reconnectAttempts);
                    }
                };
            } catch (error) {
                this.log('error', `Failed to connect: ${error.message}`);
                reject(error);
            }
        });
    }

    /**
     * Disconnect from the signaling server and close all peer connections
     */
    disconnect() {
        this.log('info', 'Disconnecting WebRTC client');

        // Send leave message
        if (this.signalingConnected) {
            this.sendSignaling({
                type: 'peer_leave',
                peer_id: this.localPeerId
            });
        }

        // Close all peer connections
        for (const [peerId, pc] of this.peerConnections) {
            this.log('info', `Closing connection to peer ${peerId}`);
            pc.close();
        }
        this.peerConnections.clear();
        this.dataChannels.clear();

        // Close signaling WebSocket
        if (this.signalingWs) {
            this.signalingWs.close();
            this.signalingWs = null;
        }

        this.signalingConnected = false;
        this.notifyStateChange('disconnected');
    }

    /**
     * Send join announcement to signaling server
     */
    announceJoin() {
        this.sendSignaling({
            type: 'peer_join',
            peer_id: this.localPeerId,
            stream_id: this.streamId,
            is_publisher: this.isPublisher
        });
    }

    /**
     * Send a signaling message
     */
    sendSignaling(message) {
        if (this.signalingWs && this.signalingConnected) {
            this.signalingWs.send(JSON.stringify(message));
            this.log('debug', `Sent signaling: ${message.type}`);
        } else {
            this.log('warn', 'Cannot send signaling - not connected');
        }
    }

    /**
     * Handle incoming signaling messages
     */
    async handleSignalingMessage(message) {
        this.log('debug', `Received signaling: ${message.type}`);

        switch (message.type) {
            case 'peer_list':
                // List of existing peers in the stream
                this.log('info', `Received peer list: ${message.peers.length} peers`);
                for (const peer of message.peers) {
                    // As subscriber, initiate connections to publishers
                    if (!this.isPublisher && peer.is_publisher) {
                        await this.initiateConnection(peer.peer_id);
                    }
                }
                break;

            case 'peer_join':
                // New peer joined the stream
                if (message.peer_id !== this.localPeerId) {
                    this.log('info', `Peer joined: ${message.peer_id} (publisher: ${message.is_publisher})`);
                    // As publisher, wait for subscribers to initiate
                    // As subscriber, initiate connection to new publishers
                    if (!this.isPublisher && message.is_publisher) {
                        await this.initiateConnection(message.peer_id);
                    }
                }
                break;

            case 'peer_leave':
                // Peer left the stream
                if (message.peer_id !== this.localPeerId) {
                    this.log('info', `Peer left: ${message.peer_id}`);
                    this.closePeerConnection(message.peer_id);
                }
                break;

            case 'offer':
                // Received SDP offer from another peer
                if (message.target_peer_id === this.localPeerId || !message.target_peer_id) {
                    await this.handleOffer(message.peer_id, message.sdp);
                }
                break;

            case 'answer':
                // Received SDP answer
                if (message.target_peer_id === this.localPeerId) {
                    await this.handleAnswer(message.peer_id, message.sdp);
                }
                break;

            case 'ice_candidate':
                // Received ICE candidate
                if (message.target_peer_id === this.localPeerId) {
                    await this.handleIceCandidate(
                        message.peer_id,
                        message.candidate,
                        message.sdp_mid,
                        message.sdp_mline_index
                    );
                }
                break;

            case 'error':
                this.log('error', `Signaling error: ${message.message}`);
                this.notifyError(new Error(message.message));
                break;

            default:
                this.log('warn', `Unknown signaling message type: ${message.type}`);
        }
    }

    /**
     * Create a new RTCPeerConnection for a peer
     */
    createPeerConnection(remotePeerId) {
        const pc = new RTCPeerConnection({
            iceServers: this.iceServers
        });

        // Handle ICE candidates
        pc.onicecandidate = (event) => {
            if (event.candidate) {
                this.sendSignaling({
                    type: 'ice_candidate',
                    peer_id: this.localPeerId,
                    target_peer_id: remotePeerId,
                    candidate: event.candidate.candidate,
                    sdp_mid: event.candidate.sdpMid,
                    sdp_mline_index: event.candidate.sdpMLineIndex
                });
            }
        };

        // Handle connection state changes
        pc.onconnectionstatechange = () => {
            this.log('info', `Connection to ${remotePeerId}: ${pc.connectionState}`);

            if (pc.connectionState === 'connected') {
                this.notifyPeerConnected(remotePeerId);
            } else if (pc.connectionState === 'disconnected' || pc.connectionState === 'failed') {
                this.closePeerConnection(remotePeerId);
            }
        };

        // Handle ICE connection state changes
        pc.oniceconnectionstatechange = () => {
            this.log('debug', `ICE state for ${remotePeerId}: ${pc.iceConnectionState}`);
        };

        this.peerConnections.set(remotePeerId, pc);
        this.pendingIceCandidates.set(remotePeerId, []);

        return pc;
    }

    /**
     * Initiate a WebRTC connection to a peer (subscriber initiates to publisher)
     */
    async initiateConnection(remotePeerId) {
        this.log('info', `Initiating connection to peer ${remotePeerId}`);

        const pc = this.createPeerConnection(remotePeerId);

        // Create data channel for streaming (subscriber creates channel to publisher)
        const dc = pc.createDataChannel('streaming', {
            ordered: true,  // Reliable, ordered delivery (like TCP)
            maxRetransmits: null  // Keep retrying until delivery
        });

        this.setupDataChannel(dc, remotePeerId);

        // Create and send offer
        try {
            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);

            this.sendSignaling({
                type: 'offer',
                peer_id: this.localPeerId,
                target_peer_id: remotePeerId,
                sdp: offer.sdp
            });

            this.log('info', `Sent offer to ${remotePeerId}`);
        } catch (error) {
            this.log('error', `Failed to create offer: ${error.message}`);
            this.closePeerConnection(remotePeerId);
        }
    }

    /**
     * Handle an incoming SDP offer
     */
    async handleOffer(remotePeerId, sdp) {
        this.log('info', `Handling offer from ${remotePeerId}`);

        let pc = this.peerConnections.get(remotePeerId);
        if (!pc) {
            pc = this.createPeerConnection(remotePeerId);
        }

        // Set up to receive data channel
        pc.ondatachannel = (event) => {
            this.log('info', `Received data channel from ${remotePeerId}`);
            this.setupDataChannel(event.channel, remotePeerId);
        };

        try {
            await pc.setRemoteDescription({ type: 'offer', sdp });

            // Apply any pending ICE candidates
            await this.applyPendingIceCandidates(remotePeerId);

            // Create and send answer
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);

            this.sendSignaling({
                type: 'answer',
                peer_id: this.localPeerId,
                target_peer_id: remotePeerId,
                sdp: answer.sdp
            });

            this.log('info', `Sent answer to ${remotePeerId}`);
        } catch (error) {
            this.log('error', `Failed to handle offer: ${error.message}`);
            this.closePeerConnection(remotePeerId);
        }
    }

    /**
     * Handle an incoming SDP answer
     */
    async handleAnswer(remotePeerId, sdp) {
        this.log('info', `Handling answer from ${remotePeerId}`);

        const pc = this.peerConnections.get(remotePeerId);
        if (!pc) {
            this.log('warn', `No peer connection for ${remotePeerId}`);
            return;
        }

        try {
            await pc.setRemoteDescription({ type: 'answer', sdp });

            // Apply any pending ICE candidates
            await this.applyPendingIceCandidates(remotePeerId);

            this.log('info', `Connection established with ${remotePeerId}`);
        } catch (error) {
            this.log('error', `Failed to handle answer: ${error.message}`);
        }
    }

    /**
     * Handle an incoming ICE candidate
     */
    async handleIceCandidate(remotePeerId, candidate, sdpMid, sdpMLineIndex) {
        const pc = this.peerConnections.get(remotePeerId);

        const iceCandidate = new RTCIceCandidate({
            candidate,
            sdpMid,
            sdpMLineIndex
        });

        if (pc && pc.remoteDescription) {
            try {
                await pc.addIceCandidate(iceCandidate);
                this.log('debug', `Added ICE candidate for ${remotePeerId}`);
            } catch (error) {
                this.log('warn', `Failed to add ICE candidate: ${error.message}`);
            }
        } else {
            // Queue candidate until remote description is set
            const pending = this.pendingIceCandidates.get(remotePeerId) || [];
            pending.push(iceCandidate);
            this.pendingIceCandidates.set(remotePeerId, pending);
            this.log('debug', `Queued ICE candidate for ${remotePeerId}`);
        }
    }

    /**
     * Apply pending ICE candidates after remote description is set
     */
    async applyPendingIceCandidates(remotePeerId) {
        const pc = this.peerConnections.get(remotePeerId);
        const pending = this.pendingIceCandidates.get(remotePeerId) || [];

        for (const candidate of pending) {
            try {
                await pc.addIceCandidate(candidate);
                this.log('debug', `Applied pending ICE candidate for ${remotePeerId}`);
            } catch (error) {
                this.log('warn', `Failed to apply pending ICE candidate: ${error.message}`);
            }
        }

        this.pendingIceCandidates.set(remotePeerId, []);
    }

    /**
     * Set up a data channel for streaming
     */
    setupDataChannel(dc, remotePeerId) {
        dc.binaryType = 'arraybuffer';

        dc.onopen = () => {
            this.log('info', `Data channel opened with ${remotePeerId}`);
            this.dataChannels.set(remotePeerId, dc);
            this.notifyStateChange('streaming');
        };

        dc.onclose = () => {
            this.log('info', `Data channel closed with ${remotePeerId}`);
            this.dataChannels.delete(remotePeerId);
        };

        dc.onerror = (error) => {
            this.log('error', `Data channel error with ${remotePeerId}: ${error}`);
        };

        dc.onmessage = (event) => {
            this.handleDataChannelMessage(remotePeerId, event.data);
        };

        this.dataChannels.set(remotePeerId, dc);
    }

    /**
     * Handle incoming data channel messages (may be chunked)
     */
    handleDataChannelMessage(peerId, data) {
        if (typeof data === 'string') {
            // JSON control message (chunk metadata)
            try {
                const msg = JSON.parse(data);
                if (msg.type === 'chunk_start') {
                    // Start receiving a chunked message
                    this.incomingChunks.set(peerId, {
                        chunks: [],
                        totalSize: 0,
                        expectedSize: msg.size
                    });
                } else if (msg.type === 'chunk_end') {
                    // Assemble chunked message
                    const state = this.incomingChunks.get(peerId);
                    if (state) {
                        const assembled = this.assembleChunks(state.chunks);
                        this.incomingChunks.delete(peerId);
                        this.notifyStreamData(assembled);
                    }
                }
            } catch (e) {
                this.log('warn', `Invalid control message: ${e.message}`);
            }
        } else if (data instanceof ArrayBuffer) {
            // Binary data (stream segment or chunk)
            const state = this.incomingChunks.get(peerId);
            if (state) {
                // Part of a chunked message
                state.chunks.push(new Uint8Array(data));
                state.totalSize += data.byteLength;
            } else {
                // Single message (not chunked)
                this.notifyStreamData(data);
            }
        }
    }

    /**
     * Assemble chunks into a single ArrayBuffer
     */
    assembleChunks(chunks) {
        const totalSize = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
        const assembled = new Uint8Array(totalSize);
        let offset = 0;
        for (const chunk of chunks) {
            assembled.set(chunk, offset);
            offset += chunk.length;
        }
        return assembled.buffer;
    }

    /**
     * Send data to all connected peers (for publishers)
     */
    sendData(data) {
        if (!this.isPublisher) {
            this.log('warn', 'Only publishers can send data');
            return;
        }

        const buffer = data instanceof ArrayBuffer ? data : data.buffer || data;

        for (const [peerId, dc] of this.dataChannels) {
            if (dc.readyState === 'open') {
                try {
                    if (buffer.byteLength > this.MAX_CHUNK_SIZE) {
                        // Send in chunks
                        this.sendChunked(dc, buffer);
                    } else {
                        dc.send(buffer);
                    }
                } catch (error) {
                    this.log('error', `Failed to send to ${peerId}: ${error.message}`);
                }
            }
        }
    }

    /**
     * Send large data in chunks
     */
    sendChunked(dc, buffer) {
        const view = new Uint8Array(buffer);
        const numChunks = Math.ceil(view.length / this.MAX_CHUNK_SIZE);

        // Send start marker
        dc.send(JSON.stringify({ type: 'chunk_start', size: buffer.byteLength }));

        // Send chunks
        for (let i = 0; i < numChunks; i++) {
            const start = i * this.MAX_CHUNK_SIZE;
            const end = Math.min(start + this.MAX_CHUNK_SIZE, view.length);
            const chunk = view.slice(start, end);
            dc.send(chunk.buffer);
        }

        // Send end marker
        dc.send(JSON.stringify({ type: 'chunk_end' }));
    }

    /**
     * Close connection to a specific peer
     */
    closePeerConnection(peerId) {
        const pc = this.peerConnections.get(peerId);
        if (pc) {
            pc.close();
            this.peerConnections.delete(peerId);
        }

        this.dataChannels.delete(peerId);
        this.pendingIceCandidates.delete(peerId);
        this.incomingChunks.delete(peerId);

        this.notifyPeerDisconnected(peerId);
    }

    /**
     * Get the number of connected peers
     */
    getConnectedPeerCount() {
        let count = 0;
        for (const dc of this.dataChannels.values()) {
            if (dc.readyState === 'open') {
                count++;
            }
        }
        return count;
    }

    /**
     * Check if any peers are connected
     */
    isConnected() {
        return this.getConnectedPeerCount() > 0;
    }

    // Notification helpers
    notifyStreamData(data) {
        if (this.onStreamData) {
            try {
                this.onStreamData(data);
            } catch (e) {
                this.log('error', `Error in onStreamData callback: ${e.message}`);
            }
        }
    }

    notifyPeerConnected(peerId) {
        if (this.onPeerConnected) {
            this.onPeerConnected(peerId);
        }
    }

    notifyPeerDisconnected(peerId) {
        if (this.onPeerDisconnected) {
            this.onPeerDisconnected(peerId);
        }
    }

    notifyError(error) {
        if (this.onError) {
            this.onError(error);
        }
    }

    notifyStateChange(state) {
        if (this.onStateChange) {
            this.onStateChange(state);
        }
    }

    /**
     * Logging helper
     */
    log(level, message) {
        const prefix = `[WebRTC:${this.localPeerId.substring(0, 8)}]`;
        switch (level) {
            case 'error':
                console.error(`${prefix} ${message}`);
                break;
            case 'warn':
                console.warn(`${prefix} ${message}`);
                break;
            case 'info':
                console.log(`${prefix} ${message}`);
                break;
            case 'debug':
                console.debug(`${prefix} ${message}`);
                break;
        }
    }
}

// Export for use in modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { WebRtcStreamClient };
}
