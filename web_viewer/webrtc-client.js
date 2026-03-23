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
        this.onStreamData = null;       // Called when stream data is received (legacy)
        this.onPeerConnected = null;    // Called when a peer connects
        this.onPeerDisconnected = null; // Called when a peer disconnects
        this.onError = null;            // Called on error
        this.onStateChange = null;      // Called on connection state changes

        // P2P streaming callbacks
        this.onStreamInfo = null;       // Called when stream_info is received from peer
        this.onSegmentReceived = null;  // Called when a segment is received: (header, buffer)
        this.onPeerListReceived = null; // Called when peer list is received: (publishers, subscribers)

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

        // P2P segment caching for late-joiners (publisher only)
        this.cachedStreamInfo = null;     // Codec metadata from server
        this.cachedInitSegment = null;    // ftyp+moov initialization segment
        this.cachedKeyframe = null;       // Most recent keyframe segment { header, buffer }
        this.segmentQueue = [];           // Rolling buffer of recent segments
        this.maxSegmentQueueSize = 30;    // Keep ~30 segments for late-joiners
        this.segmentSequence = 0;         // Monotonically increasing sequence number

        // P2P segment receiving state (subscriber)
        this.pendingSegmentHeader = new Map(); // peerId -> pending segment header

        // Relay state: allows subscribers to relay data to other browsers
        this.canRelay = false;          // true when subscriber has init segment + stream_info
        this.seenSequences = new Set(); // dedup: "${streamId}:${sequence}" keys
        this.maxSeenSequences = 500;    // prune oldest when exceeded
        this.maxRelayTTL = 3;           // max hops a segment can travel
        this.maxPeerConnections = 5;    // limit outbound peer connections

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
     * Switch to publisher mode and re-announce
     * Used when a subscriber becomes a publisher (P2P-first mode fallback)
     */
    becomePublisher() {
        if (this.isPublisher) {
            this.log('info', 'Already a publisher');
            return;
        }

        this.isPublisher = true;
        this.log('info', 'Switching to publisher mode');

        // Re-announce as publisher so other peers know
        this.sendSignaling({
            type: 'peer_update',
            peer_id: this.localPeerId,
            stream_id: this.streamId,
            is_publisher: true
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
                const publishers = message.peers.filter(p => p.is_publisher);
                const relayers = message.peers.filter(p => !p.is_publisher && p.can_relay);
                const subscribers = message.peers.filter(p => !p.is_publisher && !p.can_relay);
                this.log('info', `Received peer list: ${publishers.length} publishers, ${relayers.length} relayers, ${subscribers.length} subscribers`);

                // Notify callback
                if (this.onPeerListReceived) {
                    this.onPeerListReceived(publishers, subscribers);
                }

                // As subscriber, connect to publishers first, then relayers
                if (!this.isPublisher) {
                    // Prefer publishers (lower latency), then relayers
                    const sources = [...publishers, ...relayers];
                    for (const peer of sources) {
                        if (this.getConnectedPeerCount() >= this.maxPeerConnections) break;
                        await this.initiateConnection(peer.peer_id);
                    }
                }
                break;

            case 'peer_join':
                // New peer joined the stream
                if (message.peer_id !== this.localPeerId) {
                    this.log('info', `Peer joined: ${message.peer_id} (publisher: ${message.is_publisher})`);
                    // As subscriber, initiate connection to new publishers
                    if (!this.isPublisher && message.is_publisher) {
                        if (this.getConnectedPeerCount() < this.maxPeerConnections) {
                            await this.initiateConnection(message.peer_id);
                        }
                    }
                }
                break;

            case 'peer_update':
                // Peer updated its capabilities (e.g., became a relayer)
                if (message.peer_id !== this.localPeerId) {
                    this.log('info', `Peer updated: ${message.peer_id} (publisher: ${message.is_publisher}, can_relay: ${message.can_relay})`);
                    // As subscriber without enough sources, connect to new relayers
                    if (!this.isPublisher && message.can_relay && !this.peerConnections.has(message.peer_id)) {
                        if (this.getConnectedPeerCount() < this.maxPeerConnections) {
                            await this.initiateConnection(message.peer_id);
                        }
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

            // Publisher/relayer: send cached data to new peer (late-joiner support)
            if ((this.isPublisher || this.canRelay) && this.cachedStreamInfo) {
                this.log('info', `Sending cached stream data to new peer ${remotePeerId.substring(0, 8)}`);
                this.sendCachedDataToPeer(dc);
            }

            // Subscriber: request init data from publisher/relayer
            if (!this.isPublisher && !this.canRelay) {
                this.log('info', `Requesting init data from peer ${remotePeerId.substring(0, 8)}`);
                try {
                    dc.send(JSON.stringify({ type: 'request_init' }));
                } catch (e) {
                    this.log('warn', `Failed to send request_init: ${e.message}`);
                }
            }
        };

        dc.onclose = () => {
            this.log('info', `Data channel closed with ${remotePeerId}`);
            this.dataChannels.delete(remotePeerId);
            this.pendingSegmentHeader.delete(remotePeerId);
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
     * Supports P2P streaming protocol with segment headers and stream info
     */
    handleDataChannelMessage(peerId, data) {
        if (typeof data === 'string') {
            // JSON control message
            try {
                const msg = JSON.parse(data);
                this.handleControlMessage(peerId, msg);
            } catch (e) {
                this.log('warn', `Invalid control message: ${e.message}`);
            }
        } else if (data instanceof ArrayBuffer) {
            // Binary data (stream segment or chunk)
            const chunkState = this.incomingChunks.get(peerId);
            if (chunkState) {
                // Part of a chunked message
                chunkState.chunks.push(new Uint8Array(data));
                chunkState.totalSize += data.byteLength;
            } else {
                // Single message (not chunked) - process as segment
                this.processReceivedSegment(peerId, data);
            }
        }
    }

    /**
     * Handle JSON control messages from peers
     */
    handleControlMessage(peerId, msg) {
        switch (msg.type) {
            case 'chunk_start':
                // Start receiving a chunked message
                this.incomingChunks.set(peerId, {
                    chunks: [],
                    totalSize: 0,
                    expectedSize: msg.size
                });
                break;

            case 'chunk_end':
                // Assemble chunked message
                const state = this.incomingChunks.get(peerId);
                if (state) {
                    const assembled = this.assembleChunks(state.chunks);
                    this.incomingChunks.delete(peerId);
                    this.processReceivedSegment(peerId, assembled);
                }
                break;

            case 'stream_info':
                // Received stream metadata - cache regardless of role
                this.log('info', `Received stream_info from peer ${peerId.substring(0, 8)}`);
                this.cachedStreamInfo = msg.data;
                if (this.onStreamInfo) {
                    this.onStreamInfo(msg.data);
                }
                // Check if we can now relay (need both stream_info and init segment)
                this.checkRelayReady();
                break;

            case 'segment_header':
                // Store pending segment header - next binary message is the segment data
                this.pendingSegmentHeader.set(peerId, {
                    segment_type: msg.segment_type,
                    sequence: msg.sequence,
                    size: msg.size,
                    is_keyframe: msg.is_keyframe,
                    timestamp: msg.timestamp,
                    ttl: msg.ttl !== undefined ? msg.ttl : this.maxRelayTTL,
                    from_peer: peerId
                });
                this.log('debug', `Received segment header: ${msg.segment_type} seq=${msg.sequence} size=${msg.size} ttl=${msg.ttl}`);
                break;

            case 'request_init':
                // Peer is requesting initialization data (late-joiner)
                this.log('info', `Peer ${peerId.substring(0, 8)} requesting init data`);
                const dc = this.dataChannels.get(peerId);
                if (dc && dc.readyState === 'open') {
                    this.sendCachedDataToPeer(dc);
                }
                break;

            default:
                this.log('debug', `Unknown control message type: ${msg.type}`);
        }
    }

    /**
     * Process a received binary segment
     * @param {string} peerId - Peer that sent the segment
     * @param {ArrayBuffer} buffer - Segment data
     */
    processReceivedSegment(peerId, buffer) {
        const header = this.pendingSegmentHeader.get(peerId);
        this.pendingSegmentHeader.delete(peerId);

        if (header) {
            // We have segment metadata - use the new callback
            this.log('debug', `Processing segment: ${header.segment_type} seq=${header.sequence} (${buffer.byteLength} bytes)`);

            // Cache init segments for relay
            if (header.segment_type === 'initialization' || header.segment_type === 'init_video') {
                this.cachedInitSegment = buffer;
                this.checkRelayReady();
            }

            // Cache keyframes for late-joiners
            if (header.is_keyframe) {
                this.cachedKeyframe = { header, buffer };
            }

            if (this.onSegmentReceived) {
                this.onSegmentReceived(header, buffer);
            } else if (this.onStreamData) {
                // Fallback to legacy callback
                this.notifyStreamData(buffer);
            }

            // Relay forward if we can
            if (this.canRelay && !this.isPublisher) {
                const dedupKey = `${this.streamId}:${header.sequence}`;
                const ttl = (header.ttl !== undefined ? header.ttl : this.maxRelayTTL);
                if (!this.seenSequences.has(dedupKey) && ttl > 0) {
                    this.addSeenSequence(dedupKey);
                    // Add to segment queue for late-joiners
                    this.segmentQueue.push({ header, buffer });
                    while (this.segmentQueue.length > this.maxSegmentQueueSize) {
                        this.segmentQueue.shift();
                    }
                    this.relaySegmentToPeers(header, buffer, peerId);
                }
            }
        } else {
            // No header - legacy mode, just pass raw data
            this.notifyStreamData(buffer);
        }
    }

    /**
     * Check if this subscriber has enough data to start relaying
     */
    checkRelayReady() {
        if (!this.isPublisher && !this.canRelay && this.cachedStreamInfo && this.cachedInitSegment) {
            this.canRelay = true;
            this.log('info', 'canRelay = true (have init segment + stream_info)');

            // Announce relay capability via signaling
            this.sendSignaling({
                type: 'peer_update',
                peer_id: this.localPeerId,
                stream_id: this.streamId,
                is_publisher: false,
                can_relay: true
            });
        }
    }

    /**
     * Track seen sequences for dedup, prune when exceeding max
     */
    addSeenSequence(key) {
        this.seenSequences.add(key);
        if (this.seenSequences.size > this.maxSeenSequences) {
            // Prune oldest entries (Sets iterate in insertion order)
            const arr = Array.from(this.seenSequences);
            const toRemove = arr.slice(0, arr.length - this.maxSeenSequences);
            for (const k of toRemove) {
                this.seenSequences.delete(k);
            }
        }
    }

    /**
     * Relay a segment to all connected peers except the sender
     * @param {Object} header - Segment header with TTL
     * @param {ArrayBuffer} buffer - Segment data
     * @param {string} fromPeerId - Peer that sent us this segment (skip)
     */
    relaySegmentToPeers(header, buffer, fromPeerId) {
        const ttl = (header.ttl !== undefined ? header.ttl : this.maxRelayTTL) - 1;
        if (ttl <= 0) {
            this.log('debug', `TTL expired for segment seq=${header.sequence}, not relaying`);
            return;
        }

        const relayHeader = {
            type: 'segment_header',
            segment_type: header.segment_type,
            sequence: header.sequence,
            size: buffer.byteLength,
            is_keyframe: header.is_keyframe,
            timestamp: header.timestamp,
            ttl: ttl
        };

        let relayCount = 0;
        for (const [peerId, dc] of this.dataChannels) {
            // Don't relay back to sender
            if (peerId === fromPeerId) continue;
            if (dc.readyState === 'open') {
                this.sendSegmentToPeer(dc, relayHeader, buffer);
                relayCount++;
            }
        }
        if (relayCount > 0) {
            this.log('debug', `Relayed segment seq=${header.sequence} to ${relayCount} peers (ttl=${ttl})`);
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
        if (!this.isPublisher && !this.canRelay) {
            this.log('warn', 'Only publishers/relayers can send data');
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

    // =========================================================================
    // P2P Streaming: Publisher Methods
    // =========================================================================

    /**
     * Cache stream info for late-joiners (called by publisher when receiving from server)
     * @param {Object} streamInfo - Stream metadata (video/audio codec info)
     */
    cacheStreamInfo(streamInfo) {
        this.cachedStreamInfo = streamInfo;
        this.log('info', 'Cached stream info for late-joiners');
    }

    /**
     * Cache initialization segment for late-joiners
     * @param {ArrayBuffer} buffer - The init segment (ftyp+moov)
     */
    cacheInitSegment(buffer) {
        this.cachedInitSegment = buffer;
        this.log('info', `Cached init segment: ${buffer.byteLength} bytes`);
    }

    /**
     * Forward a segment to all connected peers (publisher function)
     * @param {ArrayBuffer} buffer - Segment data
     * @param {string} chunkType - Type of segment (initialization, video, audio, etc.)
     * @param {boolean} isKeyframe - Whether this is a keyframe segment
     */
    forwardSegmentToPeers(buffer, chunkType, isKeyframe = false) {
        if (!this.isPublisher && !this.canRelay) {
            this.log('warn', 'Only publishers/relayers can forward segments');
            return;
        }

        // Create segment header
        const sequence = this.segmentSequence++;
        const header = {
            type: 'segment_header',
            segment_type: chunkType,
            sequence: sequence,
            size: buffer.byteLength,
            is_keyframe: isKeyframe,
            timestamp: Date.now(),
            ttl: this.maxRelayTTL
        };

        // Cache special segments for late-joiners
        if (chunkType === 'initialization' || chunkType === 'init_video') {
            this.cachedInitSegment = buffer;
            this.log('info', `Cached init segment (${buffer.byteLength} bytes) for late-joiners`);
        }

        if (isKeyframe) {
            this.cachedKeyframe = { header, buffer };
            this.log('debug', `Cached keyframe segment #${sequence}`);
        }

        // Add to segment queue for late-joiners
        this.segmentQueue.push({ header, buffer });
        while (this.segmentQueue.length > this.maxSegmentQueueSize) {
            this.segmentQueue.shift();
        }

        // Send to all connected peers
        for (const [peerId, dc] of this.dataChannels) {
            if (dc.readyState === 'open') {
                this.sendSegmentToPeer(dc, header, buffer);
            }
        }
    }

    /**
     * Send a single segment to a specific peer
     * @param {RTCDataChannel} dc - The data channel
     * @param {Object} header - Segment header
     * @param {ArrayBuffer} buffer - Segment data
     */
    sendSegmentToPeer(dc, header, buffer) {
        try {
            // Send header first
            dc.send(JSON.stringify(header));

            // Send binary data (chunked if needed)
            if (buffer.byteLength > this.MAX_CHUNK_SIZE) {
                this.sendChunked(dc, buffer);
            } else {
                dc.send(buffer);
            }
        } catch (error) {
            this.log('error', `Failed to send segment: ${error.message}`);
        }
    }

    /**
     * Send cached data to a newly connected peer (late-joiner support)
     * @param {RTCDataChannel} dc - The data channel
     */
    sendCachedDataToPeer(dc) {
        if (!this.isPublisher && !this.canRelay) return;

        this.log('info', 'Sending cached data to new peer');

        try {
            // 1. Send stream info
            if (this.cachedStreamInfo) {
                dc.send(JSON.stringify({
                    type: 'stream_info',
                    data: this.cachedStreamInfo
                }));
                this.log('debug', 'Sent cached stream_info');
            }

            // 2. Send initialization segment
            if (this.cachedInitSegment) {
                const initHeader = {
                    type: 'segment_header',
                    segment_type: 'initialization',
                    sequence: -1,  // Special sequence for init
                    size: this.cachedInitSegment.byteLength,
                    is_keyframe: false,
                    timestamp: Date.now()
                };
                this.sendSegmentToPeer(dc, initHeader, this.cachedInitSegment);
                this.log('debug', 'Sent cached init segment');
            }

            // 3. Send most recent keyframe (for quick start)
            if (this.cachedKeyframe) {
                this.sendSegmentToPeer(dc, this.cachedKeyframe.header, this.cachedKeyframe.buffer);
                this.log('debug', 'Sent cached keyframe');
            }

            // 4. Send recent segments from queue (after keyframe)
            let sentCount = 0;
            for (const seg of this.segmentQueue) {
                // Skip if this is the keyframe we already sent
                if (this.cachedKeyframe && seg.header.sequence === this.cachedKeyframe.header.sequence) {
                    continue;
                }
                // Only send segments after the keyframe
                if (this.cachedKeyframe && seg.header.sequence > this.cachedKeyframe.header.sequence) {
                    this.sendSegmentToPeer(dc, seg.header, seg.buffer);
                    sentCount++;
                }
            }
            if (sentCount > 0) {
                this.log('debug', `Sent ${sentCount} queued segments to new peer`);
            }
        } catch (error) {
            this.log('error', `Failed to send cached data: ${error.message}`);
        }
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
