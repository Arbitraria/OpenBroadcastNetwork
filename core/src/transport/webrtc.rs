//! WebRTC transport implementation
//!
//! This module provides a WebRTC-based transport implementation suitable for
//! browser clients and compatible native applications. Uses data channels for
//! real-time peer-to-peer streaming in the decentralized CDN.

use crate::transport::interface::{
    Transport, TransportEvent, TransportError, Connection, ConnectionId
};
use std::future::Future;
use std::pin::Pin;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::interceptor::registry::Registry;
use tracing::{debug, error, info, warn};

/// Configuration for WebRTC transport
#[derive(Debug, Clone)]
pub struct WebRtcConfig {
    /// STUN servers for ICE negotiation
    pub stun_servers: Vec<String>,
    /// TURN servers for fallback relay
    pub turn_servers: Vec<(String, String, String)>, // (url, username, credential)
    /// ICE transport policy
    pub ice_transport_policy: IceTransportPolicy,
}

/// ICE transport policy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IceTransportPolicy {
    /// Allow all transports (default)
    All,
    /// Only allow relay transports
    Relay,
}

/// Represents an active WebRTC data channel connection
#[derive(Clone)]
pub struct WebRtcConnection {
    pub id: ConnectionId,
    pub peer_connection: Arc<RTCPeerConnection>,
    pub data_channel: Arc<RTCDataChannel>,
    pub remote_addr: Option<SocketAddr>,
}

/// WebRTC transport implementation for P2P streaming
pub struct WebRtcTransport {
    config: WebRtcConfig,
    connections: Arc<RwLock<HashMap<ConnectionId, WebRtcConnection>>>,
    running: Arc<Mutex<bool>>,
    event_tx: Arc<Mutex<Option<mpsc::UnboundedSender<TransportEvent>>>>,
    api: Arc<webrtc::api::API>,
}

impl WebRtcTransport {
    /// Create a new WebRTC transport
    pub fn new(config: WebRtcConfig) -> Result<Self, TransportError> {
        // Create WebRTC API with media engine and interceptors
        let mut media_engine = MediaEngine::default();
        media_engine.register_default_codecs().map_err(|e| {
            TransportError::InitializationFailed(format!("Failed to register codecs: {}", e))
        })?;

        let mut registry = Registry::new();
        registry = register_default_interceptors(registry, &mut media_engine).map_err(|e| {
            TransportError::InitializationFailed(format!("Failed to register interceptors: {}", e))
        })?;

        let api = APIBuilder::new()
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .build();

        Ok(Self {
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(Mutex::new(false)),
            event_tx: Arc::new(Mutex::new(None)),
            api: Arc::new(api),
        })
    }

    /// Create ICE server configuration from our config
    fn create_ice_servers(&self) -> Vec<RTCIceServer> {
        let mut servers = Vec::new();

        // Add STUN servers
        for stun_server in &self.config.stun_servers {
            servers.push(RTCIceServer {
                urls: vec![stun_server.clone()],
                ..Default::default()
            });
        }

        // Add TURN servers
        for (url, username, credential) in &self.config.turn_servers {
            servers.push(RTCIceServer {
                urls: vec![url.clone()],
                username: username.clone(),
                credential: credential.clone(),
                ..Default::default()
            });
        }

        servers
    }

    /// Generate a unique connection ID
    fn generate_connection_id() -> ConnectionId {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let id: [u8; 16] = rng.gen();
        ConnectionId(id.to_vec())
    }

    /// Set up event channel for transport events
    pub fn set_event_channel(&self, tx: mpsc::UnboundedSender<TransportEvent>) {
        if let Ok(mut event_tx) = self.event_tx.try_lock() {
            *event_tx = Some(tx);
        }
    }

    /// Send an event to the event channel
    async fn send_event(&self, event: TransportEvent) {
        if let Ok(event_tx_guard) = self.event_tx.lock().await {
            if let Some(ref tx) = *event_tx_guard {
                let _ = tx.send(event);
            }
        }
    }
}

impl Transport for WebRtcTransport {
    fn start(&mut self) -> Result<(), TransportError> {
        tokio::spawn({
            let running = Arc::clone(&self.running);
            async move {
                let mut running_guard = running.lock().await;
                *running_guard = true;
                info!("WebRTC transport started");
            }
        });
        Ok(())
    }
    
    fn stop(&mut self) -> Result<(), TransportError> {
        tokio::spawn({
            let running = Arc::clone(&self.running);
            let connections = Arc::clone(&self.connections);
            async move {
                let mut running_guard = running.lock().await;
                *running_guard = false;
                
                // Close all connections
                let connections_guard = connections.read().await;
                for (conn_id, connection) in connections_guard.iter() {
                    debug!("Closing WebRTC connection: {:?}", conn_id);
                    let _ = connection.peer_connection.close().await;
                }
                info!("WebRTC transport stopped");
            }
        });
        Ok(())
    }
    
    fn connect(&mut self, addr: SocketAddr) -> Pin<Box<dyn Future<Output = Result<Connection, TransportError>> + Send>> {
        let api = Arc::clone(&self.api);
        let ice_servers = self.create_ice_servers();
        let connections = Arc::clone(&self.connections);
        let event_tx = Arc::clone(&self.event_tx);
        
        Box::pin(async move {
            // Create RTCConfiguration with ICE servers
            let config = RTCConfiguration {
                ice_servers,
                ..Default::default()
            };

            // Create peer connection
            let peer_connection = api.new_peer_connection(config).await.map_err(|e| {
                TransportError::ConnectionFailed(format!("Failed to create peer connection: {}", e))
            })?;

            let connection_id = Self::generate_connection_id();
            
            // Create data channel for streaming
            let data_channel = peer_connection
                .create_data_channel("streaming", None)
                .await
                .map_err(|e| {
                    TransportError::ConnectionFailed(format!("Failed to create data channel: {}", e))
                })?;

            // Set up data channel message handler
            let event_tx_clone = Arc::clone(&event_tx);
            let conn_id_clone = connection_id.clone();
            data_channel.on_message(Box::new(move |msg| {
                let event_tx = Arc::clone(&event_tx_clone);
                let conn_id = conn_id_clone.clone();
                Box::pin(async move {
                    if let Ok(event_tx_guard) = event_tx.lock().await {
                        if let Some(ref tx) = *event_tx_guard {
                            let event = TransportEvent::DataReceived(conn_id, msg.data.to_vec());
                            let _ = tx.send(event);
                        }
                    }
                })
            }));

            // Set up peer connection state change handler
            let event_tx_clone = Arc::clone(&event_tx);
            let conn_id_clone = connection_id.clone();
            peer_connection.on_peer_connection_state_change(Box::new(move |state| {
                let event_tx = Arc::clone(&event_tx_clone);
                let conn_id = conn_id_clone.clone();
                Box::pin(async move {
                    match state {
                        RTCPeerConnectionState::Connected => {
                            info!("WebRTC peer connection established: {:?}", conn_id);
                        }
                        RTCPeerConnectionState::Disconnected | RTCPeerConnectionState::Closed => {
                            warn!("WebRTC peer connection closed: {:?}", conn_id);
                            if let Ok(event_tx_guard) = event_tx.lock().await {
                                if let Some(ref tx) = *event_tx_guard {
                                    let event = TransportEvent::ConnectionClosed(conn_id);
                                    let _ = tx.send(event);
                                }
                            }
                        }
                        RTCPeerConnectionState::Failed => {
                            error!("WebRTC peer connection failed: {:?}", conn_id);
                            if let Ok(event_tx_guard) = event_tx.lock().await {
                                if let Some(ref tx) = *event_tx_guard {
                                    let error = TransportError::ConnectionFailed("Peer connection failed".to_string());
                                    let event = TransportEvent::Error(error);
                                    let _ = tx.send(event);
                                }
                            }
                        }
                        _ => {}
                    }
                })
            }));

            let webrtc_connection = WebRtcConnection {
                id: connection_id.clone(),
                peer_connection: Arc::new(peer_connection),
                data_channel: Arc::new(data_channel),
                remote_addr: Some(addr),
            };

            // Store connection
            {
                let mut connections_guard = connections.write().await;
                connections_guard.insert(connection_id.clone(), webrtc_connection.clone());
            }

            // Send connection established event
            if let Ok(event_tx_guard) = event_tx.lock().await {
                if let Some(ref tx) = *event_tx_guard {
                    let connection = Connection {
                        id: connection_id.clone(),
                        remote_addr: Some(addr),
                        remote_peer_id: None,
                    };
                    let event = TransportEvent::ConnectionEstablished(connection.clone());
                    let _ = tx.send(event);
                    
                    return Ok(connection);
                }
            }

            Ok(Connection {
                id: connection_id,
                remote_addr: Some(addr),
                remote_peer_id: None,
            })
        })
    }
    
    fn send(&mut self, conn_id: &ConnectionId, data: Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send>> {
        let connections = Arc::clone(&self.connections);
        let conn_id = conn_id.clone();
        
        Box::pin(async move {
            let connections_guard = connections.read().await;
            if let Some(connection) = connections_guard.get(&conn_id) {
                connection.data_channel
                    .send(&webrtc::data_channel::data_channel_message::DataChannelMessage {
                        data: bytes::Bytes::from(data),
                    })
                    .await
                    .map_err(|e| TransportError::SendError(format!("Failed to send data: {}", e)))
            } else {
                Err(TransportError::SendError("Connection not found".to_string()))
            }
        })
    }
    
    fn close_connection(&mut self, conn_id: &ConnectionId) -> Result<(), TransportError> {
        let connections = Arc::clone(&self.connections);
        let conn_id = conn_id.clone();
        
        tokio::spawn(async move {
            let mut connections_guard = connections.write().await;
            if let Some(connection) = connections_guard.remove(&conn_id) {
                let _ = connection.peer_connection.close().await;
                debug!("Closed WebRTC connection: {:?}", conn_id);
            }
        });
        
        Ok(())
    }
    
    fn is_running(&self) -> bool {
        if let Ok(running_guard) = self.running.try_lock() {
            *running_guard
        } else {
            false
        }
    }
    
    fn next_event(&mut self) -> Pin<Box<dyn Future<Output = Option<TransportEvent>> + Send>> {
        // Events are now handled through the event channel system
        // This method can be used for polling if needed
        Box::pin(async { None })
    }
} 