//! Event handlers for the libp2p overlay
//!
//! This module contains handlers for libp2p network events used by the overlay implementation.

// Core library imports
use std::time::Duration;
use futures::SinkExt;
use tracing::{debug, error, info, warn};

// Local imports
use crate::overlay::interface::{OverlayError, OverlayEvent, StreamId};
use crate::overlay::relay::StreamChunk;
use crate::overlay::peer::{ConnectionStatus, Peer, LocalPeerId};
use crate::overlay::libp2p::behavior::OverlayBehaviorEvent;
use crate::overlay::libp2p::impl_core::Libp2pOverlay;
use crate::overlay::libp2p::topics;
use crate::overlay::libp2p::types::{to_libp2p_peer_id, from_libp2p_peer_id};

// External dependencies
use libp2p::gossipsub::Event as GossipsubEvent;
use libp2p::identify::Event as IdentifyEvent;
use libp2p::kad::Event as KademliaEvent;
use libp2p::swarm::SwarmEvent;

impl Libp2pOverlay {
    /// Process swarm events
    pub async fn process_swarm_events(&self) -> Result<(), OverlayError> {
        let mut swarm_lock = self.swarm.lock().await;
        
        let swarm = match &mut *swarm_lock {
            Some(swarm) => swarm,
            None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
        };
        
        // Poll for events using futures::StreamExt
        use futures::StreamExt;
        match swarm.next().await {
            Some(event) => match event {
            SwarmEvent::Behaviour(behavior_event) => {
                self.handle_behavior_event(behavior_event).await?
            },
            SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                let peer_id = from_libp2p_peer_id(&peer_id);
                info!("Connection established with peer: {}", peer_id);
                
                // Update peer state
                {
                    let mut peers = self.peers.write().await;
                    if let Some(peer) = peers.get_mut(&peer_id) {
                        peer.set_connected(crate::overlay::peer::PeerConnection {
                            peer_id: peer_id.clone(),
                            is_outbound: false, // assume inbound for now
                            status: crate::overlay::peer::ConnectionStatus::Connected,
                            connected_at: Some(std::time::Instant::now()),
                            last_activity: std::time::Instant::now(),
                            bytes_sent: 0,
                            bytes_received: 0,
                            latency_ms: None,
                            failed_attempts: 0,
                            is_blocked: false,
                        });
                    } else {
                        // Create PeerInfo for new peer
                        let peer_info = crate::overlay::peer::PeerInfo {
                            id: peer_id.clone(),
                            addresses: Vec::new(),
                            role: crate::overlay::peer::PeerRole::Unknown,
                            status: crate::overlay::peer::ConnectionStatus::Connected,
                            last_seen: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            protocols: Vec::new(),
                            metadata: std::collections::HashMap::new(),
                            latency_ms: None,
                            region: None,
                            bandwidth_capacity: None,
                        };
                        let mut peer = Peer::new(peer_id.clone(), peer_info);
                        peer.set_connected(crate::overlay::peer::PeerConnection {
                            peer_id: peer_id.clone(),
                            is_outbound: false, // assume inbound for now
                            status: crate::overlay::peer::ConnectionStatus::Connected,
                            connected_at: Some(std::time::Instant::now()),
                            last_activity: std::time::Instant::now(),
                            bytes_sent: 0,
                            bytes_received: 0,
                            latency_ms: None,
                            failed_attempts: 0,
                            is_blocked: false,
                        });
                        peers.insert(peer_id.clone(), peer);
                    }
                }
                
                // Emit event
                let peer_info = {
                    let peers = self.peers.read().await;
                    peers.get(&peer_id).map(|p| p.info.clone()).unwrap_or_else(|| {
                        crate::overlay::peer::PeerInfo {
                            id: peer_id.clone(),
                            addresses: Vec::new(),
                            role: crate::overlay::peer::PeerRole::Unknown,
                            status: crate::overlay::peer::ConnectionStatus::Connected,
                            last_seen: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            protocols: Vec::new(),
                            metadata: std::collections::HashMap::new(),
                            latency_ms: None,
                            region: None,
                            bandwidth_capacity: None,
                        }
                    })
                };
                let mut event_tx = self.event_tx.clone();
                let _ = event_tx.send(OverlayEvent::PeerConnected {
                    peer_id: peer_id.clone(),
                    info: peer_info,
                }).await;
            },
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                let peer_id = from_libp2p_peer_id(&peer_id);
                info!("Connection closed with peer: {}", peer_id);
                
                // Update peer state
                {
                    let mut peers = self.peers.write().await;
                    if let Some(peer) = peers.get_mut(&peer_id) {
                        peer.set_disconnected();
                    }
                }
                
                // Emit event
                let mut event_tx = self.event_tx.clone();
                let _ = event_tx.send(OverlayEvent::PeerDisconnected {
                    peer_id: peer_id.clone(),
                    reason: "Connection closed".to_string(),
                }).await;
            },
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {}", address);
            },
            event => {
                debug!("Unhandled swarm event: {:?}", event);
            }
            },
            None => {
                debug!("Swarm stream ended");
            }
        }
        
        Ok(())
    }

    /// Handle behavior events
    pub async fn handle_behavior_event(&self, event: OverlayBehaviorEvent) -> Result<(), OverlayError> {
        match event {
            OverlayBehaviorEvent::Gossipsub(gossipsub_event) => {
                self.handle_gossipsub_event(gossipsub_event).await?
            },
            OverlayBehaviorEvent::Kademlia(kad_event) => {
                self.handle_kademlia_event(kad_event).await?
            },
            OverlayBehaviorEvent::Identify(identify_event) => {
                self.handle_identify_event(identify_event).await?
            },
        }
        
        Ok(())
    }

    /// Handle gossipsub events
    pub async fn handle_gossipsub_event(&self, event: GossipsubEvent) -> Result<(), OverlayError> {
        match event {
            GossipsubEvent::Message {
                propagation_source,
                message_id,
                message,
                ..
            } => {
                let topic = message.topic.as_str();
                let data = message.data;
                let source_peer_id = from_libp2p_peer_id(&propagation_source);
                
                // Check if this is a stream data message
                if let Some(stream_id) = topics::parse_stream_id(topic) {
                    if topic.ends_with("/data") {
                        debug!("Received data message for stream {}", stream_id);
                        
                        // Forward to relay
                        let relay_node = self.relay.relay_node();
                        let chunk = StreamChunk {
                            id: 0, // Chunk ID would be generated/extracted from message
                            stream_id: stream_id.clone(),
                            data: data.clone(), // Clone data for chunk
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            sequence: 0, // Sequence number would be extracted from message
                            content_type: "application/octet-stream".to_string(), // Default content type
                            is_keyframe: false, // Would be determined from message metadata
                            source: Some(to_libp2p_peer_id(&source_peer_id)?),
                        };
                        
                        relay_node.publish_chunk(chunk).await?;
                        
                        // Add to active streams
                        {
                            let mut streams = self.streams.write().await;
                            streams.insert(stream_id.clone());
                        }
                    } else if topic.ends_with("/control") {
                        debug!("Received control message for stream {}", stream_id);
                        
                        // Process control message (implementation omitted for brevity)
                    }
                    
                    // Emit event
                    let mut event_tx = self.event_tx.clone();
                    let _ = event_tx.send(OverlayEvent::StreamData {
                        stream_id: stream_id.clone(),
                        source: source_peer_id,
                        data: data, // Use data directly since we cloned for chunk
                    }).await;
                } else if topic == topics::discovery() {
                    debug!("Received discovery message from {}", source_peer_id);
                    // Process discovery message (implementation omitted for brevity)
                }
            },
            GossipsubEvent::Subscribed { peer_id, topic } => {
                debug!("Peer {} subscribed to topic {}", peer_id, topic);
                
                // Check if this is a stream topic
                if let Some(stream_id) = topics::parse_stream_id(topic.as_str()) {
                    let peer_id = from_libp2p_peer_id(&peer_id);
                    
                    // If it's a data topic, add as subscriber
                    if topic.as_str().ends_with("/data") {
                        let relay_node = self.relay.relay_node();
                        // Convert LocalPeerId to libp2p::PeerId before passing to relay_node
                        let libp2p_peer_id = to_libp2p_peer_id(&peer_id)?;
                        relay_node.add_subscriber(&stream_id, libp2p_peer_id).await?;
                    }
                }
            },
            GossipsubEvent::Unsubscribed { peer_id, topic } => {
                debug!("Peer {} unsubscribed from topic {}", peer_id, topic);
                
                // Check if this is a stream topic
                if let Some(stream_id) = topics::parse_stream_id(topic.as_str()) {
                    let peer_id = from_libp2p_peer_id(&peer_id);
                    
                    // If it's a data topic, remove as subscriber
                    if topic.as_str().ends_with("/data") {
                        let relay_node = self.relay.relay_node();
                        // Convert LocalPeerId to libp2p::PeerId before passing to relay_node
                        let libp2p_peer_id = to_libp2p_peer_id(&peer_id)?;
                        relay_node.remove_subscriber(&stream_id, &libp2p_peer_id).await?;
                    }
                }
            },
            _ => {}
        }
        
        Ok(())
    }

    /// Handle Kademlia events
    pub async fn handle_kademlia_event(&self, event: KademliaEvent) -> Result<(), OverlayError> {
        match event {
            KademliaEvent::OutboundQueryProgressed { result, .. } => {
                match result {
                    libp2p::kad::QueryResult::GetClosestPeers(Ok(closest)) => {
                        for peer in closest.peers {
                            debug!("Found closest peer: {}", peer);
                        }
                    },
                    _ => {}
                }
            },
            _ => {}
        }
        
        Ok(())
    }

    /// Handle Identify events
    pub async fn handle_identify_event(&self, event: IdentifyEvent) -> Result<(), OverlayError> {
        match event {
            IdentifyEvent::Received {
                peer_id,
                info,
                ..
            } => {
                debug!("Identified peer {} with protocol version {}", peer_id, info.protocol_version);
                
                let peer_id = from_libp2p_peer_id(&peer_id);
                
                // Update peer information
                {
                    let mut peers = self.peers.write().await;
                    if let Some(peer) = peers.get_mut(&peer_id) {
                        // Add addresses
                        for addr in info.listen_addrs {
                            peer.info.addresses.push(addr.to_string());
                        }
                        
                        // Add protocols
                        peer.info.protocols = info.protocols.iter()
                            .map(|p| p.to_string())
                            .collect();
                            
                        // Update metadata
                        peer.info.metadata.insert("agent_version".to_string(), info.agent_version);
                        peer.info.metadata.insert("protocol_version".to_string(), info.protocol_version);
                    }
                }
            },
            _ => {}
        }
        
        Ok(())
    }

    /// Helper to rebalance topology after network changes
    pub async fn rebalance_topology(&self) -> Result<(), OverlayError> {
        let streams = self.streams.read().await;
        for stream_id in streams.iter() {
            // In a real implementation, we'd rebalance the tree and mesh for this stream
        }
        Ok(())
    }
}
