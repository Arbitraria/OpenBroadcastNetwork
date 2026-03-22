//! Event handlers for the libp2p overlay
//!
//! This module contains handlers for libp2p network events used by the overlay implementation.
//!
//! Supports the bincode `WireSegment` format for P2P media transmission.

// Core library imports
use std::time::Duration;
use tracing::{debug, info, warn};

// Local imports
use crate::media::segment::MediaType;
use crate::media::wire_format::WireSegment;
use crate::overlay::interface::{OverlayError, OverlayEvent};
use crate::overlay::libp2p::behavior::OverlayBehaviorEvent;
use crate::overlay::libp2p::impl_core::Libp2pOverlay;
use crate::discovery::stream_discovery::STREAM_ANNOUNCE_TOPIC;
use crate::overlay::libp2p::topics;
use crate::overlay::libp2p::types::{from_libp2p_peer_id, to_libp2p_peer_id};
use crate::overlay::peer::Peer;
use crate::overlay::relay::StreamChunk;

// External dependencies
use libp2p::autonat;
use libp2p::dcutr;
use libp2p::gossipsub::Event as GossipsubEvent;
use libp2p::identify::Event as IdentifyEvent;
use libp2p::kad::Event as KademliaEvent;
use libp2p::relay;
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

        // Use timeout to avoid holding the lock indefinitely, allowing other tasks to access the swarm
        match tokio::time::timeout(Duration::from_millis(10), swarm.next()).await {
            Ok(Some(event)) => match event {
                SwarmEvent::Behaviour(behavior_event) => {
                    self.handle_behavior_event(behavior_event).await?
                }
                SwarmEvent::ConnectionEstablished {
                    peer_id, endpoint: _endpoint, ..
                } => {
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
                        peers
                            .get(&peer_id)
                            .map(|p| p.info.clone())
                            .unwrap_or_else(|| crate::overlay::peer::PeerInfo {
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
                            })
                    };
                    if let Err(e) = self.event_tx.clone().try_send(
                        OverlayEvent::PeerConnected {
                            peer_id: peer_id.clone(),
                            info: peer_info,
                        },
                    ) {
                        warn!("Event channel full, dropping PeerConnected: {:?}", e);
                    }
                }
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
                    if let Err(e) = self.event_tx.clone().try_send(
                        OverlayEvent::PeerDisconnected {
                            peer_id: peer_id.clone(),
                            reason: "Connection closed".to_string(),
                        },
                    ) {
                        warn!("Event channel full, dropping PeerDisconnected: {:?}", e);
                    }
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Listening on {}", address);
                }
                event => {
                    debug!("Unhandled swarm event: {:?}", event);
                }
            },
            Ok(None) => {
                debug!("Swarm stream ended");
            }
            Err(_) => {
                // Timeout exceeded, release lock to allow other tasks to access swarm
            }
        }

        Ok(())
    }

    /// Handle behavior events
    pub async fn handle_behavior_event(
        &self,
        event: OverlayBehaviorEvent,
    ) -> Result<(), OverlayError> {
        match event {
            OverlayBehaviorEvent::Gossipsub(gossipsub_event) => {
                self.handle_gossipsub_event(gossipsub_event).await?
            }
            OverlayBehaviorEvent::Kademlia(kad_event) => {
                self.handle_kademlia_event(kad_event).await?
            }
            OverlayBehaviorEvent::Identify(identify_event) => {
                self.handle_identify_event(identify_event).await?
            }
            OverlayBehaviorEvent::RelayClient(relay_event) => {
                self.handle_relay_client_event(relay_event).await?
            }
            OverlayBehaviorEvent::Autonat(autonat_event) => {
                self.handle_autonat_event(autonat_event).await?
            }
            OverlayBehaviorEvent::Dcutr(dcutr_event) => {
                self.handle_dcutr_event(dcutr_event).await?
            }
        }

        Ok(())
    }

    /// Handle gossipsub events
    pub async fn handle_gossipsub_event(&self, event: GossipsubEvent) -> Result<(), OverlayError> {
        match event {
            GossipsubEvent::Message {
                propagation_source,
                message_id: _message_id,
                message,
                ..
            } => {
                let topic = message.topic.as_str();
                let data = message.data;
                let source_peer_id = from_libp2p_peer_id(&propagation_source);

                // Check if this is a stream data message
                if let Some(stream_id) = topics::parse_stream_id(topic) {
                    if topic.ends_with("/data") {
                        debug!(
                            "Received data message for stream {} ({} bytes)",
                            stream_id,
                            data.len()
                        );

                        // Deserialize as bincode WireSegment format
                        let (sequence, timestamp, is_keyframe, content_type) =
                            if let Ok(wire_segment) = WireSegment::from_bytes(&data)
                        {
                            // Verify content integrity
                            if !wire_segment.verify_integrity() {
                                warn!(
                                    "Content hash mismatch for stream {} seq {}",
                                    stream_id, wire_segment.sequence
                                );
                                return Ok(());
                            }
                            debug!(
                                "Deserialized WireSegment: type={}, seq={}, keyframe={}",
                                wire_segment.media_type,
                                wire_segment.sequence,
                                wire_segment.is_keyframe
                            );
                            let content_type = match MediaType::from_u8(wire_segment.media_type) {
                                Some(MediaType::Video) => "video/h264",
                                Some(MediaType::Audio) => "audio/opus",
                                Some(MediaType::Metadata) => "application/json",
                                Some(MediaType::Initialization) => "video/mp4",
                                None => "application/octet-stream",
                            };
                            (
                                wire_segment.sequence,
                                wire_segment.pts_us / 1000, // convert us to ms
                                wire_segment.is_keyframe,
                                content_type.to_string(),
                            )
                        } else {
                            warn!("Failed to deserialize as WireSegment, using defaults");
                            (
                                0,
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                                false,
                                "application/octet-stream".to_string(),
                            )
                        };

                        // Forward to relay
                        let relay_node = self.relay.relay_node();
                        let chunk = StreamChunk {
                            id: sequence,
                            stream_id: stream_id.clone(),
                            data: bytes::Bytes::from(data.clone()),
                            timestamp,
                            sequence,
                            content_type,
                            is_keyframe,
                            source: Some(to_libp2p_peer_id(&source_peer_id)),
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
                    if let Err(e) = self.event_tx.clone().try_send(
                        OverlayEvent::StreamData {
                            stream_id: stream_id.clone(),
                            source: source_peer_id,
                            data,
                        },
                    ) {
                        warn!("Event channel full, dropping StreamData: {:?}", e);
                    }
                } else if topic == topics::discovery() {
                    debug!("Received discovery message from {}", source_peer_id);
                    // Process discovery message (implementation omitted for brevity)
                } else if topic == STREAM_ANNOUNCE_TOPIC {
                    debug!(
                        "Received stream announcement from {} ({} bytes)",
                        source_peer_id,
                        data.len()
                    );

                    // Try to extract stream_id from the announcement data
                    let stream_id = match serde_json::from_slice::<serde_json::Value>(&data) {
                        Ok(val) => {
                            if let Some(sid) = val.get("stream_id") {
                                // StreamId is serialized as bytes array
                                if let Some(bytes) = sid.as_array() {
                                    let id_bytes: Vec<u8> = bytes
                                        .iter()
                                        .filter_map(|v| v.as_u64().map(|n| n as u8))
                                        .collect();
                                    crate::overlay::interface::StreamId::from_bytes(id_bytes)
                                } else {
                                    crate::overlay::interface::StreamId::from_string("unknown")
                                }
                            } else {
                                crate::overlay::interface::StreamId::from_string("unknown")
                            }
                        }
                        Err(_) => crate::overlay::interface::StreamId::from_string("unknown"),
                    };

                    if let Err(e) = self.event_tx.clone().try_send(
                        OverlayEvent::StreamAnnounced {
                            stream_id,
                            data,
                        },
                    ) {
                        warn!("Event channel full, dropping StreamAnnounced: {:?}", e);
                    }
                }
            }
            GossipsubEvent::Subscribed { peer_id, topic } => {
                debug!("Peer {} subscribed to topic {}", peer_id, topic);

                // Check if this is a stream topic
                if let Some(stream_id) = topics::parse_stream_id(topic.as_str()) {
                    let peer_id = from_libp2p_peer_id(&peer_id);

                    // If it's a data topic, add as subscriber
                    if topic.as_str().ends_with("/data") {
                        let relay_node = self.relay.relay_node();
                        // Convert LocalPeerId to libp2p::PeerId (zero-cost)
                        let libp2p_peer_id = to_libp2p_peer_id(&peer_id);
                        relay_node
                            .add_subscriber(&stream_id, libp2p_peer_id)
                            .await?;
                    }
                }
            }
            GossipsubEvent::Unsubscribed { peer_id, topic } => {
                debug!("Peer {} unsubscribed from topic {}", peer_id, topic);

                // Check if this is a stream topic
                if let Some(stream_id) = topics::parse_stream_id(topic.as_str()) {
                    let peer_id = from_libp2p_peer_id(&peer_id);

                    // If it's a data topic, remove as subscriber
                    if topic.as_str().ends_with("/data") {
                        let relay_node = self.relay.relay_node();
                        // Convert LocalPeerId to libp2p::PeerId (zero-cost)
                        let libp2p_peer_id = to_libp2p_peer_id(&peer_id);
                        relay_node
                            .remove_subscriber(&stream_id, &libp2p_peer_id)
                            .await?;
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Handle Kademlia events
    pub async fn handle_kademlia_event(&self, event: KademliaEvent) -> Result<(), OverlayError> {
        if let KademliaEvent::OutboundQueryProgressed {
            result: libp2p::kad::QueryResult::GetClosestPeers(Ok(closest)),
            ..
        } = event
        {
            for peer in closest.peers {
                debug!("Found closest peer: {}", peer);
            }
        }

        Ok(())
    }

    /// Handle Identify events
    pub async fn handle_identify_event(&self, event: IdentifyEvent) -> Result<(), OverlayError> {
        if let IdentifyEvent::Received { peer_id, info, .. } = event {
            debug!(
                "Identified peer {} with protocol version {}",
                peer_id, info.protocol_version
            );

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
                    peer.info.protocols =
                        info.protocols.iter().map(|p| p.to_string()).collect();

                    // Update metadata
                    peer.info
                        .metadata
                        .insert("agent_version".to_string(), info.agent_version);
                    peer.info
                        .metadata
                        .insert("protocol_version".to_string(), info.protocol_version);
                }
            }
        }

        Ok(())
    }

    /// Handle relay client events for NAT traversal
    pub async fn handle_relay_client_event(
        &self,
        event: relay::client::Event,
    ) -> Result<(), OverlayError> {
        match event {
            relay::client::Event::ReservationReqAccepted {
                relay_peer_id,
                renewal,
                ..
            } => {
                if renewal {
                    info!("Relay reservation renewed with {}", relay_peer_id);
                } else {
                    info!("Relay reservation accepted by {}", relay_peer_id);
                }
            }
            relay::client::Event::OutboundCircuitEstablished {
                relay_peer_id,
                ..
            } => {
                info!("Outbound circuit established through relay {}", relay_peer_id);
            }
            relay::client::Event::InboundCircuitEstablished { src_peer_id, .. } => {
                info!("Inbound circuit established from {}", src_peer_id);
            }
        }
        Ok(())
    }

    /// Handle AutoNAT events for NAT status detection
    pub async fn handle_autonat_event(
        &self,
        event: autonat::Event,
    ) -> Result<(), OverlayError> {
        match event {
            autonat::Event::StatusChanged { old, new } => {
                info!("NAT status changed: {:?} -> {:?}", old, new);
                match new {
                    autonat::NatStatus::Public(addr) => {
                        info!("NAT status: Public, reachable at {}", addr);
                    }
                    autonat::NatStatus::Private => {
                        info!("NAT status: Private, using relay for connectivity");
                    }
                    autonat::NatStatus::Unknown => {
                        debug!("NAT status: Unknown, probing...");
                    }
                }
            }
            autonat::Event::InboundProbe(probe) => {
                debug!("AutoNAT inbound probe: {:?}", probe);
            }
            autonat::Event::OutboundProbe(probe) => {
                debug!("AutoNAT outbound probe: {:?}", probe);
            }
        }
        Ok(())
    }

    /// Handle DCUtR events for direct connection upgrade through relay
    pub async fn handle_dcutr_event(
        &self,
        event: dcutr::Event,
    ) -> Result<(), OverlayError> {
        // DCUtR Event has fields: remote_peer_id and result
        match &event.result {
            Ok(connection_id) => {
                info!(
                    "Direct connection upgrade succeeded with {} (connection: {:?})",
                    event.remote_peer_id, connection_id
                );
            }
            Err(error) => {
                warn!(
                    "Direct connection upgrade failed with {}: {}",
                    event.remote_peer_id, error
                );
            }
        }
        Ok(())
    }

    /// Helper to rebalance topology after network changes
    pub async fn rebalance_topology(&self) -> Result<(), OverlayError> {
        let streams = self.streams.read().await;
        for _stream_id in streams.iter() {
            // In a real implementation, we'd rebalance the tree and mesh for this stream
        }
        Ok(())
    }
}
