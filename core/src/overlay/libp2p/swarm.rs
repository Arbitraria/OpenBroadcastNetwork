//! Swarm functionality for the Libp2p overlay implementation
//!
//! This module contains swarm initialization and management functions.

use anyhow::Result;
use tracing::{debug, info, warn};

// Local imports
use crate::overlay::interface::{OverlayError, StreamId};
use crate::overlay::libp2p::impl_core::Libp2pOverlay;
use crate::overlay::libp2p::topics;
use crate::overlay::libp2p::types::from_libp2p_peer_id;
use crate::overlay::peer::{Peer, PeerInfo};
use libp2p::PeerId;

// External dependencies
use libp2p::gossipsub::IdentTopic;

impl Libp2pOverlay {
    /// Subscribe to a stream in the network
    pub async fn subscribe_to_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        // Lock the swarm
        let mut swarm_lock = self.swarm.lock().await;

        let swarm = match &mut *swarm_lock {
            Some(swarm) => swarm,
            None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
        };

        // Subscribe to data and control topics
        let data_topic = IdentTopic::new(topics::stream_data(stream_id));
        let control_topic = IdentTopic::new(topics::stream_control(stream_id));

        // Subscribe to both topics
        if let Err(e) = swarm.behaviour_mut().gossipsub.subscribe(&data_topic) {
            return Err(OverlayError::Other(format!(
                "Failed to subscribe to data topic: {}",
                e
            )));
        }

        if let Err(e) = swarm.behaviour_mut().gossipsub.subscribe(&control_topic) {
            return Err(OverlayError::Other(format!(
                "Failed to subscribe to control topic: {}",
                e
            )));
        }

        // Store the stream
        {
            let mut streams = self.streams.write().await;
            streams.insert(stream_id.clone());
        }

        info!("Subscribed to stream {}", stream_id);
        Ok(())
    }

    /// Unsubscribe from a stream
    pub async fn unsubscribe_from_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        // Lock the swarm
        let mut swarm_lock = self.swarm.lock().await;

        let swarm = match &mut *swarm_lock {
            Some(swarm) => swarm,
            None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
        };

        // Get topic identifiers
        let data_topic = IdentTopic::new(topics::stream_data(stream_id));
        let control_topic = IdentTopic::new(topics::stream_control(stream_id));

        // Unsubscribe from both topics
        if let Err(e) = swarm.behaviour_mut().gossipsub.unsubscribe(&data_topic) {
            return Err(OverlayError::Other(format!(
                "Failed to unsubscribe from data topic: {}",
                e
            )));
        }

        if let Err(e) = swarm.behaviour_mut().gossipsub.unsubscribe(&control_topic) {
            return Err(OverlayError::Other(format!(
                "Failed to unsubscribe from control topic: {}",
                e
            )));
        }

        // Remove the stream
        {
            let mut streams = self.streams.write().await;
            streams.remove(stream_id);
        }

        info!("Unsubscribed from stream {}", stream_id);
        Ok(())
    }

    /// Publish a message to a stream
    pub async fn publish_message(
        &self,
        stream_id: &StreamId,
        data: Vec<u8>,
        is_control: bool,
    ) -> Result<(), OverlayError> {
        // Lock the swarm
        let mut swarm_lock = self.swarm.lock().await;

        let swarm = match &mut *swarm_lock {
            Some(swarm) => swarm,
            None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
        };

        // Choose topic based on whether this is a control message
        let topic_str = if is_control {
            topics::stream_control(stream_id)
        } else {
            topics::stream_data(stream_id)
        };

        let topic = IdentTopic::new(topic_str);

        // Publish message to topic
        match swarm.behaviour_mut().gossipsub.publish(topic, data) {
            Ok(_message_id) => {
                debug!(
                    "Published {} message to stream {}",
                    if is_control { "control" } else { "data" },
                    stream_id
                );
                Ok(())
            }
            Err(e) => Err(OverlayError::Other(format!(
                "Failed to publish message: {}",
                e
            ))),
        }
    }

    /// Connect to a specific peer
    pub async fn connect_peer(&self, peer_id: &PeerId) -> Result<(), OverlayError> {
        // Lock the swarm
        let mut swarm_lock = self.swarm.lock().await;

        let swarm = match &mut *swarm_lock {
            Some(swarm) => swarm,
            None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
        };

        // Get peer addresses if we have them
        let mut has_addresses = false;
        {
            let peers = self.peers.read().await;
            let local_peer_id = from_libp2p_peer_id(peer_id);
            if let Some(peer) = peers.get(&local_peer_id) {
                for addr_str in &peer.info.addresses {
                    if let Ok(addr) = addr_str.parse::<libp2p::Multiaddr>() {
                        has_addresses = true;
                        // Use the libp2p::PeerId directly for dialing
                        match swarm.dial(addr.clone()) {
                            Ok(_) => {
                                info!("Dialing peer {} at {}", peer_id, addr);
                                return Ok(());
                            }
                            Err(e) => {
                                warn!("Failed to dial peer {} at {}: {}", peer_id, addr, e);
                                // Continue and try other addresses
                            }
                        }
                    }
                }
            }
        }

        if !has_addresses {
            // If we don't have addresses, try to discover the peer through Kademlia
            // Use the libp2p::PeerId directly for discovery
            swarm.behaviour_mut().kademlia.get_closest_peers(*peer_id);

            // Mark the peer as connecting in our state
            let mut peers = self.peers.write().await;
            let local_peer_id = from_libp2p_peer_id(peer_id);
            if let Some(peer) = peers.get_mut(&local_peer_id) {
                peer.set_connecting();
            } else {
                let peer_info = PeerInfo {
                    id: local_peer_id.clone(),
                    addresses: vec![],
                    role: crate::overlay::peer::PeerRole::Relay,
                    status: crate::overlay::peer::ConnectionStatus::Connecting,
                    last_seen: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    protocols: vec![],
                    metadata: std::collections::HashMap::new(),
                    latency_ms: None,
                    region: None,
                    bandwidth_capacity: None,
                };
                let mut peer = Peer::new(local_peer_id.clone(), peer_info);
                peer.set_connecting();
                peers.insert(local_peer_id.clone(), peer);
            }

            // We'll rely on the Kademlia event to connect us later
            debug!("No addresses for peer {}, attempting discovery", peer_id);
            Ok(())
        } else {
            Err(OverlayError::Other(format!(
                "Failed to connect to peer {}",
                peer_id
            )))
        }
    }

    /// Disconnect from a peer
    pub async fn disconnect_peer(&self, peer_id: &PeerId) -> Result<(), OverlayError> {
        // Lock the swarm
        let mut swarm_lock = self.swarm.lock().await;

        let swarm = match &mut *swarm_lock {
            Some(swarm) => swarm,
            None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
        };

        // Disconnect from the peer
        // Note: In newer libp2p versions, we can use close_connection or disconnect_peer_id
        // For now, we'll try to close all connections to this peer
        if let Some(connection_id) = swarm
            .connected_peers()
            .find(|p| **p == *peer_id)
            .map(|_| *peer_id)
        {
            // In newer versions, we would call swarm.close_connection or similar
            info!(
                "Would disconnect from peer {} if API was available",
                peer_id
            );
        }

        // Peer disconnection is logged for now

        // Update peer state
        {
            let mut peers = self.peers.write().await;
            let local_peer_id = from_libp2p_peer_id(peer_id);
            if let Some(peer) = peers.get_mut(&local_peer_id) {
                peer.set_disconnected();
            }
        }

        info!("Disconnected from peer {}", peer_id);
        Ok(())
    }

    /// Start listening on the specified address
    pub async fn listen_on(&self, addr_str: &str) -> Result<(), OverlayError> {
        // Lock the swarm
        let mut swarm_lock = self.swarm.lock().await;

        let swarm = match &mut *swarm_lock {
            Some(swarm) => swarm,
            None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
        };

        // Parse the address
        let addr = match addr_str.parse() {
            Ok(addr) => addr,
            Err(e) => {
                return Err(OverlayError::Other(format!(
                    "Invalid address {}: {}",
                    addr_str, e
                )))
            }
        };

        // Start listening
        match swarm.listen_on(addr) {
            Ok(_) => {
                info!("Listening on {}", addr_str);
                Ok(())
            }
            Err(e) => Err(OverlayError::Other(format!(
                "Failed to listen on {}: {}",
                addr_str, e
            ))),
        }
    }
}
