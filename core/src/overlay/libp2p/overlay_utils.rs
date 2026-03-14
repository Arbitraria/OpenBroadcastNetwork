//! Shared utility functions for Libp2pOverlay methods
//!
//! This module contains shared operations that might be called from multiple
//! places in the modularized implementation, avoiding duplication.

use libp2p::gossipsub::{Event as GossipsubEvent, IdentTopic};
use libp2p::identify::Event as IdentifyEvent;
use libp2p::kad::{Event as KademliaEvent, QueryResult};
use libp2p::swarm::Swarm;
use libp2p::Multiaddr;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::debug;

use crate::overlay::interface::{OverlayError, StreamId};
use crate::overlay::libp2p::behavior::OverlayBehavior;
use crate::overlay::libp2p::topics;
use crate::overlay::libp2p::types::to_libp2p_peer_id;
use crate::overlay::peer::{ConnectionStatus, LocalPeerId, Peer, PeerInfo};

/// Subscribe to a stream
///
/// This function handles subscription to both data and control topics for a stream.
/// Used by both the core implementation and trait implementation.
pub async fn subscribe_to_stream(
    swarm: &Arc<Mutex<Option<Swarm<OverlayBehavior>>>>,
    streams: &RwLock<HashSet<StreamId>>,
    stream_id: &StreamId,
) -> Result<(), OverlayError> {
    debug!("Subscribing to stream {}", stream_id);

    let mut swarm_lock = swarm.lock().await;

    let swarm = match &mut *swarm_lock {
        Some(swarm) => swarm,
        None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
    };

    // Subscribe to both data and control topics
    let data_topic = IdentTopic::new(topics::stream_data(stream_id));
    let control_topic = IdentTopic::new(topics::stream_control(stream_id));

    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&data_topic)
        .map_err(|e| OverlayError::Other(format!("Failed to subscribe to data topic: {}", e)))?;

    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&control_topic)
        .map_err(|e| OverlayError::Other(format!("Failed to subscribe to control topic: {}", e)))?;

    // Add to active streams
    let mut streams_lock = streams.write().await;
    streams_lock.insert(stream_id.clone());

    Ok(())
}

/// Unsubscribe from a stream
///
/// This function handles unsubscription from both data and control topics for a stream.
/// Used by both the core implementation and trait implementation.
pub async fn unsubscribe_from_stream(
    swarm: &Arc<Mutex<Option<Swarm<OverlayBehavior>>>>,
    streams: &RwLock<HashSet<StreamId>>,
    stream_id: &StreamId,
) -> Result<(), OverlayError> {
    debug!("Unsubscribing from stream {}", stream_id);

    let mut swarm_lock = swarm.lock().await;

    let swarm = match &mut *swarm_lock {
        Some(swarm) => swarm,
        None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
    };

    // Unsubscribe from both data and control topics
    let data_topic = IdentTopic::new(topics::stream_data(stream_id));
    let control_topic = IdentTopic::new(topics::stream_control(stream_id));

    let _ = swarm.behaviour_mut().gossipsub.unsubscribe(&data_topic);
    let _ = swarm.behaviour_mut().gossipsub.unsubscribe(&control_topic);

    // Remove from active streams
    let mut streams_lock = streams.write().await;
    streams_lock.remove(stream_id);

    Ok(())
}

/// Connect to a peer
///
/// This function handles connecting to a peer by its peer ID.
/// Used by both the core implementation and trait implementation.
pub async fn connect_peer(
    swarm: &Arc<Mutex<Option<Swarm<OverlayBehavior>>>>,
    peers: &RwLock<std::collections::HashMap<LocalPeerId, Peer>>,
    peer_id: &LocalPeerId,
) -> Result<(), OverlayError> {
    debug!("Connecting to peer {}", peer_id);

    let libp2p_peer_id = to_libp2p_peer_id(peer_id);

    let mut swarm_lock = swarm.lock().await;
    let swarm = match &mut *swarm_lock {
        Some(swarm) => swarm,
        None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
    };

    // Update peer state
    let mut peers_lock = peers.write().await;
    if let Some(peer) = peers_lock.get_mut(peer_id) {
        peer.set_connecting();
    } else {
        let mut peer_info = PeerInfo::default();
        peer_info.status = ConnectionStatus::Connecting;
        let peer = Peer::new(peer_id.clone(), peer_info);
        peers_lock.insert(peer_id.clone(), peer);
    }

    // Try to connect to the peer
    if let Some(peer_score) = swarm.behaviour().gossipsub.peer_score(&libp2p_peer_id) {
        debug!("Peer already in gossipsub: {:?}", peer_score);
        Ok(())
    } else {
        swarm
            .dial(libp2p_peer_id)
            .map_err(|e| OverlayError::Other(format!("Failed to dial peer: {}", e)))?;
        Ok(())
    }
}

/// Disconnect from a peer
///
/// This function handles disconnecting from a peer by its peer ID.
/// Used by both the core implementation and trait implementation.
pub async fn disconnect_peer(
    swarm: &Arc<Mutex<Option<Swarm<OverlayBehavior>>>>,
    peers: &RwLock<std::collections::HashMap<LocalPeerId, Peer>>,
    peer_id: &LocalPeerId,
) -> Result<(), OverlayError> {
    debug!("Disconnecting from peer {}", peer_id);

    let libp2p_peer_id = to_libp2p_peer_id(peer_id);

    let mut swarm_lock = swarm.lock().await;
    let swarm = match &mut *swarm_lock {
        Some(swarm) => swarm,
        None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
    };

    // Update peer state
    let mut peers_lock = peers.write().await;
    if let Some(peer) = peers_lock.get_mut(peer_id) {
        peer.set_disconnected();
    }

    // Try to disconnect from the peer
    swarm
        .disconnect_peer_id(libp2p_peer_id)
        .map_err(|e| OverlayError::Other(format!("Failed to disconnect peer: {:?}", e)))?;

    Ok(())
}

/// Listen on a specific address
///
/// This function handles listening on a specific address.
/// Used by both the core implementation and trait implementation.
pub async fn listen_on(
    swarm: &Arc<Mutex<Option<Swarm<OverlayBehavior>>>>,
    addr: &str,
) -> Result<(), OverlayError> {
    debug!("Listening on address {}", addr);

    let mut swarm_lock = swarm.lock().await;

    let swarm = match &mut *swarm_lock {
        Some(swarm) => swarm,
        None => return Err(OverlayError::Other("Swarm not initialized".to_string())),
    };

    match addr.parse() {
        Ok(addr) => {
            swarm
                .listen_on(addr)
                .map_err(|e| OverlayError::Other(format!("Failed to listen: {:?}", e)))?;
            Ok(())
        }
        Err(e) => Err(OverlayError::Other(format!("Invalid address: {}", e))),
    }
}

/// Handle gossipsub events
///
/// This function handles Gossipsub events such as incoming messages,
/// subscriptions, and unsubscriptions.
pub async fn handle_gossipsub_event(event: GossipsubEvent) -> Result<(), OverlayError> {
    match event {
        GossipsubEvent::Message {
            propagation_source,
            message_id,
            message,
        } => {
            debug!(
                "Received gossipsub message from {}: {:?}",
                propagation_source, message_id
            );

            // Extract topic and potentially the stream ID
            let topic_str = message.topic.as_str();
            if let Some(stream_id) = topics::parse_stream_id(topic_str) {
                debug!("Message is for stream {}", stream_id);

                // Here we would normally process the message and emit an event
                // Since this is a utility function, we just log it
            }
        }
        GossipsubEvent::Subscribed { peer_id, topic } => {
            debug!("Peer {} subscribed to topic {}", peer_id, topic);
        }
        GossipsubEvent::Unsubscribed { peer_id, topic } => {
            debug!("Peer {} unsubscribed from topic {}", peer_id, topic);
        }
        _ => {}
    }

    Ok(())
}

/// Handle Kademlia events
///
/// This function handles Kademlia DHT events such as query progress,
/// routing table updates, etc.
pub async fn handle_kademlia_event(event: KademliaEvent) -> Result<(), OverlayError> {
    match event {
        KademliaEvent::OutboundQueryProgressed { result, .. } => match result {
            QueryResult::GetClosestPeers(Ok(closest)) => {
                debug!("Found {} closest peers", closest.peers.len());
                for peer in &closest.peers {
                    debug!("Found closest peer: {}", peer);
                }
            }
            _ => {}
        },
        KademliaEvent::RoutingUpdated { peer, .. } => {
            debug!("Kademlia routing updated for peer {}", peer);
        }
        _ => {}
    }

    Ok(())
}

/// Handle Identify events
///
/// This function handles Identify protocol events such as receiving
/// peer information and protocol support.
pub async fn handle_identify_event(event: IdentifyEvent) -> Result<(), OverlayError> {
    match event {
        IdentifyEvent::Received { peer_id, info, .. } => {
            debug!(
                "Identified peer {} with protocol version {}",
                peer_id, info.protocol_version
            );
            debug!("  Listening on {} addresses", info.listen_addrs.len());
            debug!("  Supports {} protocols", info.protocols.len());

            // Here we would normally update peer information in our peers map
            // Since this is a utility function we just log it
        }
        _ => {}
    }

    Ok(())
}

/// Connect to a peer implementation
pub async fn connect_peer_impl(
    overlay: &crate::overlay::libp2p::impl_core::Libp2pOverlay,
    addr: &str,
) -> Result<PeerInfo, OverlayError> {
    debug!("Connecting to peer at address: {}", addr);

    // Parse the multiaddress
    let multiaddr: Multiaddr = addr
        .parse()
        .map_err(|e| OverlayError::ConnectionError(format!("Invalid address '{}': {}", addr, e)))?;

    // Actually dial using the swarm
    {
        let mut swarm_lock = overlay.swarm.lock().await;
        if let Some(swarm) = &mut *swarm_lock {
            swarm
                .dial(multiaddr.clone())
                .map_err(|e| OverlayError::ConnectionError(format!("Failed to dial: {}", e)))?;
        } else {
            return Err(OverlayError::NotRunning);
        }
    }

    // For now, create a placeholder peer info
    let peer_info = PeerInfo {
        id: LocalPeerId::new_random(),
        addresses: vec![multiaddr.to_string()],
        role: crate::overlay::peer::PeerRole::Unknown,
        status: ConnectionStatus::Connecting,
        last_seen: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        protocols: vec!["ipfs/ping/1.0.0".to_string()],
        metadata: std::collections::HashMap::new(),
        latency_ms: None,
        region: None,
        bandwidth_capacity: None,
    };

    Ok(peer_info)
}

/// Disconnect from a peer implementation
pub async fn disconnect_peer_impl(
    _overlay: &crate::overlay::libp2p::impl_core::Libp2pOverlay,
    peer_id: &LocalPeerId,
) -> Result<(), OverlayError> {
    debug!("Disconnecting from peer: {}", peer_id);

    // Convert to libp2p peer ID (zero-cost)
    let _libp2p_peer_id = to_libp2p_peer_id(peer_id);

    // For now, this is a placeholder implementation
    // In a full implementation, this would:
    // 1. Use the swarm to disconnect from the peer
    // 2. Update the peer status in our peers map
    // 3. Clean up any associated resources

    Ok(())
}
