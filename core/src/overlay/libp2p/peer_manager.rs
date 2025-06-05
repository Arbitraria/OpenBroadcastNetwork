//! Peer connection/disconnection and info updates for libp2p overlay

use crate::overlay::peer::{PeerInfo, ConnectionStatus};
use crate::overlay::interface::OverlayError;
use crate::overlay::peer::LocalPeerId;
use tokio::sync::RwLock;
use std::collections::HashMap;

/// Get a list of connected peers from a peer map
pub async fn connected_peers(peers: &RwLock<HashMap<LocalPeerId, crate::overlay::peer::Peer>>) -> Result<Vec<PeerInfo>, OverlayError> {
    let peers = peers.read().await;
    let mut result = Vec::new();
    for (_, peer) in peers.iter() {
        if peer.is_connected() {
            result.push(peer.info.clone());
        }
    }
    Ok(result)
}

/// Remove a peer from the peer map
pub async fn remove_peer(peers: &RwLock<HashMap<LocalPeerId, crate::overlay::peer::Peer>>, peer_id: &LocalPeerId) {
    let mut peers = peers.write().await;
    peers.remove(peer_id);
}
