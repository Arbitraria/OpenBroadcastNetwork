//! Type aliases, conversions, and helpers for libp2p overlay

use crate::overlay::interface::OverlayError;
use crate::overlay::peer::LocalPeerId;
use libp2p::PeerId as Libp2pPeerId;
use std::convert::TryFrom;

/// Convert our LocalPeerId to libp2p's PeerId
pub fn to_libp2p_peer_id(peer_id: &LocalPeerId) -> Result<Libp2pPeerId, OverlayError> {
    Libp2pPeerId::try_from(peer_id)
        .map_err(|e| OverlayError::Other(format!("Invalid peer ID: {}", e)))
}

/// Convert libp2p's PeerId to our LocalPeerId
pub fn from_libp2p_peer_id(peer_id: &Libp2pPeerId) -> LocalPeerId {
    LocalPeerId::from(peer_id)
}
