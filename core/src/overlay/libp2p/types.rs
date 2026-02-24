//! Type aliases, conversions, and helpers for libp2p overlay

use crate::overlay::peer::LocalPeerId;
use libp2p::PeerId as Libp2pPeerId;

/// Convert our LocalPeerId to libp2p's PeerId (zero-cost, infallible)
///
/// Since LocalPeerId now wraps libp2p::PeerId directly, this conversion
/// is always valid and never fails.
pub fn to_libp2p_peer_id(peer_id: &LocalPeerId) -> Libp2pPeerId {
    libp2p::PeerId::from(peer_id)
}

/// Convert libp2p's PeerId to our LocalPeerId (zero-cost)
pub fn from_libp2p_peer_id(peer_id: &Libp2pPeerId) -> LocalPeerId {
    LocalPeerId::from(peer_id)
}
