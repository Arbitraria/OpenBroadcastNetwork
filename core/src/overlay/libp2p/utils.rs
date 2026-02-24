//! Utility functions for the libp2p implementation
//!
//! This module contains utility functions like peer ID conversion

use crate::overlay::peer::LocalPeerId;
use libp2p::PeerId as Libp2pPeerId;
use serde::de::{self, Visitor};
use serde::{Deserializer, Serializer};
use std::fmt;
use std::str::FromStr;

/// Convert our LocalPeerId to libp2p's PeerId (zero-cost, infallible)
///
/// Since LocalPeerId now wraps libp2p::PeerId directly, this conversion
/// is always valid and never fails.
pub fn to_libp2p_peer_id(peer_id: &LocalPeerId) -> Libp2pPeerId {
    Libp2pPeerId::from(peer_id)
}

/// Convert libp2p's PeerId to our LocalPeerId (zero-cost)
pub fn from_libp2p_peer_id(peer_id: &Libp2pPeerId) -> LocalPeerId {
    LocalPeerId::from(peer_id)
}

/// Serialize a libp2p PeerId to a string representation
pub fn serialize_peer_id<S>(peer_id: &Libp2pPeerId, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&peer_id.to_string())
}

/// Deserialize a libp2p PeerId from a string representation
pub fn deserialize_peer_id<'de, D>(deserializer: D) -> Result<Libp2pPeerId, D::Error>
where
    D: Deserializer<'de>,
{
    struct PeerIdVisitor;

    impl<'de> Visitor<'de> for PeerIdVisitor {
        type Value = Libp2pPeerId;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string containing a base58-encoded peer ID")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Libp2pPeerId::from_str(value)
                .map_err(|_| de::Error::custom(format!("Invalid peer ID: {}", value)))
        }
    }

    deserializer.deserialize_str(PeerIdVisitor)
}

/// Serialization helper for Option<PeerId>
pub fn serialize_optional_peer_id<S>(
    peer_id: &Option<Libp2pPeerId>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match peer_id {
        Some(id) => serializer.serialize_some(&id.to_string()),
        None => serializer.serialize_none(),
    }
}

/// Deserialization helper for Option<PeerId>
pub fn deserialize_optional_peer_id<'de, D>(
    deserializer: D,
) -> Result<Option<Libp2pPeerId>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptionalPeerIdVisitor;

    impl<'de> Visitor<'de> for OptionalPeerIdVisitor {
        type Value = Option<Libp2pPeerId>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string containing a base58-encoded peer ID or null")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_str(PeerIdVisitor).map(Some)
        }
    }

    struct PeerIdVisitor;

    impl<'de> Visitor<'de> for PeerIdVisitor {
        type Value = Libp2pPeerId;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string containing a base58-encoded peer ID")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Libp2pPeerId::from_str(value)
                .map_err(|_| de::Error::custom(format!("Invalid peer ID: {}", value)))
        }
    }

    deserializer.deserialize_option(OptionalPeerIdVisitor)
}
