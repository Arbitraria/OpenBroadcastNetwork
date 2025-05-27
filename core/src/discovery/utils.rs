//! Utility functions for discovery

use libp2p::core::multiaddr::{Multiaddr, Protocol};
use libp2p::PeerId as Libp2pPeerId;
use std::net::SocketAddr;
use std::convert::{TryFrom, TryInto};
use thiserror::Error;
use crate::overlay::peer::LocalPeerId;

/// Error type for discovery utilities
#[derive(Debug, Error)]
pub enum DiscoveryUtilsError {
    #[error("Failed to convert bytes to PeerId: {0}")]
    PeerIdConversion(String),
    
    #[error("Empty peer ID")]
    EmptyPeerId,
    
    #[error("Invalid multiaddr: {0}")]
    InvalidMultiaddr(String),
}

/// Convert a PeerId to a byte vector
pub(crate) fn peer_id_to_bytes(peer_id: &Libp2pPeerId) -> Vec<u8> {
    peer_id.to_bytes()
}

/// Convert a byte vector to a PeerId
pub(crate) fn bytes_to_peer_id(bytes: &[u8]) -> Result<Libp2pPeerId, DiscoveryUtilsError> {
    if bytes.is_empty() {
        return Err(DiscoveryUtilsError::EmptyPeerId);
    }
    Libp2pPeerId::from_bytes(bytes).map_err(|e| DiscoveryUtilsError::PeerIdConversion(e.to_string()))
}

/// Convert LocalPeerId to libp2p::PeerId
pub fn local_to_libp2p_peer_id(peer_id: &LocalPeerId) -> Result<Libp2pPeerId, DiscoveryUtilsError> {
    Libp2pPeerId::try_from(peer_id)
        .map_err(|e| DiscoveryUtilsError::PeerIdConversion(e.to_string()))
}

/// Convert libp2p::PeerId to LocalPeerId
pub fn libp2p_to_local_peer_id(peer_id: &Libp2pPeerId) -> LocalPeerId {
    LocalPeerId::from(peer_id)
}

/// Convert a Multiaddr to a SocketAddr if possible
pub(crate) fn multiaddr_to_socket_addr(addr: &Multiaddr) -> Option<SocketAddr> {
    let mut ip = None;
    let mut port = 0;
    
    for protocol in addr.iter() {
        match protocol {
            Protocol::Ip4(ip_addr) => ip = Some(ip_addr.into()),
            Protocol::Ip6(ip_addr) => ip = Some(ip_addr.into()),
            Protocol::Tcp(p) => port = p,
            Protocol::Udp(p) => port = p,
            _ => {}
        }
    }
    
    ip.map(|ip| SocketAddr::new(ip, port))
}

/// Convert a PeerInfo to a libp2p PeerId
pub(crate) fn peer_info_to_peer_id(info: &crate::discovery::interface::PeerInfo) -> Result<Libp2pPeerId, DiscoveryUtilsError> {
    bytes_to_peer_id(&info.id)
}

/// Convert a SocketAddr to a Multiaddr
pub(crate) fn socket_addr_to_multiaddr(addr: &SocketAddr) -> Multiaddr {
    let mut multiaddr = match addr.ip() {
        std::net::IpAddr::V4(ip) => Multiaddr::from(ip),
        std::net::IpAddr::V6(ip) => Multiaddr::from(ip),
    };
    
    // Add port if not zero
    if addr.port() != 0 {
        multiaddr.push(Protocol::Tcp(addr.port()));
    }
    
    multiaddr
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::str::FromStr;

    #[test]
    fn test_peer_id_conversion() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = Libp2pPeerId::from_public_key(&keypair.public());
        
        // Test peer_id_to_bytes and bytes_to_peer_id
        let bytes = peer_id_to_bytes(&peer_id);
        let peer_id2 = bytes_to_peer_id(&bytes).unwrap();
        assert_eq!(peer_id, peer_id2);
        
        // Test empty peer ID
        assert!(matches!(
            bytes_to_peer_id(&[]),
            Err(DiscoveryUtilsError::EmptyPeerId)
        ));
    }

    #[test]
    fn test_multiaddr_conversion() {
        // Test IPv4 with port
        let addr = "/ip4/192.168.1.1/tcp/8080".parse::<Multiaddr>().unwrap();
        let socket_addr = multiaddr_to_socket_addr(&addr).unwrap();
        assert_eq!(socket_addr.ip().to_string(), "192.168.1.1");
        assert_eq!(socket_addr.port(), 8080);
        
        // Test IPv6 with port
        let addr = "/ip6/2001:db8::1/udp/9090".parse::<Multiaddr>().unwrap();
        let socket_addr = multiaddr_to_socket_addr(&addr).unwrap();
        assert_eq!(socket_addr.ip().to_string(), "2001:db8::1");
        assert_eq!(socket_addr.port(), 9090);
        
        // Test without port
        let addr = "/ip4/192.168.1.1".parse::<Multiaddr>().unwrap();
        let socket_addr = multiaddr_to_socket_addr(&addr).unwrap();
        assert_eq!(socket_addr.port(), 0);
        
        // Test localhost addresses
        let ipv4_addr = Multiaddr::from(Ipv4Addr::LOCALHOST);
        let socket_addr = multiaddr_to_socket_addr(&ipv4_addr).unwrap();
        assert!(socket_addr.is_ipv4());
        
        let ipv6_addr = Multiaddr::from(Ipv6Addr::LOCALHOST);
        let socket_addr = multiaddr_to_socket_addr(&ipv6_addr).unwrap();
        assert!(socket_addr.is_ipv6());
        
        // Test unsupported protocol
        let unsupported = "/unix/path/to/socket".parse::<Multiaddr>().unwrap();
        assert!(multiaddr_to_socket_addr(&unsupported).is_none());
    }
    
    #[test]
    fn test_socket_addr_to_multiaddr() {
        // Test IPv4 with port
        let ipv4 = SocketAddr::from_str("192.168.1.1:8080").unwrap();
        let multiaddr = socket_addr_to_multiaddr(&ipv4);
        assert_eq!(multiaddr.to_string(), "/ip4/192.168.1.1/tcp/8080");
        
        // Test IPv6 with port
        let ipv6 = SocketAddr::from_str("[2001:db8::1]:9090").unwrap();
        let multiaddr = socket_addr_to_multiaddr(&ipv6);
        assert_eq!(multiaddr.to_string(), "/ip6/2001:db8::1/tcp/9090");
        
        // Test with zero port
        let ipv4_no_port = SocketAddr::from_str("192.168.1.1:0").unwrap();
        let multiaddr = socket_addr_to_multiaddr(&ipv4_no_port);
        assert_eq!(multiaddr.to_string(), "/ip4/192.168.1.1");
    }
}
