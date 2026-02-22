//! Transport layer interface definitions
//!
//! This module defines the common interfaces for all transport implementations in the
//! decentralized streaming network. These interfaces provide a unified API that abstracts
//! away the underlying network protocol details (WebRTC, QUIC, etc.) and allows the
//! application to focus on high-level communication patterns.
//!
//! The transport layer is responsible for:
//! - Establishing and maintaining connections between peers
//! - Providing reliable and ordered data delivery
//! - Securing communications with encryption and authentication
//! - Handling NAT traversal and connection disruptions
//! - Monitoring connection health and quality

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

/// A unique identifier for a peer-to-peer connection
///
/// Provides a stable identifier for a connection that remains valid for the
/// lifetime of the connection, regardless of changes in network conditions or
/// addresses. This identifier is used to refer to specific connections when
/// sending data or managing connection state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub Vec<u8>);

/// Events emitted by a transport implementation
///
/// These events notify the application about important changes in the transport
/// layer, such as new connections being established, connections being closed,
/// data being received, or errors occurring. Applications can subscribe to these
/// events to react to changes in the network state.
#[derive(Debug)]
pub enum TransportEvent {
    /// A new connection has been established
    ConnectionEstablished(Connection),
    /// A connection has been closed
    ConnectionClosed(ConnectionId),
    /// Data has been received on a connection
    DataReceived(ConnectionId, Vec<u8>),
    /// An error occurred on the transport
    Error(TransportError),
}

/// Errors that can occur in a transport
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// Transport initialization failed
    #[error("Initialization failed: {0}")]
    InitializationFailed(String),

    /// Connection could not be established
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Connection was lost
    #[error("Connection lost: {0}")]
    ConnectionLost(String),

    /// Data could not be sent
    #[error("Send error: {0}")]
    SendError(String),

    /// NAT traversal failed
    #[error("NAT traversal failed: {0}")]
    NatTraversalFailed(String),

    /// Generic IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// A connection to a remote peer in the network
///
/// Represents an established communication channel between the local node and a
/// remote peer. Connections are the foundation of all peer-to-peer communication
/// in the system and provide reliable, ordered, and secure data transfer between
/// nodes regardless of their network location.
#[derive(Debug)]
pub struct Connection {
    /// The unique ID of this connection
    pub id: ConnectionId,
    /// The remote address, if known
    pub remote_addr: Option<SocketAddr>,
    /// Remote peer ID, if authenticated
    pub remote_peer_id: Option<Vec<u8>>,
}

/// The core Transport trait that all transport implementations must implement
///
/// This trait defines the standard interface for network transport mechanisms.
/// It provides methods for starting and stopping the transport, establishing
/// connections to remote peers, sending data, and receiving events.
///
/// Implementations of this trait include WebRTC-based transport (for browser
/// compatibility) and QUIC-based transport (for high performance). Each
/// implementation provides the same interface but uses different underlying
/// protocols to establish connections and transfer data.
pub trait Transport {
    /// Start the transport listener
    fn start(&mut self) -> Result<(), TransportError>;

    /// Stop the transport listener
    fn stop(&mut self) -> Result<(), TransportError>;

    /// Connect to a remote peer
    fn connect(
        &mut self,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = Result<Connection, TransportError>> + Send>>;

    /// Send data to a peer
    fn send(
        &mut self,
        conn_id: &ConnectionId,
        data: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send>>;

    /// Close a specific connection
    fn close_connection(&mut self, conn_id: &ConnectionId) -> Result<(), TransportError>;

    /// Check if the transport is running
    fn is_running(&self) -> bool;

    /// Get the next event from the transport
    fn next_event(&mut self) -> Pin<Box<dyn Future<Output = Option<TransportEvent>> + Send>>;
}
