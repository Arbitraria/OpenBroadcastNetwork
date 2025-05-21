//! Transport layer interface definitions
//!
//! This module defines the common interfaces for all transport implementations.

use std::fmt;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::net::SocketAddr;

/// A unique identifier for a connection
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub Vec<u8>);

/// Events emitted by a transport
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

/// A connection to a peer
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
pub trait Transport {
    /// Start the transport listener
    fn start(&mut self) -> Result<(), TransportError>;
    
    /// Stop the transport listener
    fn stop(&mut self) -> Result<(), TransportError>;
    
    /// Connect to a remote peer
    fn connect(&mut self, addr: SocketAddr) -> Pin<Box<dyn Future<Output = Result<Connection, TransportError>> + Send>>;
    
    /// Send data to a peer
    fn send(&mut self, conn_id: &ConnectionId, data: Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send>>;
    
    /// Close a specific connection
    fn close_connection(&mut self, conn_id: &ConnectionId) -> Result<(), TransportError>;
    
    /// Check if the transport is running
    fn is_running(&self) -> bool;
    
    /// Get the next event from the transport
    fn next_event(&mut self) -> Pin<Box<dyn Future<Output = Option<TransportEvent>> + Send>>;
} 