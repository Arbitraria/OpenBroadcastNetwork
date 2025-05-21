//! Simple demonstration of the transport interface
//!
//! This example creates a mock transport and simulates basic operations.

use decentralized_stream_core::transport::{Transport, TransportEvent, TransportError, Connection, ConnectionId};
use std::net::SocketAddr;
use std::str::FromStr;

/// A simple mock transport for demonstration
struct MockTransport {
    running: bool,
}

impl MockTransport {
    fn new() -> Self {
        Self { running: false }
    }
}

impl Transport for MockTransport {
    fn start(&mut self) -> Result<(), TransportError> {
        println!("Starting mock transport");
        self.running = true;
        Ok(())
    }
    
    fn stop(&mut self) -> Result<(), TransportError> {
        println!("Stopping mock transport");
        self.running = false;
        Ok(())
    }
    
    fn connect(&mut self, addr: SocketAddr) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Connection, TransportError>> + Send>> {
        println!("Connecting to {}", addr);
        let addr_clone = addr;
        Box::pin(async move {
            // Simulate successful connection
            let conn_id = ConnectionId(vec![1, 2, 3, 4]);
            Ok(Connection {
                id: conn_id,
                remote_addr: Some(addr_clone),
                remote_peer_id: None,
            })
        })
    }
    
    fn send(&mut self, conn_id: &ConnectionId, data: Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send>> {
        println!("Sending {} bytes", data.len());
        Box::pin(async { Ok(()) })
    }
    
    fn close_connection(&mut self, conn_id: &ConnectionId) -> Result<(), TransportError> {
        println!("Closing connection");
        Ok(())
    }
    
    fn is_running(&self) -> bool {
        self.running
    }
    
    fn next_event(&mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<TransportEvent>> + Send>> {
        Box::pin(async { None })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Transport Demo");
    
    // Create and start the transport
    let mut transport = MockTransport::new();
    transport.start()?;
    
    // Connect to a peer
    let addr = SocketAddr::from_str("127.0.0.1:8080")?;
    let conn = transport.connect(addr).await?;
    println!("Connected to {:?}", conn.remote_addr);
    
    // Send some data
    let data = b"Hello, world!".to_vec();
    transport.send(&conn.id, data).await?;
    
    // Close the connection
    transport.close_connection(&conn.id)?;
    
    // Stop the transport
    transport.stop()?;
    
    println!("Demo completed successfully!");
    Ok(())
} 