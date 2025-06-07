//! Overlay event channel logic for libp2p overlay

#[cfg(feature = "libp2p")]
use super::*;
#[cfg(feature = "libp2p")]
use futures::channel::mpsc;
#[cfg(feature = "libp2p")]
use crate::overlay::interface::OverlayEvent;

/// Event handler for overlay events
#[cfg(feature = "libp2p")]
pub struct EventHandler {
    /// Event channel sender
    pub sender: mpsc::Sender<OverlayEvent>,
}

#[cfg(feature = "libp2p")]
impl EventHandler {
    pub fn new() -> (Self, mpsc::Receiver<OverlayEvent>) {
        let (sender, receiver) = mpsc::channel(100);
        (Self { sender }, receiver)
    }
    
    pub async fn send_event(&mut self, event: OverlayEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use futures::SinkExt;
        self.sender.send(event).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Stub implementation when libp2p feature is disabled
#[cfg(not(feature = "libp2p"))]
pub mod stub {
    use crate::overlay::interface::OverlayError;
    
    pub fn create_event_handler() -> Result<(), OverlayError> {
        Err(OverlayError::ProtocolError("libp2p feature not enabled".to_string()))
    }
}
