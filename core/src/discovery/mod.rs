//! Peer discovery implementation
//!
//! This module provides mechanisms for discovering peers in the decentralized network.
//! Peer discovery is a critical component that enables nodes to find and connect with
//! other participating nodes without centralized coordination.
//!
//! The discovery system supports multiple complementary mechanisms:
//! - **Bootstrap discovery**: Uses well-known bootstrap servers to find initial peers
//! - **DHT discovery**: Distributed Hash Table-based peer discovery for scalable lookup
//! - **Kademlia discovery**: Local network discovery using multicast DNS
//!
//! These mechanisms can be used individually or in combination to provide robust
//! peer discovery across different network environments and topologies.

/// Bootstrap server-based peer discovery implementation
pub mod bootstrap;
/// Distributed Hash Table (DHT) based peer discovery
pub mod dht;
/// Multicast DNS based local network peer discovery

/// Core interfaces and types for peer discovery
pub mod interface;

#[cfg(test)]
mod tests;

// Re-export the main interface types
pub use interface::{Discovery, DiscoveryError, DiscoveryEvent, PeerInfo};

// Re-export the discovery implementations
pub use bootstrap::{BootstrapDiscovery, BootstrapDiscoveryConfig};
pub use dht::{DhtDiscovery, DhtDiscoveryConfig};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time;
use tracing::{debug, error, info};

/// Configuration for the discovery manager
#[derive(Debug, Clone)]
pub struct DiscoveryManagerConfig {
    /// Enable DHT discovery
    pub enable_dht: bool,
    /// Enable bootstrap discovery
    pub enable_bootstrap: bool,
    /// DHT configuration (if enabled)
    pub dht_config: Option<DhtDiscoveryConfig>,
    /// Bootstrap configuration (if enabled)
    pub bootstrap_config: Option<BootstrapDiscoveryConfig>,
    /// How often to check for discovery events (in milliseconds)
    pub poll_interval_ms: u64,
}

impl Default for DiscoveryManagerConfig {
    fn default() -> Self {
        Self {
            enable_dht: false,
            enable_bootstrap: false,
            dht_config: None,
            bootstrap_config: None,
            poll_interval_ms: 100,
        }
    }
}

/// A unified discovery manager that integrates multiple discovery mechanisms
pub struct DiscoveryManager {
    /// Configuration
    config: DiscoveryManagerConfig,
    /// DHT discovery (if enabled)
    dht: Option<Arc<Mutex<DhtDiscovery>>>,
    /// Bootstrap discovery (if enabled)
    bootstrap: Option<Arc<Mutex<BootstrapDiscovery>>>,
    /// Known peers
    peers: Arc<RwLock<HashMap<Vec<u8>, PeerInfo>>>,
    /// Event channel sender
    event_tx: mpsc::Sender<DiscoveryEvent>,
    /// Event channel receiver
    event_rx: Mutex<mpsc::Receiver<DiscoveryEvent>>,
    /// Is the manager running
    running: Arc<RwLock<bool>>,
    /// Worker task handle
    worker_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Shutdown signal sender
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl DiscoveryManager {
    /// Create a new discovery manager with the given configuration
    pub fn new(config: DiscoveryManagerConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(100);

        Self {
            config,
            dht: None,
            bootstrap: None,
            peers: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            event_rx: Mutex::new(event_rx),
            running: Arc::new(RwLock::new(false)),
            worker_task: Mutex::new(None),
            shutdown_tx: None,
        }
    }

    /// Start the discovery manager
    pub async fn start(&mut self) -> Result<(), DiscoveryError> {
        let mut running = self.running.write().await;

        if *running {
            return Ok(());
        }

        // Initialize discovery mechanisms
        if self.config.enable_dht {
            let config = self.config.dht_config.clone().unwrap_or_default();
            let mut dht = DhtDiscovery::with_config(config);
            dht.start().await?;
            self.dht = Some(Arc::new(Mutex::new(dht)));
        }

        if self.config.enable_bootstrap {
            let config = self.config.bootstrap_config.clone().unwrap_or_default();
            let mut bootstrap = BootstrapDiscovery::with_config(config);
            bootstrap.start().await?;
            self.bootstrap = Some(Arc::new(Mutex::new(bootstrap)));
        }

        // Start the worker task

        let dht = self.dht.clone();
        let bootstrap = self.bootstrap.clone();
        let _peers = self.peers.clone(); // Kept for future implementation
        let event_tx = self.event_tx.clone();
        let running_flag = self.running.clone();
        let poll_interval = Duration::from_millis(self.config.poll_interval_ms);

        // Create a shutdown channel
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let worker = tokio::spawn(async move {
            let mut interval = time::interval(poll_interval);

            while *running_flag.read().await {
                interval.tick().await;

                // Check for discovery events with a 1-second timeout
                tokio::select! {
                    // Handle bootstrap events
                    result = async {
                        if let Some(bootstrap) = &bootstrap {
                            bootstrap.lock().await.next_event(Some(Duration::from_secs(1))).await
                        } else {
                            futures::future::pending().await
                        }
                    } => {
                        if let Ok(Some(event)) = result {
                            let _ = event_tx.send(event).await;
                        }
                    },
                    // Handle DHT events
                    result = async {
                        if let Some(dht) = &dht {
                            dht.lock().await.next_event(Some(Duration::from_secs(1))).await
                        } else {
                            futures::future::pending().await
                        }
                    } => {
                        if let Ok(Some(event)) = result {
                            let _ = event_tx.send(event).await;
                        }
                    },

                    // Handle shutdown signal
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });

        // Store worker task and mark as running
        *self.worker_task.lock().await = Some(worker);
        *running = true;

        info!("Discovery manager started");

        Ok(())
    }

    /// Stop the discovery manager
    pub async fn stop(&mut self) -> Result<(), DiscoveryError> {
        // Stop DHT discovery
        if let Some(dht) = &mut self.dht {
            dht.lock().await.stop().await?;
        }

        // Stop bootstrap discovery
        if let Some(bootstrap) = &mut self.bootstrap {
            bootstrap.lock().await.stop().await?;
        }

        // Clear known peers
        self.peers.write().await.clear();

        // Set running to false
        *self.running.write().await = false;

        info!("Discovery manager stopped");

        Ok(())
    }

    /// Handle a discovery event
    async fn handle_discovery_event(
        &self,
        event: DiscoveryEvent,
        peers: &Arc<RwLock<HashMap<Vec<u8>, PeerInfo>>>,
        event_tx: &mpsc::Sender<DiscoveryEvent>,
    ) {
        match &event {
            DiscoveryEvent::PeerDiscovered(info) => {
                // Add to known peers
                let mut peers_lock = peers.write().await;
                peers_lock.insert(info.id.clone(), info.clone());
                debug!("Discovered peer: {:?}", info);
            }
            DiscoveryEvent::PeerUpdated(info) => {
                // Update existing peer info
                let mut peers_lock = peers.write().await;
                if let Some(existing) = peers_lock.get_mut(&info.id) {
                    *existing = info.clone();
                    debug!("Updated peer: {:?}", info);
                }
            }
            DiscoveryEvent::PeerExpired(peer_id) => {
                // Remove expired peer
                let mut peers_lock = peers.write().await;
                if peers_lock.remove(peer_id).is_some() {
                    debug!("Peer expired: {:?}", peer_id);
                }
            }
            DiscoveryEvent::PeerConnectionStatusChanged {
                peer_id,
                status,
                error,
            } => {
                // Update connection status for the peer
                let mut peers_lock = peers.write().await;
                if let Some(peer_info) = peers_lock.get_mut(peer_id) {
                    peer_info.connection_status = status.clone();
                    if let Some(err) = error {
                        debug!(
                            "Peer connection status changed: {:?} -> {:?} (error: {})",
                            peer_id, status, err
                        );
                    } else {
                        debug!(
                            "Peer connection status changed: {:?} -> {:?}",
                            peer_id, status
                        );
                    }
                }
            }
            DiscoveryEvent::ServiceStarted => {
                debug!("Discovery service started");
            }
            DiscoveryEvent::ServiceStopped => {
                debug!("Discovery service stopped");
            }
            DiscoveryEvent::Error(e) => {
                error!("Discovery error: {}", e);
            }
        }

        // Forward the event
        let _ = event_tx.send(event).await;
    }

    /// Get the next discovery event
    pub async fn next_event(&self) -> Option<DiscoveryEvent> {
        let mut rx = self.event_rx.lock().await;
        rx.recv().await
    }

    /// Get a list of all known peers
    pub async fn get_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        peers.values().cloned().collect()
    }

    /// Lookup a specific peer
    pub async fn lookup_peer(&self, id: &[u8]) -> Option<PeerInfo> {
        let peers = self.peers.read().await;
        peers.get(id).cloned()
    }

    /// Announce self to the network
    pub async fn announce(&self, info: PeerInfo) -> Result<(), DiscoveryError> {
        // Announce via all active discovery mechanisms
        if let Some(dht) = &self.dht {
            dht.lock().await.announce(info.clone()).await?;
        }

        if let Some(bootstrap) = &self.bootstrap {
            bootstrap.lock().await.announce(info.clone()).await?;
        }

        Ok(())
    }

    /// Check if the discovery manager is running
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }
}

// Will be exported once implemented
// pub use bootstrap::BootstrapDiscovery;
// pub use dht::DhtDiscovery;
