//! WasmOverlay — Overlay trait implementation for wasm32 browsers
//!
//! Connects to a native relay node via WebSocket and proxies all
//! overlay operations through JSON control messages + binary frames.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use futures::channel::mpsc;
use futures::StreamExt;
use tracing::{debug, info, warn};

use crate::overlay::interface::{
    Overlay, OverlayConfig, OverlayError, OverlayEvent, OverlayStats, StreamId,
};
use crate::overlay::peer::{LocalPeerId, PeerInfo};

use super::ws_transport::{RelayMessage, WsEvent, WsTransport};

/// Internal shared state (single-threaded, Rc<RefCell<>>)
struct Inner {
    transport: Option<WsTransport>,
    local_peer_id: LocalPeerId,
    running: bool,
    subscribed_streams: HashSet<String>,
    published_streams: HashSet<String>,
    /// Wrapped in Option so `stop()` can drop it, causing
    /// `event_rx.next()` to yield `None` and terminate consumers.
    event_tx: Option<mpsc::UnboundedSender<OverlayEvent>>,
}

/// Overlay implementation for browsers using a WebSocket relay
pub struct WasmOverlay {
    inner: Rc<RefCell<Inner>>,
    event_rx: RefCell<mpsc::UnboundedReceiver<OverlayEvent>>,
    relay_url: String,
}

impl WasmOverlay {
    /// Create a new WasmOverlay that will connect to the given relay URL
    pub fn new(relay_url: &str, config: OverlayConfig) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded();
        let inner = Inner {
            transport: None,
            local_peer_id: config.local_peer_id,
            running: false,
            subscribed_streams: HashSet::new(),
            published_streams: HashSet::new(),
            event_tx: Some(event_tx),
        };
        Self {
            inner: Rc::new(RefCell::new(inner)),
            event_rx: RefCell::new(event_rx),
            relay_url: relay_url.to_string(),
        }
    }

    /// Send a ListStreams request to the relay.
    ///
    /// The response arrives asynchronously as an
    /// `OverlayEvent::StreamList` via `next_event()`.
    pub fn request_stream_list(&self) -> Result<(), OverlayError> {
        let inner = self.inner.borrow();
        if let Some(ref transport) = inner.transport {
            transport
                .send_message(&RelayMessage::ListStreams)
                .map_err(OverlayError::ConnectionError)?;
            Ok(())
        } else {
            Err(OverlayError::NotRunning)
        }
    }

    /// Spawn the background event loop that reads from WebSocket.
    ///
    /// Takes the event receiver out of the transport so the loop
    /// never holds a borrow on `Inner` across an await point.
    fn spawn_event_loop(inner: Rc<RefCell<Inner>>) {
        // Extract the WsTransport event receiver up-front
        let mut ws_events = {
            let mut borrow = inner.borrow_mut();
            borrow
                .transport
                .as_mut()
                .and_then(|t| t.take_event_receiver())
        };

        let Some(mut ws_rx) = ws_events else {
            warn!("spawn_event_loop: no transport event receiver");
            return;
        };

        wasm_bindgen_futures::spawn_local(async move {
            while let Some(event) = ws_rx.next().await {
                match event {
                    WsEvent::Binary(data) => {
                        let stream_id = StreamId::from_string("relay");
                        let borrow = inner.borrow();
                        if let Some(ref tx) = borrow.event_tx {
                            let _ = tx.unbounded_send(OverlayEvent::StreamData {
                                stream_id,
                                source: borrow.local_peer_id.clone(),
                                data,
                            });
                        }
                    }
                    WsEvent::Text(json) => match serde_json::from_str::<RelayMessage>(&json) {
                        Ok(RelayMessage::StreamList { streams }) => {
                            debug!("Relay sent {} streams", streams.len());
                            let borrow = inner.borrow();
                            if let Some(ref tx) = borrow.event_tx {
                                let _ = tx.unbounded_send(OverlayEvent::StreamList {
                                    streams: streams
                                        .into_iter()
                                        .map(|s| (s.stream_id, s.publisher))
                                        .collect(),
                                });
                            }
                        }
                        Ok(RelayMessage::Error { message }) => {
                            warn!("Relay error: {}", message);
                        }
                        Ok(msg) => {
                            debug!("Relay message: {:?}", msg);
                        }
                        Err(e) => {
                            warn!("Failed to parse relay message: {}", e);
                        }
                    },
                    WsEvent::Closed(reason) => {
                        info!("Relay WebSocket closed: {}", reason);
                        let mut borrow = inner.borrow_mut();
                        borrow.running = false;
                        borrow.transport = None;
                        break;
                    }
                    WsEvent::Open => {
                        // Already handled in wait_for_open()
                    }
                    WsEvent::Error(err) => {
                        warn!("Relay WebSocket error: {}", err);
                    }
                }
            }
        });
    }
}

#[async_trait::async_trait(?Send)]
impl Overlay for WasmOverlay {
    async fn start(&self) -> Result<(), OverlayError> {
        let mut transport =
            WsTransport::connect(&self.relay_url).map_err(|e| OverlayError::ConnectionError(e))?;

        // Wait for the WebSocket to actually open before proceeding.
        // This prevents subscribe/publish calls from failing because
        // the connection isn't ready yet.
        transport
            .wait_for_open()
            .await
            .map_err(OverlayError::ConnectionError)?;

        {
            let mut inner = self.inner.borrow_mut();
            inner.transport = Some(transport);
            inner.running = true;
        }

        Self::spawn_event_loop(Rc::clone(&self.inner));
        info!("WasmOverlay connected to {}", self.relay_url);
        Ok(())
    }

    async fn stop(&self) -> Result<(), OverlayError> {
        let mut inner = self.inner.borrow_mut();
        if let Some(ref transport) = inner.transport {
            transport.close();
        }
        inner.transport = None;
        inner.running = false;
        // Drop the event sender so event_rx yields None,
        // terminating any consumer loops (e.g. next_event callers)
        inner.event_tx = None;
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.inner.borrow().running
    }

    fn local_peer_id(&self) -> LocalPeerId {
        self.inner.borrow().local_peer_id.clone()
    }

    async fn connect_peer(&self, _addr: &str) -> Result<PeerInfo, OverlayError> {
        // In WASM mode, we don't connect to individual peers —
        // the relay handles peer connections.
        Err(OverlayError::General(
            "Direct peer connections not supported in WASM mode".into(),
        ))
    }

    async fn disconnect_peer(&self, _peer_id: &LocalPeerId) -> Result<(), OverlayError> {
        Err(OverlayError::General(
            "Direct peer management not supported in WASM mode".into(),
        ))
    }

    async fn publish_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        let sid = stream_id.to_string();
        let inner = self.inner.borrow();
        if let Some(ref transport) = inner.transport {
            transport
                .send_message(&RelayMessage::Publish {
                    stream_id: sid.clone(),
                })
                .map_err(|e| OverlayError::ConnectionError(e))?;
        } else {
            return Err(OverlayError::NotRunning);
        }
        drop(inner);
        self.inner.borrow_mut().published_streams.insert(sid);
        Ok(())
    }

    async fn relay_stream(
        &self,
        _stream_id: &StreamId,
        _target: &LocalPeerId,
    ) -> Result<(), OverlayError> {
        Err(OverlayError::General(
            "Relay not supported in WASM mode".into(),
        ))
    }

    async fn stop_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        let sid = stream_id.to_string();
        let inner = self.inner.borrow();
        if let Some(ref transport) = inner.transport {
            transport
                .send_message(&RelayMessage::Unsubscribe {
                    stream_id: sid.clone(),
                })
                .map_err(|e| OverlayError::ConnectionError(e))?;
        }
        drop(inner);
        let mut inner = self.inner.borrow_mut();
        inner.subscribed_streams.remove(&sid);
        inner.published_streams.remove(&sid);
        Ok(())
    }

    async fn next_event(&self) -> Option<OverlayEvent> {
        self.event_rx.borrow_mut().next().await
    }

    async fn connected_peers(&self) -> Result<Vec<PeerInfo>, OverlayError> {
        // WASM clients see themselves only; the relay is the peer
        Ok(vec![])
    }

    async fn active_streams(&self) -> Result<Vec<StreamId>, OverlayError> {
        let inner = self.inner.borrow();
        let streams: Vec<StreamId> = inner
            .subscribed_streams
            .iter()
            .chain(inner.published_streams.iter())
            .map(|s| StreamId::from_string(s))
            .collect();
        Ok(streams)
    }

    async fn stats(&self) -> Result<OverlayStats, OverlayError> {
        let inner = self.inner.borrow();
        Ok(OverlayStats {
            connected_peers: if inner.running { 1 } else { 0 },
            active_streams: inner.subscribed_streams.len() + inner.published_streams.len(),
            ..Default::default()
        })
    }

    async fn rebalance_topology(&self) -> Result<(), OverlayError> {
        // No-op in WASM — relay handles topology
        Ok(())
    }

    async fn subscribe_stream(&self, stream_id: &StreamId) -> Result<(), OverlayError> {
        let sid = stream_id.to_string();
        let inner = self.inner.borrow();
        if let Some(ref transport) = inner.transport {
            transport
                .send_message(&RelayMessage::Subscribe {
                    stream_id: sid.clone(),
                })
                .map_err(|e| OverlayError::ConnectionError(e))?;
        } else {
            return Err(OverlayError::NotRunning);
        }
        drop(inner);
        self.inner.borrow_mut().subscribed_streams.insert(sid);
        Ok(())
    }

    async fn publish_stream_data(
        &self,
        _stream_id: &StreamId,
        data: Vec<u8>,
    ) -> Result<(), OverlayError> {
        let inner = self.inner.borrow();
        if let Some(ref transport) = inner.transport {
            transport
                .send_binary(&data)
                .map_err(|e| OverlayError::ConnectionError(e))?;
            Ok(())
        } else {
            Err(OverlayError::NotRunning)
        }
    }

    async fn publish_to_topic(&self, _topic: &str, _data: Vec<u8>) -> Result<(), OverlayError> {
        Err(OverlayError::General(
            "Direct topic publishing not supported in WASM mode".into(),
        ))
    }
}
