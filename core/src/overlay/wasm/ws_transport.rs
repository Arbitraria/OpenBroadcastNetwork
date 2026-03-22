//! WebSocket transport for WASM overlay
//!
//! Wraps `web_sys::WebSocket` with a channel-based async interface.

use futures::channel::mpsc;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};

/// Control messages exchanged over the relay WebSocket (JSON text frames)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RelayMessage {
    /// Subscribe to a stream's data
    #[serde(rename = "subscribe")]
    Subscribe { stream_id: String },
    /// Unsubscribe from a stream
    #[serde(rename = "unsubscribe")]
    Unsubscribe { stream_id: String },
    /// Announce intent to publish a stream
    #[serde(rename = "publish")]
    Publish { stream_id: String },
    /// Request the list of available streams
    #[serde(rename = "list_streams")]
    ListStreams,
    /// Response: list of available streams
    #[serde(rename = "stream_list")]
    StreamList { streams: Vec<StreamInfo> },
    /// Error from relay
    #[serde(rename = "error")]
    Error { message: String },
}

/// Minimal stream info returned by the relay
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    pub stream_id: String,
    pub publisher: Option<String>,
}

/// Events produced by the WebSocket transport
#[derive(Debug)]
pub enum WsEvent {
    /// The WebSocket connection is open and ready
    Open,
    /// A text (JSON) message was received
    Text(String),
    /// A binary frame was received (stream data)
    Binary(Vec<u8>),
    /// The connection was closed
    Closed(String),
    /// An error occurred
    Error(String),
}

/// WebSocket transport for communicating with a native relay node
pub struct WsTransport {
    ws: WebSocket,
    events_rx: mpsc::UnboundedReceiver<WsEvent>,
    _closures: Rc<RefCell<Vec<Closure<dyn FnMut(JsValue)>>>>,
}

impl WsTransport {
    /// Connect to a relay node at the given WebSocket URL
    pub fn connect(url: &str) -> Result<Self, String> {
        let ws = WebSocket::new(url)
            .map_err(|e| format!("WebSocket::new failed: {:?}", e))?;
        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

        let (events_tx, events_rx) = mpsc::unbounded();
        let closures: Rc<RefCell<Vec<Closure<dyn FnMut(JsValue)>>>> =
            Rc::new(RefCell::new(Vec::new()));

        // onopen
        {
            let tx = events_tx.clone();
            let cb = Closure::wrap(Box::new(move |_evt: JsValue| {
                let _ = tx.unbounded_send(WsEvent::Open);
            }) as Box<dyn FnMut(JsValue)>);
            ws.set_onopen(Some(cb.as_ref().unchecked_ref()));
            closures.borrow_mut().push(cb);
        }

        // onmessage
        {
            let tx = events_tx.clone();
            let cb = Closure::wrap(Box::new(move |evt: JsValue| {
                let evt: MessageEvent = evt.unchecked_into();
                if let Ok(text) = evt.data().dyn_into::<js_sys::JsString>() {
                    let _ = tx.unbounded_send(WsEvent::Text(String::from(text)));
                } else if let Ok(buf) = evt.data().dyn_into::<js_sys::ArrayBuffer>() {
                    let arr = js_sys::Uint8Array::new(&buf);
                    let _ = tx.unbounded_send(WsEvent::Binary(arr.to_vec()));
                }
            }) as Box<dyn FnMut(JsValue)>);
            ws.set_onmessage(Some(cb.as_ref().unchecked_ref()));
            closures.borrow_mut().push(cb);
        }

        // onerror
        {
            let tx = events_tx.clone();
            let cb = Closure::wrap(Box::new(move |evt: JsValue| {
                let err: ErrorEvent = evt.unchecked_into();
                let _ = tx.unbounded_send(WsEvent::Error(err.message()));
            }) as Box<dyn FnMut(JsValue)>);
            ws.set_onerror(Some(cb.as_ref().unchecked_ref()));
            closures.borrow_mut().push(cb);
        }

        // onclose
        {
            let tx = events_tx;
            let cb = Closure::wrap(Box::new(move |evt: JsValue| {
                let close: CloseEvent = evt.unchecked_into();
                let reason = if close.reason().is_empty() {
                    format!("code={}", close.code())
                } else {
                    close.reason()
                };
                let _ = tx.unbounded_send(WsEvent::Closed(reason));
            }) as Box<dyn FnMut(JsValue)>);
            ws.set_onclose(Some(cb.as_ref().unchecked_ref()));
            closures.borrow_mut().push(cb);
        }

        Ok(Self {
            ws,
            events_rx,
            _closures: closures,
        })
    }

    /// Wait until the WebSocket `onopen` event fires.
    ///
    /// Any events received before `Open` are re-queued so they
    /// are not lost.
    pub async fn wait_for_open(&mut self) -> Result<(), String> {
        let mut buffered = Vec::new();
        while let Some(event) = self.events_rx.next().await {
            match event {
                WsEvent::Open => {
                    // Re-queue any events that arrived before Open
                    if !buffered.is_empty() {
                        let (tx, new_rx) = mpsc::unbounded();
                        for evt in buffered {
                            let _ = tx.unbounded_send(evt);
                        }
                        // Forward remaining events from old rx
                        let mut old_rx = std::mem::replace(
                            &mut self.events_rx,
                            new_rx,
                        );
                        wasm_bindgen_futures::spawn_local(
                            async move {
                                while let Some(e) =
                                    old_rx.next().await
                                {
                                    if tx.unbounded_send(e).is_err()
                                    {
                                        break;
                                    }
                                }
                            },
                        );
                    }
                    return Ok(());
                }
                WsEvent::Error(e) => {
                    return Err(format!(
                        "WebSocket error before open: {}",
                        e
                    ));
                }
                WsEvent::Closed(reason) => {
                    return Err(format!(
                        "WebSocket closed before open: {}",
                        reason
                    ));
                }
                other => buffered.push(other),
            }
        }
        Err("WebSocket event channel closed before open".into())
    }

    /// Send a JSON control message
    pub fn send_message(&self, msg: &RelayMessage) -> Result<(), String> {
        let json = serde_json::to_string(msg)
            .map_err(|e| format!("serialize failed: {}", e))?;
        self.ws
            .send_with_str(&json)
            .map_err(|e| format!("send_with_str failed: {:?}", e))
    }

    /// Send raw binary data (stream segment)
    pub fn send_binary(&self, data: &[u8]) -> Result<(), String> {
        self.ws
            .send_with_u8_array(data)
            .map_err(|e| format!("send_with_u8_array failed: {:?}", e))
    }

    /// Take ownership of the event receiver.
    ///
    /// After calling this, `next_event()` will always return `None`.
    /// The caller owns the receiver and can poll it directly.
    pub fn take_event_receiver(
        &mut self,
    ) -> Option<mpsc::UnboundedReceiver<WsEvent>> {
        Some(std::mem::replace(
            &mut self.events_rx,
            mpsc::unbounded().1,
        ))
    }

    /// Receive the next event from the WebSocket
    pub async fn next_event(&mut self) -> Option<WsEvent> {
        self.events_rx.next().await
    }

    /// Close the WebSocket connection
    pub fn close(&self) {
        let _ = self.ws.close();
    }

    /// Check if the WebSocket is open
    pub fn is_open(&self) -> bool {
        self.ws.ready_state() == WebSocket::OPEN
    }
}

impl Drop for WsTransport {
    fn drop(&mut self) {
        self.close();
    }
}
