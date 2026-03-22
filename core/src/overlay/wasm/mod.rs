//! WASM overlay implementation using WebSocket relay
//!
//! Connects browsers to the P2P network via a native relay node's
//! `/ws/relay` endpoint. Stream data flows as binary WebSocket frames
//! using `WireSegment` serialization; control messages are JSON text frames.

mod overlay_impl;
mod ws_transport;

pub use overlay_impl::WasmOverlay;
pub use ws_transport::{RelayMessage, WsTransport};
