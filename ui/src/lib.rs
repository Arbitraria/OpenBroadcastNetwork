//! OpenBroadcastNetwork WASM UI module
//!
//! Provides `#[wasm_bindgen]` entry points that let the web viewer
//! connect to the P2P network through a native relay node.
#![allow(non_snake_case)]

use std::cell::RefCell;
use std::rc::Rc;

use wasm_bindgen::prelude::*;

use OpenBroadcastNetwork_core::overlay::interface::{
    Overlay, OverlayConfig, OverlayEvent, StreamId,
};
use OpenBroadcastNetwork_core::overlay::wasm::WasmOverlay;

/// Shared handle to the overlay kept across JS calls
struct Client {
    overlay: Rc<WasmOverlay>,
    segment_callback: Option<js_sys::Function>,
}

thread_local! {
    static CLIENT: RefCell<Option<Rc<RefCell<Client>>>> = RefCell::new(None);
}

fn with_client<F, R>(f: F) -> Result<R, JsValue>
where
    F: FnOnce(&Client) -> R,
{
    CLIENT.with(|c| {
        let borrow = c.borrow();
        let client_rc = borrow
            .as_ref()
            .ok_or_else(|| JsValue::from_str("not connected"))?;
        let client_ref = client_rc.borrow();
        let result = f(&client_ref);
        Ok(result)
    })
}

/// Connect to a relay node.
///
/// `relay_url` should be a WebSocket URL, e.g. `"ws://localhost:9080/ws/relay"`.
#[wasm_bindgen]
pub async fn connect(relay_url: String) -> Result<(), JsValue> {
    web_sys::console::log_1(&format!("OBN WASM: connecting to {}", relay_url).into());

    let config = OverlayConfig::default();
    let overlay = Rc::new(WasmOverlay::new(&relay_url, config));

    overlay
        .start()
        .await
        .map_err(|e| JsValue::from_str(&format!("connect failed: {}", e)))?;

    let client = Rc::new(RefCell::new(Client {
        overlay: Rc::clone(&overlay),
        segment_callback: None,
    }));

    CLIENT.with(|c| {
        *c.borrow_mut() = Some(Rc::clone(&client));
    });

    // Spawn event loop that dispatches incoming segments to JS
    let client_weak = Rc::clone(&client);
    wasm_bindgen_futures::spawn_local(async move {
        loop {
            let event = client_weak.borrow().overlay.next_event().await;
            match event {
                Some(OpenBroadcastNetwork_core::overlay::interface::OverlayEvent::StreamData {
                    data, ..
                }) => {
                    let borrow = client_weak.borrow();
                    if let Some(ref cb) = borrow.segment_callback {
                        let arr = js_sys::Uint8Array::from(data.as_slice());
                        let _ = cb.call1(&JsValue::NULL, &arr);
                    }
                }
                Some(_) => {}
                None => break,
            }
        }
    });

    web_sys::console::log_1(&"OBN WASM: connected".into());
    Ok(())
}

/// Subscribe to a stream by its ID.
#[wasm_bindgen]
pub async fn subscribe_stream(stream_id: String) -> Result<(), JsValue> {
    let overlay = with_client(|c| Rc::clone(&c.overlay))?;
    let sid = StreamId::from_string(&stream_id);
    overlay
        .subscribe_stream(&sid)
        .await
        .map_err(|e| JsValue::from_str(&format!("subscribe failed: {}", e)))?;
    web_sys::console::log_1(
        &format!("OBN WASM: subscribed to {}", stream_id).into(),
    );
    Ok(())
}

/// Register a callback that receives each incoming segment as a `Uint8Array`.
#[wasm_bindgen]
pub fn on_segment(callback: js_sys::Function) {
    CLIENT.with(|c| {
        let borrow = c.borrow();
        if let Some(ref client) = *borrow {
            client.borrow_mut().segment_callback = Some(callback);
        }
    });
}

/// List available streams. Returns a JSON string.
///
/// Sends a `ListStreams` request to the relay and waits for the
/// `StreamList` response via the overlay event loop.
#[wasm_bindgen]
pub async fn list_streams() -> Result<JsValue, JsValue> {
    let overlay = with_client(|c| Rc::clone(&c.overlay))?;

    overlay
        .request_stream_list()
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    // Wait for the StreamList event (with a timeout via a counter)
    let mut attempts = 0u32;
    loop {
        let event = overlay.next_event().await;
        match event {
            Some(OverlayEvent::StreamList { streams }) => {
                let json = serde_json::to_string(&streams)
                    .unwrap_or_else(|_| "[]".to_string());
                return Ok(JsValue::from_str(&json));
            }
            Some(_) => {
                // Not our event — keep waiting (bounded)
                attempts += 1;
                if attempts > 100 {
                    return Ok(JsValue::from_str("[]"));
                }
            }
            None => {
                return Ok(JsValue::from_str("[]"));
            }
        }
    }
}

/// Disconnect from the relay.
#[wasm_bindgen]
pub async fn disconnect() -> Result<(), JsValue> {
    CLIENT.with(|c| {
        if let Some(client) = c.borrow_mut().take() {
            let borrow = client.borrow();
            wasm_bindgen_futures::spawn_local({
                let overlay = Rc::clone(&borrow.overlay);
                async move {
                    let _ = overlay.stop().await;
                }
            });
        }
    });
    Ok(())
}
