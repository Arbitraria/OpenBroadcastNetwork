"""
Browser automation fixtures for OpenBroadcastNetwork test automation.

Provides Playwright-based browser automation for testing web viewers,
WebRTC signaling, and MSE playback.
"""

import asyncio
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class VideoPlaybackState:
    """State of video playback in the browser."""
    ready_state: int  # 0=HAVE_NOTHING, 4=HAVE_ENOUGH_DATA
    buffered_length: float  # Seconds of buffered content
    current_time: float
    duration: float
    paused: bool
    error: Optional[str]
    width: int
    height: int


class BrowserManager:
    """
    Manages browser instances for testing with Playwright.

    Supports multiple browser types (Chromium, Firefox, WebKit) and
    provides utilities for WebRTC and MSE testing.
    """

    def __init__(
        self,
        headless: bool = True,
        browser_types: Optional[List[str]] = None,
        slow_mo: int = 0
    ):
        """
        Initialize the BrowserManager.

        Args:
            headless: Whether to run browsers in headless mode
            browser_types: List of browser types to support
            slow_mo: Slow down operations by this many milliseconds
        """
        self.headless = headless
        self.browser_types = browser_types or ["chromium"]
        self.slow_mo = slow_mo

        self._playwright = None
        self._browsers: Dict[str, Any] = {}
        self._contexts: List[Any] = []
        self._pages: List[Any] = []

    async def initialize(self):
        """Initialize Playwright and launch browsers."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise ImportError(
                "Playwright is required for browser tests. "
                "Install it with: pip install playwright && playwright install"
            )

        self._playwright = await async_playwright().start()

        for browser_type in self.browser_types:
            browser_launcher = getattr(self._playwright, browser_type, None)
            if browser_launcher:
                self._browsers[browser_type] = await browser_launcher.launch(
                    headless=self.headless,
                    slow_mo=self.slow_mo
                )

    async def cleanup(self):
        """Close all browsers and cleanup Playwright."""
        for page in self._pages:
            try:
                await page.close()
            except Exception:
                pass

        for context in self._contexts:
            try:
                await context.close()
            except Exception:
                pass

        for browser in self._browsers.values():
            try:
                await browser.close()
            except Exception:
                pass

        if self._playwright:
            await self._playwright.stop()

        self._pages.clear()
        self._contexts.clear()
        self._browsers.clear()

    async def new_page(
        self,
        browser_type: str = "chromium",
        viewport: Optional[Dict[str, int]] = None
    ) -> Any:
        """
        Create a new browser page.

        Args:
            browser_type: Which browser to use
            viewport: Optional viewport size {"width": 1280, "height": 720}

        Returns:
            A Playwright page object
        """
        if browser_type not in self._browsers:
            raise ValueError(f"Browser type '{browser_type}' not initialized")

        browser = self._browsers[browser_type]

        context_options = {}
        if viewport:
            context_options["viewport"] = viewport

        context = await browser.new_context(**context_options)
        self._contexts.append(context)

        page = await context.new_page()
        self._pages.append(page)

        return page

    async def wait_for_video_ready(
        self,
        page: Any,
        selector: str = "#video",
        timeout: float = 30000
    ) -> bool:
        """
        Wait for a video element to have buffered content.

        Args:
            page: Playwright page object
            selector: CSS selector for the video element
            timeout: Maximum time to wait in milliseconds

        Returns:
            True if video is ready, False on timeout
        """
        try:
            await page.wait_for_function(
                f"""() => {{
                    const video = document.querySelector('{selector}');
                    return video && video.buffered.length > 0;
                }}""",
                timeout=timeout
            )
            return True
        except Exception:
            return False

    async def get_video_state(
        self,
        page: Any,
        selector: str = "#video"
    ) -> VideoPlaybackState:
        """
        Get the current state of a video element.

        Args:
            page: Playwright page object
            selector: CSS selector for the video element

        Returns:
            VideoPlaybackState with current video info
        """
        state = await page.evaluate(f"""() => {{
            const video = document.querySelector('{selector}');
            if (!video) return null;

            let bufferedLength = 0;
            if (video.buffered.length > 0) {{
                bufferedLength = video.buffered.end(video.buffered.length - 1) - video.buffered.start(0);
            }}

            return {{
                readyState: video.readyState,
                bufferedLength: bufferedLength,
                currentTime: video.currentTime,
                duration: video.duration || 0,
                paused: video.paused,
                error: video.error ? video.error.message : null,
                width: video.videoWidth,
                height: video.videoHeight
            }};
        }}""")

        if state is None:
            return VideoPlaybackState(
                ready_state=0,
                buffered_length=0,
                current_time=0,
                duration=0,
                paused=True,
                error="Video element not found",
                width=0,
                height=0
            )

        return VideoPlaybackState(**state)

    async def click_connect_button(
        self,
        page: Any,
        selector: str = "#connectBtn"
    ):
        """Click the connect button on the web viewer."""
        await page.click(selector)

    async def wait_for_websocket_connection(
        self,
        page: Any,
        timeout: float = 10000
    ) -> bool:
        """
        Wait for WebSocket connection to be established.

        Args:
            page: Playwright page object
            timeout: Maximum time to wait in milliseconds

        Returns:
            True if connected, False on timeout
        """
        try:
            await page.wait_for_function(
                """() => {
                    const status = document.querySelector('#connectionState');
                    return status && status.textContent.includes('Connected');
                }""",
                timeout=timeout
            )
            return True
        except Exception:
            return False

    async def get_connection_status(self, page: Any) -> str:
        """Get the current connection status text."""
        return await page.text_content("#connectionState") or "Unknown"

    async def get_log_entries(self, page: Any, selector: str = "#log") -> List[str]:
        """Get all log entries from the debug log."""
        return await page.evaluate(f"""() => {{
            const log = document.querySelector('{selector}');
            if (!log) return [];
            return Array.from(log.querySelectorAll('.log-entry')).map(e => e.textContent);
        }}""")

    async def check_mse_support(self, page: Any) -> Dict[str, bool]:
        """Check MediaSource Extensions support in the browser."""
        return await page.evaluate("""() => {
            const result = {
                mediaSource: 'MediaSource' in window,
                h264: false,
                aac: false,
                webrtc: 'RTCPeerConnection' in window
            };

            if (result.mediaSource) {
                result.h264 = MediaSource.isTypeSupported('video/mp4; codecs="avc1.42E01E"');
                result.aac = MediaSource.isTypeSupported('audio/mp4; codecs="mp4a.40.2"');
            }

            return result;
        }""")

    async def inject_webrtc_test_helpers(self, page: Any):
        """Inject helper functions for WebRTC testing."""
        await page.evaluate("""() => {
            window.__webrtcTestHelpers = {
                peerConnections: [],
                dataChannels: [],

                // Hook RTCPeerConnection
                originalRTCPeerConnection: window.RTCPeerConnection,

                hookPeerConnection: function() {
                    const self = this;
                    window.RTCPeerConnection = function(...args) {
                        const pc = new self.originalRTCPeerConnection(...args);
                        self.peerConnections.push(pc);

                        const origCreateDataChannel = pc.createDataChannel.bind(pc);
                        pc.createDataChannel = function(...dcArgs) {
                            const dc = origCreateDataChannel(...dcArgs);
                            self.dataChannels.push(dc);
                            return dc;
                        };

                        return pc;
                    };
                },

                getPeerConnectionCount: function() {
                    return this.peerConnections.length;
                },

                getDataChannelCount: function() {
                    return this.dataChannels.length;
                },

                getDataChannelStates: function() {
                    return this.dataChannels.map(dc => ({
                        label: dc.label,
                        readyState: dc.readyState
                    }));
                }
            };

            window.__webrtcTestHelpers.hookPeerConnection();
        }""")

    async def get_webrtc_state(self, page: Any) -> Dict[str, Any]:
        """Get the state of WebRTC connections."""
        return await page.evaluate("""() => {
            if (!window.__webrtcTestHelpers) {
                return { error: 'Test helpers not injected' };
            }

            return {
                peerConnectionCount: window.__webrtcTestHelpers.getPeerConnectionCount(),
                dataChannelCount: window.__webrtcTestHelpers.getDataChannelCount(),
                dataChannels: window.__webrtcTestHelpers.getDataChannelStates()
            };
        }""")


class WebSocketTestClient:
    """
    Test client for WebSocket connections.

    Provides utilities for testing the WebSocket streaming protocol.
    """

    def __init__(self, url: str):
        """
        Initialize the WebSocket test client.

        Args:
            url: WebSocket URL to connect to
        """
        self.url = url
        self._websocket = None
        self._messages: List[Any] = []

    async def connect(self, timeout: float = 5.0):
        """Connect to the WebSocket server."""
        import websockets

        self._websocket = await asyncio.wait_for(
            websockets.connect(self.url),
            timeout=timeout
        )

    async def disconnect(self):
        """Disconnect from the WebSocket server."""
        if self._websocket:
            await self._websocket.close()
            self._websocket = None

    async def receive_message(self, timeout: float = 5.0) -> Any:
        """Receive a message from the WebSocket."""
        if not self._websocket:
            raise RuntimeError("Not connected")

        message = await asyncio.wait_for(
            self._websocket.recv(),
            timeout=timeout
        )

        self._messages.append(message)
        return message

    async def receive_json(self, timeout: float = 5.0) -> dict:
        """Receive and parse a JSON message."""
        import json

        message = await self.receive_message(timeout)
        if isinstance(message, str):
            return json.loads(message)
        raise ValueError("Expected text message, got binary")

    async def receive_binary(self, timeout: float = 5.0) -> bytes:
        """Receive a binary message."""
        message = await self.receive_message(timeout)
        if isinstance(message, bytes):
            return message
        raise ValueError("Expected binary message, got text")

    async def send_json(self, data: dict):
        """Send a JSON message."""
        import json

        if not self._websocket:
            raise RuntimeError("Not connected")

        await self._websocket.send(json.dumps(data))

    @property
    def message_count(self) -> int:
        """Return the number of messages received."""
        return len(self._messages)

    def get_messages(self) -> List[Any]:
        """Get all received messages."""
        return self._messages.copy()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
