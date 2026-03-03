"""
Pytest configuration and shared fixtures for OpenBroadcastNetwork test automation.

This module provides fixtures for:
- Node management (bootstrap server, relay nodes)
- Browser automation (Playwright)
- WebSocket connections
"""

import pytest
import asyncio
import os
import sys

# Add the fixtures directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from fixtures.node_manager import NodeManager
from fixtures.browser_fixtures import BrowserManager


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--headed",
        action="store_true",
        default=False,
        help="Run browser tests in headed mode (visible browser)"
    )
    parser.addoption(
        "--browser",
        action="append",
        default=[],
        help="Browser(s) to test: chromium, firefox, webkit"
    )
    parser.addoption(
        "--video-file",
        action="store",
        default="test_simple.mp4",
        help="Video file to use for streaming tests"
    )
    parser.addoption(
        "--base-port",
        action="store",
        default=9100,
        type=int,
        help="Base port for test servers (avoids conflicts with dev servers)"
    )
    parser.addoption(
        "--cargo-release",
        action="store_true",
        default=False,
        help="Use release build instead of debug"
    )


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "browser: marks tests that require browser automation"
    )
    config.addinivalue_line(
        "markers", "network: marks tests that require multiple nodes"
    )
    config.addinivalue_line(
        "markers", "webrtc: marks tests that require WebRTC support"
    )


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def video_file(request, project_root):
    """Return the path to the test video file."""
    video_name = request.config.getoption("--video-file")
    video_path = os.path.join(project_root, video_name)

    if not os.path.exists(video_path):
        pytest.skip(f"Video file not found: {video_path}")

    return video_path


@pytest.fixture(scope="session")
def base_port(request):
    """Return the base port for test servers."""
    return request.config.getoption("--base-port")


@pytest.fixture(scope="session")
def cargo_release(request):
    """Return whether to use release build."""
    return request.config.getoption("--cargo-release")


@pytest.fixture(scope="session")
def headed_mode(request):
    """Return whether to run browsers in headed mode."""
    return request.config.getoption("--headed")


@pytest.fixture(scope="session")
def browser_types(request):
    """Return the list of browsers to test."""
    browsers = request.config.getoption("--browser")
    if not browsers:
        browsers = ["chromium"]  # Default to Chromium
    return browsers


@pytest.fixture
async def node_manager(project_root, base_port, cargo_release):
    """
    Provide a NodeManager instance for managing relay nodes.

    Usage:
        async def test_example(node_manager):
            bootstrap = await node_manager.start_bootstrap_server(port=9100)
            relay = await node_manager.start_relay(port=9101, bootstrap=bootstrap.multiaddr)
    """
    manager = NodeManager(
        project_root=project_root,
        base_port=base_port,
        release=cargo_release
    )

    yield manager

    # Cleanup: stop all nodes
    await manager.cleanup()


@pytest.fixture
async def browser_manager(headed_mode, browser_types):
    """
    Provide a BrowserManager instance for browser automation.

    Usage:
        async def test_example(browser_manager):
            page = await browser_manager.new_page()
            await page.goto("http://localhost:8080")
    """
    from playwright.async_api import async_playwright

    playwright = await async_playwright().start()
    browsers = {}

    for browser_type in browser_types:
        browser_launcher = getattr(playwright, browser_type, None)
        if browser_launcher:
            browsers[browser_type] = await browser_launcher.launch(
                headless=not headed_mode
            )

    # Create a simple manager-like object
    class SimpleBrowserManager:
        def __init__(self, browsers, playwright):
            self._browsers = browsers
            self._playwright = playwright
            self._pages = []

        async def new_page(self, browser_type="chromium"):
            browser = self._browsers.get(browser_type)
            if not browser:
                browser = list(self._browsers.values())[0]
            page = await browser.new_page()
            self._pages.append(page)
            return page

        async def click_connect_button(self, page, selector="#connectBtn"):
            await page.click(selector)

        async def wait_for_websocket_connection(self, page, timeout=10000):
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

        async def get_connection_status(self, page):
            return await page.text_content("#connectionState") or "Unknown"

        async def get_video_state(self, page, selector="#video"):
            from fixtures.browser_fixtures import VideoPlaybackState
            state = await page.evaluate(f"""() => {{
                const video = document.querySelector('{selector}');
                if (!video) return null;
                let bufferedLength = 0;
                if (video.buffered.length > 0) {{
                    bufferedLength = video.buffered.end(video.buffered.length - 1) - video.buffered.start(0);
                }}
                return {{
                    ready_state: video.readyState,
                    buffered_length: bufferedLength,
                    current_time: video.currentTime,
                    duration: video.duration || 0,
                    paused: video.paused,
                    error: video.error ? video.error.message : null,
                    width: video.videoWidth,
                    height: video.videoHeight
                }};
            }}""")
            if state is None:
                return VideoPlaybackState(
                    ready_state=0, buffered_length=0, current_time=0,
                    duration=0, paused=True, error="Video element not found",
                    width=0, height=0
                )
            return VideoPlaybackState(**state)

        async def wait_for_video_ready(self, page, selector="#video", timeout=30000):
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

        async def check_mse_support(self, page):
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

        async def get_log_entries(self, page, selector="#log"):
            return await page.evaluate(f"""() => {{
                const log = document.querySelector('{selector}');
                if (!log) return [];
                return Array.from(log.querySelectorAll('.log-entry')).map(e => e.textContent);
            }}""")

        async def inject_webrtc_test_helpers(self, page):
            pass  # Simplified for now

        async def get_webrtc_state(self, page):
            return {"peerConnectionCount": 0, "dataChannelCount": 0, "dataChannels": []}

    manager = SimpleBrowserManager(browsers, playwright)

    yield manager

    # Cleanup
    for page in manager._pages:
        try:
            await page.close()
        except Exception:
            pass
    for browser in browsers.values():
        try:
            await browser.close()
        except Exception:
            pass
    await playwright.stop()


@pytest.fixture
async def web_viewer_server(project_root, video_file, base_port):
    """
    Start a web viewer server for testing.

    Returns the server info including URL and port.
    """
    import subprocess
    import os
    import asyncio
    from fixtures.node_manager import NodeInfo

    port = base_port + 50  # Use offset to avoid conflicts
    binary = os.path.join(project_root, "target", "debug", "relay-node")

    process = subprocess.Popen(
        [binary, "web-viewer", "--port", str(port), "--video", video_file],
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    # Wait for server to start
    await asyncio.sleep(2)

    server = NodeInfo(
        pid=process.pid,
        port=port,
        role="web_viewer",
        process=process,
        url=f"http://127.0.0.1:{port}"
    )

    yield server

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


@pytest.fixture
async def browser_page(browser_manager):
    """
    Provide a fresh browser page for each test.

    The page is automatically closed after the test.
    """
    page = await browser_manager.new_page()

    yield page

    await page.close()


# Async test configuration
pytest_plugins = ['pytest_asyncio']
