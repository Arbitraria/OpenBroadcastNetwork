# OpenBroadcastNetwork Test Automation

Automated testing infrastructure for OpenBroadcastNetwork, including multi-node network testing and browser automation for WebRTC/MSE playback.

## Quick Start

```bash
# Install dependencies
cd test_automation
pip install -r requirements.txt
playwright install chromium

# Run all tests (excluding slow tests)
python run_tests.py

# Run specific test categories
python run_tests.py --browser-only    # Browser automation tests
python run_tests.py --network-only    # Multi-node network tests
python run_tests.py --webrtc-only     # WebRTC signaling tests

# Include slow tests
python run_tests.py --include-slow

# Run in headed mode (visible browser)
python run_tests.py --headed
```

## Directory Structure

```
test_automation/
├── conftest.py              # Pytest configuration and shared fixtures
├── pytest.ini               # Pytest settings
├── requirements.txt         # Python dependencies
├── run_tests.py             # Convenience test runner script
├── README.md                # This file
├── fixtures/
│   ├── __init__.py
│   ├── node_manager.py      # Start/stop relay nodes, bootstrap servers
│   └── browser_fixtures.py  # Playwright browser automation
└── tests/
    ├── __init__.py
    ├── test_network_discovery.py   # Multi-node peer discovery
    ├── test_stream_relay.py        # Publisher -> subscriber streaming
    ├── test_web_viewer.py          # Browser video playback
    ├── test_webrtc_signaling.py    # WebRTC P2P tests
    └── test_codec_compatibility.py # MSE codec tests per browser
```

## Test Categories

### Network Tests (`test_network_discovery.py`, `test_stream_relay.py`)

Tests for the P2P network layer:
- Bootstrap server startup
- Relay node discovery via DHT
- Multi-node mesh formation
- Stream publishing and subscription

### Browser Tests (`test_web_viewer.py`)

Tests for the web viewer interface:
- Page loading and initial state
- WebSocket connection
- MediaSource initialization
- Video buffering and playback

### WebRTC Tests (`test_webrtc_signaling.py`)

Tests for WebRTC P2P streaming:
- Signaling WebSocket connection
- Peer discovery messages
- Offer/answer exchange
- DataChannel establishment

### Codec Tests (`test_codec_compatibility.py`)

Tests for codec support across browsers:
- H.264 profile support (Baseline, Main, High)
- AAC codec support
- AC-3 fallback handling
- Multi-browser compatibility

## Usage Examples

### Basic Usage

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_web_viewer.py -v

# Run specific test class
pytest tests/test_web_viewer.py::TestVideoBuffering -v

# Run specific test
pytest tests/test_web_viewer.py::TestVideoBuffering::test_video_buffers_content -v
```

### Browser Options

```bash
# Run headless (default)
pytest tests/ -v

# Run with visible browser
pytest tests/ -v --headed

# Test multiple browsers
pytest tests/ -v --browser chromium --browser firefox

# Test with specific video
pytest tests/ -v --video-file sample_video.mp4
```

### Filtering Tests

```bash
# Exclude slow tests (default)
pytest tests/ -v -m "not slow"

# Only slow tests
pytest tests/ -v -m slow

# Only browser tests
pytest tests/ -v -m browser

# Only network tests
pytest tests/ -v -m network

# Combine markers
pytest tests/ -v -m "browser and not slow"
```

### Generating Reports

```bash
# HTML report
pytest tests/ -v --html=report.html --self-contained-html

# JUnit XML (for CI)
pytest tests/ -v --junitxml=results.xml
```

## Writing Tests

### Using Node Manager

```python
async def test_example(node_manager, video_file, base_port):
    # Start bootstrap server
    bootstrap = await node_manager.start_bootstrap_server(port=base_port)

    # Start relay nodes
    relay1 = await node_manager.start_relay(
        port=base_port + 1,
        bootstrap=bootstrap.multiaddr
    )
    relay2 = await node_manager.start_relay(
        port=base_port + 2,
        bootstrap=bootstrap.multiaddr
    )

    # Wait for peer discovery
    found = await node_manager.wait_for_peers(relay1, count=1, timeout=15.0)
    assert found

    # Start web viewer
    server = await node_manager.start_web_viewer(
        port=base_port + 10,
        video=video_file
    )

    # Cleanup is automatic
```

### Using Browser Manager

```python
async def test_browser_example(browser_manager, web_viewer_server):
    page = await browser_manager.new_page()

    try:
        await page.goto(web_viewer_server.url)
        await page.wait_for_load_state("networkidle")

        # Click connect
        await browser_manager.click_connect_button(page)

        # Wait for connection
        connected = await browser_manager.wait_for_websocket_connection(page)
        assert connected

        # Check video state
        state = await browser_manager.get_video_state(page)
        assert state.buffered_length > 0

    finally:
        await page.close()
```

### Using WebSocket Test Client

```python
async def test_websocket_example(web_viewer_server):
    from fixtures.browser_fixtures import WebSocketTestClient

    ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/stream"

    async with WebSocketTestClient(ws_url) as client:
        # Receive stream_info
        message = await client.receive_json(timeout=5.0)
        assert message["type"] == "stream_info"

        # Receive binary data
        data = await client.receive_binary(timeout=5.0)
        assert len(data) > 0
```

## CI Integration

### GitHub Actions Example

```yaml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Install Rust
        uses: dtolnay/rust-action@stable

      - name: Build
        run: cargo build -p OpenBroadcastNetwork-node

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install test dependencies
        run: |
          cd test_automation
          pip install -r requirements.txt
          playwright install chromium --with-deps

      - name: Run tests
        run: |
          cd test_automation
          pytest tests/ -v --junitxml=results.xml

      - name: Upload results
        uses: actions/upload-artifact@v4
        with:
          name: test-results
          path: test_automation/results.xml
```

## Troubleshooting

### Playwright Browser Not Found

```bash
# Install browsers
playwright install

# Or specific browser
playwright install chromium
```

### Port Conflicts

Use the `--base-port` option to change the starting port:

```bash
pytest tests/ -v --base-port 9200
```

### Process Cleanup

If tests leave orphan processes:

```bash
# Kill orphan processes
pkill -f OpenBroadcastNetwork-node

# Or use the built-in cleanup
python -c "from fixtures.node_manager import NodeManager; NodeManager.kill_orphan_processes()"
```

### Debug Output

```bash
# Show more output
pytest tests/ -v -s

# Show captured output even on success
pytest tests/ -v -s --capture=no
```
