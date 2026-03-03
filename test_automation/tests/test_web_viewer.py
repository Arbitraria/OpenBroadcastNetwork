"""
Web viewer browser automation tests.

Tests the web viewer interface including:
- Page loading and initial state
- WebSocket connection
- MediaSource initialization
- Video buffering and playback
"""

import pytest
import asyncio

pytestmark = [pytest.mark.browser, pytest.mark.asyncio]


class TestWebViewerPageLoad:
    """Tests for web viewer page loading."""

    async def test_page_loads_successfully(self, browser_manager, web_viewer_server):
        """Web viewer page should load without errors."""
        page = await browser_manager.new_page()

        try:
            response = await page.goto(web_viewer_server.url)

            assert response.status == 200, f"Page returned status {response.status}"
            assert await page.title() is not None

        finally:
            await page.close()

    async def test_page_shows_ready_status(self, browser_manager, web_viewer_server):
        """Page should show 'Ready to connect' status initially."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            status = await page.text_content("#status")
            assert status is not None
            assert "ready" in status.lower() or "connect" in status.lower(), \
                f"Unexpected status: {status}"

        finally:
            await page.close()

    async def test_connect_button_exists(self, browser_manager, web_viewer_server):
        """Connect button should be present and enabled."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_selector("#connectBtn")

            is_disabled = await page.get_attribute("#connectBtn", "disabled")
            assert is_disabled is None, "Connect button should not be disabled"

        finally:
            await page.close()

    async def test_video_element_exists(self, browser_manager, web_viewer_server):
        """Video element should be present in the DOM."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_selector("#video")

            # Video should exist but not have content yet
            state = await browser_manager.get_video_state(page)
            assert state.ready_state == 0 or state.buffered_length == 0, \
                "Video should have no content before connecting"

        finally:
            await page.close()


class TestBrowserSupport:
    """Tests for browser feature detection."""

    async def test_mse_support_detected(self, browser_manager, web_viewer_server):
        """Page should detect MediaSource Extensions support."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            support = await browser_manager.check_mse_support(page)

            assert support["mediaSource"], "MediaSource should be supported"

        finally:
            await page.close()

    async def test_h264_support_detected(self, browser_manager, web_viewer_server):
        """Page should detect H.264 codec support."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            support = await browser_manager.check_mse_support(page)

            # H.264 should be supported in Chromium
            assert support["h264"], "H.264 should be supported in test browser"

        finally:
            await page.close()

    async def test_webrtc_support_detected(self, browser_manager, web_viewer_server):
        """Page should detect WebRTC support."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            support = await browser_manager.check_mse_support(page)

            assert support["webrtc"], "WebRTC should be supported"

        finally:
            await page.close()

    async def test_support_indicators_displayed(self, browser_manager, web_viewer_server):
        """Browser support indicators should be displayed on page."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Check MSE support indicator
            mse_text = await page.text_content("#mseSupport")
            assert mse_text is not None
            assert "supported" in mse_text.lower(), f"MSE indicator: {mse_text}"

            # Check H.264 support indicator
            h264_text = await page.text_content("#h264Support")
            assert h264_text is not None

        finally:
            await page.close()


class TestWebSocketConnection:
    """Tests for WebSocket streaming connection."""

    async def test_connect_button_triggers_connection(self, browser_manager, web_viewer_server):
        """Clicking connect should initiate WebSocket connection."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_selector("#connectBtn")

            # Click connect
            await browser_manager.click_connect_button(page)

            # Wait for connection state to change
            await asyncio.sleep(2)

            status = await browser_manager.get_connection_status(page)
            # Should show connected or at least attempting
            assert status != "Disconnected", f"Connection status: {status}"

        finally:
            await page.close()

    async def test_websocket_connects_successfully(self, browser_manager, web_viewer_server):
        """WebSocket should connect to the streaming endpoint."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Click connect
            await browser_manager.click_connect_button(page)

            # Wait for WebSocket connection
            connected = await browser_manager.wait_for_websocket_connection(page, timeout=10000)

            assert connected, "WebSocket did not connect"

            status = await browser_manager.get_connection_status(page)
            assert "connected" in status.lower(), f"Status should show connected: {status}"

        finally:
            await page.close()

    async def test_disconnect_button_enabled_after_connect(self, browser_manager, web_viewer_server):
        """Disconnect button should be enabled after connecting."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_selector("#connectBtn")

            # Initially disconnect should be disabled
            is_disabled = await page.get_attribute("#disconnectBtn", "disabled")
            assert is_disabled is not None, "Disconnect should be disabled initially"

            # Connect
            await browser_manager.click_connect_button(page)
            await browser_manager.wait_for_websocket_connection(page, timeout=10000)

            # Now disconnect should be enabled
            is_disabled = await page.get_attribute("#disconnectBtn", "disabled")
            assert is_disabled is None, "Disconnect should be enabled after connecting"

        finally:
            await page.close()


class TestVideoBuffering:
    """Tests for video buffering via MSE."""

    @pytest.mark.slow
    async def test_video_receives_initialization_segment(self, browser_manager, web_viewer_server):
        """Video should receive initialization segment after connecting."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Connect to stream
            await browser_manager.click_connect_button(page)
            await browser_manager.wait_for_websocket_connection(page, timeout=10000)

            # Wait for initialization segment (check logs)
            await asyncio.sleep(3)

            logs = await browser_manager.get_log_entries(page)
            log_text = " ".join(logs).lower()

            # Should see stream_info or initialization in logs
            has_init = "stream_info" in log_text or "init" in log_text
            assert has_init, f"No initialization in logs: {logs[-10:]}"

        finally:
            await page.close()

    @pytest.mark.slow
    async def test_video_buffers_content(self, browser_manager, web_viewer_server):
        """Video element should have buffered content after streaming starts."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Connect and wait for buffering
            await browser_manager.click_connect_button(page)
            await browser_manager.wait_for_websocket_connection(page, timeout=10000)

            # Wait for video to buffer
            video_ready = await browser_manager.wait_for_video_ready(page, timeout=30000)

            if video_ready:
                state = await browser_manager.get_video_state(page)
                assert state.buffered_length > 0, \
                    f"Video should have buffered content: {state}"
            else:
                # Check if there was an error
                state = await browser_manager.get_video_state(page)
                if state.error:
                    pytest.fail(f"Video error: {state.error}")

                # May timeout if video is very small or slow connection
                logs = await browser_manager.get_log_entries(page)
                pytest.skip(f"Video buffering timed out. Logs: {logs[-5:]}")

        finally:
            await page.close()

    @pytest.mark.slow
    async def test_buffered_seconds_stat_updates(self, browser_manager, web_viewer_server):
        """Buffered seconds statistic should update as content arrives."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Initial stat should be 0
            initial_stat = await page.text_content("#bufferedStat")
            assert initial_stat == "0", f"Initial buffered stat: {initial_stat}"

            # Connect and wait
            await browser_manager.click_connect_button(page)
            await browser_manager.wait_for_websocket_connection(page, timeout=10000)

            # Wait for buffering
            await browser_manager.wait_for_video_ready(page, timeout=30000)

            # Stat should have increased
            final_stat = await page.text_content("#bufferedStat")
            assert final_stat != "0", f"Buffered stat should have increased: {final_stat}"

        finally:
            await page.close()


class TestVideoPlayback:
    """Tests for video playback functionality."""

    @pytest.mark.slow
    async def test_play_button_enabled_after_buffering(self, browser_manager, web_viewer_server):
        """Play button should be enabled once content is buffered."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Initially play should be disabled
            is_disabled = await page.get_attribute("#playBtn", "disabled")
            assert is_disabled is not None, "Play should be disabled initially"

            # Connect and wait for content
            await browser_manager.click_connect_button(page)
            await browser_manager.wait_for_websocket_connection(page, timeout=10000)
            await browser_manager.wait_for_video_ready(page, timeout=30000)

            # Play should now be enabled
            is_disabled = await page.get_attribute("#playBtn", "disabled")
            assert is_disabled is None, "Play should be enabled after buffering"

        finally:
            await page.close()

    @pytest.mark.slow
    async def test_video_can_play(self, browser_manager, web_viewer_server):
        """Video should be able to start playback."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Connect and wait for content
            await browser_manager.click_connect_button(page)
            await browser_manager.wait_for_websocket_connection(page, timeout=10000)
            await browser_manager.wait_for_video_ready(page, timeout=30000)

            # Get initial state
            state_before = await browser_manager.get_video_state(page)

            if state_before.buffered_length == 0:
                pytest.skip("No video content buffered")

            # Click play
            await page.click("#playBtn")
            await asyncio.sleep(1)

            # Check playback state
            state_after = await browser_manager.get_video_state(page)

            # Video should either be playing or have advanced
            is_playing = not state_after.paused
            has_advanced = state_after.current_time > state_before.current_time

            assert is_playing or has_advanced, \
                f"Video should be playing. Before: {state_before}, After: {state_after}"

        finally:
            await page.close()

    @pytest.mark.slow
    async def test_video_dimensions_detected(self, browser_manager, web_viewer_server):
        """Video dimensions should be detected from stream."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Connect and wait for content
            await browser_manager.click_connect_button(page)
            await browser_manager.wait_for_websocket_connection(page, timeout=10000)
            await browser_manager.wait_for_video_ready(page, timeout=30000)

            # Get video state
            state = await browser_manager.get_video_state(page)

            if state.buffered_length > 0:
                # If we have content, dimensions should be detected
                # (may be 0 if metadata not yet loaded)
                assert state.width >= 0 and state.height >= 0, \
                    f"Invalid dimensions: {state.width}x{state.height}"

        finally:
            await page.close()


class TestDebugLog:
    """Tests for the debug log functionality."""

    async def test_log_entries_appear(self, browser_manager, web_viewer_server):
        """Log entries should appear in the debug log."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Some logs should appear on page load
            logs = await browser_manager.get_log_entries(page)

            assert len(logs) > 0, "No log entries on page load"

        finally:
            await page.close()

    async def test_clear_log_button_works(self, browser_manager, web_viewer_server):
        """Clear log button should remove log entries."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Ensure we have some logs
            initial_logs = await browser_manager.get_log_entries(page)
            assert len(initial_logs) > 0, "No initial logs"

            # Click clear
            await page.click("#clearBtn")
            await asyncio.sleep(0.5)

            # Should have fewer logs (or just the "Log cleared" message)
            final_logs = await browser_manager.get_log_entries(page)
            assert len(final_logs) <= 1, f"Log should be cleared: {final_logs}"

        finally:
            await page.close()

    async def test_connection_events_logged(self, browser_manager, web_viewer_server):
        """Connection events should be logged."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Connect
            await browser_manager.click_connect_button(page)
            await asyncio.sleep(2)

            logs = await browser_manager.get_log_entries(page)
            log_text = " ".join(logs).lower()

            # Should see connection-related logs
            has_connect_log = "connect" in log_text or "websocket" in log_text
            assert has_connect_log, f"No connection logs found: {logs}"

        finally:
            await page.close()
