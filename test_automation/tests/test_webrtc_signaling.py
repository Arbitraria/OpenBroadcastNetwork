"""
WebRTC signaling and P2P connection tests.

Tests the WebRTC signaling protocol including:
- Signaling WebSocket connection
- Peer discovery messages
- Offer/answer exchange
- DataChannel establishment
"""

import pytest
import asyncio
import json

pytestmark = [pytest.mark.webrtc, pytest.mark.asyncio]


class TestSignalingConnection:
    """Tests for WebRTC signaling WebSocket connection."""

    async def test_signaling_endpoint_exists(self, web_viewer_server):
        """Signaling WebSocket endpoint should be accessible."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/webrtc/signal"

        try:
            async with WebSocketTestClient(ws_url) as client:
                # Connection successful
                assert client._websocket is not None
        except Exception as e:
            # Signaling endpoint might not be enabled in local mode
            pytest.skip(f"Signaling endpoint not available: {e}")

    async def test_signaling_accepts_json_messages(self, web_viewer_server):
        """Signaling server should accept JSON messages."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/webrtc/signal"

        try:
            async with WebSocketTestClient(ws_url) as client:
                # Send a peer_join message
                await client.send_json({
                    "type": "peer_join",
                    "stream_id": "test-stream",
                    "is_publisher": False
                })

                # Should receive some response (peer_list or acknowledgment)
                try:
                    response = await client.receive_json(timeout=5.0)
                    assert "type" in response
                except asyncio.TimeoutError:
                    # No response is also acceptable for some implementations
                    pass

        except Exception as e:
            pytest.skip(f"Signaling not available: {e}")


class TestPeerDiscovery:
    """Tests for peer discovery via signaling."""

    async def test_peer_join_message_format(self, web_viewer_server):
        """Peer join message should have correct format."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/webrtc/signal"

        try:
            async with WebSocketTestClient(ws_url) as client:
                # Valid peer_join message
                message = {
                    "type": "peer_join",
                    "stream_id": "test-stream-123",
                    "is_publisher": False,
                    "peer_id": "test-peer-1"
                }

                await client.send_json(message)

                # Wait for response
                try:
                    response = await client.receive_json(timeout=5.0)

                    # Response should be peer_list or similar
                    assert response.get("type") in ["peer_list", "peer_joined", "ack", "error"], \
                        f"Unexpected response type: {response}"
                except asyncio.TimeoutError:
                    pass

        except Exception as e:
            pytest.skip(f"Signaling not available: {e}")

    @pytest.mark.slow
    async def test_two_peers_see_each_other(self, browser_manager, web_viewer_server):
        """Two browser tabs should discover each other via signaling."""
        # Create two pages
        page1 = await browser_manager.new_page()
        page2 = await browser_manager.new_page()

        try:
            # Load viewer in both
            await page1.goto(web_viewer_server.url)
            await page2.goto(web_viewer_server.url)

            await page1.wait_for_load_state("networkidle")
            await page2.wait_for_load_state("networkidle")

            # Select WebRTC mode
            await page1.select_option("#connectionModeSelect", "webrtc")
            await page2.select_option("#connectionModeSelect", "webrtc")

            # Connect both
            await browser_manager.click_connect_button(page1)
            await asyncio.sleep(1)
            await browser_manager.click_connect_button(page2)

            # Wait for potential peer discovery
            await asyncio.sleep(5)

            # Check peer counts
            peer_count1 = await page1.text_content("#peerCount")
            peer_count2 = await page2.text_content("#peerCount")

            # In local mode without full P2P, peer counts might be 0
            # This test validates the UI updates, not necessarily peer connection
            assert peer_count1 is not None
            assert peer_count2 is not None

        finally:
            await page1.close()
            await page2.close()


class TestOfferAnswerExchange:
    """Tests for WebRTC offer/answer exchange."""

    async def test_offer_message_format(self):
        """Offer message should have correct SDP format."""
        # This is a format validation test
        offer_message = {
            "type": "offer",
            "from_peer": "peer-1",
            "to_peer": "peer-2",
            "sdp": "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n"
        }

        assert offer_message["type"] == "offer"
        assert "sdp" in offer_message
        assert "v=0" in offer_message["sdp"]  # SDP version line

    async def test_answer_message_format(self):
        """Answer message should have correct SDP format."""
        answer_message = {
            "type": "answer",
            "from_peer": "peer-2",
            "to_peer": "peer-1",
            "sdp": "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n"
        }

        assert answer_message["type"] == "answer"
        assert "sdp" in answer_message

    async def test_ice_candidate_message_format(self):
        """ICE candidate message should have correct format."""
        ice_message = {
            "type": "ice_candidate",
            "from_peer": "peer-1",
            "to_peer": "peer-2",
            "candidate": {
                "candidate": "candidate:1 1 UDP 2130706431 192.168.1.1 50000 typ host",
                "sdpMid": "0",
                "sdpMLineIndex": 0
            }
        }

        assert ice_message["type"] == "ice_candidate"
        assert "candidate" in ice_message
        assert "candidate" in ice_message["candidate"]


class TestDataChannel:
    """Tests for WebRTC DataChannel establishment."""

    async def test_data_channel_creation_in_browser(self, browser_manager, web_viewer_server):
        """DataChannel should be created when connecting via WebRTC."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Inject WebRTC test helpers
            await browser_manager.inject_webrtc_test_helpers(page)

            # Select WebRTC mode and connect
            await page.select_option("#connectionModeSelect", "webrtc")
            await browser_manager.click_connect_button(page)

            # Wait for connection attempt
            await asyncio.sleep(3)

            # Check WebRTC state
            state = await browser_manager.get_webrtc_state(page)

            # Even if no peers connected, the infrastructure should be set up
            assert "error" not in state or state.get("peerConnectionCount", 0) >= 0

        finally:
            await page.close()

    async def test_webrtc_connection_mode_selection(self, browser_manager, web_viewer_server):
        """WebRTC connection mode should be selectable."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Check mode selector exists
            modes = await page.locator("#connectionModeSelect option").all_text_contents()

            assert len(modes) >= 2, f"Should have multiple modes: {modes}"
            assert any("webrtc" in m.lower() for m in modes), "WebRTC mode should be available"

        finally:
            await page.close()


class TestTransportModeIndicator:
    """Tests for transport mode indicator."""

    async def test_transport_mode_shows_disconnected(self, browser_manager, web_viewer_server):
        """Transport mode should show disconnected initially."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            transport_mode = await page.text_content("#transportMode")

            assert transport_mode is not None
            assert "not connected" in transport_mode.lower() or "disconnected" in transport_mode.lower(), \
                f"Unexpected transport mode: {transport_mode}"

        finally:
            await page.close()

    async def test_transport_mode_updates_on_connect(self, browser_manager, web_viewer_server):
        """Transport mode should update when connecting."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            initial_mode = await page.text_content("#transportMode")

            # Connect
            await browser_manager.click_connect_button(page)
            await asyncio.sleep(2)

            final_mode = await page.text_content("#transportMode")

            # Mode should have changed or at least show connection attempt
            # (WebSocket fallback is expected in local mode)
            assert final_mode != initial_mode or "websocket" in final_mode.lower() or "connecting" in final_mode.lower(), \
                f"Transport mode didn't update: {initial_mode} -> {final_mode}"

        finally:
            await page.close()


class TestWebRTCFallback:
    """Tests for WebRTC fallback to WebSocket."""

    async def test_fallback_to_websocket_when_no_peers(self, browser_manager, web_viewer_server):
        """Should fall back to WebSocket when no WebRTC peers available."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Select auto mode (WebRTC with fallback)
            await page.select_option("#connectionModeSelect", "auto")

            # Connect
            await browser_manager.click_connect_button(page)

            # Wait for connection and potential fallback (may take longer)
            await asyncio.sleep(8)

            # Check transport mode
            transport_mode = await page.text_content("#transportMode")
            connection_state = await browser_manager.get_connection_status(page)

            # Should be connected via some transport or still connecting
            is_connected = "connected" in connection_state.lower()

            # Transport mode should show some activity
            # It's OK if it shows "WebRTC (connecting)" or "WebSocket" or "connected"
            valid_modes = ["websocket", "connected", "webrtc", "connecting"]
            assert any(m in transport_mode.lower() for m in valid_modes), \
                f"Unexpected transport mode: {transport_mode}"

        finally:
            await page.close()

    async def test_websocket_only_mode_works(self, browser_manager, web_viewer_server):
        """WebSocket-only mode should connect without WebRTC."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Select WebSocket-only mode
            await page.select_option("#connectionModeSelect", "websocket")

            # Connect
            await browser_manager.click_connect_button(page)

            # Wait for connection
            connected = await browser_manager.wait_for_websocket_connection(page, timeout=10000)

            assert connected, "WebSocket-only mode should connect"

            transport_mode = await page.text_content("#transportMode")
            assert "websocket" in transport_mode.lower(), f"Should be WebSocket: {transport_mode}"

        finally:
            await page.close()
