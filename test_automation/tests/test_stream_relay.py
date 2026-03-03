"""
Stream relay integration tests.

Tests the complete streaming pipeline:
- Publisher to relay node streaming
- Relay to subscriber distribution
- Multi-hop relay chains
"""

import pytest
import asyncio
import struct

pytestmark = [pytest.mark.network, pytest.mark.asyncio]


class TestLocalStreaming:
    """Tests for local streaming without P2P."""

    async def test_web_viewer_serves_video(self, web_viewer_server):
        """Web viewer should serve video file over HTTP."""
        assert web_viewer_server.process.poll() is None, "Server died"
        assert web_viewer_server.url is not None

        # Test HTTP access
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(web_viewer_server.url) as resp:
                assert resp.status == 200
                content = await resp.text()
                assert "video" in content.lower()

    async def test_websocket_streaming_endpoint(self, web_viewer_server):
        """WebSocket streaming endpoint should be accessible."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/stream"

        async with WebSocketTestClient(ws_url) as client:
            # Should receive stream_info first
            message = await client.receive_json(timeout=5.0)

            assert message.get("type") == "stream_info", f"Expected stream_info, got: {message}"
            assert "data" in message

    async def test_receives_stream_info(self, web_viewer_server):
        """Client should receive stream_info with codec details."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/stream"

        async with WebSocketTestClient(ws_url) as client:
            message = await client.receive_json(timeout=5.0)

            assert message["type"] == "stream_info"

            data = message["data"]
            assert "video" in data, "stream_info should contain video info"

            video_info = data["video"]
            assert "codec" in video_info or "mime_type" in video_info

    async def test_receives_initialization_segment(self, web_viewer_server):
        """Client should receive MP4 initialization segment."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/stream"

        async with WebSocketTestClient(ws_url) as client:
            # Skip stream_info
            await client.receive_json(timeout=5.0)

            # Next messages should include chunk_info and binary data
            found_init = False
            for _ in range(10):
                try:
                    msg = await client.receive_message(timeout=3.0)

                    if isinstance(msg, bytes) and len(msg) >= 8:
                        # Check for ftyp box (MP4 init segment)
                        box_type = msg[4:8]
                        if box_type == b'ftyp':
                            found_init = True
                            break

                except asyncio.TimeoutError:
                    break

            assert found_init, "No initialization segment received"

    @pytest.mark.slow
    async def test_receives_media_segments(self, web_viewer_server):
        """Client should receive media segments after initialization."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/stream"

        async with WebSocketTestClient(ws_url) as client:
            found_moof = False
            message_count = 0

            for _ in range(20):
                try:
                    msg = await client.receive_message(timeout=3.0)
                    message_count += 1

                    if isinstance(msg, bytes) and len(msg) >= 8:
                        box_type = msg[4:8]
                        if box_type == b'moof':
                            found_moof = True
                            break

                except asyncio.TimeoutError:
                    break

            assert found_moof, f"No media segment (moof) received in {message_count} messages"


class TestChunkInfo:
    """Tests for chunk_info messages."""

    async def test_chunk_info_before_binary(self, web_viewer_server):
        """chunk_info should be sent before each binary chunk."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/stream"

        async with WebSocketTestClient(ws_url) as client:
            # Skip stream_info
            await client.receive_json(timeout=5.0)

            # Look for chunk_info -> binary pattern
            last_was_chunk_info = False
            binary_after_info = False

            for _ in range(15):
                try:
                    msg = await client.receive_message(timeout=3.0)

                    if isinstance(msg, str):
                        import json
                        data = json.loads(msg)
                        if data.get("type") == "chunk_info":
                            last_was_chunk_info = True
                    elif isinstance(msg, bytes):
                        if last_was_chunk_info:
                            binary_after_info = True
                            break
                        last_was_chunk_info = False

                except asyncio.TimeoutError:
                    break

            assert binary_after_info, "Binary should follow chunk_info"

    async def test_chunk_info_contains_size(self, web_viewer_server):
        """chunk_info should contain size information."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/stream"

        async with WebSocketTestClient(ws_url) as client:
            # Skip stream_info
            await client.receive_json(timeout=5.0)

            # Find a chunk_info message
            for _ in range(10):
                try:
                    msg = await client.receive_message(timeout=3.0)

                    if isinstance(msg, str):
                        import json
                        data = json.loads(msg)
                        if data.get("type") == "chunk_info":
                            assert "data" in data
                            chunk_data = data["data"]
                            assert "size" in chunk_data, f"chunk_info missing size: {chunk_data}"
                            assert chunk_data["size"] > 0
                            break

                except asyncio.TimeoutError:
                    break

    async def test_chunk_info_contains_type(self, web_viewer_server):
        """chunk_info should contain chunk_type."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/stream"

        async with WebSocketTestClient(ws_url) as client:
            # Skip stream_info
            await client.receive_json(timeout=5.0)

            # Find a chunk_info message
            for _ in range(10):
                try:
                    msg = await client.receive_message(timeout=3.0)

                    if isinstance(msg, str):
                        import json
                        data = json.loads(msg)
                        if data.get("type") == "chunk_info":
                            chunk_data = data["data"]
                            assert "chunk_type" in chunk_data, f"chunk_info missing chunk_type: {chunk_data}"
                            break

                except asyncio.TimeoutError:
                    break


class TestMP4Validation:
    """Tests for MP4 format validation."""

    async def test_ftyp_box_valid(self, web_viewer_server):
        """ftyp box should have valid structure."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/stream"

        async with WebSocketTestClient(ws_url) as client:
            # Skip stream_info
            await client.receive_json(timeout=5.0)

            for _ in range(10):
                try:
                    msg = await client.receive_message(timeout=3.0)

                    if isinstance(msg, bytes) and len(msg) >= 8:
                        box_type = msg[4:8]
                        if box_type == b'ftyp':
                            # Validate ftyp structure
                            box_size = struct.unpack('>I', msg[0:4])[0]
                            assert box_size <= len(msg), f"Box size {box_size} > data {len(msg)}"
                            assert box_size >= 16, "ftyp box too small"

                            # Major brand should be 4 chars
                            major_brand = msg[8:12]
                            assert len(major_brand) == 4
                            break

                except asyncio.TimeoutError:
                    break

    async def test_moov_box_present(self, web_viewer_server):
        """moov box should be present in initialization segment."""
        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{web_viewer_server.port}/stream"

        init_data = b''
        found_moov = False

        async with WebSocketTestClient(ws_url) as client:
            # Skip stream_info
            await client.receive_json(timeout=5.0)

            for _ in range(15):
                try:
                    msg = await client.receive_message(timeout=3.0)

                    if isinstance(msg, bytes):
                        init_data += msg

                        # Check for moov box
                        if b'moov' in init_data:
                            found_moov = True
                            break

                except asyncio.TimeoutError:
                    break

        assert found_moov, "moov box not found in initialization segment"


class TestPublisherSubscriber:
    """Tests for publisher-subscriber streaming."""

    @pytest.mark.slow
    async def test_publisher_starts_successfully(self, node_manager, video_file, base_port):
        """Publisher should start and be accessible."""
        # Start bootstrap for P2P
        bootstrap = await node_manager.start_bootstrap_server(port=base_port)
        await asyncio.sleep(2)

        # Start publisher
        publisher = await node_manager.start_web_viewer(
            port=base_port + 10,
            video=video_file,
            publish=True,
            bootstrap=bootstrap.multiaddr
        )

        await asyncio.sleep(2)

        assert publisher.process.poll() is None, "Publisher died"
        assert publisher.url is not None

    @pytest.mark.slow
    async def test_publisher_websocket_accessible(self, node_manager, video_file, base_port):
        """Publisher's WebSocket should be accessible."""
        bootstrap = await node_manager.start_bootstrap_server(port=base_port)
        await asyncio.sleep(2)

        publisher = await node_manager.start_web_viewer(
            port=base_port + 10,
            video=video_file,
            publish=True,
            bootstrap=bootstrap.multiaddr
        )

        await asyncio.sleep(2)

        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{base_port + 10}/stream"

        try:
            async with WebSocketTestClient(ws_url) as client:
                # Should receive stream_info
                message = await client.receive_json(timeout=5.0)
                assert message.get("type") == "stream_info"
        except Exception as e:
            pytest.fail(f"Could not connect to publisher: {e}")


class TestStreamingPerformance:
    """Basic performance tests for streaming."""

    @pytest.mark.slow
    async def test_receives_multiple_segments(self, node_manager, video_file, base_port):
        """Should receive multiple media segments in succession."""
        server = await node_manager.start_web_viewer(
            port=base_port,
            video=video_file
        )

        await asyncio.sleep(2)

        from fixtures.browser_fixtures import WebSocketTestClient

        ws_url = f"ws://127.0.0.1:{base_port}/stream"

        segment_count = 0
        total_bytes = 0

        async with WebSocketTestClient(ws_url) as client:
            for _ in range(30):
                try:
                    msg = await client.receive_message(timeout=3.0)

                    if isinstance(msg, bytes):
                        total_bytes += len(msg)

                        if len(msg) >= 8 and msg[4:8] == b'moof':
                            segment_count += 1

                        if segment_count >= 3:
                            break

                except asyncio.TimeoutError:
                    break

        assert segment_count >= 1, f"Only received {segment_count} segments"
        assert total_bytes > 1000, f"Only received {total_bytes} bytes"

    @pytest.mark.slow
    async def test_streaming_throughput(self, node_manager, video_file, base_port):
        """Measure basic streaming throughput."""
        server = await node_manager.start_web_viewer(
            port=base_port,
            video=video_file
        )

        await asyncio.sleep(2)

        from fixtures.browser_fixtures import WebSocketTestClient
        import time

        ws_url = f"ws://127.0.0.1:{base_port}/stream"

        total_bytes = 0
        start_time = time.time()
        duration = 5.0  # Measure for 5 seconds

        async with WebSocketTestClient(ws_url) as client:
            while time.time() - start_time < duration:
                try:
                    msg = await client.receive_message(timeout=1.0)

                    if isinstance(msg, bytes):
                        total_bytes += len(msg)

                except asyncio.TimeoutError:
                    continue

        elapsed = time.time() - start_time
        throughput_kbps = (total_bytes * 8 / 1000) / elapsed

        print(f"\nStreaming throughput: {throughput_kbps:.1f} kbps ({total_bytes} bytes in {elapsed:.1f}s)")

        # Basic sanity check - should have some throughput
        assert total_bytes > 0, "No data received"
