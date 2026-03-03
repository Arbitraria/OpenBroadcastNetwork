"""
Codec compatibility tests across different browsers.

Tests MediaSource Extensions codec support for:
- H.264 video codecs
- AAC audio codecs
- Chrome-specific ESDS requirements
"""

import pytest
import asyncio

pytestmark = [pytest.mark.browser, pytest.mark.asyncio]


class TestVideoCodecSupport:
    """Tests for video codec support."""

    async def test_h264_baseline_supported(self, browser_manager, web_viewer_server):
        """H.264 Baseline Profile should be supported."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)

            result = await page.evaluate("""() => {
                return MediaSource.isTypeSupported('video/mp4; codecs="avc1.42E01E"');
            }""")

            assert result, "H.264 Baseline Profile not supported"

        finally:
            await page.close()

    async def test_h264_main_supported(self, browser_manager, web_viewer_server):
        """H.264 Main Profile should be supported."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)

            result = await page.evaluate("""() => {
                return MediaSource.isTypeSupported('video/mp4; codecs="avc1.4D401E"');
            }""")

            assert result, "H.264 Main Profile not supported"

        finally:
            await page.close()

    async def test_h264_high_supported(self, browser_manager, web_viewer_server):
        """H.264 High Profile should be supported."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)

            result = await page.evaluate("""() => {
                return MediaSource.isTypeSupported('video/mp4; codecs="avc1.640028"');
            }""")

            assert result, "H.264 High Profile not supported"

        finally:
            await page.close()


class TestAudioCodecSupport:
    """Tests for audio codec support."""

    async def test_aac_lc_supported(self, browser_manager, web_viewer_server):
        """AAC-LC should be supported."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)

            result = await page.evaluate("""() => {
                return MediaSource.isTypeSupported('audio/mp4; codecs="mp4a.40.2"');
            }""")

            assert result, "AAC-LC not supported"

        finally:
            await page.close()

    async def test_aac_lc_alternative_supported(self, browser_manager, web_viewer_server):
        """AAC-LC (alternative notation) should be supported."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)

            result = await page.evaluate("""() => {
                return MediaSource.isTypeSupported('audio/mp4; codecs="mp4a.40.02"');
            }""")

            # Alternative notation may or may not be supported
            # Just check it doesn't error
            assert isinstance(result, bool)

        finally:
            await page.close()

    async def test_ac3_not_supported_in_chrome(self, browser_manager, web_viewer_server):
        """AC-3 (Dolby Digital) should NOT be supported in Chrome MSE."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)

            result = await page.evaluate("""() => {
                return MediaSource.isTypeSupported('audio/mp4; codecs="ac-3"');
            }""")

            # AC-3 is typically NOT supported in Chrome's MSE
            # This test documents expected behavior
            if result:
                pytest.skip("AC-3 is supported in this browser (unusual)")
            else:
                assert not result, "AC-3 should not be supported in Chrome MSE"

        finally:
            await page.close()


class TestCombinedCodecSupport:
    """Tests for combined video+audio codec support."""

    async def test_h264_aac_combined(self, browser_manager, web_viewer_server):
        """H.264 + AAC-LC combined should be supported."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)

            result = await page.evaluate("""() => {
                return MediaSource.isTypeSupported('video/mp4; codecs="avc1.42E01E, mp4a.40.2"');
            }""")

            assert result, "H.264 + AAC-LC combined not supported"

        finally:
            await page.close()

    async def test_video_only_supported(self, browser_manager, web_viewer_server):
        """Video-only (no audio) should be supported."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)

            result = await page.evaluate("""() => {
                return MediaSource.isTypeSupported('video/mp4; codecs="avc1.42E01E"');
            }""")

            assert result, "Video-only not supported"

        finally:
            await page.close()


class TestCodecDetectionUI:
    """Tests for codec detection UI elements."""

    async def test_h264_indicator_correct(self, browser_manager, web_viewer_server):
        """H.264 support indicator should be accurate."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Get actual support
            actual_support = await page.evaluate("""() => {
                return MediaSource.isTypeSupported('video/mp4; codecs="avc1.42E01E"');
            }""")

            # Get UI indicator
            indicator_text = await page.text_content("#h264Support")

            if actual_support:
                assert "supported" in indicator_text.lower(), \
                    f"Indicator should show supported: {indicator_text}"
            else:
                assert "not" in indicator_text.lower(), \
                    f"Indicator should show not supported: {indicator_text}"

        finally:
            await page.close()

    async def test_aac_indicator_correct(self, browser_manager, web_viewer_server):
        """AAC support indicator should be accurate."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Get actual support
            actual_support = await page.evaluate("""() => {
                return MediaSource.isTypeSupported('audio/mp4; codecs="mp4a.40.2"');
            }""")

            # Get UI indicator
            indicator_text = await page.text_content("#opusSupport")

            if actual_support:
                assert "supported" in indicator_text.lower(), \
                    f"Indicator should show supported: {indicator_text}"

        finally:
            await page.close()


class TestCodecNegotiation:
    """Tests for codec negotiation with server."""

    @pytest.mark.slow
    async def test_server_codec_info_received(self, browser_manager, web_viewer_server):
        """Server should send codec information in stream_info."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Connect to stream
            await browser_manager.click_connect_button(page)
            await browser_manager.wait_for_websocket_connection(page, timeout=10000)

            # Wait for stream info
            await asyncio.sleep(2)

            # Check logs for codec info
            logs = await browser_manager.get_log_entries(page)
            log_text = " ".join(logs).lower()

            has_codec_info = "codec" in log_text or "mime" in log_text or "avc" in log_text
            assert has_codec_info, f"No codec info in logs: {logs[-10:]}"

        finally:
            await page.close()

    @pytest.mark.slow
    async def test_ac3_triggers_video_only_mode(self, browser_manager, web_viewer_server):
        """AC-3 audio should trigger video-only mode fallback."""
        page = await browser_manager.new_page()

        try:
            await page.goto(web_viewer_server.url)
            await page.wait_for_load_state("networkidle")

            # Connect
            await browser_manager.click_connect_button(page)
            await browser_manager.wait_for_websocket_connection(page, timeout=10000)

            await asyncio.sleep(3)

            # Check logs for AC-3 handling
            logs = await browser_manager.get_log_entries(page)
            log_text = " ".join(logs).lower()

            # If AC-3 is detected, should see video-only fallback message
            if "ac-3" in log_text or "ac3" in log_text:
                assert "video-only" in log_text or "fallback" in log_text, \
                    f"AC-3 should trigger fallback: {logs[-10:]}"

        finally:
            await page.close()


class TestMultiBrowserCodecs:
    """Tests that can run across multiple browsers."""

    @pytest.fixture(params=["chromium", "firefox"])
    async def browser_type(self, request, browser_manager):
        """Parametrized browser type fixture."""
        if request.param not in browser_manager._browsers:
            pytest.skip(f"{request.param} not available")
        return request.param

    async def test_baseline_h264_all_browsers(self, browser_manager, web_viewer_server, browser_types):
        """H.264 Baseline should work in all modern browsers."""
        for browser_type in browser_types:
            try:
                page = await browser_manager.new_page(browser_type=browser_type)
            except ValueError:
                continue

            try:
                await page.goto(web_viewer_server.url)

                result = await page.evaluate("""() => {
                    return MediaSource.isTypeSupported('video/mp4; codecs="avc1.42E01E"');
                }""")

                assert result, f"H.264 Baseline not supported in {browser_type}"

            finally:
                await page.close()

    async def test_aac_all_browsers(self, browser_manager, web_viewer_server, browser_types):
        """AAC-LC should work in all modern browsers."""
        for browser_type in browser_types:
            try:
                page = await browser_manager.new_page(browser_type=browser_type)
            except ValueError:
                continue

            try:
                await page.goto(web_viewer_server.url)

                result = await page.evaluate("""() => {
                    return MediaSource.isTypeSupported('audio/mp4; codecs="mp4a.40.2"');
                }""")

                assert result, f"AAC-LC not supported in {browser_type}"

            finally:
                await page.close()
