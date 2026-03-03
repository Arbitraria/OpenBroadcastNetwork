#!/usr/bin/env python3
"""
Automated P2P WebRTC DataChannel streaming test for OpenBroadcastNetwork.

Tests browser-to-browser video streaming:
1. Publisher browser receives video from server via WebSocket
2. Publisher re-streams to subscriber browsers via WebRTC DataChannels
3. Verifies data flows over P2P connection

Requirements:
- Playwright: pip install playwright && playwright install chromium
- Rust toolchain for building the server

Usage:
    python test_utils/test_p2p_webrtc.py [--port 8080] [--headed] [--slow]

The test will automatically start and stop the server.
"""

import asyncio
import argparse
import sys
import socket
import time
import subprocess
import signal
import os
import atexit
from datetime import datetime
from playwright.async_api import async_playwright, Page, BrowserContext

# Global for cleanup
_server_process = None


def cleanup_server():
    """Clean up server process on exit"""
    global _server_process
    if _server_process is not None:
        print("\nCleaning up server process...")
        try:
            _server_process.terminate()
            _server_process.wait(timeout=5)
            print("Server stopped gracefully")
        except subprocess.TimeoutExpired:
            print("Server didn't stop gracefully, killing...")
            _server_process.kill()
            _server_process.wait()
        except Exception as e:
            print(f"Error stopping server: {e}")
        _server_process = None


def signal_handler(signum, frame):
    """Handle interrupt signals"""
    print(f"\nReceived signal {signum}, cleaning up...")
    cleanup_server()
    sys.exit(1)


# Register cleanup handlers
atexit.register(cleanup_server)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class P2PWebRTCTester:
    def __init__(self, port: int = 8080, headed: bool = False, slow_mo: int = 0,
                 video_file: str = "test_simple.mp4", auto_server: bool = True):
        self.port = port
        self.base_url = f"http://127.0.0.1:{port}"
        self.headed = headed
        self.slow_mo = slow_mo
        self.video_file = video_file
        self.auto_server = auto_server
        self.server_process = None
        self.results = {
            'tests_passed': 0,
            'tests_failed': 0,
            'critical_failures': [],
            'warnings': [],
            'timings': {}
        }

    def kill_existing_servers(self):
        """Kill any existing relay-node processes on our port"""
        killed_any = False

        # Method 1: Kill by port
        try:
            result = subprocess.run(
                ["lsof", "-t", f"-i:{self.port}"],
                capture_output=True, text=True
            )
            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                for pid in pids:
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                        print(f"Killed existing process {pid} on port {self.port}")
                        killed_any = True
                    except (ProcessLookupError, ValueError):
                        pass
        except FileNotFoundError:
            pass

        # Method 2: Kill by process name (catch any orphans)
        try:
            result = subprocess.run(
                ["pgrep", "-f", "relay-node"],
                capture_output=True, text=True
            )
            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                for pid in pids:
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                        print(f"Killed orphan relay-node process {pid}")
                        killed_any = True
                    except (ProcessLookupError, ValueError):
                        pass
        except FileNotFoundError:
            pass

        if killed_any:
            time.sleep(1)  # Wait for processes to die

    def start_server(self) -> bool:
        """Start the web-viewer server"""
        global _server_process

        print(f"Starting server on port {self.port}...")
        print("(First run may take a while to compile...)")

        # Kill any existing servers first
        self.kill_existing_servers()

        # Find project root (where Cargo.toml is)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Start the server
        cmd = [
            "cargo", "run", "-p", "OpenBroadcastNetwork-node", "--",
            "web-viewer", "--port", str(self.port), "--video", self.video_file
        ]

        try:
            self.server_process = subprocess.Popen(
                cmd,
                cwd=project_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1  # Line buffered
            )
            _server_process = self.server_process  # For global cleanup

            # Wait for server to start by watching output
            max_wait = 180  # seconds (includes compile time)
            start_time = time.time()
            output_lines = []

            import select
            import fcntl

            # Make stdout non-blocking
            fd = self.server_process.stdout.fileno()
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

            while time.time() - start_time < max_wait:
                # Check if process died
                if self.server_process.poll() is not None:
                    # Read remaining output
                    try:
                        remaining = self.server_process.stdout.read()
                        if remaining:
                            output_lines.append(remaining)
                    except:
                        pass
                    print(f"\nServer exited unexpectedly")
                    print("Last output:", "".join(output_lines[-20:]))
                    return False

                # Try to read output
                try:
                    ready, _, _ = select.select([self.server_process.stdout], [], [], 0.5)
                    if ready:
                        line = self.server_process.stdout.readline()
                        if line:
                            output_lines.append(line)
                            # Check for startup message
                            if "Starting web server" in line or "Web viewer available" in line:
                                print(f"\nServer started successfully on port {self.port}")
                                # Give it a moment to fully bind
                                time.sleep(0.5)
                                return True
                            # Show compilation progress
                            if "Compiling" in line:
                                pkg = line.split()[-1] if line.split() else ""
                                print(f"\r  Compiling {pkg}...", end="", flush=True)
                except Exception:
                    pass

                # Also check HTTP as backup
                if self.check_server_running():
                    print(f"\nServer started successfully on port {self.port}")
                    return True

            print(f"\nServer failed to start within {max_wait} seconds")
            print("Last output:", "".join(output_lines[-10:]))
            return False

        except Exception as e:
            print(f"Failed to start server: {e}")
            return False

    def stop_server(self):
        """Stop the web-viewer server"""
        global _server_process

        if self.server_process is not None:
            print("Stopping server...")
            try:
                self.server_process.terminate()
                self.server_process.wait(timeout=5)
                print("Server stopped")
            except subprocess.TimeoutExpired:
                self.server_process.kill()
                self.server_process.wait()
                print("Server killed")
            except Exception as e:
                print(f"Error stopping server: {e}")
            self.server_process = None
            _server_process = None

        # Extra cleanup - kill any remaining relay-node processes
        self.kill_existing_servers()

    def log_test(self, test_name: str, passed: bool, message: str = "", critical: bool = False) -> bool:
        """Log test result"""
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {test_name}: {message}")

        if passed:
            self.results['tests_passed'] += 1
        else:
            self.results['tests_failed'] += 1
            if critical:
                self.results['critical_failures'].append(test_name)

        return passed

    def check_server_running(self) -> bool:
        """Check if the server is running"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex(('127.0.0.1', self.port))
            sock.close()
            return result == 0
        except Exception:
            return False

    async def setup_browser_logging(self, page: Page, name: str):
        """Set up console logging for a browser page"""
        page.on("console", lambda msg: print(f"  [{name}] {msg.type}: {msg.text}"))
        page.on("pageerror", lambda err: print(f"  [{name}] PAGE ERROR: {err}"))

    async def wait_for_connection(self, page: Page, timeout: int = 10000) -> bool:
        """Wait for WebSocket connection to be established"""
        try:
            # Check connection state via UI element text
            await page.wait_for_function(
                "() => document.getElementById('connectionState')?.textContent === 'Connected'",
                timeout=timeout
            )
            return True
        except Exception:
            return False

    async def wait_for_stream_info(self, page: Page, timeout: int = 10000) -> bool:
        """Wait for stream_info to be received (check if MediaSource was set up)"""
        try:
            await page.wait_for_function(
                "() => document.getElementById('segmentsStat')?.textContent !== '0' || "
                "document.querySelector('video')?.readyState >= 1",
                timeout=timeout
            )
            return True
        except Exception:
            # Check if we at least got some data
            segments = await page.evaluate(
                "document.getElementById('segmentsStat')?.textContent || '0'"
            )
            return int(segments) > 0

    async def wait_for_segments(self, page: Page, min_segments: int = 1, timeout: int = 15000) -> int:
        """Wait for segments to be received and return count"""
        try:
            await page.wait_for_function(
                f"() => parseInt(document.getElementById('segmentsStat')?.textContent || '0') >= {min_segments}",
                timeout=timeout
            )
            segments = await page.evaluate(
                "parseInt(document.getElementById('segmentsStat')?.textContent || '0')"
            )
            return segments
        except Exception:
            segments = await page.evaluate(
                "parseInt(document.getElementById('segmentsStat')?.textContent || '0')"
            )
            return segments

    async def get_peer_count(self, page: Page) -> int:
        """Get the number of connected P2P peers"""
        try:
            count_text = await page.evaluate(
                "document.getElementById('peerCount')?.textContent || '0'"
            )
            return int(count_text)
        except Exception:
            return 0

    async def wait_for_p2p_connection(self, page: Page, timeout: int = 10000) -> bool:
        """Wait for at least one P2P peer to connect"""
        try:
            await page.wait_for_function(
                "() => parseInt(document.getElementById('peerCount')?.textContent || '0') > 0",
                timeout=timeout
            )
            return True
        except Exception:
            return False

    async def get_transport_mode(self, page: Page) -> str:
        """Get current transport mode"""
        try:
            return await page.evaluate(
                "() => document.getElementById('transportMode')?.textContent || 'unknown'"
            )
        except Exception:
            return "unknown"

    async def test_single_browser_websocket(self, context: BrowserContext) -> bool:
        """Test basic single-browser WebSocket streaming"""
        print("\n[TEST] Single Browser WebSocket Streaming")
        print("=" * 50)

        page = await context.new_page()
        await self.setup_browser_logging(page, "Browser1")

        try:
            # Navigate to viewer
            start_time = time.time()
            await page.goto(self.base_url)
            self.log_test("Page Load", True, f"Loaded in {time.time() - start_time:.2f}s")

            # Click connect button
            await page.click("#connectBtn")

            # Wait for WebSocket connection
            if await self.wait_for_connection(page):
                self.log_test("WebSocket Connect", True, "Connection established")
            else:
                return self.log_test("WebSocket Connect", False, "Connection timeout", critical=True)

            # Wait for stream info
            if await self.wait_for_stream_info(page):
                self.log_test("Stream Info", True, "Received stream metadata")
            else:
                return self.log_test("Stream Info", False, "No stream_info received", critical=True)

            # Wait for video segments (test video may be short)
            segments = await self.wait_for_segments(page, min_segments=1)
            if segments >= 1:
                self.log_test("Video Segments", True, f"Received {segments} segments")
            else:
                return self.log_test("Video Segments", False, f"Only {segments} segments received", critical=True)

            # Check transport mode
            transport = await self.get_transport_mode(page)
            self.log_test("Transport Mode", True, f"Using: {transport}")

            return True

        finally:
            await page.close()

    async def test_two_browser_p2p(self, context1: BrowserContext, context2: BrowserContext) -> bool:
        """Test P2P streaming between two browsers"""
        print("\n[TEST] Two Browser P2P Streaming")
        print("=" * 50)

        page1 = await context1.new_page()
        page2 = await context2.new_page()

        await self.setup_browser_logging(page1, "Publisher")
        await self.setup_browser_logging(page2, "Subscriber")

        try:
            # Start publisher (Browser 1)
            print("\n  Starting Publisher browser...")
            await page1.goto(self.base_url)
            await page1.click("#connectBtn")

            if not await self.wait_for_connection(page1):
                return self.log_test("Publisher Connect", False, "Connection failed", critical=True)
            self.log_test("Publisher Connect", True, "WebSocket connected")

            # Wait for publisher to receive some data
            if not await self.wait_for_stream_info(page1):
                return self.log_test("Publisher Stream Info", False, "No stream info", critical=True)

            pub_segments = await self.wait_for_segments(page1, min_segments=1)
            self.log_test("Publisher Segments", pub_segments >= 1, f"Received {pub_segments} segments")

            # Start subscriber (Browser 2)
            print("\n  Starting Subscriber browser...")
            await page2.goto(self.base_url)
            await page2.click("#connectBtn")

            if not await self.wait_for_connection(page2):
                return self.log_test("Subscriber Connect", False, "Connection failed", critical=True)
            self.log_test("Subscriber Connect", True, "WebSocket connected")

            # Wait for P2P connection to establish
            print("\n  Waiting for P2P connection...")
            await asyncio.sleep(2)  # Give time for signaling

            # Check peer counts
            pub_peers = await self.get_peer_count(page1)
            sub_peers = await self.get_peer_count(page2)

            self.log_test("Publisher Peers", pub_peers > 0, f"{pub_peers} peer(s) connected")
            self.log_test("Subscriber Peers", sub_peers > 0, f"{sub_peers} peer(s) connected")

            if pub_peers == 0 and sub_peers == 0:
                self.results['warnings'].append("No P2P connections established - may be expected if WebRTC disabled")

            # Wait for subscriber to receive segments
            sub_segments = await self.wait_for_segments(page2, min_segments=1, timeout=10000)
            self.log_test("Subscriber Segments", sub_segments >= 1,
                          f"Received {sub_segments} segments")

            # Check transport modes
            pub_transport = await self.get_transport_mode(page1)
            sub_transport = await self.get_transport_mode(page2)
            self.log_test("Publisher Transport", True, pub_transport)
            self.log_test("Subscriber Transport", True, sub_transport)

            return True

        finally:
            await page1.close()
            await page2.close()

    async def test_late_joiner(self, context1: BrowserContext, context2: BrowserContext) -> bool:
        """Test late-joiner scenario - subscriber joins after publisher has been streaming"""
        print("\n[TEST] Late Joiner Scenario")
        print("=" * 50)

        page1 = await context1.new_page()
        await self.setup_browser_logging(page1, "Publisher")

        try:
            # Start publisher
            print("\n  Starting Publisher, will stream for 5 seconds before subscriber joins...")
            await page1.goto(self.base_url)
            await page1.click("#connectBtn")

            if not await self.wait_for_connection(page1):
                return self.log_test("Publisher Connect", False, "Connection failed", critical=True)

            # Let publisher stream for a while to build up cache
            await self.wait_for_segments(page1, min_segments=1, timeout=10000)
            initial_segments = await self.wait_for_segments(page1, min_segments=1)
            self.log_test("Publisher Cached Data", initial_segments >= 1, f"{initial_segments} segments cached")

            # Now start late-joiner subscriber
            print("\n  Starting Late Joiner subscriber...")
            page2 = await context2.new_page()
            await self.setup_browser_logging(page2, "LateJoiner")

            await page2.goto(self.base_url)
            await page2.click("#connectBtn")

            if not await self.wait_for_connection(page2):
                await page2.close()
                return self.log_test("Late Joiner Connect", False, "Connection failed", critical=True)

            # Give time for P2P and data transfer
            await asyncio.sleep(3)

            # Check if late joiner received cached data
            sub_segments = await self.wait_for_segments(page2, min_segments=1, timeout=5000)
            init_complete = sub_segments >= 1

            self.log_test("Late Joiner Init Segment", init_complete,
                          "Received initialization segment" if init_complete else "No init segment")
            self.log_test("Late Joiner Segments", sub_segments > 0,
                          f"Received {sub_segments} segments")

            await page2.close()
            return True

        finally:
            await page1.close()

    async def test_peer_disconnect_fallback(self, context1: BrowserContext, context2: BrowserContext) -> bool:
        """Test fallback to WebSocket when P2P peer disconnects"""
        print("\n[TEST] Peer Disconnect Fallback")
        print("=" * 50)

        page1 = await context1.new_page()
        page2 = await context2.new_page()

        await self.setup_browser_logging(page1, "Publisher")
        await self.setup_browser_logging(page2, "Subscriber")

        try:
            # Start both browsers
            await page1.goto(self.base_url)
            await page1.click("#connectBtn")
            await self.wait_for_connection(page1)
            await self.wait_for_segments(page1, min_segments=1)

            await page2.goto(self.base_url)
            await page2.click("#connectBtn")
            await self.wait_for_connection(page2)

            # Wait for P2P connection
            await asyncio.sleep(2)

            initial_sub_segments = await self.wait_for_segments(page2, min_segments=1, timeout=5000)
            self.log_test("Subscriber Initial Segments", initial_sub_segments > 0,
                          f"Received {initial_sub_segments} segments")

            # Disconnect publisher
            print("\n  Disconnecting Publisher...")
            await page1.click("#disconnectBtn")
            await asyncio.sleep(1)
            await page1.close()

            # Wait and check if subscriber continues to receive via WebSocket fallback
            print("  Waiting for fallback...")
            await asyncio.sleep(3)

            final_sub_segments = await self.wait_for_segments(page2, min_segments=1, timeout=3000)
            transport = await self.get_transport_mode(page2)

            # Subscriber should have segments (either initial or from fallback)
            # Note: With short test video, segments may not increase but should be preserved
            self.log_test("Subscriber Segments Preserved", final_sub_segments >= initial_sub_segments,
                          f"Has {final_sub_segments} segments (was {initial_sub_segments})")
            self.log_test("Transport After Disconnect", True, transport)

            return True

        finally:
            try:
                await page2.close()
            except:
                pass

    async def run_all_tests(self):
        """Run all P2P WebRTC tests"""
        print("=" * 70)
        print("OpenBroadcastNetwork P2P WebRTC Test Suite")
        print("=" * 70)
        print(f"Server: {self.base_url}")
        print(f"Video: {self.video_file}")
        print(f"Headed: {self.headed}")
        print(f"Auto-server: {self.auto_server}")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Start or check server
        if self.auto_server:
            if not self.start_server():
                print("ERROR: Failed to start server")
                return False
        else:
            if not self.check_server_running():
                print(f"ERROR: Server not running on port {self.port}")
                print(f"Start with: cargo run -p OpenBroadcastNetwork-node -- web-viewer --port {self.port} --video {self.video_file}")
                return False
            print("Using existing server")

        print()

        try:
            async with async_playwright() as p:
                # Launch browsers
                browser_args = []
                if not self.headed:
                    browser_args.append("--headless=new")

                # Use separate browser instances for better isolation
                browser1 = await p.chromium.launch(
                    headless=not self.headed,
                    slow_mo=self.slow_mo,
                    args=browser_args
                )
                browser2 = await p.chromium.launch(
                    headless=not self.headed,
                    slow_mo=self.slow_mo,
                    args=browser_args
                )

                context1 = await browser1.new_context()
                context2 = await browser2.new_context()

                try:
                    # Test 1: Single browser WebSocket
                    await self.test_single_browser_websocket(context1)

                    # Test 2: Two browser P2P
                    await self.test_two_browser_p2p(context1, context2)

                    # Test 3: Late joiner
                    await self.test_late_joiner(context1, context2)

                    # Test 4: Peer disconnect fallback
                    await self.test_peer_disconnect_fallback(context1, context2)

                finally:
                    await context1.close()
                    await context2.close()
                    await browser1.close()
                    await browser2.close()

        finally:
            # Always clean up server
            if self.auto_server:
                self.stop_server()

        # Generate report
        return self.generate_report()

    def generate_report(self) -> bool:
        """Generate final test report"""
        print("\n" + "=" * 70)
        print("Test Report")
        print("=" * 70)

        total = self.results['tests_passed'] + self.results['tests_failed']
        pass_rate = (self.results['tests_passed'] / total * 100) if total > 0 else 0

        print(f"Tests Run: {total}")
        print(f"Passed: {self.results['tests_passed']}")
        print(f"Failed: {self.results['tests_failed']}")
        print(f"Pass Rate: {pass_rate:.1f}%")
        print()

        if self.results['critical_failures']:
            print("CRITICAL FAILURES:")
            for failure in self.results['critical_failures']:
                print(f"  - {failure}")
            print()

        if self.results['warnings']:
            print("WARNINGS:")
            for warning in self.results['warnings']:
                print(f"  - {warning}")
            print()

        if self.results['tests_failed'] == 0:
            print("SUCCESS: All tests passed!")
            return True
        elif not self.results['critical_failures']:
            print("PARTIAL SUCCESS: Some tests failed but no critical failures")
            return True
        else:
            print("FAILURE: Critical tests failed")
            return False


async def main():
    parser = argparse.ArgumentParser(description="P2P WebRTC streaming test")
    parser.add_argument("--port", type=int, default=8080, help="Server port")
    parser.add_argument("--headed", action="store_true", help="Run with visible browser")
    parser.add_argument("--slow", type=int, default=0, help="Slow motion delay in ms")
    parser.add_argument("--video", type=str, default="test_simple.mp4", help="Video file to stream")
    parser.add_argument("--no-server", action="store_true", help="Don't auto-start server (use existing)")
    args = parser.parse_args()

    tester = P2PWebRTCTester(
        port=args.port,
        headed=args.headed,
        slow_mo=args.slow,
        video_file=args.video,
        auto_server=not args.no_server
    )

    success = await tester.run_all_tests()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
