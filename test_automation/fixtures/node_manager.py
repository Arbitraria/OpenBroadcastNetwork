"""
Node management for OpenBroadcastNetwork test automation.

Provides utilities to start, stop, and manage relay nodes, bootstrap servers,
and web viewers for integration testing.
"""

import asyncio
import os
import re
import signal
import subprocess
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import psutil


@dataclass
class NodeInfo:
    """Information about a running node."""
    pid: int
    port: int
    role: str  # 'bootstrap', 'relay', 'web_viewer', 'publisher', 'subscriber'
    process: subprocess.Popen
    multiaddr: Optional[str] = None
    peer_id: Optional[str] = None
    stream_id: Optional[str] = None
    log_file: Optional[str] = None
    url: Optional[str] = None
    _log_buffer: List[str] = field(default_factory=list)

    async def stop(self):
        """Stop this node."""
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()

    def get_logs(self) -> str:
        """Get accumulated logs from this node."""
        return "\n".join(self._log_buffer)

    def append_log(self, line: str):
        """Append a log line to the buffer."""
        self._log_buffer.append(line)


class NodeManager:
    """
    Manages OpenBroadcastNetwork nodes for testing.

    Handles starting and stopping bootstrap servers, relay nodes,
    and web viewers with proper cleanup.
    """

    def __init__(
        self,
        project_root: str,
        base_port: int = 9100,
        release: bool = False,
        log_dir: Optional[str] = None
    ):
        """
        Initialize the NodeManager.

        Args:
            project_root: Path to the OpenBroadcastNetwork project root
            base_port: Starting port for allocating node ports
            release: Whether to use release build
            log_dir: Directory for log files (defaults to project_root/logs/test)
        """
        self.project_root = project_root
        self.base_port = base_port
        self.release = release
        self.log_dir = log_dir or os.path.join(project_root, "logs", "test")
        self.nodes: Dict[int, NodeInfo] = {}
        self._port_counter = 0

        # Ensure log directory exists
        os.makedirs(self.log_dir, exist_ok=True)

        # Build type for cargo
        self.build_type = "release" if release else "debug"
        self.binary_path = os.path.join(
            project_root, "target", self.build_type, "OpenBroadcastNetwork-node"
        )

    def _allocate_port(self) -> int:
        """Allocate a unique port for a new node."""
        port = self.base_port + self._port_counter
        self._port_counter += 1
        return port

    def _create_log_file(self, role: str, port: int) -> str:
        """Create a log file path for a node."""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"{role}_{port}_{timestamp}.log"
        return os.path.join(self.log_dir, filename)

    async def _wait_for_output(
        self,
        process: subprocess.Popen,
        pattern: str,
        timeout: float = 30.0,
        node_info: Optional[NodeInfo] = None
    ) -> Optional[re.Match]:
        """
        Wait for a specific pattern in the process output.

        Args:
            process: The subprocess to monitor
            pattern: Regex pattern to match
            timeout: Maximum time to wait in seconds
            node_info: Optional NodeInfo to accumulate logs

        Returns:
            The regex match object, or None if timeout
        """
        compiled_pattern = re.compile(pattern)
        start_time = time.time()

        while time.time() - start_time < timeout:
            if process.poll() is not None:
                # Process exited
                return None

            # Read available output
            try:
                line = process.stdout.readline()
                if line:
                    line = line.decode('utf-8', errors='replace').strip()
                    if node_info:
                        node_info.append_log(line)

                    match = compiled_pattern.search(line)
                    if match:
                        return match
            except Exception:
                pass

            await asyncio.sleep(0.1)

        return None

    async def start_bootstrap_server(
        self,
        port: Optional[int] = None,
        timeout: float = 30.0
    ) -> NodeInfo:
        """
        Start a bootstrap server for DHT peer discovery.

        Args:
            port: Port to listen on (auto-allocated if not specified)
            timeout: Maximum time to wait for startup

        Returns:
            NodeInfo with the running bootstrap server details
        """
        if port is None:
            port = self._allocate_port()

        log_file = self._create_log_file("bootstrap", port)

        # Use the built binary directly to avoid cargo compilation output
        binary = os.path.join(
            self.project_root, "target", self.build_type, "relay-node"
        )

        if os.path.exists(binary):
            cmd = [binary, "bootstrap-server", "--port", str(port)]
        else:
            # Fallback to cargo run if binary doesn't exist
            cmd = ["cargo", "run", "-p", "OpenBroadcastNetwork-node", "--",
                   "bootstrap-server", "--port", str(port)]

        process = subprocess.Popen(
            cmd,
            cwd=self.project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0  # Unbuffered
        )

        node_info = NodeInfo(
            pid=process.pid,
            port=port,
            role="bootstrap",
            process=process,
            log_file=log_file
        )

        # Wait for the bootstrap server to be ready and extract peer ID
        # Look for: "Local peer ID: <peer_id>" or similar
        peer_pattern = r"Local peer [Ii][Dd]:\s*(\w+)"
        multiaddr_pattern = r"Listening on:\s*(/ip4/[^\s]+)"

        match = await self._wait_for_output(process, peer_pattern, timeout, node_info)
        if match:
            node_info.peer_id = match.group(1)

        # Also try to get the multiaddr
        match = await self._wait_for_output(process, multiaddr_pattern, timeout=5.0, node_info=node_info)
        if match:
            node_info.multiaddr = match.group(1)
        elif node_info.peer_id:
            # Construct multiaddr from port and peer ID
            node_info.multiaddr = f"/ip4/127.0.0.1/tcp/{port}/p2p/{node_info.peer_id}"

        self.nodes[port] = node_info
        return node_info

    async def start_relay(
        self,
        port: Optional[int] = None,
        bootstrap: Optional[str] = None,
        geo_aware: bool = False,
        timeout: float = 30.0
    ) -> NodeInfo:
        """
        Start a relay node.

        Args:
            port: Port to listen on (auto-allocated if not specified)
            bootstrap: Bootstrap server multiaddr for DHT discovery
            geo_aware: Enable geo-aware rebalancing
            timeout: Maximum time to wait for startup

        Returns:
            NodeInfo with the running relay node details
        """
        if port is None:
            port = self._allocate_port()

        log_file = self._create_log_file("relay", port)

        # Use the built binary directly
        binary = os.path.join(
            self.project_root, "target", self.build_type, "relay-node"
        )

        if os.path.exists(binary):
            cmd = [binary, "run", "--role", "relay",
                   "--listen", f"0.0.0.0:{port}", "--dht"]
        else:
            cmd = ["cargo", "run", "-p", "OpenBroadcastNetwork-node", "--",
                   "run", "--role", "relay",
                   "--listen", f"0.0.0.0:{port}", "--dht"]

        if bootstrap:
            cmd.extend(["--bootstrap", bootstrap])

        if geo_aware:
            cmd.append("--geo-aware")

        process = subprocess.Popen(
            cmd,
            cwd=self.project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0  # Unbuffered
        )

        node_info = NodeInfo(
            pid=process.pid,
            port=port,
            role="relay",
            process=process,
            log_file=log_file
        )

        # Wait for startup
        peer_pattern = r"Local peer [Ii][Dd]:\s*(\w+)"
        match = await self._wait_for_output(process, peer_pattern, timeout, node_info)
        if match:
            node_info.peer_id = match.group(1)
            node_info.multiaddr = f"/ip4/127.0.0.1/tcp/{port}/p2p/{node_info.peer_id}"

        self.nodes[port] = node_info
        return node_info

    async def start_web_viewer(
        self,
        port: Optional[int] = None,
        video: Optional[str] = None,
        publish: bool = False,
        stream_id: Optional[str] = None,
        bootstrap: Optional[str] = None,
        timeout: float = 30.0
    ) -> NodeInfo:
        """
        Start a web viewer server.

        Args:
            port: HTTP port to listen on (auto-allocated if not specified)
            video: Path to video file for local playback or publishing
            publish: Whether to publish the video to the P2P network
            stream_id: Stream ID for subscribing to a P2P stream
            bootstrap: Bootstrap server multiaddr for P2P
            timeout: Maximum time to wait for startup

        Returns:
            NodeInfo with the running web viewer details
        """
        if port is None:
            port = self._allocate_port()

        log_file = self._create_log_file("web_viewer", port)

        # Use the built binary directly
        binary = os.path.join(
            self.project_root, "target", self.build_type, "relay-node"
        )

        if os.path.exists(binary):
            cmd = [binary, "web-viewer", "--port", str(port)]
        else:
            cmd = ["cargo", "run", "-p", "OpenBroadcastNetwork-node", "--",
                   "web-viewer", "--port", str(port)]

        if video:
            cmd.extend(["--video", video])

        if publish:
            cmd.append("--publish")

        if stream_id:
            cmd.extend(["--stream-id", stream_id])

        if bootstrap:
            cmd.extend(["--bootstrap", bootstrap])

        process = subprocess.Popen(
            cmd,
            cwd=self.project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0  # Unbuffered
        )

        node_info = NodeInfo(
            pid=process.pid,
            port=port,
            role="publisher" if publish else ("subscriber" if stream_id else "web_viewer"),
            process=process,
            log_file=log_file,
            url=f"http://127.0.0.1:{port}",
            stream_id=stream_id
        )

        # Wait for the server to be ready
        # Look for: "Listening on http://127.0.0.1:port" or similar
        ready_pattern = r"[Ll]istening|[Ss]tarted|[Ss]erving|[Rr]eady"
        match = await self._wait_for_output(process, ready_pattern, timeout, node_info)

        if process.poll() is not None:
            # Process exited unexpectedly
            raise RuntimeError(
                f"Web viewer failed to start. Exit code: {process.returncode}\n"
                f"Logs: {node_info.get_logs()}"
            )

        self.nodes[port] = node_info
        return node_info

    async def wait_for_peers(
        self,
        node: NodeInfo,
        count: int = 1,
        timeout: float = 30.0
    ) -> bool:
        """
        Wait for a node to discover a minimum number of peers.

        Args:
            node: The node to monitor
            count: Minimum number of peers to discover
            timeout: Maximum time to wait

        Returns:
            True if the peer count was reached, False on timeout
        """
        # Look for peer discovery messages
        pattern = r"[Dd]iscovered|[Cc]onnected.*peer|[Pp]eer.*joined"
        peers_found = 0
        start_time = time.time()

        while time.time() - start_time < timeout:
            if node.process.poll() is not None:
                return False

            try:
                line = node.process.stdout.readline()
                if line:
                    line = line.decode('utf-8', errors='replace').strip()
                    node.append_log(line)

                    if re.search(pattern, line):
                        peers_found += 1
                        if peers_found >= count:
                            return True
            except Exception:
                pass

            await asyncio.sleep(0.1)

        return False

    async def cleanup(self):
        """Stop all managed nodes and clean up resources."""
        for port, node in list(self.nodes.items()):
            try:
                await node.stop()
            except Exception as e:
                print(f"Warning: Failed to stop node on port {port}: {e}")

        self.nodes.clear()

    async def stop_node(self, port: int):
        """Stop a specific node by port."""
        if port in self.nodes:
            await self.nodes[port].stop()
            del self.nodes[port]

    def get_node(self, port: int) -> Optional[NodeInfo]:
        """Get information about a running node."""
        return self.nodes.get(port)

    def list_nodes(self) -> List[NodeInfo]:
        """List all running nodes."""
        return list(self.nodes.values())

    @staticmethod
    def kill_orphan_processes(pattern: str = "OpenBroadcastNetwork"):
        """
        Kill any orphan processes matching the pattern.

        Useful for cleanup after failed tests.
        """
        killed = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if pattern in cmdline:
                    proc.terminate()
                    killed.append(proc.info['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        return killed
