# OpenBroadcastNetwork

A decentralized peer-to-peer live streaming CDN built in Rust, exploring resilient infrastructure design, distributed peer discovery, and hybrid overlay topologies.

## Overview

OpenBroadcastNetwork (OBN) is a distributed streaming system built on libp2p that investigates how live media distribution can function without centralized CDN infrastructure. It supports peer discovery via Kademlia DHT and bootstrap servers, pub/sub messaging via GossipSub, and real-time media delivery through WebSocket/MSE with P2P relay forwarding. The system simulates publisher, relay, and consumer roles in a decentralized network.

## Architecture

<img width="1536" height="1024" alt="OpenBroadcastNetwork Architecture" src="https://github.com/user-attachments/assets/398e7892-5405-4a90-9d1c-8eaf6d3e0e34" />

### Workspace Structure

| Directory | Description |
|-----------|-------------|
| `core/` | Core networking, streaming protocols, and media pipeline |
| `node/` | CLI relay node and web viewer server |
| `proto/` | Protocol definitions and shared types |
| `ui/` | Web-based viewer interface (WASM) |
| `web_viewer/` | Browser-based stream viewer (HTML/JS/CSS) |
| `docs/` | Architecture specs, type references, dependency docs |
| `test_utils/` | Python test scripts for WebSocket and codec testing |

### Core Technologies

- **Rust** with **tokio** async runtime
- **libp2p** 0.53 (Kademlia DHT, GossipSub, Identify, Noise, Yamux, Relay, AutoNAT, DCUtR)
- **Hybrid tree-mesh overlay** topology for stream distribution
- **MP4 parsing and fMP4 fragmentation** for MSE-compatible streaming
- **WebSocket + Media Source Extensions** for browser playback

## Quick Start

```bash
# 1. Start a bootstrap server for peer discovery
cargo run -p OpenBroadcastNetwork-node -- bootstrap-server --port 9000

# 2. Start a publisher (streams an MP4 file to the P2P network)
cargo run -p OpenBroadcastNetwork-node -- web-viewer \
  --port 9080 --video test_simple.mp4 --publish \
  --bootstrap /ip4/127.0.0.1/tcp/9000/p2p/<BOOTSTRAP_PEER_ID>

# 3. Open http://127.0.0.1:9080/ in a browser to view the stream

# For local-only playback (no P2P):
cargo run -p OpenBroadcastNetwork-node -- web-viewer --port 8080 --video sample.mp4
```

## CLI Reference

```
OpenBroadcastNetwork-node <COMMAND>

Commands:
  run               Run a relay node with optional DHT/geo-aware rebalancing
  status            View network status and topology of a running node
  list-streams      List active streams on a running node
  visualize         Visualize the network topology (text, JSON, or DOT output)
  stream            Run streaming demo with synthetic content or a real MP4 file
  web-viewer        Start web viewer server for browser-based stream playback
  bootstrap-server  Start a bootstrap server for DHT-based peer discovery
```

## Build & Test

```bash
cargo build                  # Build entire workspace
cargo test                   # Run all tests
cargo clippy                 # Run lints
cargo fmt --check            # Check formatting

# Build specific packages
cargo build -p OpenBroadcastNetwork-core
cargo build -p OpenBroadcastNetwork-node
```

## Project Status

- **Phase 1 (Complete):** Core P2P protocol - peer discovery, GossipSub pub/sub, tree-mesh hybrid relay, geo-aware rebalancing, relay node CLI
- **Phase 2 (In Progress):** Streaming pipeline - MP4 parsing and fMP4 fragmentation working, WebSocket/MSE web viewer functional, P2P stream forwarding implemented. WebRTC transport and WASM integration are next.
- **Phase 3 (Planned):** UI and tooling - CLI broadcasting, web UI, stream registry

## Author

**Ian Glenn** - Infrastructure & Systems Deployment Engineer
[github.com/Arbitraria](https://github.com/Arbitraria)
