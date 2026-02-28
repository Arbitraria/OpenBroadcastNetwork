# OpenBroadcastNetwork

A decentralized peer-to-peer live streaming CDN built with Rust and libp2p.

## Project Purpose

OpenBroadcastNetwork creates a scalable, decentralized streaming system that minimizes reliance on centralized infrastructure while providing high-quality, low-latency streaming capabilities across various platforms.

## Key Features

- **Hybrid Tree-Mesh Overlay**: Optimized topology combining tree and mesh structures for efficient content distribution
- **libp2p Integration**: Built on libp2p 0.53.0 for robust peer-to-peer networking
- **Multiple Transport Protocols**: WebRTC and QUIC support for browser and native clients
- **DHT-Based Discovery**: Kademlia DHT for decentralized peer discovery
- **GossipSub Messaging**: Efficient pub/sub communication protocol
- **Geographic Clustering**: Topology rebalancing for latency optimization
- **Rich CLI Visualization**: Professional network monitoring and visualization tools
- **End-to-End Encryption**: Content security through libp2p's built-in encryption

## Project Structure

This project uses a mono-repo structure:
- `/core`: Core networking and streaming protocols
- `/node`: CLI relay node implementation
- `/ui`: Web-based viewer interface
- `/proto`: Protocol definitions and shared types
- `/scripts`: Development and deployment utilities

## Getting Started

### Quick Demo

Try the interactive demo to see the network visualization in action:

```bash
# Run the simple demo with interactive menu
./simple_demo.sh
```

### Running a Relay Node

```bash
# Build the project
cargo build --release

# Start a relay node with DHT discovery and geo-aware rebalancing
cargo run -p OpenBroadcastNetwork-node -- run --role relay --listen 127.0.0.1:9000 --dht --geo-aware

# In another terminal, check network status
cargo run -p OpenBroadcastNetwork-node -- status --node 127.0.0.1:9000

# Visualize the network topology
cargo run -p OpenBroadcastNetwork-node -- visualize --node 127.0.0.1:9000 --format text
```

### CLI Commands

- **`run`**: Start a relay node with various roles (relay, publisher, consumer)
- **`status`**: Display network status with beautiful Unicode formatting
- **`list-streams`**: Show active streams in the network
- **`visualize`**: Generate network topology in multiple formats (text, dot, json)
- **`bootstrap-server`**: Run a DHT bootstrap server for peer discovery
- **`web-viewer`**: Web-based video viewer with P2P streaming support

### P2P Streaming

Stream video peer-to-peer with automatic discovery via Kademlia DHT:

```bash
# Terminal 1: Start bootstrap server for peer discovery
cargo run -p OpenBroadcastNetwork-node -- bootstrap-server --port 9000

# Terminal 2: Publisher - stream a video file (note the Peer ID from bootstrap output)
cargo run -p OpenBroadcastNetwork-node -- web-viewer \
  --port 9080 --video test_simple.mp4 --publish \
  --bootstrap /ip4/127.0.0.1/tcp/9000/p2p/<BOOTSTRAP_PEER_ID>

# Terminal 3: Subscriber - watch the stream (use Stream ID from publisher output)
cargo run -p OpenBroadcastNetwork-node -- web-viewer \
  --port 9081 --stream-id <STREAM_ID> \
  --bootstrap /ip4/127.0.0.1/tcp/9000/p2p/<BOOTSTRAP_PEER_ID>
```

Open `http://127.0.0.1:9080` (publisher) or `http://127.0.0.1:9081` (subscriber) in a browser.

## Development

### Prerequisites
- Rust toolchain (latest stable)
- wasm-pack (for UI development)
- trunk (for UI development)

### Building

```bash
# Build the entire workspace
cargo build

# Build the node CLI (release mode recommended)
cargo build --release --package OpenBroadcastNetwork-node

# Build the UI (requires wasm-pack)
cd ui
wasm-pack build
```

### Testing

```bash
# Run all tests
cargo test

# Run integration tests
cargo test --package OpenBroadcastNetwork-core --test phase1_integration

# Run specific test categories
cargo test discovery
cargo test overlay
```

## Current Status

**Phase 1 Complete**: ✅ 
- libp2p 0.53.0 integration
- DHT-based peer discovery (Kademlia)
- Bootstrap peer discovery
- GossipSub pub/sub messaging
- Hybrid tree-mesh overlay topology
- Geographic-aware rebalancing
- Complete CLI with visualization
- Comprehensive test coverage

**Note**: mDNS local discovery has been removed in favor of DHT-based discovery for better scalability and reduced dependencies.

## AI Prompt Style Guide

When generating code for this project:
- Be explicit about architecture (traits, modules, file names)
- Include unit and property-based tests
- Use a sketch-then-complete approach
- Keep modules focused and single-responsibility
- Maintain Rust idioms and naming conventions

## License

*TBD* 