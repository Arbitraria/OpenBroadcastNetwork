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

# Start a relay node
cargo run --package OpenBroadcastNetwork-node --bin relay-node -- run --role relay --listen 127.0.0.1:9000

# In another terminal, check network status
cargo run --package OpenBroadcastNetwork-node --bin relay-node -- status --node 127.0.0.1:9000

# Visualize the network topology
cargo run --package OpenBroadcastNetwork-node --bin relay-node -- visualize --node 127.0.0.1:9000 --format text
```

### CLI Commands

- **`run`**: Start a relay node with various roles (relay, publisher, consumer)
- **`status`**: Display network status with beautiful Unicode formatting
- **`list-streams`**: Show active streams in the network
- **`visualize`**: Generate network topology in multiple formats (text, dot, json)

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