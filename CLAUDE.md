# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OpenBroadcastNetwork is a decentralized live streaming CDN built with Rust using a hybrid tree-mesh overlay network. The system provides peer-to-peer streaming with WebRTC and QUIC transport layers, end-to-end encryption, and geographic clustering for optimal performance.

### Key Features
- **Decentralized P2P CDN**: All viewers are nodes; headless relays permitted
- **Hybrid topology**: Tree-based primary relay with mesh fallback
- **Cross-platform**: Initial browser support (WebRTC), future native apps
- **Privacy-focused**: One-hop removal from identity, no persistent ID required
- **Scalable**: Designed for millions of concurrent users
- **Incentivized**: Token economy for relaying, content, and moderation

### Development Philosophy
- **AI-first development**: Optimized for Claude/Cursor workflows
- **Type-driven**: Strict type system with comprehensive reference docs
- **Test-driven**: Unit and integration tests for all components
- **Modular**: Single-responsibility modules with clear interfaces

## Architecture

### Workspace Structure
- `core/` - Core networking and streaming protocols (main library)
- `node/` - CLI relay node implementation
- `ui/` - Web-based viewer interface (WASM)
- `proto/` - Protocol definitions and shared types

### Core Architecture Patterns

The system follows a modular overlay network architecture:

1. **Overlay Network** (`core/src/overlay/`)
   - `interface.rs` - Core trait definitions and types
   - `libp2p/` - libp2p-based implementation with modular components:
     - `peer_manager.rs` - Peer connection management
     - `relay_manager.rs` - Stream relaying
     - `mesh_manager.rs` - Mesh topology management
     - `swarm.rs` - libp2p swarm initialization and event loop
   - `topology/` - Network topology management
   - `relay/` - Stream relay functionality

2. **Core Type System** (see `docs/TYPE_REFERENCE.md`)
   - `LocalPeerId` - Local wrapper around libp2p::PeerId
   - `OverlayConfig` - Main configuration struct
   - `StreamId` - Unique stream identifiers
   - Manager types wrapped in `Arc<T>` for shared ownership

3. **Concurrency Model**
   - Uses tokio runtime exclusively
   - `Arc<Mutex<T>>` for exclusive access to shared state
   - `Arc<RwLock<T>>` for read-heavy data structures
   - Manager components are shared via `Arc<>` across threads

## Common Development Commands

### Building
```bash
# Build entire workspace
cargo build

# Build specific package
cargo build -p OpenBroadcastNetwork-core
cargo build -p OpenBroadcastNetwork-node

# Check for compilation errors (faster)
cargo check

# Build UI (requires wasm-pack)
cd ui && wasm-pack build
```

### Testing
```bash
# Run all tests
cargo test

# Run tests for specific package
cargo test -p OpenBroadcastNetwork-core

# Run specific test
cargo test overlay_integration

# Run tests with output
cargo test -- --nocapture
```

### Development Workflow
```bash
# Check code formatting
cargo fmt --check

# Apply formatting
cargo fmt

# Run clippy lints
cargo clippy

# Run clippy with all features
cargo clippy --all-features --all-targets

# Full check pipeline (recommended before commits)
cargo check && cargo test && cargo clippy
```

### CLI Usage
```bash
# Run relay node with default settings
cargo run -p OpenBroadcastNetwork-node -- run

# Run with geo-aware rebalancing and DHT discovery
cargo run -p OpenBroadcastNetwork-node -- run --geo-aware --dht

# Run with specific role and listen address
cargo run -p OpenBroadcastNetwork-node -- run --role relay --listen 0.0.0.0:9000

# View network status
cargo run -p OpenBroadcastNetwork-node -- status

# Visualize the network topology
cargo run -p OpenBroadcastNetwork-node -- visualize --format text
```

### P2P Streaming Commands
```bash
# Start bootstrap server for peer discovery (DHT-based)
cargo run -p OpenBroadcastNetwork-node -- bootstrap-server --port 9000

# Start web viewer as publisher (streams local video to P2P network)
cargo run -p OpenBroadcastNetwork-node -- web-viewer \
  --port 9080 --video test_simple.mp4 --publish \
  --bootstrap /ip4/127.0.0.1/tcp/9000/p2p/<PEER_ID>

# Start web viewer as subscriber (receives P2P stream)
cargo run -p OpenBroadcastNetwork-node -- web-viewer \
  --port 9081 --stream-id <STREAM_ID> \
  --bootstrap /ip4/127.0.0.1/tcp/9000/p2p/<PEER_ID>

# Web viewer for local playback only (no P2P)
cargo run -p OpenBroadcastNetwork-node -- web-viewer --port 8080 --video sample.mp4
```

## Logging and Debugging

### Server Logs Location
- **Primary logs directory**: `logs/` (from project root)
- **Current server log**: `logs/server_current.log` or `logs/server_sample.log`
- **Historical logs**: `logs/server_final_working.log`, `logs/server_output.log`, etc.

### Accessing Logs
```bash
# View recent server output
tail -f logs/server_current.log

# Check server startup logs
head -20 logs/server_startup.log

# Search for specific errors
grep -i "error\|fail" logs/*.log

# Find log files by pattern
ls -la logs/server*.log
```

### Test Files and Debugging
- **Test videos**: 
  - `test_simple.mp4` (32KB, short, video-only)
  - `sample_video.mp4` (257MB, full-length, H.264 + AAC)
  - `Stargate SG1 S01E03.mp4` (360MB, target test file)
  - `bigtroublelittlechina.mp4` (1.9GB, target test file)
- **WebSocket tests**: 
  - `test_utils/test_websocket_order.py` - Message flow analysis
  - `test_utils/test_chrome_codec.py` - Chrome compatibility testing
  - `test_utils/test_modernized_viewer.py` - Comprehensive end-to-end test
- **Browser testing**: `http://127.0.0.1:8080/` (modernized universal viewer)

## Key Implementation Guidelines

### Dependency Management (Critical)
- **libp2p version**: Standardize on 0.53.0 across all crates
- **Features**: Only use compatible features: `["tokio", "tcp", "dns", "gossipsub", "identify", "kad", "noise", "yamux", "relay", "autonat", "dcutr", "macros"]`
- **Avoid**: `async-io`, `quic` (dependency conflicts), `async-std`
- **Runtime**: Use tokio exclusively (not async-std)
- **Documentation**: Update `docs/DEPENDENCIES.md` for any dependency changes
- **Pinning**: All versions must be pinned in Cargo.toml

### Type Conversions
- Always use proper conversions between `LocalPeerId` and `libp2p::PeerId`
- Use `.into()` for standard conversions
- Reference `docs/TYPE_REFERENCE.md` for method signatures

### Error Handling
- Use `OverlayError` variants for overlay network errors
- Convert third-party errors appropriately
- Include context in error messages

### Configuration
- Follow patterns in `docs/CODE_ORGANIZATION.md`
- Manager types should accept config structs in constructors
- Use `Default` implementations for sensible defaults

### Code Quality
- Maximum line length: 100 characters
- Run `cargo fmt` and `cargo clippy` before commits
- Document all public items
- Use `#[deny(warnings)]` for strict compilation

## Current State

The libp2p refactoring has landed on `main`. The codebase uses a modular libp2p-based architecture with:

- No mDNS dependency (DHT + bootstrap discovery)
- Modular peer and relay management
- Standardized type system
- WebSocket/MSE-based web viewer for streaming

## Development Phases

### Phase 1 (Complete): Core P2P Protocol
- [x] Basic peer discovery and connection
- [x] Pub/sub topic creation (GossipSub)
- [x] Tree-mesh hybrid relay logic
- [x] Geo-aware rebalancing
- [x] Relay node CLI with logging

### Phase 2 (Complete): Streaming Pipeline
- [x] Audio/video chunking and distribution (MP4 parsing, fMP4 fragmentation)
- [x] Web-based viewer with WebSocket/MSE
- [x] WebRTC transport integration (browser-native via webrtc-client.js + signaling server)
- [x] Stream validation (content hash integrity, composite validators, keypair signing)

### Phase 3: UI and Tooling
- [x] CLI broadcasting tool (`publish` subcommand wrapping web-viewer --publish)
- [x] Web UI for viewing streams (stream browser sidebar in main viewer)
- [x] Stream registry and discovery (`list-streams` queries running node's HTTP API)
- [x] WASM integration (dual-target core + ui crate with relay proxy)

### Future Phases
- Moderation and privacy features
- Token economy and incentives
- Cross-platform native clients
- Production scaling

## AI Development Workflow

This project follows structured AI-assisted development patterns defined in `docs/promptplan.json`:

1. **PLAN MODE**: Analyze and create detailed action plans
2. **IMPLEMENT MODE**: Make agreed-upon changes with real code
3. **TEST MODE**: Run checks and tests, report issues

### AI Prompt Guidelines
- Be explicit about architecture (traits, modules, file names)
- Request unit and integration tests
- Use sketch-then-complete approach for large features
- Keep tasks modular and focused (300-500 lines per request)
- Follow Rust idioms and project conventions

## Essential Reference Documents

When working on this codebase, always reference:
- `docs/TYPE_REFERENCE.md` - Type definitions and conversion patterns
- `docs/CODE_ORGANIZATION.md` - Modular structure and refactoring guidelines
- `docs/DEPENDENCIES.md` - Dependency management and compatibility
- `.windsurfrules` - Project-specific development rules
- `docs/Decentralized Streaming Spec` - Complete system requirements