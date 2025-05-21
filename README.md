# Decentralized Live Streaming CDN

A peer-to-peer, decentralized live streaming content delivery network built with Rust.

## Project Purpose

This project aims to create a scalable, decentralized live-streaming system that minimizes reliance on centralized infrastructure while providing high-quality, low-latency streaming capabilities across various platforms.

Key features:
- Hybrid tree-mesh overlay network for optimal streaming performance
- WebRTC and QUIC transport layers for browser and native clients
- End-to-end encryption for content security
- Geographic clustering for latency optimization
- Decentralized moderation system
- Creator-controlled trusted relay networks

## Project Structure

This project uses a mono-repo structure:
- `/core`: Core networking and streaming protocols
- `/node`: CLI relay node implementation
- `/ui`: Web-based viewer interface
- `/proto`: Protocol definitions and shared types
- `/scripts`: Development and deployment utilities

## Getting Started

*Documentation coming soon*

## Development

### Prerequisites
- Rust toolchain (latest stable)
- wasm-pack (for UI development)
- trunk (for UI development)

### Building

```bash
# Build the entire workspace
cargo build

# Build the node CLI
cargo build -p decentralized-stream-node

# Build the UI (requires wasm-pack)
cd ui
wasm-pack build
```

## AI Prompt Style Guide

When generating code for this project:
- Be explicit about architecture (traits, modules, file names)
- Include unit and property-based tests
- Use a sketch-then-complete approach
- Keep modules focused and single-responsibility
- Maintain Rust idioms and naming conventions

## License

*TBD* 