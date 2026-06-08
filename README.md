# OpenBroadcastNetwork

[![CI](https://github.com/Arbitraria/OpenBroadcastNetwork/actions/workflows/ci.yml/badge.svg)](https://github.com/Arbitraria/OpenBroadcastNetwork/actions/workflows/ci.yml)

A decentralized peer-to-peer live streaming CDN built in Rust. Peers discover each other via Kademlia DHT, relay media through GossipSub pub/sub, and play video in the browser using WebSocket/MSE — no central server required.

## Quick Start

### Docker (recommended)

```bash
docker compose up
# Open http://localhost:8080 — video streams through the local relay
```

### From source

```bash
# Prerequisites: Rust toolchain, libopus-dev, pkg-config
cargo build -p OpenBroadcastNetwork-node
cargo run -p OpenBroadcastNetwork-node -- web-viewer --port 8080 --video test_simple.mp4
# Open http://127.0.0.1:8080/
```

See [USAGE.md](USAGE.md) for P2P multi-node setup, relay nodes, and CLI reference.

## What Works Today

- **Local streaming pipeline** — MP4 parsing, fMP4 fragmentation, WebSocket delivery, MSE playback in Chrome/Firefox/Safari. ~4,000 lines of hand-written MP4 parser.
- **P2P relay** — Two+ nodes discover each other via Kademlia DHT and exchange stream data through GossipSub. Relay nodes forward media to downstream peers.
- **Web viewer** — Browser-based player with transport selection (WebSocket/WebRTC), stream browser sidebar, connection stats dashboard, and log viewer.
- **Moderation** — Block/unblock peers, flag streams, JSON export/import, auto-persistence. Enforced at the signaling and connection level.
- **Privacy** — Hop-removal anonymization replaces source PeerIds at the relay level.
- **Security** — Admin auth on stream control, per-IP rate limiting, security headers, CORS.
- **127 tests** across the workspace.

## Architecture

```
                  ┌─────────────────┐
                  │ Bootstrap Server │  ← Kademlia DHT peer discovery
                  └────────┬────────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
        ┌─────┴─────┐ ┌───┴───┐ ┌─────┴─────┐
        │ Publisher  │ │ Relay │ │ Subscriber │
        │ (web-     │ │  Node │ │ (web-      │
        │  viewer)  │ │       │ │  viewer)   │
        └─────┬─────┘ └───┬───┘ └─────┬─────┘
              │            │            │
              └──── GossipSub Pub/Sub ──┘
                           │
                    ┌──────┴──────┐
                    │   Browser   │  ← WebSocket/MSE or WebRTC
                    │   Viewer    │
                    └─────────────┘
```

### Workspace

| Crate | Description |
|-------|-------------|
| `core/` | Networking, streaming protocols, media pipeline, overlay network |
| `node/` | CLI relay node, web server, bootstrap server |
| `proto/` | Wire protocol message definitions |
| `ui/` | WASM viewer interface (experimental) |

### Key Technologies

- **Rust** + **tokio** async runtime
- **libp2p** 0.53 — Kademlia, GossipSub, Noise encryption, Yamux multiplexing, Relay, AutoNAT, DCUtR
- **MP4 → fMP4** fragmentation pipeline for MSE-compatible browser streaming
- **WebSocket + MSE** for browser playback, **WebRTC** for mesh P2P

## Build & Test

```bash
cargo build            # Build workspace
cargo test             # Run tests
cargo clippy           # Lint
cargo fmt --check      # Check formatting
```

## Roadmap

- [ ] Prometheus metrics export
- [ ] Real GeoIP integration (currently uses stub mapping)
- [ ] End-to-end content encryption
- [ ] Load testing and benchmarks
- [ ] Native desktop/mobile clients

## Contributing

Contributions welcome! Check out the [issues](https://github.com/Arbitraria/OpenBroadcastNetwork/issues) for good first issues.

```bash
# Development workflow
cargo check && cargo test && cargo clippy
```

## Author

**Ian Glenn** — Infrastructure & Systems Deployment Engineer
[github.com/Arbitraria](https://github.com/Arbitraria)

## License

This project is open source. See [LICENSE](LICENSE) for details.
