# Usage Guide

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
  publish           Publish a video file to the P2P network
  bootstrap-server  Start a bootstrap server for DHT-based peer discovery
```

## Local Playback (no P2P)

Stream a local MP4 file to the browser via WebSocket/MSE:

```bash
cargo run -p OpenBroadcastNetwork-node -- web-viewer --port 8080 --video sample.mp4
# Open http://127.0.0.1:8080/
```

## P2P Streaming

### 1. Start a bootstrap server

```bash
cargo run -p OpenBroadcastNetwork-node -- bootstrap-server --port 9000
# Note the Peer ID printed in the logs
```

### 2. Start a publisher

```bash
cargo run -p OpenBroadcastNetwork-node -- web-viewer \
  --port 8081 --video test_simple.mp4 --publish --dht \
  --bootstrap /ip4/127.0.0.1/tcp/9000/p2p/<BOOTSTRAP_PEER_ID>
# Note the Stream ID printed in the logs
```

### 3. Start a subscriber

```bash
cargo run -p OpenBroadcastNetwork-node -- web-viewer \
  --port 8082 --stream-id <STREAM_ID> --dht \
  --bootstrap /ip4/127.0.0.1/tcp/9000/p2p/<BOOTSTRAP_PEER_ID>
# Open http://127.0.0.1:8082/ to watch the stream via P2P relay
```

## Relay Node

Run a headless relay node that forwards streams between peers:

```bash
cargo run -p OpenBroadcastNetwork-node -- run --role relay --dht \
  --bootstrap /ip4/127.0.0.1/tcp/9000/p2p/<BOOTSTRAP_PEER_ID>
```

Add `--geo-aware` for geographic topology optimization.

## Stream Discovery

Query a running node's HTTP API for active streams:

```bash
cargo run -p OpenBroadcastNetwork-node -- list-streams --node 127.0.0.1:8080
```

## Network Visualization

```bash
# Text output
cargo run -p OpenBroadcastNetwork-node -- visualize --format text

# DOT graph (for Graphviz)
cargo run -p OpenBroadcastNetwork-node -- visualize --format dot --output topology.dot

# JSON
cargo run -p OpenBroadcastNetwork-node -- visualize --format json
```

## Admin API

When running with `--admin-token <TOKEN>`, the following endpoints require
`Authorization: Bearer <TOKEN>`:

- `POST /api/stream/start` — Start the streaming session
- `POST /api/stream/stop` — Stop the streaming session
- `GET /api/moderation/state` — Get moderation state
- `POST /api/moderation/block` — Block a peer
- `POST /api/moderation/unblock` — Unblock a peer
- `POST /api/moderation/flag` — Flag a stream
- `GET /api/stats` — Relay and signaling stats

## Build Commands

```bash
cargo build                                    # Build workspace
cargo build -p OpenBroadcastNetwork-core       # Build core library
cargo build -p OpenBroadcastNetwork-node       # Build CLI node
cargo test                                     # Run all tests
cargo clippy                                   # Run lints
cargo fmt                                      # Format code
```
