# Dependency Management

This document outlines the dependencies used in the OpenBroadcastNetwork project, along with rationales for their selection and any specific considerations.

## Core Dependencies

### libp2p (0.53.0)
- **Purpose**: Provides the peer-to-peer networking foundation
- **Features Used**:
  - `tcp`: For TCP transport
  - `dns`: For DNS resolution
  - `gossipsub`: For efficient pub/sub messaging
  - `identify`: For peer identification protocol
  - `kad`: For Kademlia DHT-based peer discovery
  - `noise`: For secure encrypted communications
  - `tokio`: For tokio runtime compatibility
- **Removed Features**:
  - ❌ `mdns`: Removed local network peer discovery for better scalability
  - ❌ `async-std`: Migrated to tokio for consistency
- **Considerations**: 
  - We avoid the `quic` feature due to dependency conflicts
  - DHT-based discovery replaces mDNS for better P2P scalability
  - All networking now uses tokio runtime for consistency

### tokio (1.28)
- **Purpose**: Async runtime for the application
- **Features**: "full" to enable all async functionality

### serde (1.0)
- **Purpose**: Serialization and deserialization for data structures
- **Features**: "derive" to enable the derive macros

### tracing (0.1) and tracing-subscriber (0.3)
- **Purpose**: Structured logging and telemetry
- **Rationale**: Provides more context and better filtering than simple logging

### thiserror (1.0) and anyhow (1.0)
- **Purpose**: Error handling
- **Rationale**: thiserror for defining error types, anyhow for propagation

### webrtc (0.7)
- **Purpose**: WebRTC support for browser compatibility
- **Rationale**: Required for browser-to-peer communication

## Additional Dependencies

### geo-ip (0.1.0)
- **Purpose**: Geo-location functionality for topology optimization
- **Rationale**: Enables location-aware peer connections to minimize latency

### rand (0.8)
- **Purpose**: Random number generation
- **Rationale**: Used for various randomization needs throughout the codebase

### bs58 (0.4)
- **Purpose**: Base58 encoding/decoding
- **Rationale**: Used for human-readable representation of binary IDs

### serde_json (1.0)
- **Purpose**: JSON serialization
- **Rationale**: Human-readable data exchange format

### bytes (1.4)
- **Purpose**: Efficient byte buffer manipulation
- **Rationale**: Used for handling binary data streams

### sha2 (0.10) and hex (0.4.3)
- **Purpose**: Cryptographic hashing and hex encoding
- **Rationale**: Used for content addressing and ID generation
- **Note**: sha2 is pinned to compatible versions to avoid conflicts with other crates

### lru (0.10)
- **Purpose**: LRU cache implementation
- **Rationale**: Used for caching frequently accessed data

## CLI-Specific Dependencies

### clap (4.3)
- **Purpose**: Command-line argument parsing
- **Features**: "derive" for declarative CLI definition

### serde_json (1.0)
- **Purpose**: JSON output formatting for CLI visualization
- **Rationale**: Enables machine-readable network topology export

## Visualization Dependencies

- **Unicode Box Drawing**: Built-in Rust Unicode support for beautiful CLI tables
- **DOT Graph Generation**: Custom implementation for Graphviz compatibility
- **No external CLI formatting dependencies**: Reduced dependency footprint

## Recent Changes (Phase 1 Completion)

### Removed Dependencies
1. **mDNS support**: Removed mdns feature from libp2p to reduce complexity
2. **async-std**: Migrated to tokio for runtime consistency  
3. **External CLI formatting**: Removed term-table and colored for reduced footprint

### Updated Dependencies
1. **libp2p**: Updated to 0.53.0 with streamlined feature set
2. **tokio**: Standardized on tokio runtime throughout the project
3. **Custom visualization**: Built beautiful Unicode tables without external dependencies

## Known Dependency Issues

1. **libp2p quic feature**: Avoided due to rustls/subtle version conflicts
2. **geo-ip versioning**: Using compatible version 0.1.0

## Licensing

All dependencies have been reviewed for license compatibility. The project remains under the MIT license, which is compatible with all dependencies used.

## Dependency Updates

When updating dependencies:

1. Pin specific versions in Cargo.toml
2. Document changes in this file
3. Check for feature compatibility, especially with libp2p
4. Run all tests to ensure compatibility 