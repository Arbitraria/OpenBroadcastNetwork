# Dependency Management

This document outlines the dependencies used in the Decentralized Streaming CDN project, along with rationales for their selection and any specific considerations.

## Core Dependencies

### libp2p (0.55.0)
- **Purpose**: Provides the peer-to-peer networking foundation
- **Features Used**:
  - `async-std`: For async runtime compatibility
  - `tcp`: For TCP transport
  - `dns`: For DNS resolution
  - `mdns`: For local network peer discovery
  - `gossipsub`: For efficient pub/sub messaging
  - `identify`: For peer identification protocol
  - `kad`: For DHT-based peer discovery
  - `noise`: For secure encrypted communications
- **Considerations**: 
  - We explicitly avoid the `async-io` feature as it's not compatible with version 0.55.0
  - We also avoid the `quic` feature due to dependency conflicts with the `subtle` crate
  - The `webrtc` feature is replaced by our separate WebRTC dependency for browser compatibility

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

### term-table (1.3) and colored (2.0)
- **Purpose**: Terminal output formatting
- **Rationale**: Provides readable output for CLI users

## Known Dependency Issues

1. **libp2p quic feature**: The quic feature has a dependency on rustls which requires subtle ≥2.5.0, but sha2 requires subtle =2.4.0. This creates an irresolvable dependency conflict.

2. **geo-ip versioning**: We use geo-ip 0.1.0 as the newer versions have incompatible APIs.

## Licensing

All dependencies have been reviewed for license compatibility. The project remains under the MIT license, which is compatible with all dependencies used.

## Dependency Updates

When updating dependencies:

1. Pin specific versions in Cargo.toml
2. Document changes in this file
3. Check for feature compatibility, especially with libp2p
4. Run all tests to ensure compatibility 