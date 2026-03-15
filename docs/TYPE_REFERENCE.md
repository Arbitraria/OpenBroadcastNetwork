# OpenBroadcastNetwork - Type Reference Guide

This document serves as a quick reference for important types, structures, and patterns used throughout the codebase. Use this as a guide during refactoring to maintain consistency.

**Status**: Updated for Phase 1 completion with libp2p 0.53.0 integration

## Core Types and Locations

### Peer Types
- `LocalPeerId` - `/core/src/overlay/peer.rs` - Wrapper around libp2p::PeerId with conversion methods
- `PeerId` (libp2p) - External dependency - Used as parameter in many libp2p functions
- `PeerRole` - `/core/src/overlay/peer.rs` - Enum with variants: Publisher, Relay, Consumer, HybridRelay, Gateway, Unknown
- `Peer` - `/core/src/overlay/peer.rs` - Contains peer metadata and connection status
- `PeerInfo` - `/core/src/overlay/peer.rs` - Public-facing peer information structure

### Network Structures
- `Libp2pOverlay` - `/core/src/overlay/libp2p_impl.rs` - Main implementation of the Overlay trait
- `OverlayBehavior` - `/core/src/overlay/libp2p/behavior.rs` - Combined NetworkBehaviour implementation
- `Swarm<OverlayBehavior>` - libp2p type - Central networking component

### Manager Types
- `TopologyManager` - `/core/src/overlay/topology/manager.rs` - Manages peer connections in topology
- `RelayManager` - `/core/src/overlay/relay/manager.rs` - Handles stream relaying
- `MeshNetwork` - `/core/src/overlay/mesh/mod.rs` - Manages mesh connections

### Configuration Types
- `OverlayConfig` - `/core/src/overlay/interface.rs` - Main overlay configuration
- `TopologyConfig` - `/core/src/overlay/topology/config.rs` - Topology-specific configuration
- `RelayConfig` - `/core/src/overlay/relay/config.rs` - Relay-specific configuration
- `MeshConfig` - `/core/src/overlay/mesh/config.rs` - Mesh network configuration

### Stream Types
- `StreamId` - `/core/src/overlay/interface.rs` - Identifier for data streams
- `StreamChunk` - `/core/src/overlay/relay/mod.rs` - Individual piece of stream data

## Concurrency Patterns

### Standard Patterns

The codebase uses these concurrency primitives with the following conventions:

1. **Arc<T>** - Used for shared ownership of a value across threads.
   - Common for manager types: `Arc<TopologyManager>`, `Arc<RelayManager>`, etc.
   - Used when multiple components need access to the same instance.

2. **Mutex<T>** - Used for exclusive access to a value.
   - Typically wrapped in Arc when shared: `Arc<Mutex<T>>`
   - Example: `swarm: Arc<Mutex<Option<Swarm<OverlayBehavior>>>>`

3. **RwLock<T>** - Used for shared read access with exclusive write access.
   - Used for data structures that are read frequently but written to occasionally
   - Example: `peers: RwLock<HashMap<LocalPeerId, Peer>>`

### Implementation Note

When refactoring, follow this guideline:
- If a component needs to be shared across threads, use `Arc<T>`
- For mutable shared state, use either:
  - `Arc<Mutex<T>>` for state that needs exclusive access
  - `Arc<RwLock<T>>` for state that can benefit from concurrent reads

## Important Method Signatures

### TopologyManager
```rust
pub fn new(
    local_peer_id: PeerId,
    config: TopologyConfig,
    metrics: Option<Arc<MetricsRegistry>>,
) -> Self

pub async fn add_peer_to_stream(
    &self,
    stream_id: &StreamId,
    peer_id: LocalPeerId,
    role: PeerRole,
) -> Result<(), OverlayError>

pub async fn remove_peer_from_stream(
    &self,
    stream_id: &StreamId,
    peer_id: &LocalPeerId,
) -> Result<(), OverlayError>
```

### RelayManager
```rust
pub fn new(
    local_peer_id: PeerId,
    config: RelayConfig,
    topology: Arc<TopologyManager>,
) -> Self

pub async fn create_stream(
    &self,
    stream_id: StreamId,
    publisher: LocalPeerId,
) -> Result<(), OverlayError>

pub async fn remove_stream(
    &self,
    stream_id: &StreamId,
) -> Result<(), OverlayError>
```

### Libp2pOverlay
```rust
pub fn new(config: OverlayConfig) -> Self

pub async fn start(&self) -> Result<(), OverlayError>

pub async fn stop(&self) -> Result<(), OverlayError>

pub async fn connect_peer(
    &self,
    peer_id: LocalPeerId,
    addr: &str,
) -> Result<(), OverlayError>
```

## Module Dependencies

The dependency flow in the overlay network is as follows:

```
libp2p_impl.rs
├── Uses: libp2p::behavior
├── Uses: overlay::topology
├── Uses: overlay::relay
└── Uses: overlay::mesh

libp2p::behavior.rs
├── Uses: libp2p Gossipsub
├── Uses: libp2p Kademlia
├── Uses: libp2p MDNS
└── Uses: libp2p Identify

topology::manager.rs
└── Uses: peer.rs

relay::manager.rs
└── Uses: topology::manager.rs
```

## Common Refactoring Patterns

When refactoring, look for these common patterns that need fixing:

1. **Type conversion inconsistencies**:
   - `LocalPeerId` ↔ `PeerId` - Use `.into()` or explicit conversion methods

2. **Method signature mismatches**:
   - Check parameter types (reference vs. value)
   - Verify correct enum variants (e.g., `PeerRole::Consumer` not `PeerRole::Subscriber`)

3. **Concurrency wrapper inconsistencies**:
   - Missing `Arc<>` around manager types
   - Improper nesting of `Arc<Mutex<>>` vs just `Mutex<>`

4. **Config field mismatches**:
   - Ensure config struct field names match actual definition
   - Check for renamed or removed fields

5. **Error handling**:
   - Use appropriate `OverlayError` variants
   - Convert third-party errors to `OverlayError` variants
