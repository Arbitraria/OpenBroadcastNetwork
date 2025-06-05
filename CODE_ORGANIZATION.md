# OpenBroadcastNetwork Code Organization Rules
# Version: 2.0.0
# Date: 2025-06-05
# Status: Phase 1 Complete - libp2p 0.53.0 Integration

# CURRENT ARCHITECTURE OVERVIEW
architecture:
  core_crates:
    - core: Core networking, discovery, overlay, media, pubsub
    - node: CLI relay node implementation with visualization
    - ui: Web-based viewer interface (planned)
    - proto: Protocol definitions and shared types

# MODULAR STRUCTURE (Updated)
modular_structure:
  discovery:
    - mod.rs: Discovery manager and interfaces
    - bootstrap.rs: Bootstrap peer discovery implementation
    - dht.rs: Kademlia DHT-based discovery
    - interface.rs: Discovery trait definitions
    - NOTE: mDNS support removed for scalability

  overlay:
    - interface.rs: Core overlay traits and types
    - peer.rs: Peer management and connection status
    - network.rs: Low-level network abstractions
    - libp2p/: libp2p-specific implementations
        - impl_core.rs: Main libp2p overlay implementation
        - types.rs: Type conversions (LocalPeerId ↔ libp2p::PeerId)
        - behavior.rs: Composed libp2p behaviors
        - swarm.rs: Swarm management and event processing
        - event_handlers.rs: Event processing logic
        - overlay_trait.rs: Overlay trait implementation
        - peer_manager.rs: Peer lifecycle management
        - mesh_manager.rs: Mesh topology management
        - relay_manager.rs: Stream relay functionality
        - overlay_utils.rs: Utility functions
        - utils.rs: Helper functions and conversions
    - hybrid/: Tree-mesh hybrid topology
    - topology/: Geographic and topology management
    - mesh/: Mesh network implementations
    - relay/: Stream relay functionality

  pubsub:
    - interface.rs: PubSub trait definitions
    - gossipsub.rs: GossipSub implementation
    - topic.rs: Topic management
    - message.rs: Message types and serialization
    - validation.rs: Message validation
    - metrics.rs: PubSub metrics

  media:
    - interface.rs: Media streaming interfaces
    - pipeline.rs: Media processing pipeline
    - codec.rs: Codec management
    - quality.rs: Quality adaptation
    - stream.rs: Stream abstractions

  visualization:
    - node/src/visualization.rs: CLI visualization utilities
    - Beautiful Unicode table formatting
    - DOT graph generation for Graphviz
    - JSON export for APIs

# REFACTORING PATTERNS
refactoring:
  - monolith_to_modules:
      - extract_by_concern: Separate code by functional area
      - maintain_interfaces: Keep public APIs consistent during refactoring
      - incremental_migration: Move one component at a time, ensuring compilation
  - dependency_management:
      - minimize_cross_module_dependencies: Each module should have clear responsibilities
      - use_type_aliases: Define common types in types.rs
      - prefer_references: Pass references to shared state rather than duplicating

# CONCURRENCY PATTERNS
concurrency:
  - state_management:
      - arc_mutex: Use Arc<Mutex<T>> for exclusive access to shared state
      - rwlock: Use RwLock<T> for read-heavy data structures
      - prefer_tokio_sync: Use tokio::sync primitives for async context

# ASYNC PATTERNS
async:
  - trait_implementation:
      - use_async_trait: Use #[async_trait::async_trait] for async trait methods
      - avoid_explicit_lifetimes: Let async_trait handle lifetime management
      - avoid_boxed_futures: Use direct async fn returns instead of Pin<Box<dyn Future>>

# TESTING
testing:
  integration_tests:
    - core/tests/phase1_integration.rs: Comprehensive Phase 1 tests
    - Discovery mechanism testing (bootstrap, DHT)
    - Overlay network basic functionality
    - Stream management and relay testing
    - Two-node communication tests
    - Topology rebalancing verification
    - Error handling and edge cases

  unit_tests:
    - Per-module test coverage
    - Mock dependencies for isolated testing
    - Property-based testing where applicable

  test_commands:
    - cargo test: Run all tests
    - cargo test discovery: Test discovery mechanisms
    - cargo test overlay: Test overlay functionality
    - cargo test --test phase1_integration: Run integration tests

# PHASE 1 COMPLETION STATUS
phase1_status:
  completed_features:
    - ✅ libp2p 0.53.0 integration
    - ✅ DHT-based peer discovery (Kademlia)
    - ✅ Bootstrap peer discovery
    - ✅ GossipSub pub/sub messaging
    - ✅ Hybrid tree-mesh overlay topology
    - ✅ Geographic-aware rebalancing
    - ✅ CLI with rich visualization features
    - ✅ Comprehensive test coverage
    - ✅ Professional Unicode formatting
    - ✅ Multiple output formats (text, DOT, JSON)

  removed_features:
    - ❌ mDNS local discovery (removed for scalability)
    - Reason: DHT-based discovery provides better scalability
    - Impact: No breaking changes to public APIs

# CLI VISUALIZATION FEATURES
cli_features:
  commands:
    - run: Start relay nodes with various roles
    - status: Network status with Unicode formatting
    - list-streams: Active stream monitoring
    - visualize: Multi-format topology visualization

  output_formats:
    - text: Human-readable tables with Unicode box drawing
    - dot: Graphviz-compatible network graphs
    - json: Machine-readable data for APIs

  visualization_components:
    - Network status dashboard with metrics
    - Peer tables with roles and connection status
    - Stream tables with bandwidth monitoring
    - Geographic location display (when available)
    - Real-time latency and bandwidth metrics
