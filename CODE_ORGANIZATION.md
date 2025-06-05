# OpenBroadcastNetwork Code Organization Rules
# Version: 1.0.0
# Date: 2025-06-04

# MODULAR STRUCTURE
modular_structure:
  - overlay:
      - libp2p:
          - types.rs: Type aliases, conversions between LocalPeerId and LibP2P PeerId
          - peer_manager.rs: Peer connection/disconnection and info management
          - swarm.rs: Swarm initialization and event loop
          - relay_manager.rs: Stream relay functionality
          - mesh_manager.rs: Mesh network topology management
          - event_handler.rs: Overlay event channel logic
          - topics.rs: PubSub topic definitions and utilities
          - mod.rs: Module exports and integration

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
  - modularity_benefits:
      - isolated_components: Each module can be tested independently
      - mock_dependencies: Easier to mock smaller modules
      - improved_test_coverage: Better targeting of specific functionality
