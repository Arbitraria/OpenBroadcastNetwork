OpenBroadcastNetwork

A decentralized peer-to-peer live streaming prototype exploring resilient infrastructure design, distributed peer discovery, and hybrid overlay topologies.

This project investigates how live media distribution can function without centralized CDN infrastructure by leveraging modern peer-to-peer networking primitives.

🚀 Overview

OpenBroadcastNetwork (OBN) is a Rust-based distributed streaming system built using libp2p. It supports peer discovery, pub/sub messaging, and real-time transport options suitable for browser and CLI-based clients.

The project is structured to simulate publisher, relay, and consumer roles in a decentralized network, allowing experimentation with scalable overlay topologies and fault tolerance strategies.

🏗 Architecture

Core technologies:

Rust

libp2p

Kademlia DHT (peer discovery)

GossipSub (pub/sub messaging)

WebRTC transport

QUIC transport

Network Model:

Hybrid tree-mesh overlay topology

DHT-based peer bootstrapping

Pub/sub content propagation

CLI-based relay and node roles

The hybrid overlay design explores tradeoffs between:

Bandwidth efficiency

Latency

Redundancy

Resilience under peer churn

🎯 Goals

Investigate decentralized CDN-like media routing

Explore fault-tolerant peer discovery mechanisms

Evaluate WebRTC and QUIC transport suitability

Prototype scalable relay hierarchies

Develop reproducible build and test workflows

🛠 Engineering Highlights

Modular repository structure separating protocol, core networking, and node logic

CLI tooling for different network roles (publisher / relay / consumer)

Automated test scripts validating peer discovery and messaging flow

Structured version control workflow

Documentation of architecture tradeoffs

📦 Build & Test
cargo build
cargo test

Additional integration scripts available in /scripts.

🔍 Why This Project Matters

OpenBroadcastNetwork serves as a research and engineering exploration of:

Distributed systems design

Infrastructure resilience

Peer-to-peer architecture

Real-time media transport

Deployment-ready networking components

While currently a prototype, the architecture is structured to support incremental evolution toward a production-ready decentralized streaming layer.

📌 Author

Ian Glenn
Infrastructure & Systems Deployment Engineer
github.com/Arbitraria
