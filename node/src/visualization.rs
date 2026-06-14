//! Network visualization utilities for the CLI
//!
//! This module provides functions to visualize the network topology,
//! peer connections, and stream status in various formats.
//!
//! The `writeln!`/`write!` calls below target an in-memory `String`, which can
//! never fail, so their `Result` is discarded with `.ok()` rather than unwrapped.

use libp2p::PeerId;
use std::collections::HashMap;
use std::fmt::Write;
use OpenBroadcastNetwork_core::overlay::interface::{OverlayStats, StreamId};
use OpenBroadcastNetwork_core::overlay::peer::{PeerInfo, PeerRole};

/// Create a formatted table showing active streams
pub fn create_stream_table(streams: &[StreamId]) -> String {
    let mut output = String::new();

    if streams.is_empty() {
        output.push_str("No active streams\n");
        return output;
    }

    writeln!(
        output,
        "┌─────────────────────────────────────┬─────────────┬─────────────┐"
    )
    .ok();
    writeln!(
        output,
        "│ Stream ID                           │ Subscribers │ Bandwidth   │"
    )
    .ok();
    writeln!(
        output,
        "├─────────────────────────────────────┼─────────────┼─────────────┤"
    )
    .ok();

    for (i, stream) in streams.iter().enumerate() {
        let stream_id = format!("{}", stream);
        let short_id = if stream_id.len() > 35 {
            format!("{}...", &stream_id[0..32])
        } else {
            stream_id
        };

        // Mock data for demonstration
        let subscribers = (i + 1) * 3;
        let bandwidth = format_bytes(((i + 1) * 150_000) as u64);

        writeln!(
            output,
            "│ {:<35} │ {:<11} │ {:<11} │",
            short_id, subscribers, bandwidth
        )
        .ok();
    }

    writeln!(
        output,
        "└─────────────────────────────────────┴─────────────┴─────────────┘"
    )
    .ok();
    output
}

/// Create a formatted table showing connected peers
pub fn create_peer_table(peers: &HashMap<PeerId, PeerInfo>, show_location: bool) -> String {
    let mut output = String::new();

    if peers.is_empty() {
        output.push_str("No connected peers\n");
        return output;
    }

    if show_location {
        writeln!(
            output,
            "┌─────────────────┬─────────────┬─────────────┬─────────────┬─────────────┐"
        )
        .ok();
        writeln!(
            output,
            "│ Peer ID         │ Role        │ Connection  │ Location    │ Latency     │"
        )
        .ok();
        writeln!(
            output,
            "├─────────────────┼─────────────┼─────────────┼─────────────┼─────────────┤"
        )
        .ok();
    } else {
        writeln!(
            output,
            "┌─────────────────┬─────────────┬─────────────┬─────────────┐"
        )
        .ok();
        writeln!(
            output,
            "│ Peer ID         │ Role        │ Connection  │ Latency     │"
        )
        .ok();
        writeln!(
            output,
            "├─────────────────┼─────────────┼─────────────┼─────────────┤"
        )
        .ok();
    }

    for (peer_id, peer_info) in peers {
        let short_id = format!("{}", peer_id).chars().take(15).collect::<String>();
        let role = match peer_info.role {
            PeerRole::Publisher => "Publisher",
            PeerRole::Consumer => "Consumer",
            PeerRole::Relay => "Relay",
            PeerRole::HybridRelay => "Hybrid",
            PeerRole::Gateway => "Gateway",
            PeerRole::Unknown => "Unknown",
        };
        let connection = match peer_info.status {
            OpenBroadcastNetwork_core::overlay::peer::ConnectionStatus::Connected => "✓ Connected",
            OpenBroadcastNetwork_core::overlay::peer::ConnectionStatus::Connecting => {
                "⏳ Connecting"
            }
            OpenBroadcastNetwork_core::overlay::peer::ConnectionStatus::Disconnected => {
                "✗ Disconnected"
            }
            OpenBroadcastNetwork_core::overlay::peer::ConnectionStatus::Error => "❌ Error",
        };

        // Mock latency data
        let latency = format!("{}ms", (peer_id.to_bytes().len() % 100) + 10);

        if show_location {
            let location = "Unknown"; // Geo lookup would go here
            writeln!(
                output,
                "│ {:<15} │ {:<11} │ {:<11} │ {:<11} │ {:<11} │",
                short_id, role, connection, location, latency
            )
            .ok();
        } else {
            writeln!(
                output,
                "│ {:<15} │ {:<11} │ {:<11} │ {:<11} │",
                short_id, role, connection, latency
            )
            .ok();
        }
    }

    if show_location {
        writeln!(
            output,
            "└─────────────────┴─────────────┴─────────────┴─────────────┴─────────────┘"
        )
        .ok();
    } else {
        writeln!(
            output,
            "└─────────────────┴─────────────┴─────────────┴─────────────┘"
        )
        .ok();
    }

    output
}

/// Generate a DOT graph for network topology visualization
pub fn generate_dot_graph(
    peers: &HashMap<PeerId, PeerInfo>,
    stream_id: Option<&StreamId>,
) -> String {
    let mut output = String::new();

    writeln!(output, "digraph NetworkTopology {{").ok();
    writeln!(output, "  rankdir=LR;").ok();
    writeln!(
        output,
        "  node [shape=box, style=filled, fontname=\"Arial\"];"
    )
    .ok();
    writeln!(output, "  edge [fontname=\"Arial\"];").ok();
    writeln!(output).ok();

    if let Some(stream) = stream_id {
        writeln!(
            output,
            "  label=\"Network Topology for Stream: {}\";",
            stream
        )
        .ok();
    } else {
        writeln!(output, "  label=\"Global Network Topology\";").ok();
    }
    writeln!(output, "  labelloc=\"t\";").ok();
    writeln!(output).ok();

    // Add nodes
    for (peer_id, peer_info) in peers {
        let short_id = format!("{}", peer_id).chars().take(12).collect::<String>();
        let color = match peer_info.role {
            PeerRole::Publisher => "lightblue",
            PeerRole::Consumer => "lightgreen",
            PeerRole::Relay => "lightyellow",
            PeerRole::HybridRelay => "lightcoral",
            PeerRole::Gateway => "lightpink",
            PeerRole::Unknown => "lightgray",
        };

        writeln!(
            output,
            "  \"{}\" [label=\"{}\\n{:?}\", fillcolor=\"{}\"];",
            short_id, short_id, peer_info.role, color
        )
        .ok();
    }

    writeln!(output).ok();

    // Add edges (simplified - connect all peers for now)
    let peer_ids: Vec<_> = peers.keys().collect();
    for (i, peer1) in peer_ids.iter().enumerate() {
        for peer2 in peer_ids.iter().skip(i + 1) {
            let short_id1 = format!("{}", peer1).chars().take(12).collect::<String>();
            let short_id2 = format!("{}", peer2).chars().take(12).collect::<String>();
            writeln!(output, "  \"{}\" -- \"{}\";", short_id1, short_id2).ok();
        }
    }

    writeln!(output, "}}").ok();
    output
}

/// Visualize a tree structure for streaming topology
pub fn visualize_tree(root: &PeerInfo, writer: &mut dyn std::io::Write) -> std::io::Result<()> {
    writeln!(writer, "Tree Topology:")?;
    writeln!(writer)?;
    writeln!(writer, "Root: {:?}", root.role)?;
    writeln!(writer, "├── Child nodes would be displayed here")?;
    writeln!(
        writer,
        "└── Tree visualization available when topology data is accessible"
    )?;
    Ok(())
}

/// Create a detailed network status display
pub fn create_network_status(stats: &OverlayStats, uptime: std::time::Duration) -> String {
    let mut output = String::new();

    writeln!(
        output,
        "╔══════════════════════════════════════════════════════════════╗"
    )
    .ok();
    writeln!(
        output,
        "║                    NETWORK STATUS                           ║"
    )
    .ok();
    writeln!(
        output,
        "╠══════════════════════════════════════════════════════════════╣"
    )
    .ok();
    writeln!(
        output,
        "║ Uptime:              {:<35} ║",
        format_duration(uptime)
    )
    .ok();
    writeln!(
        output,
        "║ Connected Peers:     {:<35} ║",
        stats.connected_peers
    )
    .ok();
    writeln!(
        output,
        "║ Discovered Peers:    {:<35} ║",
        stats.discovered_peers
    )
    .ok();
    writeln!(
        output,
        "║ Active Streams:      {:<35} ║",
        stats.active_streams
    )
    .ok();
    writeln!(output, "║ Relay Nodes:         {:<35} ║", stats.relay_nodes).ok();
    writeln!(
        output,
        "║ Incoming Bandwidth:  {:<35} ║",
        format_bytes(stats.incoming_bandwidth)
    )
    .ok();
    writeln!(
        output,
        "║ Outgoing Bandwidth:  {:<35} ║",
        format_bytes(stats.outgoing_bandwidth)
    )
    .ok();
    writeln!(
        output,
        "║ Average Latency:     {:<35} ║",
        format!("{}ms", stats.average_latency_ms)
    )
    .ok();
    writeln!(
        output,
        "╚══════════════════════════════════════════════════════════════╝"
    )
    .ok();

    output
}

/// Placeholder for visualization functionality
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes < KB {
        format!("{} B", bytes)
    } else if bytes < MB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else if bytes < GB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes < TB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    }
}

/// Placeholder for visualization functionality
pub fn format_duration(duration: std::time::Duration) -> String {
    let seconds = duration.as_secs();

    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        let minutes = seconds / 60;
        let secs = seconds % 60;
        format!("{}m {}s", minutes, secs)
    } else {
        let hours = seconds / 3600;
        let minutes = (seconds % 3600) / 60;
        format!("{}h {}m", hours, minutes)
    }
}
