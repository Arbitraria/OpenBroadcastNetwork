//! CLI relay node library for OpenBroadcastNetwork.
#![allow(non_snake_case)]

pub mod streaming_demo;
pub mod visualization;

// Re-export key types
pub use streaming_demo::{StreamingDemo, StreamingDemoConfig};
pub use visualization::{
    create_peer_table, create_stream_table, format_bytes, format_duration, visualize_tree,
};

/// Get information about the node
pub fn node_info() -> &'static str {
    "Decentralized Streaming Relay Node"
}

/// Check if node has a feature
pub fn has_feature(feature: &str) -> bool {
    match feature {
        "visualization" => cfg!(feature = "visualization"),
        "metrics" => cfg!(feature = "metrics"),
        _ => false,
    }
}

/// Add two numbers (placeholder utility).
pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
