pub mod visualization;

// Re-export key types
pub use visualization::{
    StreamInfo, PeerDisplayInfo, format_bytes, format_duration,
    create_stream_table, create_peer_table, visualize_tree, dot_to_ascii
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
