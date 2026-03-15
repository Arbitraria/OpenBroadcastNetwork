//! Tests for overlay implementations
//!
//! This module contains tests for the tree, mesh, and hybrid overlay implementations.

#[cfg(test)]
mod tests {
    use crate::overlay::interface::StreamId;

    // Helper to create a StreamId
    fn create_stream_id(id: &str) -> StreamId {
        StreamId::from_string(id)
    }

    #[test]
    fn test_stream_id_display() {
        let stream_id = create_stream_id("test_stream");
        let display = format!("{}", stream_id);
        assert!(!display.is_empty());
    }

    #[test]
    fn test_stream_id_creation() {
        let stream_id = StreamId::new_random();
        assert!(!stream_id.as_bytes().is_empty());

        let stream_id2 = StreamId::from_string("test");
        assert_eq!(stream_id2.as_bytes(), b"test");

        let stream_id3 = StreamId::from_bytes(vec![1, 2, 3, 4]);
        assert_eq!(stream_id3.as_bytes(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_basic_functionality() {
        // This is a basic test to ensure the overlay module compiles and works
        let stream_id = create_stream_id("basic_test");
        assert_eq!(stream_id.as_bytes(), b"basic_test");
    }
}
