//! MP4 fragment writing and generation functionality
//!
//! This module handles creating and writing MP4 fragments for streaming.
//! It provides functionality to generate MSE-compatible segments.

use std::io;
use tracing::{debug, error, info, warn};

pub use super::fragment_parser::{BoxInfo, Mp4ParseError};
/// Re-export core types from mp4_parser
pub use super::mp4_parser::{BoxContent, Mp4Track, MseSegment};

/// Fragment writer for generating MP4 segments
pub struct FragmentWriter {
    /// Track information for writing
    tracks: Vec<Mp4Track>,
    /// Sequence number for fragments
    sequence_number: u32,
}

impl FragmentWriter {
    /// Create a new fragment writer
    pub fn new() -> Self {
        Self {
            tracks: Vec::new(),
            sequence_number: 1,
        }
    }

    /// Add track information
    pub fn add_track(&mut self, track: Mp4Track) {
        self.tracks.push(track);
    }

    /// Generate MSE segments from parsed boxes
    pub fn generate_mse_segments(
        &mut self,
        boxes: &[BoxInfo],
        is_fragmented: bool,
    ) -> Result<Vec<MseSegment>, io::Error> {
        if is_fragmented {
            self.extract_fragmented_segments(boxes)
        } else {
            self.create_fragments_from_regular_mp4(boxes)
        }
    }

    /// Extract segments from an already fragmented MP4 file
    fn extract_fragmented_segments(&self, boxes: &[BoxInfo]) -> Result<Vec<MseSegment>, io::Error> {
        let mut segments = Vec::new();

        // Create initialization segment from ftyp + moov boxes
        let mut init_data = Vec::new();

        for box_info in boxes {
            match box_info.header.box_type.as_str() {
                "ftyp" | "moov" => {
                    let box_data = self.serialize_box(box_info);

                    // Validate the serialized box
                    if box_data.len() >= 8 {
                        let serialized_size = u32::from_be_bytes([
                            box_data[0],
                            box_data[1],
                            box_data[2],
                            box_data[3],
                        ]);

                        if serialized_size as usize != box_data.len() {
                            error!(
                                "Serialized box size mismatch for {}: header says {}, actual {}",
                                box_info.header.box_type,
                                serialized_size,
                                box_data.len()
                            );
                            continue;
                        }

                        init_data.extend_from_slice(&box_data);
                        debug!(
                            "Added {} box to initialization segment ({} bytes)",
                            box_info.header.box_type,
                            box_data.len()
                        );
                    }
                }
                _ => {}
            }
        }

        // Create initialization segment
        if !init_data.is_empty() {
            segments.push(MseSegment {
                segment_type: "initialization".to_string(),
                data: init_data,
                timestamp: None,
                duration: None,
                is_keyframe: true,
                track_id: 0,
            });
            info!(
                "Created initialization segment ({} bytes)",
                segments[0].data.len()
            );
        }

        // Process moof+mdat pairs for media segments
        let mut i = 0;
        while i < boxes.len() {
            if boxes[i].header.box_type == "moof"
                && i + 1 < boxes.len()
                && boxes[i + 1].header.box_type == "mdat"
            {
                let moof_data = self.serialize_box(&boxes[i]);
                let mdat_data = self.serialize_box(&boxes[i + 1]);

                let mut media_data = Vec::new();
                media_data.extend_from_slice(&moof_data);
                media_data.extend_from_slice(&mdat_data);

                // Calculate timestamp and duration based on segment index
                let segment_index = segments.len() - 1; // Subtract 1 for init segment
                let timestamp = if segment_index > 0 {
                    Some((segment_index as u64) * 1000)
                } else {
                    Some(0)
                };
                let duration = Some(1000); // Default 1 second duration

                // Determine if this is a keyframe by checking for SPS/PPS or using segment index
                let is_keyframe = self.is_likely_keyframe(&media_data, segment_index);

                segments.push(MseSegment {
                    segment_type: "media".to_string(),
                    data: media_data,
                    timestamp,
                    duration,
                    is_keyframe,
                    track_id: 1,
                });

                let media_data_len = segments.last().unwrap().data.len();
                debug!(
                    "Created media segment {} ({} bytes)",
                    segments.len() - 1,
                    media_data_len
                );
                i += 2; // Skip both moof and mdat
            } else {
                i += 1;
            }
        }

        info!("Generated {} segments from fragmented MP4", segments.len());
        Ok(segments)
    }

    /// Create fragments from a regular (non-fragmented) MP4 file
    fn create_fragments_from_regular_mp4(
        &self,
        boxes: &[BoxInfo],
    ) -> Result<Vec<MseSegment>, io::Error> {
        warn!("Converting regular MP4 to fragmented format - this is a simplified implementation");

        let mut segments = Vec::new();

        // For now, create a single initialization segment with ftyp + moov
        let mut init_data = Vec::new();
        let mut mdat_data = Vec::new();

        for box_info in boxes {
            match box_info.header.box_type.as_str() {
                "ftyp" | "moov" => {
                    init_data.extend_from_slice(&self.serialize_box(box_info));
                }
                "mdat" => {
                    // Store mdat for chunking
                    if let BoxContent::Raw(data) = &box_info.content {
                        mdat_data = data.clone();
                    }
                }
                _ => {}
            }
        }

        // Create initialization segment
        if !init_data.is_empty() {
            segments.push(MseSegment {
                segment_type: "initialization".to_string(),
                data: init_data,
                timestamp: None,
                duration: None,
                is_keyframe: true,
                track_id: 0,
            });
            info!(
                "Created initialization segment ({} bytes)",
                segments[0].data.len()
            );
        }

        // Create proper fragmented segments from mdat content
        if !mdat_data.is_empty() {
            // Split mdat into smaller chunks for streaming
            let chunk_size = 64 * 1024; // 64KB chunks
            let num_chunks = mdat_data.len().div_ceil(chunk_size);

            for (chunk_index, chunk) in mdat_data.chunks(chunk_size).enumerate() {
                let timestamp = Some((chunk_index as u64) * 1000); // 1 second per chunk
                let is_keyframe = chunk_index == 0 || self.is_likely_keyframe(chunk, chunk_index);

                let media_segment =
                    self.create_simple_media_segment_with_timing(chunk, timestamp, is_keyframe)?;
                segments.push(media_segment);
            }

            info!("Created {} media segments from regular MP4", num_chunks);
        }

        info!("Generated {} segments from regular MP4", segments.len());
        Ok(segments)
    }

    /// Check if data likely contains a keyframe based on common patterns
    fn is_likely_keyframe(&self, data: &[u8], segment_index: usize) -> bool {
        // Every 5th segment is a keyframe (rough approximation)
        if segment_index.is_multiple_of(5) {
            return true;
        }

        // Check for H.264 SPS/PPS NAL units (0x67, 0x68)
        if data.len() > 4 {
            for window in data.windows(4) {
                if window[0] == 0x00 && window[1] == 0x00 && window[2] == 0x01 {
                    let nal_type = window[3] & 0x1F;
                    if nal_type == 0x07 || nal_type == 0x08 {
                        // SPS or PPS
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Create a simple media segment with timing information
    fn create_simple_media_segment_with_timing(
        &self,
        mdat_content: &[u8],
        timestamp: Option<u64>,
        is_keyframe: bool,
    ) -> Result<MseSegment, io::Error> {
        let mut segment_data = Vec::new();

        // Create a minimal moof box
        let moof_content = self.create_minimal_moof(mdat_content.len() as u32)?;
        segment_data.extend_from_slice(&moof_content);

        // Add mdat box with content
        let mdat_size = 8 + mdat_content.len() as u32;
        segment_data.extend_from_slice(&mdat_size.to_be_bytes()); // Size
        segment_data.extend_from_slice(b"mdat"); // Type
        segment_data.extend_from_slice(mdat_content); // Content

        Ok(MseSegment {
            segment_type: "media".to_string(),
            data: segment_data,
            timestamp,
            duration: Some(1000),
            is_keyframe,
            track_id: 1,
        })
    }

    /// Create a minimal moof (movie fragment) box
    fn create_minimal_moof(&self, data_size: u32) -> Result<Vec<u8>, io::Error> {
        let mut moof_data = Vec::new();

        // moof box header will be added at the end
        let mut moof_content = Vec::new();

        // Add mfhd (movie fragment header)
        let mfhd_content = vec![
            0, 0, 0, 0, // version + flags
        ];
        let mut mfhd_content = mfhd_content;
        mfhd_content.extend_from_slice(&self.sequence_number.to_be_bytes());

        let mfhd_size = 8 + mfhd_content.len() as u32;
        moof_content.extend_from_slice(&mfhd_size.to_be_bytes());
        moof_content.extend_from_slice(b"mfhd");
        moof_content.extend_from_slice(&mfhd_content);

        // Add traf (track fragment) - simplified
        let traf_content = self.create_minimal_traf(data_size)?;
        moof_content.extend_from_slice(&traf_content);

        // Add moof header
        let moof_size = 8 + moof_content.len() as u32;
        moof_data.extend_from_slice(&moof_size.to_be_bytes());
        moof_data.extend_from_slice(b"moof");
        moof_data.extend_from_slice(&moof_content);

        Ok(moof_data)
    }

    /// Create a minimal traf (track fragment) box
    fn create_minimal_traf(&self, _data_size: u32) -> Result<Vec<u8>, io::Error> {
        let mut traf_content = Vec::new();

        // Add tfhd (track fragment header)
        let tfhd_content = vec![
            0, 0, 0, 0, // version + flags
            0, 0, 0, 1, // track_id = 1
        ];
        let tfhd_size = 8 + tfhd_content.len() as u32;
        traf_content.extend_from_slice(&tfhd_size.to_be_bytes());
        traf_content.extend_from_slice(b"tfhd");
        traf_content.extend_from_slice(&tfhd_content);

        // Add trun (track fragment run)
        let mut trun_content = vec![
            0, 0, 0, 1, // version + flags (data-offset-present)
            0, 0, 0, 1, // sample_count = 1
        ];
        trun_content.extend_from_slice(&(8u32 + traf_content.len() as u32 + 16).to_be_bytes()); // data_offset

        let trun_size = 8 + trun_content.len() as u32;
        traf_content.extend_from_slice(&trun_size.to_be_bytes());
        traf_content.extend_from_slice(b"trun");
        traf_content.extend_from_slice(&trun_content);

        // Add traf header
        let mut traf_data = Vec::new();
        let traf_size = 8 + traf_content.len() as u32;
        traf_data.extend_from_slice(&traf_size.to_be_bytes());
        traf_data.extend_from_slice(b"traf");
        traf_data.extend_from_slice(&traf_content);

        Ok(traf_data)
    }

    /// Serialize a box to bytes
    fn serialize_box(&self, box_info: &BoxInfo) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(box_info.header.size as u32).to_be_bytes());
        data.extend_from_slice(box_info.header.box_type.as_bytes());

        if let BoxContent::Raw(content) = &box_info.content {
            data.extend_from_slice(content);
        }

        data
    }
}

impl Default for FragmentWriter {
    fn default() -> Self {
        Self::new()
    }
}
