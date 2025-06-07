//! MP4 fragment parsing functionality
//!
//! This module handles reading and parsing MP4 boxes and fragmented MP4 structures.
//! It provides read-only operations for understanding MP4 file structure.

use std::io::{self, Read, Seek, SeekFrom};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

/// Re-export core types from mp4_parser
pub use super::mp4_parser::{
    BoxHeader, BoxContent, MseSegment, Mp4Track,
    Mp4Parser
};

/// Alias for backward compatibility
pub type TrackInfo = Mp4Track;

/// Error type for MP4 parsing
#[derive(Debug, thiserror::Error)]
pub enum Mp4ParseError {
    #[error("Invalid data: {0}")]
    InvalidData(String),
    #[error("Invalid box size: {0}")]
    InvalidBoxSize(usize),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Stream type for tracks
#[derive(Debug, Clone, PartialEq)]
pub enum StreamType {
    Video,
    Audio,
    Subtitle,
}

/// Box information structure
#[derive(Debug, Clone)]
pub struct BoxInfo {
    pub header: BoxHeader,
    pub content: BoxContent,
    pub offset: u64,
}

/// Fragment parser for reading MP4 structures
pub struct FragmentParser {
    /// Parsed boxes
    pub boxes: Vec<BoxInfo>,
    /// Track information
    pub tracks: Vec<Mp4Track>,
    /// Whether this is a fragmented MP4
    pub is_fragmented: bool,
    /// Total file size
    pub file_size: u64,
}

impl FragmentParser {
    /// Create a new fragment parser
    pub fn new() -> Self {
        Self {
            boxes: Vec::new(),
            tracks: Vec::new(),
            is_fragmented: false,
            file_size: 0,
        }
    }
    
    /// Parse MP4 data from a byte slice
    pub fn parse(&mut self, data: &[u8]) -> Result<(), Mp4ParseError> {
        self.file_size = data.len() as u64;
        self.parse_boxes(data)?;
        self.extract_track_info()?;
        self.check_fragmentation();
        Ok(())
    }
    
    /// Parse MP4 boxes from data
    fn parse_boxes(&mut self, data: &[u8]) -> Result<(), Mp4ParseError> {
        let mut offset = 0;
        
        while offset < data.len() {
            if offset + 8 > data.len() {
                break; // Not enough data for a box header
            }
            
            let header = self.parse_box_header(&data[offset..])?;
            let box_size = header.size as usize;
            
            if box_size < 8 {
                return Err(Mp4ParseError::InvalidBoxSize(box_size));
            }
            
            if offset + box_size > data.len() {
                warn!("Box extends beyond data: offset={}, size={}, data_len={}", 
                      offset, box_size, data.len());
                break;
            }
            
            let content_start = offset + 8;
            let content_end = offset + box_size;
            let content = if content_end <= data.len() {
                BoxContent::Raw(data[content_start..content_end].to_vec())
            } else {
                BoxContent::Raw(Vec::new())
            };
            
            let box_info = BoxInfo {
                header,
                content,
                offset: offset as u64,
            };
            
            debug!("Parsed box: {} at offset {} with size {}", 
                   box_info.header.box_type, offset, box_size);
            
            self.boxes.push(box_info);
            offset += box_size;
        }
        
        info!("Parsed {} boxes from MP4 data", self.boxes.len());
        Ok(())
    }
    
    /// Parse a box header
    fn parse_box_header(&self, data: &[u8]) -> Result<BoxHeader, Mp4ParseError> {
        if data.len() < 8 {
            return Err(Mp4ParseError::InvalidData("Insufficient data for box header".to_string()));
        }
        
        let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as u64;
        let box_type = String::from_utf8_lossy(&data[4..8]).to_string();
        
        Ok(BoxHeader { 
            box_type, 
            size, 
            content_start: 8 
        })
    }
    
    /// Extract track information from parsed boxes
    fn extract_track_info(&mut self) -> Result<(), Mp4ParseError> {
        // Implementation would extract track info from moov/trak boxes
        // For now, create a placeholder track
        if self.has_box("moov") {
            let track = Mp4Track {
                track_id: 1,
                media_type: "vide".to_string(),
                timescale: 30000,
                duration: 1800000, // 60 seconds at 30000 timescale
                codec: "H.264".to_string(),
                codec_mime_type: "video/mp4; codecs=\"avc1.42E01E\"".to_string(),
                codec_params: Some("avc1.42E01E".to_string()),
            };
            self.tracks.push(track);
        }
        
        Ok(())
    }
    
    /// Check if this is a fragmented MP4
    fn check_fragmentation(&mut self) {
        self.is_fragmented = self.has_box("moof") || self.has_box("mvex");
    }
    
    /// Check if a box type exists
    pub fn has_box(&self, box_type: &str) -> bool {
        self.boxes.iter().any(|b| b.header.box_type == box_type)
    }
    
    /// Get boxes of a specific type
    pub fn get_boxes(&self, box_type: &str) -> Vec<&BoxInfo> {
        self.boxes.iter().filter(|b| b.header.box_type == box_type).collect()
    }
    
    /// Get tracks
    pub fn get_tracks(&self) -> &[Mp4Track] {
        &self.tracks
    }
    
    /// Check if this is a fragmented MP4
    pub fn is_fragmented(&self) -> bool {
        self.is_fragmented
    }
    
    /// Get summary of parsed data
    pub fn get_summary(&self) -> String {
        format!("FragmentParser: {} boxes, {} tracks, fragmented: {}", 
                self.boxes.len(), self.tracks.len(), self.is_fragmented)
    }
}

impl Default for FragmentParser {
    fn default() -> Self {
        Self::new()
    }
}