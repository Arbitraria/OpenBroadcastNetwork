//! MP4 Parser and MSE Segment Generator
//!
//! This module provides functionality to parse MP4 files and generate Media Source Extensions (MSE)
//! compatible segments for browser-based video streaming.
//!
//! # MSE Requirements
//! 
//! For MSE to work properly, we need to provide:
//! 1. **Initialization Segment**: Contains `ftyp` and `moov` boxes with codec information
//! 2. **Media Segments**: Contains `moof` and `mdat` boxes with actual video/audio data
//!
//! # MP4 Box Structure
//! 
//! MP4 files are composed of "boxes" (also called "atoms"). Each box has:
//! - 4 bytes: Box size (big-endian)
//! - 4 bytes: Box type (ASCII)
//! - Variable: Box payload
//!
//! Common box types:
//! - `ftyp`: File type and compatibility
//! - `moov`: Movie metadata (tracks, codecs, etc.)
//! - `mdat`: Media data (actual video/audio samples)
//! - `moof`: Movie fragment (used in fragmented MP4)
//! - `traf`: Track fragment
//! - `trun`: Track run (sample data within fragment)

use std::io::{self, Read, Seek, SeekFrom};
use std::collections::HashMap;
use tracing::{debug, info, warn, error};

/// MP4 box header information
#[derive(Debug, Clone)]
pub struct BoxHeader {
    /// Box type as 4-character ASCII string
    pub box_type: String,
    /// Total size of the box including header
    pub size: u64,
    /// Position where box content starts (after header)
    pub content_start: u64,
}

/// MP4 box content for important boxes
#[derive(Debug, Clone)]
pub enum BoxContent {
    /// File type box - contains compatibility information
    FileType {
        major_brand: String,
        minor_version: u32,
        compatible_brands: Vec<String>,
    },
    /// Movie header box - contains global metadata
    MovieHeader {
        creation_time: u64,
        modification_time: u64,
        timescale: u32,
        duration: u64,
    },
    /// Track information
    Track {
        track_id: u32,
        duration: u64,
        media_type: String, // "vide", "soun", etc.
    },
    /// Raw box data for boxes we don't parse in detail
    Raw(Vec<u8>),
}

/// Parsed MP4 box with header and content
#[derive(Debug, Clone)]
pub struct Mp4Box {
    pub header: BoxHeader,
    pub content: BoxContent,
}

/// MP4 track information extracted from parsing
#[derive(Debug, Clone)]
pub struct Mp4Track {
    pub track_id: u32,
    pub media_type: String, // "vide" for video, "soun" for audio
    pub timescale: u32,
    pub duration: u64,
    pub codec: String,
    pub codec_mime_type: String, // MSE-compatible MIME type
    pub codec_params: Option<String>, // Additional codec parameters
}

/// MSE-compatible segment that can be sent to browsers
#[derive(Debug, Clone)]
pub struct MseSegment {
    /// Segment type - either "initialization" or "media"
    pub segment_type: String,
    /// Raw MP4 data for this segment
    pub data: Vec<u8>,
    /// Timestamp for media segments (None for initialization segments)
    pub timestamp: Option<u64>,
    /// Duration for media segments (None for initialization segments)
    pub duration: Option<u64>,
    /// Whether this is a keyframe/random access point
    pub is_keyframe: bool,
}

/// Main MP4 parser that can extract MSE-compatible segments
pub struct Mp4Parser {
    /// All boxes found in the MP4 file
    boxes: Vec<Mp4Box>,
    /// Track information
    tracks: Vec<Mp4Track>,
    /// Whether the file is fragmented MP4 (already MSE-compatible)
    is_fragmented: bool,
    /// Whether ESDS modification will be applied (for dynamic codec string generation)
    will_modify_esds: bool,
}

impl Mp4Parser {
    /// Create a new MP4 parser
    pub fn new() -> Self {
        Self {
            boxes: Vec::new(),
            tracks: Vec::new(),
            is_fragmented: false,
            will_modify_esds: false,
        }
    }

    /// Parse an MP4 file from raw data
    /// 
    /// This function reads the MP4 box structure and extracts relevant information
    /// for generating MSE-compatible segments.
    pub fn parse(&mut self, data: &[u8]) -> Result<(), io::Error> {
        info!("Starting MP4 parsing of {} bytes", data.len());
        
        let mut cursor = std::io::Cursor::new(data);
        self.boxes.clear();
        self.tracks.clear();
        self.will_modify_esds = false;
        
        // Parse all top-level boxes
        while cursor.position() < data.len() as u64 {
            match self.parse_box(&mut cursor) {
                Ok(box_info) => {
                    debug!("Parsed box: {} (size: {})", box_info.header.box_type, box_info.header.size);
                    
                    // Check if this is a fragmented MP4
                    if box_info.header.box_type == "moof" {
                        self.is_fragmented = true;
                        info!("Detected fragmented MP4 file");
                    }
                    
                    self.boxes.push(box_info);
                }
                Err(e) => {
                    error!("Failed to parse box at position {}: {}", cursor.position(), e);
                    break;
                }
            }
        }
        
        info!("MP4 parsing complete. Found {} boxes, fragmented: {}", 
              self.boxes.len(), self.is_fragmented);
        
        Ok(())
    }

    /// Parse a single MP4 box from the current cursor position
    fn parse_box(&mut self, cursor: &mut std::io::Cursor<&[u8]>) -> Result<Mp4Box, io::Error> {
        let start_pos = cursor.position();
        
        // Read box header (8 bytes minimum)
        let mut header_buf = [0u8; 8];
        cursor.read_exact(&mut header_buf)?;
        
        // Parse box size (big-endian)
        let box_size = u32::from_be_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]) as u64;
        
        // Parse box type (ASCII)
        let box_type = String::from_utf8_lossy(&header_buf[4..8]).to_string();
        
        // Handle extended size (64-bit) if size is 1
        let (actual_size, content_start) = if box_size == 1 {
            let mut extended_size_buf = [0u8; 8];
            cursor.read_exact(&mut extended_size_buf)?;
            let extended_size = u64::from_be_bytes(extended_size_buf);
            (extended_size, start_pos + 16)
        } else {
            (box_size, start_pos + 8)
        };
        
        let header = BoxHeader {
            box_type: box_type.clone(),
            size: actual_size,
            content_start,
        };
        
        // Calculate content size
        let content_size = actual_size - (content_start - start_pos);
        
        // Read box content
        let mut content_data = vec![0u8; content_size as usize];
        cursor.read_exact(&mut content_data)?;
        
        // Parse specific box types
        let content = match box_type.as_str() {
            "ftyp" => {
                // For MSE, we need the raw ftyp box data, so just validate and store raw
                if let Err(e) = self.parse_ftyp_box(&content_data) {
                    warn!("Invalid ftyp box: {}", e);
                }
                BoxContent::Raw(content_data)
            }
            "moov" => {
                // Parse moov box to extract track information
                if let Err(e) = self.parse_moov_box(&content_data) {
                    warn!("Failed to parse moov box: {}", e);
                }
                BoxContent::Raw(content_data)
            }
            _ => BoxContent::Raw(content_data),
        };
        
        Ok(Mp4Box { header, content })
    }
    
    /// Parse movie (moov) box to extract track information
    fn parse_moov_box(&mut self, data: &[u8]) -> Result<(), io::Error> {
        info!("Parsing moov box ({} bytes) for track information", data.len());
        
        let mut cursor = std::io::Cursor::new(data);
        
        // Parse child boxes within moov
        while cursor.position() < data.len() as u64 {
            let pos_before = cursor.position();
            
            // Read box header
            let mut size_bytes = [0u8; 4];
            let mut type_bytes = [0u8; 4];
            
            if cursor.read_exact(&mut size_bytes).is_err() || 
               cursor.read_exact(&mut type_bytes).is_err() {
                break; // End of data
            }
            
            let box_size = u32::from_be_bytes(size_bytes) as u64;
            let box_type = String::from_utf8_lossy(&type_bytes).to_string();
            
            if box_size < 8 {
                break; // Invalid box
            }
            
            debug!("Found moov child box: {} (size: {})", box_type, box_size);
            
            // Parse track boxes (trak)
            if box_type == "trak" {
                let content_size = (box_size - 8) as usize;
                let mut content_data = vec![0u8; content_size];
                if cursor.read_exact(&mut content_data).is_ok() {
                    if let Err(e) = self.parse_trak_box(&content_data) {
                        warn!("Failed to parse trak box: {}", e);
                    }
                }
            } else {
                // Skip other boxes
                let skip_size = box_size - 8;
                cursor.seek(SeekFrom::Current(skip_size as i64)).ok();
            }
        }
        
        info!("Finished parsing moov box, found {} tracks", self.tracks.len());
        Ok(())
    }
    
    /// Parse track (trak) box to extract individual track information
    fn parse_trak_box(&mut self, data: &[u8]) -> Result<(), io::Error> {
        let mut cursor = std::io::Cursor::new(data);
        let mut track_id = 0;
        let mut media_type = String::new();
        let mut timescale = 1000;
        let mut duration = 0;
        let mut codec = String::new();
        let mut codec_mime_type = String::new();
        let mut codec_params = None;
        
        // Parse child boxes within trak
        while cursor.position() < data.len() as u64 {
            let mut size_bytes = [0u8; 4];
            let mut type_bytes = [0u8; 4];
            
            if cursor.read_exact(&mut size_bytes).is_err() || 
               cursor.read_exact(&mut type_bytes).is_err() {
                break;
            }
            
            let box_size = u32::from_be_bytes(size_bytes) as u64;
            let box_type = String::from_utf8_lossy(&type_bytes).to_string();
            
            if box_size < 8 {
                break;
            }
            
            let content_size = (box_size - 8) as usize;
            
            match box_type.as_str() {
                "tkhd" => {
                    // Track header - extract track ID
                    if let Ok(track_id_parsed) = self.parse_tkhd_box(&mut cursor, content_size) {
                        track_id = track_id_parsed;
                    } else {
                        cursor.seek(SeekFrom::Current(content_size as i64)).ok();
                    }
                }
                "mdia" => {
                    // Media box - contains track media information
                    let mut content_data = vec![0u8; content_size];
                    if cursor.read_exact(&mut content_data).is_ok() {
                        if let Ok((parsed_media_type, parsed_timescale, parsed_duration, parsed_codec, parsed_mime, parsed_params)) = 
                            self.parse_mdia_box(&content_data) {
                            media_type = parsed_media_type;
                            timescale = parsed_timescale;
                            duration = parsed_duration;
                            codec = parsed_codec;
                            codec_mime_type = parsed_mime;
                            codec_params = parsed_params;
                        }
                    }
                }
                _ => {
                    // Skip other boxes
                    cursor.seek(SeekFrom::Current(content_size as i64)).ok();
                }
            }
        }
        
        // Create track if we have valid information
        if track_id > 0 && !media_type.is_empty() {
            let track = Mp4Track {
                track_id,
                media_type: media_type.clone(),
                timescale,
                duration,
                codec: codec.clone(),
                codec_mime_type: codec_mime_type.clone(),
                codec_params,
            };
            
            info!("Found track {}: type={}, codec={}, mime={}", 
                  track_id, media_type, codec, codec_mime_type);
            
            self.tracks.push(track);
        }
        
        Ok(())
    }
    
    /// Parse track header (tkhd) box to extract track ID
    fn parse_tkhd_box(&self, cursor: &mut std::io::Cursor<&[u8]>, content_size: usize) -> Result<u32, io::Error> {
        if content_size < 12 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "tkhd box too small"));
        }
        
        let mut data = vec![0u8; content_size];
        cursor.read_exact(&mut data)?;
        
        // Skip version and flags (4 bytes), creation time (4 bytes), modification time (4 bytes)
        if data.len() >= 16 {
            let track_id = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);
            Ok(track_id)
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidData, "tkhd box data too small"))
        }
    }
    
    /// Parse media (mdia) box to extract codec information
    fn parse_mdia_box(&mut self, data: &[u8]) -> Result<(String, u32, u64, String, String, Option<String>), io::Error> {
        let mut cursor = std::io::Cursor::new(data);
        let mut media_type = String::new();
        let mut timescale = 1000;
        let mut duration = 0;
        let mut codec = String::new();
        let mut codec_mime_type = String::new();
        let mut codec_params = None;
        
        while cursor.position() < data.len() as u64 {
            let mut size_bytes = [0u8; 4];
            let mut type_bytes = [0u8; 4];
            
            if cursor.read_exact(&mut size_bytes).is_err() || 
               cursor.read_exact(&mut type_bytes).is_err() {
                break;
            }
            
            let box_size = u32::from_be_bytes(size_bytes) as u64;
            let box_type = String::from_utf8_lossy(&type_bytes).to_string();
            
            if box_size < 8 {
                break;
            }
            
            let content_size = (box_size - 8) as usize;
            
            match box_type.as_str() {
                "mdhd" => {
                    // Media header - extract timescale and duration
                    if let Ok((ts, dur)) = self.parse_mdhd_box(&mut cursor, content_size) {
                        timescale = ts;
                        duration = dur;
                    } else {
                        cursor.seek(SeekFrom::Current(content_size as i64)).ok();
                    }
                }
                "hdlr" => {
                    // Handler reference - extract media type
                    if let Ok(handler_type) = self.parse_hdlr_box(&mut cursor, content_size) {
                        media_type = handler_type;
                    } else {
                        cursor.seek(SeekFrom::Current(content_size as i64)).ok();
                    }
                }
                "minf" => {
                    // Media information - contains codec details
                    let mut content_data = vec![0u8; content_size];
                    if cursor.read_exact(&mut content_data).is_ok() {
                        if let Ok((parsed_codec, parsed_mime, parsed_params)) = 
                            self.parse_minf_box(&content_data, &media_type) {
                            codec = parsed_codec;
                            codec_mime_type = parsed_mime;
                            codec_params = parsed_params;
                        }
                    }
                }
                _ => {
                    cursor.seek(SeekFrom::Current(content_size as i64)).ok();
                }
            }
        }
        
        Ok((media_type, timescale, duration, codec, codec_mime_type, codec_params))
    }
    
    /// Parse media header (mdhd) box
    fn parse_mdhd_box(&self, cursor: &mut std::io::Cursor<&[u8]>, content_size: usize) -> Result<(u32, u64), io::Error> {
        if content_size < 20 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "mdhd box too small"));
        }
        
        let mut data = vec![0u8; content_size];
        cursor.read_exact(&mut data)?;
        
        // Skip version and flags (4 bytes), creation time (4 bytes), modification time (4 bytes)
        let timescale = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);
        let duration = u32::from_be_bytes([data[16], data[17], data[18], data[19]]) as u64;
        
        Ok((timescale, duration))
    }
    
    /// Parse handler reference (hdlr) box
    fn parse_hdlr_box(&self, cursor: &mut std::io::Cursor<&[u8]>, content_size: usize) -> Result<String, io::Error> {
        if content_size < 12 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "hdlr box too small"));
        }
        
        let mut data = vec![0u8; content_size];
        cursor.read_exact(&mut data)?;
        
        // Skip version, flags, and pre_defined (8 bytes total)
        let handler_type = String::from_utf8_lossy(&data[8..12]).to_string();
        
        Ok(handler_type)
    }
    
    /// Parse media information (minf) box to extract codec details
    fn parse_minf_box(&mut self, data: &[u8], media_type: &str) -> Result<(String, String, Option<String>), io::Error> {
        let mut cursor = std::io::Cursor::new(data);
        
        while cursor.position() < data.len() as u64 {
            let mut size_bytes = [0u8; 4];
            let mut type_bytes = [0u8; 4];
            
            if cursor.read_exact(&mut size_bytes).is_err() || 
               cursor.read_exact(&mut type_bytes).is_err() {
                break;
            }
            
            let box_size = u32::from_be_bytes(size_bytes) as u64;
            let box_type = String::from_utf8_lossy(&type_bytes).to_string();
            
            if box_size < 8 {
                break;
            }
            
            let content_size = (box_size - 8) as usize;
            
            if box_type == "stbl" {
                // Sample table - contains codec information
                let mut content_data = vec![0u8; content_size];
                if cursor.read_exact(&mut content_data).is_ok() {
                    return self.parse_stbl_box(&content_data, media_type);
                }
            } else {
                cursor.seek(SeekFrom::Current(content_size as i64)).ok();
            }
        }
        
        // Default fallback
        let (codec, mime_type) = match media_type {
            "vide" => ("H.264".to_string(), "video/mp4; codecs=\"avc1.42E01E\"".to_string()),
            "soun" => ("AAC".to_string(), "audio/mp4; codecs=\"mp4a.40.2\"".to_string()),
            _ => ("Unknown".to_string(), "application/octet-stream".to_string()),
        };
        
        Ok((codec, mime_type, None))
    }
    
    /// Parse sample table (stbl) box to extract codec information
    fn parse_stbl_box(&mut self, data: &[u8], media_type: &str) -> Result<(String, String, Option<String>), io::Error> {
        let mut cursor = std::io::Cursor::new(data);
        
        while cursor.position() < data.len() as u64 {
            let mut size_bytes = [0u8; 4];
            let mut type_bytes = [0u8; 4];
            
            if cursor.read_exact(&mut size_bytes).is_err() || 
               cursor.read_exact(&mut type_bytes).is_err() {
                break;
            }
            
            let box_size = u32::from_be_bytes(size_bytes) as u64;
            let box_type = String::from_utf8_lossy(&type_bytes).to_string();
            
            if box_size < 8 {
                break;
            }
            
            let content_size = (box_size - 8) as usize;
            
            if box_type == "stsd" {
                // Sample description - contains actual codec information
                let mut content_data = vec![0u8; content_size];
                if cursor.read_exact(&mut content_data).is_ok() {
                    return self.parse_stsd_box(&content_data, media_type);
                }
            } else {
                cursor.seek(SeekFrom::Current(content_size as i64)).ok();
            }
        }
        
        Err(io::Error::new(io::ErrorKind::NotFound, "No sample description found"))
    }
    
    /// Parse sample description (stsd) box to extract exact codec information
    fn parse_stsd_box(&mut self, data: &[u8], media_type: &str) -> Result<(String, String, Option<String>), io::Error> {
        if data.len() < 8 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "stsd box too small"));
        }
        
        // Skip version, flags, and entry count (8 bytes)
        let remaining_data = &data[8..];
        
        if remaining_data.len() < 8 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "No sample description entries"));
        }
        
        // Read first sample description entry
        let entry_size = u32::from_be_bytes([remaining_data[0], remaining_data[1], remaining_data[2], remaining_data[3]]) as usize;
        let format = String::from_utf8_lossy(&remaining_data[4..8]).to_string();
        
        info!("Found sample description: format='{}', entry_size={}", format, entry_size);
        
        // Determine codec and MIME type based on format
        let (codec, mime_type, params) = match (media_type, format.as_str()) {
            ("vide", "avc1") => {
                // H.264 video - extract profile/level from avcC box if available
                let profile_level = self.extract_avc_profile(&remaining_data[8..]);
                let codec_string = format!("avc1.{}", profile_level);
                ("H.264".to_string(), 
                 format!("video/mp4; codecs=\"{}\"", codec_string), 
                 Some(profile_level))
            }
            ("soun", "mp4a") => {
                // AAC audio - extract object type from esds box if available
                // Skip the standard mp4a sample entry fields (28 bytes) to get to the esds box
                let esds_offset = if remaining_data.len() > 36 { 36 } else { 8 };
                let object_type = self.extract_aac_object_type(&remaining_data[esds_offset..]);
                
                // CRITICAL INSIGHT: mp4a codec string format investigation
                // RFC 6381 specifies: mp4a.ObjectTypeIndication.AudioObjectType
                // Object type 0x40 = hex 40, which should be mp4a.40.X where X is the AudioSpecificConfig AOT
                // But maybe browsers expect the AudioSpecificConfig AOT in the second parameter?
                
                // From our analysis: AudioSpecificConfig shows AAC Object Type 2 (AAC-LC)
                // So maybe it should be mp4a.40.02 (with zero-padding) or mp4a.40.02
                
                // Chrome validation: objectTypeIndication=0x40 + audioObjectType=2 in AudioSpecificConfig
                // We keep objectTypeIndication=0x40 but ensure AudioSpecificConfig.audioObjectType=2
                let codec_string = match object_type {
                    0x40 => {
                        // Object type 0x40 (MPEG-4 Audio) - will ensure AudioSpecificConfig has audioObjectType=2
                        self.will_modify_esds = true;
                        info!("Object type 0x40 (MPEG-4 Audio) - will fix AudioSpecificConfig for AAC-LC, using mp4a.40.2");
                        "mp4a.40.2".to_string()  // Chrome-supported codec string
                    }
                    0x02 => {
                        info!("Object type 0x02 (AAC-LC) detected - using mp4a.40.2");
                        "mp4a.40.2".to_string()  // Use standard codec string
                    }
                    0x05 => "mp4a.40.5".to_string(),   // HE-AAC 
                    0x1d => "mp4a.40.029".to_string(), // HE-AAC v2
                    _ => {
                        warn!("Unknown AAC object type 0x{:02x}, defaulting to AAC-LC", object_type);
                        "mp4a.40.2".to_string()
                    }
                };
                
                ("AAC".to_string(), 
                 format!("audio/mp4; codecs=\"{}\"", codec_string), 
                 Some(format!("{:02x}", object_type)))
            }
            ("soun", format) if format.starts_with("mp4a") => {
                // Generic AAC
                ("AAC".to_string(), "audio/mp4; codecs=\"mp4a.40.2\"".to_string(), Some("2".to_string()))
            }
            ("vide", _) => {
                // Generic video
                ("H.264".to_string(), "video/mp4; codecs=\"avc1.42E01E\"".to_string(), None)
            }
            ("soun", _) => {
                // Generic audio
                ("AAC".to_string(), "audio/mp4; codecs=\"mp4a.40.2\"".to_string(), None)
            }
            _ => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, 
                    format!("Unsupported format: {} for media type {}", format, media_type)));
            }
        };
        
        info!("Detected codec: {} -> {}", codec, mime_type);
        Ok((codec, mime_type, params))
    }
    
    /// Extract H.264 profile and level from avcC box
    fn extract_avc_profile(&self, data: &[u8]) -> String {
        // Look for avcC box in the sample description
        let mut cursor = std::io::Cursor::new(data);
        
        while cursor.position() + 8 <= data.len() as u64 {
            let mut size_bytes = [0u8; 4];
            let mut type_bytes = [0u8; 4];
            
            if cursor.read_exact(&mut size_bytes).is_err() || 
               cursor.read_exact(&mut type_bytes).is_err() {
                break;
            }
            
            let box_size = u32::from_be_bytes(size_bytes) as u64;
            let box_type = String::from_utf8_lossy(&type_bytes).to_string();
            
            if box_type == "avcC" && box_size >= 11 {
                // Read avcC configuration
                let mut config_data = vec![0u8; (box_size - 8) as usize];
                if cursor.read_exact(&mut config_data).is_ok() && config_data.len() >= 3 {
                    // avcC format: version(1) + profile(1) + compatibility(1) + level(1) + ...
                    let profile = config_data[1];
                    let compatibility = config_data[2];
                    let level = config_data[3];
                    return format!("{:02X}{:02X}{:02X}", profile, compatibility, level);
                }
            } else if box_size >= 8 {
                cursor.seek(SeekFrom::Current((box_size - 8) as i64)).ok();
            } else {
                break;
            }
        }
        
        // Default H.264 Baseline profile
        "42E01E".to_string()
    }
    
    /// Extract AAC object type from esds box
    fn extract_aac_object_type(&self, data: &[u8]) -> u8 {
        info!("Looking for ESDS box in {} bytes of sample description data", data.len());
        
        // Look for esds box in the sample description
        let mut cursor = std::io::Cursor::new(data);
        
        while cursor.position() + 8 <= data.len() as u64 {
            let mut size_bytes = [0u8; 4];
            let mut type_bytes = [0u8; 4];
            
            if cursor.read_exact(&mut size_bytes).is_err() || 
               cursor.read_exact(&mut type_bytes).is_err() {
                break;
            }
            
            let box_size = u32::from_be_bytes(size_bytes) as u64;
            let box_type = String::from_utf8_lossy(&type_bytes).to_string();
            
            info!("Found box in sample description: type='{}', size={}", box_type, box_size);
            
            if box_type == "esds" && box_size >= 20 {
                info!("Found ESDS box with size {}", box_size);
                // Read esds data and parse the decoder configuration
                let mut esds_data = vec![0u8; (box_size - 8) as usize];
                if cursor.read_exact(&mut esds_data).is_ok() {
                    return self.parse_esds_for_aac_object_type(&esds_data);
                }
            } else if box_size >= 8 {
                cursor.seek(SeekFrom::Current((box_size - 8) as i64)).ok();
            } else {
                break;
            }
        }
        
        warn!("No ESDS box found in sample description, defaulting to AAC-LC");
        // Default to AAC-LC
        2
    }
    
    /// Parse ESDS (Elementary Stream Descriptor) to extract AAC object type and analyze AudioSpecificConfig
    fn parse_esds_for_aac_object_type(&self, esds_data: &[u8]) -> u8 {
        info!("Parsing ESDS data ({} bytes)", esds_data.len());
        
        if esds_data.len() < 20 {
            warn!("ESDS data too small: {} bytes", esds_data.len());
            return 2; // Default AAC-LC
        }
        
        // Log first 20 bytes for debugging
        let debug_bytes: Vec<String> = esds_data[0..std::cmp::min(20, esds_data.len())].iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        info!("ESDS first bytes: {}", debug_bytes.join(" "));
        
        // Skip version/flags (4 bytes)
        let mut offset = 4;
        
        // Look for the decoder config descriptor (tag 0x04)
        while offset + 5 < esds_data.len() {
            let tag = esds_data[offset];
            info!("Checking tag at offset {}: 0x{:02x}", offset, tag);
            
            if tag == 0x04 { // DecoderConfigDescriptor tag
                info!("Found DecoderConfigDescriptor at offset {}", offset);
                let decoder_config_start = offset;
                
                // Skip tag (1 byte) and variable length encoding
                offset += 1;
                
                // Skip length bytes (variable length encoding)
                while offset < esds_data.len() && (esds_data[offset] & 0x80) != 0 {
                    offset += 1;
                }
                if offset < esds_data.len() {
                    offset += 1; // Skip the last length byte
                }
                
                // Now we should be at the object type indicator
                if offset < esds_data.len() {
                    let object_type = esds_data[offset];
                    info!("Found AAC object type in ESDS at offset {}: 0x{:02x}", offset, object_type);
                    
                    // Continue parsing to find the AudioSpecificConfig
                    self.analyze_audio_specific_config(esds_data, decoder_config_start, object_type);
                    
                    // Return the actual object type found in the ESDS
                    return object_type;
                }
                break;
            }
            offset += 1;
        }
        
        // If we didn't find the decoder config, default to AAC-LC
        warn!("Could not find DecoderConfigDescriptor (tag 0x04) in ESDS, defaulting to AAC-LC");
        2
    }
    
    /// Analyze AudioSpecificConfig within ESDS to understand profile configuration
    fn analyze_audio_specific_config(&self, esds_data: &[u8], decoder_config_start: usize, object_type: u8) {
        info!("Analyzing AudioSpecificConfig for object type 0x{:02x}", object_type);
        
        let mut offset = decoder_config_start + 1; // Start after DecoderConfigDescriptor tag
        
        // Skip DecoderConfigDescriptor length
        while offset < esds_data.len() && (esds_data[offset] & 0x80) != 0 {
            offset += 1;
        }
        if offset < esds_data.len() {
            offset += 1; // Skip the last length byte
        }
        
        // Skip object type (1 byte), stream type (1 byte), buffer size (3 bytes), max bitrate (4 bytes), avg bitrate (4 bytes)
        offset += 13;
        
        // Look for DecSpecificInfoDescriptor (tag 0x05) which contains AudioSpecificConfig
        while offset + 2 < esds_data.len() {
            let tag = esds_data[offset];
            info!("Looking for AudioSpecificConfig, found tag 0x{:02x} at offset {}", tag, offset);
            
            if tag == 0x05 { // DecSpecificInfoDescriptor tag
                info!("Found DecSpecificInfoDescriptor (AudioSpecificConfig) at offset {}", offset);
                offset += 1; // Skip tag
                
                // Skip length encoding
                let mut config_length = 0;
                if offset < esds_data.len() {
                    if (esds_data[offset] & 0x80) == 0 {
                        // Single byte length
                        config_length = esds_data[offset] as usize;
                        offset += 1;
                    } else {
                        // Multi-byte length encoding
                        while offset < esds_data.len() && (esds_data[offset] & 0x80) != 0 {
                            offset += 1;
                        }
                        if offset < esds_data.len() {
                            config_length = esds_data[offset] as usize;
                            offset += 1;
                        }
                    }
                }
                
                // Now we're at the actual AudioSpecificConfig
                if offset + config_length <= esds_data.len() {
                    let asc_data = &esds_data[offset..offset + config_length];
                    self.parse_audio_specific_config(asc_data, object_type);
                }
                break;
            }
            offset += 1;
        }
    }
    
    /// Parse AudioSpecificConfig to extract AAC profile information
    fn parse_audio_specific_config(&self, asc_data: &[u8], object_type: u8) {
        if asc_data.is_empty() {
            warn!("AudioSpecificConfig is empty");
            return;
        }
        
        info!("Parsing AudioSpecificConfig ({} bytes) for object type 0x{:02x}", asc_data.len(), object_type);
        
        // Log the raw AudioSpecificConfig bytes
        let asc_hex: Vec<String> = asc_data.iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        info!("AudioSpecificConfig bytes: {}", asc_hex.join(" "));
        
        if asc_data.len() >= 1 {
            let first_byte = asc_data[0];
            
            // Extract AAC profile from first 5 bits
            let aac_object_type = (first_byte >> 3) & 0x1F;
            
            // Extract sampling frequency index from next 4 bits (spans first and second byte)
            let mut sampling_freq_index = (first_byte & 0x07) << 1;
            if asc_data.len() >= 2 {
                sampling_freq_index |= (asc_data[1] >> 7) & 0x01;
                
                // Extract channel configuration from next 4 bits
                let channel_config = (asc_data[1] >> 3) & 0x0F;
                
                info!("AudioSpecificConfig analysis:");
                info!("  AAC Object Type: {} ({})", aac_object_type, self.get_aac_object_type_name(aac_object_type));
                info!("  Sampling Frequency Index: {}", sampling_freq_index);
                info!("  Channel Configuration: {}", channel_config);
                
                // Check if this matches expected values for mp4a.40.02
                if object_type == 0x40 {
                    match aac_object_type {
                        2 => info!("✓ AAC Object Type 2 (AAC-LC) matches mp4a.40.02 expectation"),
                        _ => warn!("⚠ AAC Object Type {} does not match AAC-LC (2) expected for mp4a.40.02", aac_object_type),
                    }
                } else {
                    info!("Object type 0x{:02x} with AAC Object Type {}", object_type, aac_object_type);
                }
            }
        }
    }
    
    /// Get human-readable name for AAC object type
    fn get_aac_object_type_name(&self, object_type: u8) -> &'static str {
        match object_type {
            1 => "AAC Main",
            2 => "AAC-LC (Low Complexity)",
            3 => "AAC SSR (Scalable Sample Rate)",
            4 => "AAC LTP (Long Term Prediction)",
            5 => "SBR (Spectral Band Replication)",
            6 => "AAC Scalable",
            _ => "Unknown",
        }
    }

    /// Parse file type (ftyp) box
    fn parse_ftyp_box(&self, data: &[u8]) -> Result<BoxContent, io::Error> {
        if data.len() < 8 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "ftyp box too small"));
        }
        
        let major_brand = String::from_utf8_lossy(&data[0..4]).to_string();
        let minor_version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        
        let mut compatible_brands = Vec::new();
        for chunk in data[8..].chunks(4) {
            if chunk.len() == 4 {
                compatible_brands.push(String::from_utf8_lossy(chunk).to_string());
            }
        }
        
        debug!("Parsed ftyp: brand={}, version={}, compatible={:?}", 
               major_brand, minor_version, compatible_brands);
        
        Ok(BoxContent::FileType {
            major_brand,
            minor_version,
            compatible_brands,
        })
    }

    /// Generate MSE-compatible segments from the parsed MP4
    /// 
    /// This is the main function that converts a regular MP4 file into segments
    /// that can be fed to Media Source Extensions in browsers.
    pub fn generate_mse_segments(&self) -> Result<Vec<MseSegment>, io::Error> {
        info!("Generating MSE segments from MP4 data");
        
        if self.is_fragmented {
            // File is already fragmented, we can use segments directly
            return self.extract_fragmented_segments();
        } else {
            // File is not fragmented, we need to create segments
            return self.create_fragments_from_regular_mp4();
        }
    }

    /// Fix ESDS box to ensure AudioSpecificConfig has correct audioObjectType=2 for Chrome compatibility
    /// Chrome validates: objectTypeIndication=0x40 AND AudioSpecificConfig.audioObjectType=2
    /// This function recursively searches through the MP4 box hierarchy to find and fix ESDS boxes
    fn modify_esds_object_type(&self, data: &[u8]) -> Vec<u8> {
        let mut modified_data = data.to_vec();
        let mut modifications_made = 0;
        
        self.modify_esds_recursive(&mut modified_data, 0, &mut modifications_made);
        
        if modifications_made > 0 {
            info!("Successfully applied {} ESDS AudioSpecificConfig fixes for Chrome compatibility", modifications_made);
        } else {
            info!("No ESDS AudioSpecificConfig fixes needed - already compatible");
        }
        
        modified_data
    }
    
    /// Recursively search and modify ESDS boxes in MP4 box hierarchy
    fn modify_esds_recursive(&self, data: &mut [u8], start_offset: usize, modifications_made: &mut usize) {
        let mut cursor = start_offset;
        
        info!("Starting ESDS recursive search at offset {}, data length: {}", start_offset, data.len());
        
        while cursor + 8 <= data.len() {
            // Read box header
            let box_size = u32::from_be_bytes([
                data[cursor], 
                data[cursor + 1],
                data[cursor + 2], 
                data[cursor + 3]
            ]) as usize;
            
            if box_size < 8 || cursor + box_size > data.len() {
                debug!("Invalid box size {} at offset {}, stopping search", box_size, cursor);
                break;
            }
            
            let box_type = String::from_utf8_lossy(&data[cursor + 4..cursor + 8]);
            debug!("ESDS Search: Processing box '{}' at offset {}, size {}", box_type, cursor, box_size);
            
            match box_type.as_ref() {
                "esds" => {
                    // Found ESDS box - fix AudioSpecificConfig
                    info!("FOUND ESDS BOX at offset {} with size {}!", cursor, box_size);
                    if self.fix_esds_audio_specific_config(data, cursor, box_size) {
                        *modifications_made += 1;
                        info!("Successfully fixed ESDS AudioSpecificConfig at offset {} (size: {})", cursor, box_size);
                    } else {
                        warn!("Failed to fix ESDS box at offset {}", cursor);
                    }
                }
                "trak" | "mdia" | "minf" | "stbl" => {
                    // These boxes contain child boxes, recurse into them
                    info!("ESDS Search: Recursing into {} box at offset {}, child area: {} to {}", 
                          box_type, cursor, cursor + 8, cursor + box_size);
                    self.modify_esds_recursive(data, cursor + 8, modifications_made);
                }
                "stsd" => {
                    // Sample description box has: 8 bytes header + 4 bytes version/flags + 4 bytes entry_count
                    let child_start = cursor + 8 + 8; // box header + stsd fields
                    info!("ESDS Search: Sample description box at offset {}, size {}", cursor, box_size);
                    info!("ESDS Search: Recursing into stsd children from offset {} to {}", 
                          child_start, cursor + box_size);
                    if child_start < cursor + box_size {
                        self.modify_esds_recursive(data, child_start, modifications_made);
                    } else {
                        warn!("stsd box too small to contain sample entries: size {}, child_start would be {}", 
                              box_size, child_start);
                    }
                }
                "mp4a" => {
                    // Audio sample entry - skip sample entry fields
                    info!("ESDS Search: Found mp4a box at offset {}, size {}", cursor, box_size);
                    info!("ESDS Search: Sample entry format - skipping to child boxes");
                    
                    // For sample entries, child boxes start after the sample entry data
                    // Sample entry header: 8 bytes box header + 8 bytes reserved/data_reference_index
                    // Audio sample entry: + 8 bytes reserved + 4 bytes channel_count/sample_size/etc.
                    // Total sample entry fields: 28 bytes
                    let child_start = cursor + 8 + 28; // box header + sample entry fields
                    if child_start < cursor + box_size {
                        info!("ESDS Search: Recursing into mp4a children from offset {} to {}", 
                              child_start, cursor + box_size);
                        self.modify_esds_recursive(data, child_start, modifications_made);
                    } else {
                        warn!("mp4a box too small to contain child boxes: size {}, child_start would be {}", 
                              box_size, child_start);
                    }
                }
                _ => {
                    // Other boxes - skip content
                    debug!("ESDS Search: Skipping {} box at offset {}", box_type, cursor);
                }
            }
            
            cursor += box_size;
        }
        
        info!("Finished ESDS recursive search starting at offset {}", start_offset);
    }
    
    /// Fix the AudioSpecificConfig within a specific ESDS box
    /// Chrome expects: objectTypeIndication=0x40 AND AudioSpecificConfig.audioObjectType=2
    fn fix_esds_audio_specific_config(&self, data: &mut [u8], box_offset: usize, box_size: usize) -> bool {
        // ESDS box structure:
        // [box_size:4][box_type:4][version:1][flags:3][esds_data...]
        let esds_data_start = box_offset + 8 + 4; // Skip box header + version/flags
        let esds_data_end = box_offset + box_size;
        
        if esds_data_start >= esds_data_end {
            warn!("ESDS box too small at offset {}", box_offset);
            return false;
        }
        
        debug!("Analyzing ESDS content from offset {} to {}", esds_data_start, esds_data_end);
        
        // Log first few bytes for debugging
        let debug_len = std::cmp::min(16, esds_data_end - esds_data_start);
        let debug_bytes: Vec<String> = data[esds_data_start..esds_data_start + debug_len]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        debug!("ESDS data starts with: {}", debug_bytes.join(" "));
        
        // Search for DecoderConfigDescriptor (tag 0x04) to find AudioSpecificConfig
        for i in esds_data_start..esds_data_end - 1 {
            if data[i] == 0x04 {
                // Found DecoderConfigDescriptor tag
                debug!("Found DecoderConfigDescriptor (0x04) at offset {}", i);
                
                // Parse DecoderConfigDescriptor structure
                let mut j = i + 1;
                
                // Skip length bytes (they have bit 7 set except for the last one)
                while j < esds_data_end && (data[j] & 0x80) != 0 {
                    j += 1;
                }
                if j < esds_data_end {
                    j += 1; // Skip the last length byte
                }
                
                // j points to objectTypeIndication - verify it's 0x40
                if j < esds_data_end {
                    let object_type = data[j];
                    debug!("Found object type 0x{:02x} at offset {}", object_type, j);
                    
                    if object_type == 0x40 {
                        info!("Found object type 0x40 - searching for AudioSpecificConfig to fix audioObjectType");
                        
                        // Skip objectTypeIndication(1) + streamType(1) + bufferSizeDB(3) + maxBitrate(4) + avgBitrate(4) = 13 bytes
                        let asc_search_start = j + 13;
                        
                        // Look for DecSpecificInfoDescriptor (tag 0x05) containing AudioSpecificConfig
                        for k in asc_search_start..esds_data_end - 2 {
                            if data[k] == 0x05 {
                                debug!("Found DecSpecificInfoDescriptor (0x05) at offset {}", k);
                                
                                // Skip tag and length to get to AudioSpecificConfig
                                let mut asc_offset = k + 1;
                                while asc_offset < esds_data_end && (data[asc_offset] & 0x80) != 0 {
                                    asc_offset += 1;
                                }
                                if asc_offset < esds_data_end {
                                    asc_offset += 1; // Skip last length byte
                                }
                                
                                // asc_offset now points to AudioSpecificConfig
                                if asc_offset < esds_data_end {
                                    let asc_first_byte = data[asc_offset];
                                    let audio_object_type = (asc_first_byte >> 3) & 0x1F;
                                    
                                    debug!("AudioSpecificConfig: audioObjectType={} at offset {}", audio_object_type, asc_offset);
                                    
                                    if audio_object_type != 2 {
                                        info!("Fixing AudioSpecificConfig: changing audioObjectType from {} to 2 (AAC-LC)", audio_object_type);
                                        
                                        // Clear upper 5 bits and set audioObjectType=2
                                        let new_first_byte = (asc_first_byte & 0x07) | (2 << 3);
                                        data[asc_offset] = new_first_byte;
                                        
                                        info!("Successfully fixed AudioSpecificConfig at offset {}: 0x{:02x} -> 0x{:02x}", 
                                              asc_offset, asc_first_byte, new_first_byte);
                                        return true;
                                    } else {
                                        info!("AudioSpecificConfig already has correct audioObjectType=2");
                                        return false; // No change needed
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
                break;
            }
        }
        
        debug!("No modifications needed for ESDS box at offset {}", box_offset);
        false
    }

    /// Extract segments from an already fragmented MP4 file
    fn extract_fragmented_segments(&self) -> Result<Vec<MseSegment>, io::Error> {
        let mut segments = Vec::new();
        
        // Create initialization segment from ftyp + moov boxes
        let mut init_data = Vec::new();
        
        let mut included_boxes = Vec::new();
        for box_info in &self.boxes {
            match box_info.header.box_type.as_str() {
                "ftyp" => {
                    // Add ftyp box as-is
                    let box_data = self.serialize_box(box_info);
                    
                    // Validate the serialized box
                    if box_data.len() >= 8 {
                        let serialized_size = u32::from_be_bytes([box_data[0], box_data[1], box_data[2], box_data[3]]);
                        let serialized_type = String::from_utf8_lossy(&box_data[4..8]);
                        
                        if serialized_size as usize != box_data.len() {
                            error!("Serialized box size mismatch for {}: header says {}, actual {}", 
                                   box_info.header.box_type, serialized_size, box_data.len());
                            continue; // Skip this box
                        }
                        
                        if serialized_type != box_info.header.box_type {
                            error!("Serialized box type mismatch for {}: header says '{}', actual '{}'", 
                                   box_info.header.box_type, box_info.header.box_type, serialized_type);
                            continue; // Skip this box
                        }
                        
                        debug!("Validated {} box: size={}, type='{}', data_len={}", 
                               box_info.header.box_type, serialized_size, serialized_type, box_data.len());
                    } else {
                        error!("Serialized box too small for {}: {} bytes", box_info.header.box_type, box_data.len());
                        continue; // Skip this box
                    }
                    
                    included_boxes.push(format!("{}({} bytes)", box_info.header.box_type, box_data.len()));
                    init_data.extend_from_slice(&box_data);
                }
                "moov" => {
                    // Add moov box with ESDS modification if needed
                    let original_box_data = self.serialize_box(box_info);
                    
                    // Check if we need to modify ESDS (only for audio tracks with object type 0x40)
                    info!("Checking tracks for ESDS modification need:");
                    for track in &self.tracks {
                        info!("Track {}: media_type='{}', codec_params={:?}", 
                              track.track_id, track.media_type, track.codec_params);
                    }
                    
                    // Check if ESDS needs AudioSpecificConfig fix for Chrome compatibility
                    let needs_esds_modification = self.will_modify_esds;
                    
                    // Log track info for debugging
                    for track in &self.tracks {
                        if track.media_type == "soun" {
                            info!("Audio track {}: codec_params={:?}, object_type_0x40_is_correct_for_mp4a.40.x", 
                                  track.track_id, track.codec_params);
                        }
                    }
                    
                    info!("ESDS modification needed: {}", needs_esds_modification);
                    
                    let box_data = if needs_esds_modification {
                        info!("Applying ESDS modification to moov box for AAC object type 0x40 → 0x02 compatibility");
                        // Extract just the content part (without the box header)
                        if let BoxContent::Raw(content) = &box_info.content {
                            let modified_content = self.modify_esds_object_type(content);
                            
                            // Reconstruct the box with modified content
                            let mut modified_box_data = Vec::new();
                            let total_size = 8 + modified_content.len() as u32;
                            modified_box_data.extend_from_slice(&total_size.to_be_bytes());
                            modified_box_data.extend_from_slice(b"moov");
                            modified_box_data.extend_from_slice(&modified_content);
                            modified_box_data
                        } else {
                            warn!("moov box content is not Raw, cannot modify ESDS");
                            original_box_data
                        }
                    } else {
                        original_box_data
                    };
                    
                    // Validate the serialized box
                    if box_data.len() >= 8 {
                        let serialized_size = u32::from_be_bytes([box_data[0], box_data[1], box_data[2], box_data[3]]);
                        let serialized_type = String::from_utf8_lossy(&box_data[4..8]);
                        
                        if serialized_size as usize != box_data.len() {
                            error!("Serialized box size mismatch for {}: header says {}, actual {}", 
                                   box_info.header.box_type, serialized_size, box_data.len());
                            continue; // Skip this box
                        }
                        
                        if serialized_type != box_info.header.box_type {
                            error!("Serialized box type mismatch for {}: header says '{}', actual '{}'", 
                                   box_info.header.box_type, box_info.header.box_type, serialized_type);
                            continue; // Skip this box
                        }
                        
                        debug!("Validated {} box: size={}, type='{}', data_len={}", 
                               box_info.header.box_type, serialized_size, serialized_type, box_data.len());
                    } else {
                        error!("Serialized box too small for {}: {} bytes", box_info.header.box_type, box_data.len());
                        continue; // Skip this box
                    }
                    
                    included_boxes.push(format!("{}({} bytes)", box_info.header.box_type, box_data.len()));
                    init_data.extend_from_slice(&box_data);
                }
                _ => {}
            }
        }
        
        if !init_data.is_empty() {
            info!("Creating initialization segment with boxes: {}", included_boxes.join(" + "));
            
            // Log the first few bytes of the init segment for debugging
            if init_data.len() >= 16 {
                let first_bytes: Vec<String> = init_data[0..16].iter()
                    .map(|b| format!("{:02x}", b))
                    .collect();
                info!("Init segment first 16 bytes: {}", first_bytes.join(" "));
                
                // Validate first box
                if init_data.len() >= 8 {
                    let box_size = u32::from_be_bytes([init_data[0], init_data[1], init_data[2], init_data[3]]);
                    let box_type = String::from_utf8_lossy(&init_data[4..8]);
                    info!("Init segment first box: type='{}', size={}", box_type, box_size);
                    
                    // Check if the box size matches expectations
                    if box_size == 0 {
                        error!("Invalid box size 0 in initialization segment!");
                    } else if box_size as usize > init_data.len() {
                        error!("Box size {} exceeds init segment length {}", box_size, init_data.len());
                    }
                }
            }
            
            // Final validation of the complete initialization segment
            let mut validation_offset = 0;
            let mut box_count = 0;
            while validation_offset + 8 <= init_data.len() {
                let box_size = u32::from_be_bytes([
                    init_data[validation_offset],
                    init_data[validation_offset + 1],
                    init_data[validation_offset + 2],
                    init_data[validation_offset + 3],
                ]) as usize;
                
                let box_type = String::from_utf8_lossy(&init_data[validation_offset + 4..validation_offset + 8]);
                
                debug!("Init segment box {}: type='{}', size={} at offset {}", 
                       box_count, box_type, box_size, validation_offset);
                
                if box_size < 8 {
                    error!("Invalid box size {} for {} at offset {}", box_size, box_type, validation_offset);
                    break;
                }
                
                if validation_offset + box_size > init_data.len() {
                    error!("Box {} extends beyond segment: offset={}, size={}, segment_len={}", 
                           box_type, validation_offset, box_size, init_data.len());
                    break;
                }
                
                validation_offset += box_size;
                box_count += 1;
            }
            
            if validation_offset != init_data.len() {
                error!("Initialization segment validation failed: processed {} bytes, total {}", 
                       validation_offset, init_data.len());
            } else {
                info!("Initialization segment validation passed: {} boxes, {} bytes", box_count, init_data.len());
            }
            
            segments.push(MseSegment {
                segment_type: "initialization".to_string(),
                data: init_data,
                timestamp: None,
                duration: None,
                is_keyframe: true,
            });
            info!("Created initialization segment ({} bytes)", segments[0].data.len());
        }
        
        // Create media segments from moof + mdat pairs
        let mut i = 0;
        while i < self.boxes.len() {
            if self.boxes[i].header.box_type == "moof" && 
               i + 1 < self.boxes.len() && 
               self.boxes[i + 1].header.box_type == "mdat" {
                
                let mut media_data = Vec::new();
                media_data.extend_from_slice(&self.serialize_box(&self.boxes[i])); // moof
                media_data.extend_from_slice(&self.serialize_box(&self.boxes[i + 1])); // mdat
                
                let media_data_len = media_data.len();
                
                segments.push(MseSegment {
                    segment_type: "media".to_string(),
                    data: media_data,
                    timestamp: Some(0), // TODO: Parse actual timestamp from moof
                    duration: Some(1000), // TODO: Parse actual duration
                    is_keyframe: true, // TODO: Determine from track run data
                });
                
                debug!("Created media segment {} ({} bytes)", segments.len() - 1, media_data_len);
                i += 2; // Skip both moof and mdat
            } else {
                i += 1;
            }
        }
        
        info!("Generated {} segments from fragmented MP4", segments.len());
        Ok(segments)
    }

    /// Create fragments from a regular (non-fragmented) MP4 file
    /// 
    /// This is more complex as we need to:
    /// 1. Extract the moov box and modify it for fragmented streaming
    /// 2. Split the mdat content into chunks
    /// 3. Create moof boxes for each chunk
    fn create_fragments_from_regular_mp4(&self) -> Result<Vec<MseSegment>, io::Error> {
        warn!("Converting regular MP4 to fragmented format - this is a simplified implementation");
        
        let mut segments = Vec::new();
        
        // For now, create a single initialization segment with ftyp + moov
        let mut init_data = Vec::new();
        let mut mdat_data = Vec::new();
        
        for box_info in &self.boxes {
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
            });
            info!("Created initialization segment ({} bytes)", segments[0].data.len());
        }
        
        // For now, create a single media segment with all the mdat content
        // TODO: Implement proper fragmentation with moof boxes
        if !mdat_data.is_empty() {
            // Create a simple moof + mdat pair
            let media_segment = self.create_simple_media_segment(&mdat_data)?;
            segments.push(media_segment);
            info!("Created media segment ({} bytes)", segments[1].data.len());
        }
        
        info!("Generated {} segments from regular MP4", segments.len());
        Ok(segments)
    }

    /// Create a simple media segment with minimal moof box
    /// 
    /// This creates a basic fragmented MP4 segment that should work with MSE.
    /// For production use, this would need to be much more sophisticated.
    fn create_simple_media_segment(&self, mdat_content: &[u8]) -> Result<MseSegment, io::Error> {
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
            timestamp: Some(0),
            duration: Some(1000), // 1 second
            is_keyframe: true,
        })
    }

    /// Create a minimal moof (movie fragment) box
    /// 
    /// This creates the most basic moof box structure needed for MSE.
    /// A proper implementation would include more detailed track information.
    fn create_minimal_moof(&self, data_size: u32) -> Result<Vec<u8>, io::Error> {
        let mut moof_data = Vec::new();
        
        // moof box header will be added at the end
        let mut moof_content = Vec::new();
        
        // Add mfhd (movie fragment header)
        let mfhd_content = vec![
            0, 0, 0, 0, // version + flags
            0, 0, 0, 1, // sequence number
        ];
        let mfhd_size = 8 + mfhd_content.len() as u32;
        moof_content.extend_from_slice(&mfhd_size.to_be_bytes());
        moof_content.extend_from_slice(b"mfhd");
        moof_content.extend_from_slice(&mfhd_content);
        
        // Add traf (track fragment) - simplified
        let mut traf_content = Vec::new();
        
        // tfhd (track fragment header)
        let tfhd_content = vec![
            0, 0, 0, 0, // version + flags
            0, 0, 0, 1, // track_id = 1
        ];
        let tfhd_size = 8 + tfhd_content.len() as u32;
        traf_content.extend_from_slice(&tfhd_size.to_be_bytes());
        traf_content.extend_from_slice(b"tfhd");
        traf_content.extend_from_slice(&tfhd_content);
        
        // trun (track run) - simplified
        let trun_content = vec![
            0, 0, 0, 0, // version + flags
            0, 0, 0, 1, // sample_count = 1
            data_size.to_be_bytes()[0], data_size.to_be_bytes()[1], 
            data_size.to_be_bytes()[2], data_size.to_be_bytes()[3], // data_offset
        ];
        let trun_size = 8 + trun_content.len() as u32;
        traf_content.extend_from_slice(&trun_size.to_be_bytes());
        traf_content.extend_from_slice(b"trun");
        traf_content.extend_from_slice(&trun_content);
        
        // Complete traf box
        let traf_size = 8 + traf_content.len() as u32;
        moof_content.extend_from_slice(&traf_size.to_be_bytes());
        moof_content.extend_from_slice(b"traf");
        moof_content.extend_from_slice(&traf_content);
        
        // Complete moof box
        let moof_size = 8 + moof_content.len() as u32;
        moof_data.extend_from_slice(&moof_size.to_be_bytes());
        moof_data.extend_from_slice(b"moof");
        moof_data.extend_from_slice(&moof_content);
        
        debug!("Created minimal moof box ({} bytes)", moof_data.len());
        Ok(moof_data)
    }

    /// Serialize an MP4 box back to binary format
    fn serialize_box(&self, box_info: &Mp4Box) -> Vec<u8> {
        let mut data = Vec::new();
        
        // VALIDATION: Check that box_info has valid data before serialization
        if box_info.header.box_type.len() != 4 {
            error!("Invalid box type length for {}: {} bytes", box_info.header.box_type, box_info.header.box_type.len());
            return Vec::new();
        }
        
        // Get content first to calculate correct size
        let content_data = match &box_info.content {
            BoxContent::Raw(content) => {
                content.clone()
            }
            BoxContent::FileType { major_brand, minor_version, compatible_brands } => {
                let mut ftyp_content = Vec::new();
                ftyp_content.extend_from_slice(major_brand.as_bytes());
                ftyp_content.extend_from_slice(&minor_version.to_be_bytes());
                for brand in compatible_brands {
                    ftyp_content.extend_from_slice(brand.as_bytes());
                }
                ftyp_content
            }
            _ => {
                warn!("Cannot serialize parsed box content for {}", box_info.header.box_type);
                Vec::new()
            }
        };
        
        // VALIDATION: Ensure content data is reasonable
        if content_data.len() > 100_000_000 { // 100MB limit
            error!("Content data too large for {}: {} bytes", box_info.header.box_type, content_data.len());
            return Vec::new();
        }
        
        // Calculate correct box size (header + content)
        let total_size = 8 + content_data.len() as u32;
        
        // VALIDATION: Ensure total size is reasonable
        if total_size > 100_000_000 { // 100MB limit
            error!("Total box size too large for {}: {} bytes", box_info.header.box_type, total_size);
            return Vec::new();
        }
        
        // Box header with correct size
        data.extend_from_slice(&total_size.to_be_bytes());
        data.extend_from_slice(box_info.header.box_type.as_bytes());
        
        // Box content
        data.extend_from_slice(&content_data);
        
        // Debug log the serialized data
        if data.len() >= 8 {
            let box_type = String::from_utf8_lossy(&data[4..8]);
            let parsed_size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            debug!("Serialized {} box: {} bytes total, header size: {}, content size: {}", 
                   box_type, data.len(), parsed_size, content_data.len());
            
            if parsed_size != data.len() as u32 {
                error!("Box size mismatch! Header says {}, actual size {}", parsed_size, data.len());
                // Additional debugging for size mismatches
                error!("Box info original size: {}, content size: {}", box_info.header.size, content_data.len());
                error!("Box type in header: '{}'", box_info.header.box_type);
                return Vec::new(); // Don't return corrupted data
            }
            
            // Validate that the box type in the serialized data matches expectation
            if box_type != box_info.header.box_type {
                error!("Box type mismatch! Expected '{}', got '{}'", box_info.header.box_type, box_type);
                return Vec::new(); // Don't return corrupted data
            }
            
            // Log first few bytes
            let first_bytes: Vec<String> = data[0..std::cmp::min(8, data.len())].iter()
                .map(|b| format!("{:02x}", b))
                .collect();
            debug!("First 8 bytes: {}", first_bytes.join(" "));
        }
        
        data
    }

    /// Get information about tracks in the MP4 file
    pub fn get_tracks(&self) -> &[Mp4Track] {
        &self.tracks
    }

    /// Check if the MP4 file is already fragmented
    pub fn is_fragmented(&self) -> bool {
        self.is_fragmented
    }

    /// Get summary information about the parsed MP4
    pub fn get_summary(&self) -> String {
        format!(
            "MP4 Summary: {} boxes, {} tracks, fragmented: {}, box types: [{}]",
            self.boxes.len(),
            self.tracks.len(),
            self.is_fragmented,
            self.boxes.iter()
                .map(|b| b.header.box_type.clone())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
    
    
    /// Get video track codec information
    pub fn get_video_codec_info(&self) -> Option<(&str, &str)> {
        self.tracks.iter()
            .find(|track| track.media_type == "vide")
            .map(|track| (track.codec.as_str(), track.codec_mime_type.as_str()))
    }
    
    /// Get audio track codec information
    pub fn get_audio_codec_info(&self) -> Option<(&str, &str)> {
        self.tracks.iter()
            .find(|track| track.media_type == "soun")
            .map(|track| (track.codec.as_str(), track.codec_mime_type.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mp4_parser_creation() {
        let parser = Mp4Parser::new();
        assert_eq!(parser.boxes.len(), 0);
        assert_eq!(parser.tracks.len(), 0);
        assert!(!parser.is_fragmented());
    }

    #[test]
    fn test_ftyp_parsing() {
        let mut parser = Mp4Parser::new();
        
        // Create minimal ftyp box data
        let ftyp_data = b"mp41\x00\x00\x00\x00mp41isom";
        
        let content = parser.parse_ftyp_box(ftyp_data).unwrap();
        
        match content {
            BoxContent::FileType { major_brand, minor_version, compatible_brands } => {
                assert_eq!(major_brand, "mp41");
                assert_eq!(minor_version, 0);
                assert_eq!(compatible_brands, vec!["mp41", "isom"]);
            }
            _ => panic!("Expected FileType content"),
        }
    }
}