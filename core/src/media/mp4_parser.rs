//! MP4 Parser and MSE Segment Generator
//!
//! This module provides functionality to parse MP4 files and generate Media Source Extensions (MSE)
//! compatible segments for browser-based video streaming.
//!
//! # Overview
//!
//! The MP4 parser handles two main tasks:
//! 1. **Parsing**: Reading MP4 box structure and extracting codec information
//! 2. **Segmentation**: Converting regular MP4 files to MSE-compatible fragmented format
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
//!
//! # Codec Detection
//!
//! The parser detects codecs by examining sample description boxes:
//! - **Video**: Looks for `avc1` (H.264), `hvc1` (H.265), etc.
//! - **Audio**: Looks for `mp4a` (AAC), `ac-3` (Dolby Digital), etc.
//!
//! # Browser Compatibility
//!
//! Different browsers have different codec support:
//! - **Chrome**: Strict validation, expects objectTypeIndication to match codec string
//! - **Firefox**: More forgiving, supports wider range of codecs
//! - **Safari**: Good support for Apple-standard codecs
//!
//! # Known Issues
//!
//! 1. Chrome rejects AAC with objectTypeIndication 0x40 despite standards compliance
//! 2. AC-3 audio is not supported in most browser MediaSource implementations
//! 3. Large initialization segments may need WebSocket chunking

use crate::media::fmp4_converter::{FragmentedMp4Converter, Sample};
use std::io::{self, Read, Seek, SeekFrom};
use tracing::{debug, error, info, warn};

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
    pub codec_mime_type: String,      // MSE-compatible MIME type
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
///
/// # Usage
/// ```ignore
/// let mut parser = Mp4Parser::new();
/// parser.parse(&mp4_data)?;
/// let segments = parser.generate_mse_segments()?;
/// ```
pub struct Mp4Parser {
    /// All boxes found in the MP4 file
    boxes: Vec<Mp4Box>,
    /// Track information extracted from moov box
    tracks: Vec<Mp4Track>,
    /// Whether the file is fragmented MP4 (already MSE-compatible)
    is_fragmented: bool,
    /// Fragmented MP4 converter for proper MSE segments
    fmp4_converter: FragmentedMp4Converter,
}

impl Mp4Parser {
    /// Create a new MP4 parser instance
    pub fn new() -> Self {
        Self {
            boxes: Vec::new(),
            tracks: Vec::new(),
            is_fragmented: false,
            fmp4_converter: FragmentedMp4Converter::new(),
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

        // Parse all top-level boxes
        while cursor.position() < data.len() as u64 {
            match self.parse_box(&mut cursor) {
                Ok(box_info) => {
                    debug!(
                        "Parsed box: {} (size: {})",
                        box_info.header.box_type, box_info.header.size
                    );

                    // Check if this is a fragmented MP4
                    if box_info.header.box_type == "moof" {
                        self.is_fragmented = true;
                        info!("Detected fragmented MP4 file");
                    }

                    self.boxes.push(box_info);
                }
                Err(e) => {
                    error!(
                        "Failed to parse box at position {}: {}",
                        cursor.position(),
                        e
                    );
                    break;
                }
            }
        }

        info!(
            "MP4 parsing complete. Found {} boxes, fragmented: {}",
            self.boxes.len(),
            self.is_fragmented
        );

        Ok(())
    }

    /// Parse a single MP4 box from the current cursor position
    fn parse_box(&mut self, cursor: &mut std::io::Cursor<&[u8]>) -> Result<Mp4Box, io::Error> {
        let start_pos = cursor.position();

        // Read box header (8 bytes minimum)
        let mut header_buf = [0u8; 8];
        cursor.read_exact(&mut header_buf)?;

        // Parse box size (big-endian)
        let box_size =
            u32::from_be_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]) as u64;

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
        info!(
            "Parsing moov box ({} bytes) for track information",
            data.len()
        );

        let mut cursor = std::io::Cursor::new(data);

        // Parse child boxes within moov
        while cursor.position() < data.len() as u64 {
            let pos_before = cursor.position();

            // Read box header
            let mut size_bytes = [0u8; 4];
            let mut type_bytes = [0u8; 4];

            if cursor.read_exact(&mut size_bytes).is_err()
                || cursor.read_exact(&mut type_bytes).is_err()
            {
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

        info!(
            "Finished parsing moov box, found {} tracks",
            self.tracks.len()
        );
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

            if cursor.read_exact(&mut size_bytes).is_err()
                || cursor.read_exact(&mut type_bytes).is_err()
            {
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
                        if let Ok((
                            parsed_media_type,
                            parsed_timescale,
                            parsed_duration,
                            parsed_codec,
                            parsed_mime,
                            parsed_params,
                        )) = self.parse_mdia_box(&content_data)
                        {
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

            info!(
                "Found track {}: type={}, codec={}, mime={}",
                track_id, media_type, codec, codec_mime_type
            );

            self.tracks.push(track);
        }

        Ok(())
    }

    /// Parse track header (tkhd) box to extract track ID
    fn parse_tkhd_box(
        &self,
        cursor: &mut std::io::Cursor<&[u8]>,
        content_size: usize,
    ) -> Result<u32, io::Error> {
        if content_size < 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tkhd box too small",
            ));
        }

        let mut data = vec![0u8; content_size];
        cursor.read_exact(&mut data)?;

        // Skip version and flags (4 bytes), creation time (4 bytes), modification time (4 bytes)
        if data.len() >= 16 {
            let track_id = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);
            Ok(track_id)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tkhd box data too small",
            ))
        }
    }

    /// Parse media (mdia) box to extract codec information
    fn parse_mdia_box(
        &mut self,
        data: &[u8],
    ) -> Result<(String, u32, u64, String, String, Option<String>), io::Error> {
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

            if cursor.read_exact(&mut size_bytes).is_err()
                || cursor.read_exact(&mut type_bytes).is_err()
            {
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
                            self.parse_minf_box(&content_data, &media_type)
                        {
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

        Ok((
            media_type,
            timescale,
            duration,
            codec,
            codec_mime_type,
            codec_params,
        ))
    }

    /// Parse media header (mdhd) box
    fn parse_mdhd_box(
        &self,
        cursor: &mut std::io::Cursor<&[u8]>,
        content_size: usize,
    ) -> Result<(u32, u64), io::Error> {
        if content_size < 20 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mdhd box too small",
            ));
        }

        let mut data = vec![0u8; content_size];
        cursor.read_exact(&mut data)?;

        // Skip version and flags (4 bytes), creation time (4 bytes), modification time (4 bytes)
        let timescale = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);
        let duration = u32::from_be_bytes([data[16], data[17], data[18], data[19]]) as u64;

        Ok((timescale, duration))
    }

    /// Parse handler reference (hdlr) box
    fn parse_hdlr_box(
        &self,
        cursor: &mut std::io::Cursor<&[u8]>,
        content_size: usize,
    ) -> Result<String, io::Error> {
        if content_size < 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "hdlr box too small",
            ));
        }

        let mut data = vec![0u8; content_size];
        cursor.read_exact(&mut data)?;

        // Skip version, flags, and pre_defined (8 bytes total)
        let handler_type = String::from_utf8_lossy(&data[8..12]).to_string();

        Ok(handler_type)
    }

    /// Parse media information (minf) box to extract codec details
    fn parse_minf_box(
        &mut self,
        data: &[u8],
        media_type: &str,
    ) -> Result<(String, String, Option<String>), io::Error> {
        let mut cursor = std::io::Cursor::new(data);

        while cursor.position() < data.len() as u64 {
            let mut size_bytes = [0u8; 4];
            let mut type_bytes = [0u8; 4];

            if cursor.read_exact(&mut size_bytes).is_err()
                || cursor.read_exact(&mut type_bytes).is_err()
            {
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
            "vide" => (
                "H.264".to_string(),
                "video/mp4; codecs=\"avc1.42E01E\"".to_string(),
            ),
            "soun" => (
                "AAC".to_string(),
                "audio/mp4; codecs=\"mp4a.40.2\"".to_string(),
            ),
            _ => (
                "Unknown".to_string(),
                "application/octet-stream".to_string(),
            ),
        };

        Ok((codec, mime_type, None))
    }

    /// Parse sample table (stbl) box to extract codec information
    fn parse_stbl_box(
        &mut self,
        data: &[u8],
        media_type: &str,
    ) -> Result<(String, String, Option<String>), io::Error> {
        let mut cursor = std::io::Cursor::new(data);

        while cursor.position() < data.len() as u64 {
            let mut size_bytes = [0u8; 4];
            let mut type_bytes = [0u8; 4];

            if cursor.read_exact(&mut size_bytes).is_err()
                || cursor.read_exact(&mut type_bytes).is_err()
            {
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

        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "No sample description found",
        ))
    }

    /// Parse sample description (stsd) box to extract exact codec information
    ///
    /// The stsd box contains codec-specific information that browsers need to
    /// initialize decoders. This includes:
    /// - Video: Resolution, profile, level (e.g., avc1.42E01E)
    /// - Audio: Sample rate, channels, object type (e.g., mp4a.40.2)
    ///
    /// # Format Detection
    ///
    /// Common formats:
    /// - `avc1`: H.264/AVC video
    /// - `hvc1`/`hev1`: H.265/HEVC video
    /// - `mp4a`: AAC audio (requires ESDS parsing)
    /// - `ac-3`: Dolby Digital audio
    /// - `ec-3`: Dolby Digital Plus audio
    fn parse_stsd_box(
        &mut self,
        data: &[u8],
        media_type: &str,
    ) -> Result<(String, String, Option<String>), io::Error> {
        if data.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stsd box too small",
            ));
        }

        // Skip version (1 byte), flags (3 bytes), and entry count (4 bytes)
        let remaining_data = &data[8..];

        if remaining_data.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No sample description entries",
            ));
        }

        // Read first sample description entry
        let entry_size = u32::from_be_bytes([
            remaining_data[0],
            remaining_data[1],
            remaining_data[2],
            remaining_data[3],
        ]) as usize;
        let format = String::from_utf8_lossy(&remaining_data[4..8]).to_string();

        info!(
            "Found sample description: format='{}', entry_size={}",
            format, entry_size
        );

        // Determine codec and MIME type based on format
        let (codec, mime_type, params) = match (media_type, format.as_str()) {
            ("vide", "avc1") => {
                // H.264 video - extract profile/level from avcC box if available
                let profile_level = self.extract_avc_profile(&remaining_data[8..]);
                let codec_string = format!("avc1.{}", profile_level);
                (
                    "H.264".to_string(),
                    format!("video/mp4; codecs=\"{}\"", codec_string),
                    Some(profile_level),
                )
            }
            ("soun", "mp4a") => {
                // AAC audio - extract object type from esds box if available
                // Skip the standard mp4a sample entry fields (28 bytes) to get to the esds box
                let esds_offset = if remaining_data.len() > 36 { 36 } else { 8 };
                let object_type = self.extract_aac_object_type(&remaining_data[esds_offset..]);

                // CRITICAL INSIGHT: mp4a codec string format investigation
                // RFC 6381 specifies: mp4a.ObjectTypeIndication.AudioObjectType
                // Object type 0x40 = hex 40, which should be mp4a.40.X where X is the AudioSpecificConfig AOT
                //
                // Chrome requires the full codec string format: mp4a.40.2 (with audioObjectType)
                // The ESDS is regenerated with simple length encoding (not extended 80 80 80 XX)
                // to ensure Chrome can properly parse and validate the audioObjectType
                let codec_string = match object_type {
                    0x40 => {
                        // Object type 0x40 (MPEG-4 Audio) - most common AAC
                        // Use mp4a.40.2 for AAC-LC (audioObjectType=2 in AudioSpecificConfig)
                        // Note: For Chrome compatibility, preprocess video with FFmpeg:
                        // ffmpeg -i input.mp4 -c:v copy -c:a aac -profile:a aac_low -movflags frag_keyframe+empty_moov output.mp4
                        info!(
                            "Object type 0x40 (MPEG-4 Audio) - using mp4a.40.2 for AAC-LC"
                        );
                        "mp4a.40.2".to_string() // Full codec string with audioObjectType
                    }
                    0x02 => {
                        info!("Object type 0x02 (AAC-LC) detected - using mp4a.40.2");
                        "mp4a.40.2".to_string()
                    }
                    0x05 => "mp4a.40.5".to_string(),   // HE-AAC
                    0x1d => "mp4a.40.29".to_string(),  // HE-AAC v2
                    _ => {
                        warn!(
                            "Unknown AAC object type 0x{:02x}, defaulting to mp4a.40.2",
                            object_type
                        );
                        "mp4a.40.2".to_string()
                    }
                };

                (
                    "AAC".to_string(),
                    format!("audio/mp4; codecs=\"{}\"", codec_string),
                    Some(format!("{:02x}", object_type)),
                )
            }
            ("soun", format) if format.starts_with("mp4a") => {
                // Generic AAC
                (
                    "AAC".to_string(),
                    "audio/mp4; codecs=\"mp4a.40.2\"".to_string(),
                    Some("2".to_string()),
                )
            }
            ("soun", "ac-3") => {
                // AC-3/Dolby Digital audio
                info!("Detected AC-3 audio codec - using audio/mp4; codecs=\"ac-3\"");
                (
                    "AC-3".to_string(),
                    "audio/mp4; codecs=\"ac-3\"".to_string(),
                    None,
                )
            }
            ("vide", _) => {
                // Generic video
                (
                    "H.264".to_string(),
                    "video/mp4; codecs=\"avc1.42E01E\"".to_string(),
                    None,
                )
            }
            ("soun", _) => {
                // Unknown audio - assume AAC for compatibility
                warn!("Unknown audio format '{}', defaulting to AAC", format);
                (
                    "AAC".to_string(),
                    "audio/mp4; codecs=\"mp4a.40.2\"".to_string(),
                    None,
                )
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Unsupported format: {} for media type {}",
                        format, media_type
                    ),
                ));
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

            if cursor.read_exact(&mut size_bytes).is_err()
                || cursor.read_exact(&mut type_bytes).is_err()
            {
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
        info!(
            "Looking for ESDS box in {} bytes of sample description data",
            data.len()
        );

        // Look for esds box in the sample description
        let mut cursor = std::io::Cursor::new(data);

        while cursor.position() + 8 <= data.len() as u64 {
            let mut size_bytes = [0u8; 4];
            let mut type_bytes = [0u8; 4];

            if cursor.read_exact(&mut size_bytes).is_err()
                || cursor.read_exact(&mut type_bytes).is_err()
            {
                break;
            }

            let box_size = u32::from_be_bytes(size_bytes) as u64;
            let box_type = String::from_utf8_lossy(&type_bytes).to_string();

            info!(
                "Found box in sample description: type='{}', size={}",
                box_type, box_size
            );

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
        let debug_bytes: Vec<String> = esds_data[0..std::cmp::min(20, esds_data.len())]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        info!("ESDS first bytes: {}", debug_bytes.join(" "));

        // Skip version/flags (4 bytes)
        let mut offset = 4;

        // Look for the decoder config descriptor (tag 0x04)
        while offset + 5 < esds_data.len() {
            let tag = esds_data[offset];
            info!("Checking tag at offset {}: 0x{:02x}", offset, tag);

            if tag == 0x04 {
                // DecoderConfigDescriptor tag
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
                    info!(
                        "Found AAC object type in ESDS at offset {}: 0x{:02x}",
                        offset, object_type
                    );

                    // Continue parsing to find the AudioSpecificConfig
                    self.analyze_audio_specific_config(
                        esds_data,
                        decoder_config_start,
                        object_type,
                    );

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
    fn analyze_audio_specific_config(
        &self,
        esds_data: &[u8],
        decoder_config_start: usize,
        object_type: u8,
    ) {
        info!(
            "Analyzing AudioSpecificConfig for object type 0x{:02x}",
            object_type
        );

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
            info!(
                "Looking for AudioSpecificConfig, found tag 0x{:02x} at offset {}",
                tag, offset
            );

            if tag == 0x05 {
                // DecSpecificInfoDescriptor tag
                info!(
                    "Found DecSpecificInfoDescriptor (AudioSpecificConfig) at offset {}",
                    offset
                );
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

        info!(
            "Parsing AudioSpecificConfig ({} bytes) for object type 0x{:02x}",
            asc_data.len(),
            object_type
        );

        // Log the raw AudioSpecificConfig bytes
        let asc_hex: Vec<String> = asc_data.iter().map(|b| format!("{:02x}", b)).collect();
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
                info!(
                    "  AAC Object Type: {} ({})",
                    aac_object_type,
                    self.get_aac_object_type_name(aac_object_type)
                );
                info!("  Sampling Frequency Index: {}", sampling_freq_index);
                info!("  Channel Configuration: {}", channel_config);

                // Check if this matches expected values for mp4a.40.02
                if object_type == 0x40 {
                    match aac_object_type {
                        2 => info!("✓ AAC Object Type 2 (AAC-LC) matches mp4a.40.02 expectation"),
                        _ => warn!("⚠ AAC Object Type {} does not match AAC-LC (2) expected for mp4a.40.02", aac_object_type),
                    }
                } else {
                    info!(
                        "Object type 0x{:02x} with AAC Object Type {}",
                        object_type, aac_object_type
                    );
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ftyp box too small",
            ));
        }

        let major_brand = String::from_utf8_lossy(&data[0..4]).to_string();
        let minor_version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);

        let mut compatible_brands = Vec::new();
        for chunk in data[8..].chunks(4) {
            if chunk.len() == 4 {
                compatible_brands.push(String::from_utf8_lossy(chunk).to_string());
            }
        }

        debug!(
            "Parsed ftyp: brand={}, version={}, compatible={:?}",
            major_brand, minor_version, compatible_brands
        );

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
    pub fn generate_mse_segments(&mut self) -> Result<Vec<MseSegment>, io::Error> {
        info!("Generating MSE segments from MP4 data");

        if self.is_fragmented {
            // File is already fragmented, we can use segments directly
            return self.extract_fragmented_segments();
        } else {
            // File is not fragmented, we need to create segments
            return self.create_fragments_from_regular_mp4();
        }
    }

    /// Extract segments from an already fragmented MP4 file
    fn extract_fragmented_segments(&self) -> Result<Vec<MseSegment>, io::Error> {
        let mut segments = Vec::new();

        // Check for AC-3 audio which needs video-only mode (Chrome MSE doesn't support AC-3)
        let has_ac3_audio = self.tracks.iter().any(|track| {
            track.media_type == "soun" && track.codec == "AC-3"
        });

        // Use video-only mode for Chrome MSE compatibility when AC-3 audio is present
        if has_ac3_audio {
            warn!("AC-3 audio detected - using video-only mode for Chrome MSE compatibility");
            warn!("(Chrome MSE does not support AC-3/Dolby Digital audio codec)");
        }

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
                        let serialized_size = u32::from_be_bytes([
                            box_data[0],
                            box_data[1],
                            box_data[2],
                            box_data[3],
                        ]);
                        let serialized_type = String::from_utf8_lossy(&box_data[4..8]);

                        if serialized_size as usize != box_data.len() {
                            error!(
                                "Serialized box size mismatch for {}: header says {}, actual {}",
                                box_info.header.box_type,
                                serialized_size,
                                box_data.len()
                            );
                            continue; // Skip this box
                        }

                        if serialized_type != box_info.header.box_type {
                            error!("Serialized box type mismatch for {}: header says '{}', actual '{}'", 
                                   box_info.header.box_type, box_info.header.box_type, serialized_type);
                            continue; // Skip this box
                        }

                        debug!(
                            "Validated {} box: size={}, type='{}', data_len={}",
                            box_info.header.box_type,
                            serialized_size,
                            serialized_type,
                            box_data.len()
                        );
                    } else {
                        error!(
                            "Serialized box too small for {}: {} bytes",
                            box_info.header.box_type,
                            box_data.len()
                        );
                        continue; // Skip this box
                    }

                    included_boxes.push(format!(
                        "{}({} bytes)",
                        box_info.header.box_type,
                        box_data.len()
                    ));
                    init_data.extend_from_slice(&box_data);
                }
                "moov" => {
                    // Check if we need video-only mode for AC-3 audio
                    if has_ac3_audio {
                        info!("Creating video-only moov box due to AC-3 audio incompatibility");

                        // Create video-only moov box by removing audio tracks
                        if let BoxContent::Raw(content) = &box_info.content {
                            let video_only_content = self.create_video_only_moov(content);

                            // Reconstruct the box with video-only content
                            let mut video_only_box = Vec::new();
                            let total_size = 8 + video_only_content.len() as u32;
                            video_only_box.extend_from_slice(&total_size.to_be_bytes());
                            video_only_box.extend_from_slice(b"moov");
                            video_only_box.extend_from_slice(&video_only_content);

                            info!(
                                "Created video-only moov box: {} bytes (original: {} bytes)",
                                video_only_box.len(),
                                self.serialize_box(box_info).len()
                            );

                            included_boxes
                                .push(format!("moov-video-only({} bytes)", video_only_box.len()));
                            init_data.extend_from_slice(&video_only_box);
                            continue; // Skip the regular moov processing
                        }
                    }

                    // Regular moov processing (with ESDS modification if needed)
                    let original_box_data = self.serialize_box(box_info);

                    // Log track info for debugging
                    info!("Checking tracks:");
                    for track in &self.tracks {
                        info!(
                            "Track {}: media_type='{}', codec_params={:?}",
                            track.track_id, track.media_type, track.codec_params
                        );
                    }

                    // Use original moov box data unchanged
                    // (ESDS modification was removed - it corrupted MP4 structure)
                    let box_data = original_box_data;

                    // Validate the serialized box
                    if box_data.len() >= 8 {
                        let serialized_size = u32::from_be_bytes([
                            box_data[0],
                            box_data[1],
                            box_data[2],
                            box_data[3],
                        ]);
                        let serialized_type = String::from_utf8_lossy(&box_data[4..8]);

                        if serialized_size as usize != box_data.len() {
                            error!(
                                "Serialized box size mismatch for {}: header says {}, actual {}",
                                box_info.header.box_type,
                                serialized_size,
                                box_data.len()
                            );
                            continue; // Skip this box
                        }

                        if serialized_type != box_info.header.box_type {
                            error!("Serialized box type mismatch for {}: header says '{}', actual '{}'", 
                                   box_info.header.box_type, box_info.header.box_type, serialized_type);
                            continue; // Skip this box
                        }

                        debug!(
                            "Validated {} box: size={}, type='{}', data_len={}",
                            box_info.header.box_type,
                            serialized_size,
                            serialized_type,
                            box_data.len()
                        );
                    } else {
                        error!(
                            "Serialized box too small for {}: {} bytes",
                            box_info.header.box_type,
                            box_data.len()
                        );
                        continue; // Skip this box
                    }

                    included_boxes.push(format!(
                        "{}({} bytes)",
                        box_info.header.box_type,
                        box_data.len()
                    ));
                    init_data.extend_from_slice(&box_data);
                }
                _ => {}
            }
        }

        if !init_data.is_empty() {
            info!(
                "Creating initialization segment with boxes: {}",
                included_boxes.join(" + ")
            );

            // Log the first few bytes of the init segment for debugging
            if init_data.len() >= 16 {
                let first_bytes: Vec<String> = init_data[0..16]
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect();
                info!("Init segment first 16 bytes: {}", first_bytes.join(" "));

                // Validate first box
                if init_data.len() >= 8 {
                    let box_size = u32::from_be_bytes([
                        init_data[0],
                        init_data[1],
                        init_data[2],
                        init_data[3],
                    ]);
                    let box_type = String::from_utf8_lossy(&init_data[4..8]);
                    info!(
                        "Init segment first box: type='{}', size={}",
                        box_type, box_size
                    );

                    // Check if the box size matches expectations
                    if box_size == 0 {
                        error!("Invalid box size 0 in initialization segment!");
                    } else if box_size as usize > init_data.len() {
                        error!(
                            "Box size {} exceeds init segment length {}",
                            box_size,
                            init_data.len()
                        );
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

                let box_type = String::from_utf8_lossy(
                    &init_data[validation_offset + 4..validation_offset + 8],
                );

                debug!(
                    "Init segment box {}: type='{}', size={} at offset {}",
                    box_count, box_type, box_size, validation_offset
                );

                if box_size < 8 {
                    error!(
                        "Invalid box size {} for {} at offset {}",
                        box_size, box_type, validation_offset
                    );
                    break;
                }

                if validation_offset + box_size > init_data.len() {
                    error!(
                        "Box {} extends beyond segment: offset={}, size={}, segment_len={}",
                        box_type,
                        validation_offset,
                        box_size,
                        init_data.len()
                    );
                    break;
                }

                validation_offset += box_size;
                box_count += 1;
            }

            if validation_offset != init_data.len() {
                error!(
                    "Initialization segment validation failed: processed {} bytes, total {}",
                    validation_offset,
                    init_data.len()
                );
            } else {
                info!(
                    "Initialization segment validation passed: {} boxes, {} bytes",
                    box_count,
                    init_data.len()
                );
            }

            segments.push(MseSegment {
                segment_type: "initialization".to_string(),
                data: init_data,
                timestamp: None,
                duration: None,
                is_keyframe: true,
            });
            info!(
                "Created initialization segment ({} bytes)",
                segments[0].data.len()
            );
        }

        // Create media segments from moof + mdat pairs
        let mut i = 0;
        while i < self.boxes.len() {
            if self.boxes[i].header.box_type == "moof"
                && i + 1 < self.boxes.len()
                && self.boxes[i + 1].header.box_type == "mdat"
            {
                let mut media_data = Vec::new();

                // Fix TFHD flags in moof box for MSE compatibility
                let fixed_moof_data = self.fix_moof_tfhd_flags(&self.boxes[i])?;
                media_data.extend_from_slice(&fixed_moof_data); // moof with MSE-compatible TFHD
                media_data.extend_from_slice(&self.serialize_box(&self.boxes[i + 1])); // mdat

                let media_data_len = media_data.len();

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
                });

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
    ///
    /// This creates proper MSE-compatible fragmented MP4 with:
    /// 1. ftyp box (file type)
    /// 2. Modified moov box with mvex (movie extends) for MSE compatibility
    /// 3. Proper moof + mdat fragment pairs
    fn create_fragments_from_regular_mp4(&mut self) -> Result<Vec<MseSegment>, io::Error> {
        info!("Converting regular MP4 to MSE-compatible fragmented format");

        // Check if we have AC-3 audio that needs video-only mode
        let has_ac3_audio = self.tracks.iter().any(|track| {
            if track.media_type == "soun" && track.codec_params.as_deref() == Some("AC-3") {
                error!(
                    "🚨 AC-3 AUDIO DETECTED in track {} - Chrome MSE does not support AC-3!",
                    track.track_id
                );
                error!("🎬 ACTIVATING VIDEO-ONLY MODE for Chrome compatibility");
                true
            } else {
                false
            }
        });

        if has_ac3_audio {
            error!("⚠️ AC-3 AUDIO INCOMPATIBLE WITH CHROME MSE - Creating video-only initialization segment");
            error!("🎬 VIDEO-ONLY MODE ACTIVE - Audio tracks will be removed from moov box");
        }

        let mut segments = Vec::new();

        // Create MSE-compatible initialization segment with ftyp + modified moov
        let mut init_data = Vec::new();
        let mut mdat_data = Vec::new();
        let mut original_moov: Option<&Mp4Box> = None;

        for box_info in &self.boxes {
            match box_info.header.box_type.as_str() {
                "ftyp" => {
                    // Copy ftyp box as-is
                    init_data.extend_from_slice(&self.serialize_box(box_info));
                }
                "moov" => {
                    // Store reference to original moov for MSE conversion
                    original_moov = Some(box_info);
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

        // Create MSE-compatible moov box with mvex structure
        if let Some(moov_box) = original_moov {
            let mse_moov = self.create_mse_compatible_moov(moov_box)?;
            init_data.extend_from_slice(&mse_moov);
            info!("Created MSE-compatible moov box with mvex structure");
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No moov box found in MP4",
            ));
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
            info!(
                "Created initialization segment ({} bytes)",
                segments[0].data.len()
            );
        }

        // Create proper fragmented segments from mdat content
        if !mdat_data.is_empty() {
            // Get video track timescale for proper timestamp calculation
            let video_track = self.tracks.iter().find(|t| t.media_type == "vide");
            let timescale = video_track.map(|t| t.timescale).unwrap_or(1000);

            // Split mdat into smaller chunks for streaming
            let chunk_size = 64 * 1024; // 64KB chunks
            let num_chunks = (mdat_data.len() + chunk_size - 1) / chunk_size;

            info!(
                "Creating {} segments from mdat content using timescale={}",
                num_chunks, timescale
            );

            for (chunk_index, chunk) in mdat_data.chunks(chunk_size).enumerate() {
                // Calculate timestamp in timescale units (1 second per chunk in track time units)
                let timestamp_in_track_units = (chunk_index as u64) * (timescale as u64);
                // Convert to milliseconds for MSE
                let timestamp_ms = (timestamp_in_track_units * 1000) / (timescale as u64);
                let timestamp = Some(timestamp_ms);

                let is_keyframe = chunk_index == 0 || self.is_likely_keyframe(chunk, chunk_index);

                info!(
                    "Segment {}: timestamp_track_units={}, timestamp_ms={}, keyframe={}",
                    chunk_index, timestamp_in_track_units, timestamp_ms, is_keyframe
                );

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
        if segment_index % 5 == 0 {
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

    /// Create a proper fMP4 media segment with timing information
    fn create_simple_media_segment_with_timing(
        &mut self,
        mdat_content: &[u8],
        timestamp: Option<u64>,
        is_keyframe: bool,
    ) -> Result<MseSegment, io::Error> {
        // Get the actual video track timescale
        let video_track = self.tracks.iter().find(|t| t.media_type == "vide");
        let timescale = video_track.map(|t| t.timescale).unwrap_or(1000);
        let track_id = video_track.map(|t| t.track_id).unwrap_or(1);

        // Calculate proper duration based on timescale
        // For MSE, we want reasonable durations - use 1 second worth of timescale units
        let duration = timescale; // 1 second duration in track units
        let sample = Sample::new(mdat_content.to_vec(), duration, is_keyframe);

        info!("Creating fMP4 segment: track_id={}, timescale={}, duration={}, size={} bytes, keyframe={}", 
              track_id, timescale, duration, mdat_content.len(), is_keyframe);

        // Use the proper fMP4 converter to create the segment
        let fragment_data = self
            .fmp4_converter
            .create_fragment(&[sample], track_id, timescale)?;

        info!(
            "Created proper fMP4 segment: {} bytes (was minimal moof: {} bytes)",
            fragment_data.len(),
            mdat_content.len() + 100
        ); // Rough estimate of old size

        Ok(MseSegment {
            segment_type: "media".to_string(),
            data: fragment_data,
            timestamp,
            duration: Some(duration as u64),
            is_keyframe,
        })
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

        // tfhd (track fragment header) with MSE-compatible flags
        let tfhd_content = vec![
            0, // version
            0x02, 0x00,
            0x38, // flags = 0x020038 (MSE-compatible: default-base-is-moof + other required flags)
            0, 0, 0, 1, // track_id = 1
        ];
        let tfhd_size = 8 + tfhd_content.len() as u32;
        traf_content.extend_from_slice(&tfhd_size.to_be_bytes());
        traf_content.extend_from_slice(b"tfhd");
        traf_content.extend_from_slice(&tfhd_content);

        // trun (track run) - simplified
        let trun_content = vec![
            0,
            0,
            0,
            0, // version + flags
            0,
            0,
            0,
            1, // sample_count = 1
            data_size.to_be_bytes()[0],
            data_size.to_be_bytes()[1],
            data_size.to_be_bytes()[2],
            data_size.to_be_bytes()[3], // data_offset
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

    /// Create MSE-compatible moov box with mvex structure
    ///
    /// Takes the original moov box and creates a new one with:
    /// - All original track information
    /// - Added mvex (Movie Extends) box with trex for each track
    /// - Proper MSE fragmentation headers
    fn create_mse_compatible_moov(&self, original_moov: &Mp4Box) -> Result<Vec<u8>, io::Error> {
        info!("Creating MSE-compatible moov box with mvex structure");

        // Get the original moov content
        let original_content = match &original_moov.content {
            BoxContent::Raw(data) => data,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid moov box content",
                ))
            }
        };

        // Check if we have AC-3 audio that needs video-only mode
        let has_ac3_audio = self.tracks.iter().any(|track| {
            track.media_type == "soun" && track.codec_params.as_deref() == Some("AC-3")
        });

        let modified_content = if has_ac3_audio {
            error!("🚨 AC-3 AUDIO IN MOOV - Creating video-only moov box!");
            error!("🎬 REMOVING AUDIO TRACKS from moov box for Chrome MSE compatibility");
            self.create_video_only_moov(original_content)
        } else {
            original_content.clone()
        };

        // Create new moov content with mvex appended
        let mut new_moov_content = modified_content;

        // Create and append mvex box
        let mvex_box = self.create_mvex_box()?;
        new_moov_content.extend_from_slice(&mvex_box);

        // Create complete moov box with updated size
        let moov_size = 8 + new_moov_content.len() as u32;
        let mut moov_data = Vec::new();
        moov_data.extend_from_slice(&moov_size.to_be_bytes());
        moov_data.extend_from_slice(b"moov");
        moov_data.extend_from_slice(&new_moov_content);

        info!(
            "Created MSE-compatible moov box: {} bytes (original: {} bytes)",
            moov_data.len(),
            original_moov.header.size
        );

        Ok(moov_data)
    }

    /// Create mvex (Movie Extends) box required for MSE
    ///
    /// The mvex box contains:
    /// - mehd (Movie Extends Header) - optional
    /// - trex (Track Extends) boxes for each track
    fn create_mvex_box(&self) -> Result<Vec<u8>, io::Error> {
        info!(
            "Creating mvex box with trex entries for {} tracks",
            self.tracks.len()
        );

        let mut mvex_content = Vec::new();

        // Create trex (Track Extends) box for each track
        for track in &self.tracks {
            let trex_box = self.create_trex_box(track.track_id)?;
            mvex_content.extend_from_slice(&trex_box);
            debug!("Added trex box for track {}", track.track_id);
        }

        // Create complete mvex box
        let mvex_size = 8 + mvex_content.len() as u32;
        let mut mvex_data = Vec::new();
        mvex_data.extend_from_slice(&mvex_size.to_be_bytes());
        mvex_data.extend_from_slice(b"mvex");
        mvex_data.extend_from_slice(&mvex_content);

        info!(
            "Created mvex box: {} bytes with {} trex entries",
            mvex_data.len(),
            self.tracks.len()
        );
        Ok(mvex_data)
    }

    /// Create trex (Track Extends) box for a specific track
    ///
    /// The trex box defines default values for track fragments:
    /// - track_id: Track identifier
    /// - default_sample_description_index: Usually 1
    /// - default_sample_duration: Default duration per sample
    /// - default_sample_size: Default size per sample (0 = variable)
    /// - default_sample_flags: Default sample flags
    fn create_trex_box(&self, track_id: u32) -> Result<Vec<u8>, io::Error> {
        // trex box content:
        // version (1) + flags (3) + track_id (4) + default_sample_description_index (4) +
        // default_sample_duration (4) + default_sample_size (4) + default_sample_flags (4)
        let trex_content = vec![
            0,
            0,
            0,
            0, // version + flags
            // track_id (4 bytes, big-endian)
            (track_id >> 24) as u8,
            (track_id >> 16) as u8,
            (track_id >> 8) as u8,
            track_id as u8,
            0,
            0,
            0,
            1, // default_sample_description_index = 1
            0,
            0,
            0,
            0, // default_sample_duration = 0 (variable)
            0,
            0,
            0,
            0, // default_sample_size = 0 (variable)
            0,
            0,
            0,
            0, // default_sample_flags = 0
        ];

        // Create complete trex box
        let trex_size = 8 + trex_content.len() as u32;
        let mut trex_data = Vec::new();
        trex_data.extend_from_slice(&trex_size.to_be_bytes());
        trex_data.extend_from_slice(b"trex");
        trex_data.extend_from_slice(&trex_content);

        debug!(
            "Created trex box for track {}: {} bytes",
            track_id,
            trex_data.len()
        );
        Ok(trex_data)
    }

    /// Fix TFHD flags in a moof box for MSE compatibility
    ///
    /// Chrome's MSE implementation requires TFHD boxes to use relative addressing
    /// (default-base-is-moof flag) instead of absolute addressing (base-data-offset-present flag).
    ///
    /// This function modifies TFHD flags from the problematic patterns to MSE-compatible ones:
    /// - Removes base-data-offset-present flag (0x000001)
    /// - Sets default-base-is-moof flag (0x020000)
    /// - Preserves other flags as needed
    fn fix_moof_tfhd_flags(&self, moof_box: &Mp4Box) -> Result<Vec<u8>, io::Error> {
        info!("Fixing TFHD flags in moof box for Chrome MSE compatibility");

        // Get the raw moof data
        let original_data = match &moof_box.content {
            BoxContent::Raw(data) => data.clone(),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid moof box content",
                ))
            }
        };

        // Create a modified copy
        let mut fixed_data = original_data.clone();
        let mut modifications_made = 0;

        // Search for TFHD boxes within the moof and fix their flags
        self.fix_tfhd_flags_recursive(&mut fixed_data, 0, &mut modifications_made);

        if modifications_made > 0 {
            info!(
                "Successfully fixed {} TFHD boxes for MSE compatibility",
                modifications_made
            );
        } else {
            info!("No TFHD flag fixes needed - already MSE compatible");
        }

        // Reconstruct the moof box with fixed content
        let moof_size = 8 + fixed_data.len() as u32;
        let mut moof_data = Vec::new();
        moof_data.extend_from_slice(&moof_size.to_be_bytes());
        moof_data.extend_from_slice(b"moof");
        moof_data.extend_from_slice(&fixed_data);

        Ok(moof_data)
    }

    /// Recursively search and fix TFHD flags in MP4 box hierarchy
    fn fix_tfhd_flags_recursive(
        &self,
        data: &mut [u8],
        start_offset: usize,
        modifications_made: &mut usize,
    ) {
        let mut cursor = start_offset;

        debug!(
            "Starting TFHD flags search at offset {}, data length: {}",
            start_offset,
            data.len()
        );

        while cursor + 8 <= data.len() {
            // Read box header
            let box_size = u32::from_be_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
            ]) as usize;

            if box_size < 8 || cursor + box_size > data.len() {
                debug!(
                    "Invalid box size {} at offset {}, stopping search",
                    box_size, cursor
                );
                break;
            }

            let box_type = String::from_utf8_lossy(&data[cursor + 4..cursor + 8]);
            debug!(
                "TFHD Search: Processing box '{}' at offset {}, size {}",
                box_type, cursor, box_size
            );

            match box_type.as_ref() {
                "tfhd" => {
                    // Found TFHD box - fix its flags
                    info!(
                        "FOUND TFHD BOX at offset {} with size {}!",
                        cursor, box_size
                    );
                    if self.fix_single_tfhd_flags(data, cursor, box_size) {
                        *modifications_made += 1;
                        info!(
                            "Successfully fixed TFHD flags at offset {} (size: {})",
                            cursor, box_size
                        );
                    } else {
                        debug!("TFHD flags at offset {} already MSE compatible", cursor);
                    }
                }
                "traf" | "mfhd" => {
                    // These boxes can contain child boxes, recurse into them
                    debug!(
                        "TFHD Search: Recursing into {} box at offset {}",
                        box_type, cursor
                    );
                    self.fix_tfhd_flags_recursive(data, cursor + 8, modifications_made);
                }
                _ => {
                    // Other boxes - skip content
                    debug!(
                        "TFHD Search: Skipping {} box at offset {}",
                        box_type, cursor
                    );
                }
            }

            cursor += box_size;
        }

        debug!(
            "Finished TFHD flags search starting at offset {}",
            start_offset
        );
    }

    /// Fix flags in a single TFHD box for MSE compatibility
    ///
    /// Chrome requires TFHD to use relative addressing. This function:
    /// 1. Removes base-data-offset-present flag (0x000001) if present
    /// 2. Sets default-base-is-moof flag (0x020000) for relative addressing
    /// 3. Preserves other useful flags like sample-duration-present, etc.
    fn fix_single_tfhd_flags(&self, data: &mut [u8], box_offset: usize, box_size: usize) -> bool {
        // TFHD box structure:
        // [box_size:4][box_type:4][version:1][flags:3][track_id:4][optional_fields...]

        if box_size < 16 {
            warn!(
                "TFHD box too small at offset {}: {} bytes",
                box_offset, box_size
            );
            return false;
        }

        let flags_offset = box_offset + 8 + 1; // Skip box header + version

        if flags_offset + 3 > data.len() {
            warn!("TFHD flags extend beyond data at offset {}", box_offset);
            return false;
        }

        // Read current flags
        let current_flags = [
            data[flags_offset],
            data[flags_offset + 1],
            data[flags_offset + 2],
        ];
        let flags_u32 =
            u32::from_be_bytes([0, current_flags[0], current_flags[1], current_flags[2]]);

        debug!(
            "Current TFHD flags at offset {}: {:02x} {:02x} {:02x} (0x{:06x})",
            box_offset, current_flags[0], current_flags[1], current_flags[2], flags_u32
        );

        // Check for problematic flags
        let has_base_data_offset = (flags_u32 & 0x000001) != 0;
        let has_default_base_is_moof = (flags_u32 & 0x020000) != 0;

        if has_base_data_offset {
            warn!("TFHD at offset {} has base-data-offset-present flag - this causes Chrome MSE errors!", box_offset);
        }

        if has_default_base_is_moof && !has_base_data_offset {
            debug!(
                "TFHD at offset {} already has correct MSE flags",
                box_offset
            );
            return false; // No modification needed
        }

        // Create MSE-compatible flags:
        // 1. Remove base-data-offset-present (0x000001)
        // 2. Add default-base-is-moof (0x020000)
        // 3. Preserve other flags
        let mut new_flags = flags_u32;
        new_flags &= !0x000001; // Remove base-data-offset-present
        new_flags |= 0x020000; // Add default-base-is-moof

        // Write the new flags
        let new_flags_bytes = new_flags.to_be_bytes();
        data[flags_offset] = new_flags_bytes[1]; // Skip first byte (always 0)
        data[flags_offset + 1] = new_flags_bytes[2];
        data[flags_offset + 2] = new_flags_bytes[3];

        info!(
            "Fixed TFHD flags at offset {}: 0x{:06x} -> 0x{:06x}",
            box_offset, flags_u32, new_flags
        );
        info!("  Removed: base-data-offset-present (absolute addressing)");
        info!("  Added: default-base-is-moof (relative addressing for MSE)");

        true
    }

    /// Create a video-only moov box by removing audio tracks
    fn create_video_only_moov(&self, moov_content: &[u8]) -> Vec<u8> {
        error!("🎬 CREATING VIDEO-ONLY MOOV BOX - Removing all audio tracks");
        info!("Original moov box size: {} bytes", moov_content.len());

        let mut output = Vec::new();
        let mut cursor = 0;

        // Parse child boxes within moov
        while cursor + 8 <= moov_content.len() {
            let box_size = u32::from_be_bytes([
                moov_content[cursor],
                moov_content[cursor + 1],
                moov_content[cursor + 2],
                moov_content[cursor + 3],
            ]) as usize;

            if box_size < 8 || cursor + box_size > moov_content.len() {
                warn!("Invalid box size {} at offset {}", box_size, cursor);
                break;
            }

            let box_type = String::from_utf8_lossy(&moov_content[cursor + 4..cursor + 8]);
            debug!(
                "Processing moov child box '{}' at offset {}, size {}",
                box_type, cursor, box_size
            );

            match box_type.as_ref() {
                "trak" => {
                    // Check if this is a video track by parsing the trak box
                    if self.is_video_track(&moov_content[cursor..cursor + box_size]) {
                        info!("Including video track in video-only moov");
                        output.extend_from_slice(&moov_content[cursor..cursor + box_size]);
                    } else {
                        info!("Skipping audio track in video-only moov");
                    }
                }
                _ => {
                    // Include all other boxes (mvhd, udta, etc.)
                    debug!("Including '{}' box in video-only moov", box_type);
                    output.extend_from_slice(&moov_content[cursor..cursor + box_size]);
                }
            }

            cursor += box_size;
        }

        info!(
            "Video-only moov created: {} bytes (reduced from {} bytes)",
            output.len(),
            moov_content.len()
        );

        output
    }

    /// Check if a trak box contains a video track
    fn is_video_track(&self, trak_data: &[u8]) -> bool {
        // Parse trak box to find mdia/hdlr box
        let mut cursor = 8; // Skip trak box header

        while cursor + 8 <= trak_data.len() {
            let box_size = u32::from_be_bytes([
                trak_data[cursor],
                trak_data[cursor + 1],
                trak_data[cursor + 2],
                trak_data[cursor + 3],
            ]) as usize;

            if box_size < 8 || cursor + box_size > trak_data.len() {
                break;
            }

            let box_type = String::from_utf8_lossy(&trak_data[cursor + 4..cursor + 8]);

            if box_type == "mdia" {
                // Found mdia box, look for hdlr inside it
                let mdia_end = cursor + box_size;
                let mut mdia_cursor = cursor + 8; // Skip mdia header

                while mdia_cursor + 8 <= mdia_end {
                    let inner_box_size = u32::from_be_bytes([
                        trak_data[mdia_cursor],
                        trak_data[mdia_cursor + 1],
                        trak_data[mdia_cursor + 2],
                        trak_data[mdia_cursor + 3],
                    ]) as usize;

                    if inner_box_size < 8 || mdia_cursor + inner_box_size > mdia_end {
                        break;
                    }

                    let inner_box_type =
                        String::from_utf8_lossy(&trak_data[mdia_cursor + 4..mdia_cursor + 8]);

                    if inner_box_type == "hdlr" && mdia_cursor + 16 <= mdia_end {
                        // hdlr box structure: size(4) + type(4) + version(1) + flags(3) + component_type(4)
                        let handler_type =
                            String::from_utf8_lossy(&trak_data[mdia_cursor + 12..mdia_cursor + 16]);
                        debug!("Found handler type: '{}'", handler_type);

                        // Return true if this is a video handler
                        return handler_type == "vide";
                    }

                    mdia_cursor += inner_box_size;
                }
            }

            cursor += box_size;
        }

        // Default to false if we couldn't determine
        false
    }

    /// Serialize an MP4 box back to binary format
    fn serialize_box(&self, box_info: &Mp4Box) -> Vec<u8> {
        let mut data = Vec::new();

        // VALIDATION: Check that box_info has valid data before serialization
        if box_info.header.box_type.len() != 4 {
            error!(
                "Invalid box type length for {}: {} bytes",
                box_info.header.box_type,
                box_info.header.box_type.len()
            );
            return Vec::new();
        }

        // Get content first to calculate correct size
        let content_data = match &box_info.content {
            BoxContent::Raw(content) => content.clone(),
            BoxContent::FileType {
                major_brand,
                minor_version,
                compatible_brands,
            } => {
                let mut ftyp_content = Vec::new();
                ftyp_content.extend_from_slice(major_brand.as_bytes());
                ftyp_content.extend_from_slice(&minor_version.to_be_bytes());
                for brand in compatible_brands {
                    ftyp_content.extend_from_slice(brand.as_bytes());
                }
                ftyp_content
            }
            _ => {
                warn!(
                    "Cannot serialize parsed box content for {}",
                    box_info.header.box_type
                );
                Vec::new()
            }
        };

        // VALIDATION: Ensure content data is reasonable
        if content_data.len() > 100_000_000 {
            // 100MB limit
            error!(
                "Content data too large for {}: {} bytes",
                box_info.header.box_type,
                content_data.len()
            );
            return Vec::new();
        }

        // Calculate correct box size (header + content)
        let total_size = 8 + content_data.len() as u32;

        // VALIDATION: Ensure total size is reasonable
        if total_size > 100_000_000 {
            // 100MB limit
            error!(
                "Total box size too large for {}: {} bytes",
                box_info.header.box_type, total_size
            );
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
            debug!(
                "Serialized {} box: {} bytes total, header size: {}, content size: {}",
                box_type,
                data.len(),
                parsed_size,
                content_data.len()
            );

            if parsed_size != data.len() as u32 {
                error!(
                    "Box size mismatch! Header says {}, actual size {}",
                    parsed_size,
                    data.len()
                );
                // Additional debugging for size mismatches
                error!(
                    "Box info original size: {}, content size: {}",
                    box_info.header.size,
                    content_data.len()
                );
                error!("Box type in header: '{}'", box_info.header.box_type);
                return Vec::new(); // Don't return corrupted data
            }

            // Validate that the box type in the serialized data matches expectation
            if box_type != box_info.header.box_type {
                error!(
                    "Box type mismatch! Expected '{}', got '{}'",
                    box_info.header.box_type, box_type
                );
                return Vec::new(); // Don't return corrupted data
            }

            // Log first few bytes
            let first_bytes: Vec<String> = data[0..std::cmp::min(8, data.len())]
                .iter()
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
            self.boxes
                .iter()
                .map(|b| b.header.box_type.clone())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    /// Get video track codec information
    pub fn get_video_codec_info(&self) -> Option<(&str, &str)> {
        self.tracks
            .iter()
            .find(|track| track.media_type == "vide")
            .map(|track| (track.codec.as_str(), track.codec_mime_type.as_str()))
    }

    /// Get audio track codec information
    pub fn get_audio_codec_info(&self) -> Option<(&str, &str)> {
        self.tracks
            .iter()
            .find(|track| track.media_type == "soun")
            .map(|track| (track.codec.as_str(), track.codec_mime_type.as_str()))
    }

    /// Generate separate video-only and audio-only initialization segments
    ///
    /// Chrome MSE requires separate init segments for separate SourceBuffers.
    /// This function creates:
    /// - Video-only init segment: ftyp + moov with only video track
    /// - Audio-only init segment: ftyp + moov with only audio track
    ///
    /// Returns `(video_init, audio_init)` where audio_init is None if no audio track exists.
    pub fn generate_separate_init_segments(&self) -> Result<(Vec<u8>, Option<Vec<u8>>), io::Error> {
        info!("Generating separate video and audio initialization segments for Chrome MSE");

        let mut ftyp_data: Option<Vec<u8>> = None;
        let mut moov_data: Option<Vec<u8>> = None;

        // Find ftyp and moov boxes
        for box_info in &self.boxes {
            match box_info.header.box_type.as_str() {
                "ftyp" => {
                    ftyp_data = Some(self.serialize_box(box_info));
                }
                "moov" => {
                    if let BoxContent::Raw(content) = &box_info.content {
                        moov_data = Some(content.clone());
                    }
                }
                _ => {}
            }
        }

        let ftyp = ftyp_data.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "No ftyp box found in MP4")
        })?;

        let moov_content = moov_data.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "No moov box found in MP4")
        })?;

        // Check if we have audio and video tracks
        let has_video = self.tracks.iter().any(|t| t.media_type == "vide");
        let has_audio = self.tracks.iter().any(|t| t.media_type == "soun");

        if !has_video {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No video track found in MP4",
            ));
        }

        // Create video-only init segment
        let video_moov_content = self.filter_moov_for_track_type(&moov_content, "vide");
        let video_moov = self.build_moov_box(&video_moov_content);
        let mut video_init = ftyp.clone();
        video_init.extend_from_slice(&video_moov);

        info!(
            "Created video-only init segment: {} bytes (ftyp: {}, moov: {})",
            video_init.len(),
            ftyp.len(),
            video_moov.len()
        );

        // Create audio-only init segment if audio track exists
        let audio_init = if has_audio {
            let audio_moov_content = self.filter_moov_for_track_type(&moov_content, "soun");
            let audio_moov = self.build_moov_box(&audio_moov_content);
            let mut audio_init = ftyp.clone();
            audio_init.extend_from_slice(&audio_moov);

            info!(
                "Created audio-only init segment: {} bytes (ftyp: {}, moov: {})",
                audio_init.len(),
                ftyp.len(),
                audio_moov.len()
            );

            Some(audio_init)
        } else {
            info!("No audio track found - skipping audio init segment");
            None
        };

        Ok((video_init, audio_init))
    }

    /// Filter moov box content to keep only tracks of a specific type
    ///
    /// This function:
    /// 1. Keeps mvhd, iods, udta boxes unchanged
    /// 2. Filters trak boxes to keep only those matching the specified handler type ("vide" or "soun")
    /// 3. Filters mvex/trex entries to match kept tracks
    fn filter_moov_for_track_type(&self, moov_content: &[u8], keep_type: &str) -> Vec<u8> {
        info!(
            "Filtering moov content for track type '{}' (original size: {} bytes)",
            keep_type,
            moov_content.len()
        );

        let mut output = Vec::new();
        let mut cursor = 0;
        let mut kept_track_ids: Vec<u32> = Vec::new();

        // First pass: identify track IDs to keep and copy non-trak/non-mvex boxes
        let mut mvex_data: Option<(usize, usize)> = None; // Store mvex position for second pass

        while cursor + 8 <= moov_content.len() {
            let box_size = u32::from_be_bytes([
                moov_content[cursor],
                moov_content[cursor + 1],
                moov_content[cursor + 2],
                moov_content[cursor + 3],
            ]) as usize;

            if box_size < 8 || cursor + box_size > moov_content.len() {
                warn!(
                    "Invalid box size {} at offset {} in moov",
                    box_size, cursor
                );
                break;
            }

            let box_type = String::from_utf8_lossy(&moov_content[cursor + 4..cursor + 8]);

            match box_type.as_ref() {
                "trak" => {
                    // Check if this track matches the type we want to keep
                    let trak_data = &moov_content[cursor..cursor + box_size];
                    if self.track_has_handler_type(trak_data, keep_type) {
                        // Extract track ID from tkhd
                        if let Some(track_id) = self.extract_track_id_from_trak(trak_data) {
                            kept_track_ids.push(track_id);
                            debug!("Keeping {} track with ID {}", keep_type, track_id);
                        }
                        output.extend_from_slice(trak_data);
                    } else {
                        debug!("Skipping track (not type '{}')", keep_type);
                    }
                }
                "mvex" => {
                    // Store position for second pass - we need to filter trex entries
                    mvex_data = Some((cursor, box_size));
                }
                _ => {
                    // Keep all other boxes (mvhd, iods, udta, etc.)
                    debug!("Keeping '{}' box ({} bytes)", box_type, box_size);
                    output.extend_from_slice(&moov_content[cursor..cursor + box_size]);
                }
            }

            cursor += box_size;
        }

        // Second pass: filter mvex box to only include trex for kept tracks
        if let Some((mvex_start, mvex_size)) = mvex_data {
            let filtered_mvex =
                self.filter_mvex_for_tracks(&moov_content[mvex_start..mvex_start + mvex_size], &kept_track_ids);
            output.extend_from_slice(&filtered_mvex);
        }

        info!(
            "Filtered moov for '{}': {} bytes (from {} bytes), kept {} tracks",
            keep_type,
            output.len(),
            moov_content.len(),
            kept_track_ids.len()
        );

        output
    }

    /// Check if a trak box has a specific handler type in its mdia/hdlr box
    fn track_has_handler_type(&self, trak_data: &[u8], handler_type: &str) -> bool {
        // Parse trak box to find mdia/hdlr box
        let mut cursor = 8; // Skip trak box header

        while cursor + 8 <= trak_data.len() {
            let box_size = u32::from_be_bytes([
                trak_data[cursor],
                trak_data[cursor + 1],
                trak_data[cursor + 2],
                trak_data[cursor + 3],
            ]) as usize;

            if box_size < 8 || cursor + box_size > trak_data.len() {
                break;
            }

            let box_type = String::from_utf8_lossy(&trak_data[cursor + 4..cursor + 8]);

            if box_type == "mdia" {
                // Found mdia box, look for hdlr inside it
                let mdia_end = cursor + box_size;
                let mut mdia_cursor = cursor + 8; // Skip mdia header

                while mdia_cursor + 8 <= mdia_end {
                    let inner_box_size = u32::from_be_bytes([
                        trak_data[mdia_cursor],
                        trak_data[mdia_cursor + 1],
                        trak_data[mdia_cursor + 2],
                        trak_data[mdia_cursor + 3],
                    ]) as usize;

                    if inner_box_size < 8 || mdia_cursor + inner_box_size > mdia_end {
                        break;
                    }

                    let inner_box_type =
                        String::from_utf8_lossy(&trak_data[mdia_cursor + 4..mdia_cursor + 8]);

                    if inner_box_type == "hdlr" && mdia_cursor + 16 <= mdia_end {
                        // hdlr box structure: size(4) + type(4) + version(1) + flags(3) + pre_defined(4) + handler_type(4)
                        // Handler type is at offset 16 from start of hdlr box
                        let found_type =
                            String::from_utf8_lossy(&trak_data[mdia_cursor + 16..mdia_cursor + 20]);
                        return found_type == handler_type;
                    }

                    mdia_cursor += inner_box_size;
                }
            }

            cursor += box_size;
        }

        false
    }

    /// Extract track ID from trak box's tkhd child
    fn extract_track_id_from_trak(&self, trak_data: &[u8]) -> Option<u32> {
        let mut cursor = 8; // Skip trak box header

        while cursor + 8 <= trak_data.len() {
            let box_size = u32::from_be_bytes([
                trak_data[cursor],
                trak_data[cursor + 1],
                trak_data[cursor + 2],
                trak_data[cursor + 3],
            ]) as usize;

            if box_size < 8 || cursor + box_size > trak_data.len() {
                break;
            }

            let box_type = String::from_utf8_lossy(&trak_data[cursor + 4..cursor + 8]);

            if box_type == "tkhd" {
                // tkhd structure: size(4) + type(4) + version(1) + flags(3) + ...
                // For version 0: creation_time(4) + modification_time(4) + track_id(4)
                // Track ID is at offset 20 from start of tkhd box (8 header + 4 ver/flags + 4 + 4)
                if cursor + 24 <= trak_data.len() {
                    let track_id = u32::from_be_bytes([
                        trak_data[cursor + 20],
                        trak_data[cursor + 21],
                        trak_data[cursor + 22],
                        trak_data[cursor + 23],
                    ]);
                    return Some(track_id);
                }
            }

            cursor += box_size;
        }

        None
    }

    /// Filter mvex box to only include trex entries for specified track IDs
    fn filter_mvex_for_tracks(&self, mvex_data: &[u8], kept_track_ids: &[u32]) -> Vec<u8> {
        if mvex_data.len() < 8 {
            return Vec::new();
        }

        let mut mvex_content = Vec::new();
        let mut cursor = 8; // Skip mvex box header

        while cursor + 8 <= mvex_data.len() {
            let box_size = u32::from_be_bytes([
                mvex_data[cursor],
                mvex_data[cursor + 1],
                mvex_data[cursor + 2],
                mvex_data[cursor + 3],
            ]) as usize;

            if box_size < 8 || cursor + box_size > mvex_data.len() {
                break;
            }

            let box_type = String::from_utf8_lossy(&mvex_data[cursor + 4..cursor + 8]);

            if box_type == "trex" {
                // trex structure: size(4) + type(4) + version(1) + flags(3) + track_id(4) + ...
                // Track ID is at offset 12 from start of trex box
                if cursor + 16 <= mvex_data.len() {
                    let track_id = u32::from_be_bytes([
                        mvex_data[cursor + 12],
                        mvex_data[cursor + 13],
                        mvex_data[cursor + 14],
                        mvex_data[cursor + 15],
                    ]);

                    if kept_track_ids.contains(&track_id) {
                        debug!("Keeping trex for track {}", track_id);
                        mvex_content.extend_from_slice(&mvex_data[cursor..cursor + box_size]);
                    } else {
                        debug!("Filtering out trex for track {}", track_id);
                    }
                }
            } else {
                // Keep other mvex child boxes (mehd, etc.)
                debug!("Keeping '{}' box in mvex ({} bytes)", box_type, box_size);
                mvex_content.extend_from_slice(&mvex_data[cursor..cursor + box_size]);
            }

            cursor += box_size;
        }

        // Build complete mvex box with filtered content
        if mvex_content.is_empty() {
            return Vec::new();
        }

        let mvex_size = 8 + mvex_content.len() as u32;
        let mut result = Vec::new();
        result.extend_from_slice(&mvex_size.to_be_bytes());
        result.extend_from_slice(b"mvex");
        result.extend_from_slice(&mvex_content);

        debug!("Filtered mvex: {} bytes", result.len());
        result
    }

    /// Build a complete moov box from content
    fn build_moov_box(&self, content: &[u8]) -> Vec<u8> {
        let moov_size = 8 + content.len() as u32;
        let mut moov_data = Vec::new();
        moov_data.extend_from_slice(&moov_size.to_be_bytes());
        moov_data.extend_from_slice(b"moov");
        moov_data.extend_from_slice(content);
        moov_data
    }

    /// Check if the file has an audio track
    pub fn has_audio_track(&self) -> bool {
        self.tracks.iter().any(|t| t.media_type == "soun")
    }

    /// Check if the file has a video track
    pub fn has_video_track(&self) -> bool {
        self.tracks.iter().any(|t| t.media_type == "vide")
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
            BoxContent::FileType {
                major_brand,
                minor_version,
                compatible_brands,
            } => {
                assert_eq!(major_brand, "mp41");
                assert_eq!(minor_version, 0);
                assert_eq!(compatible_brands, vec!["mp41", "isom"]);
            }
            _ => panic!("Expected FileType content"),
        }
    }
}
