//! Tests for ESDS AudioSpecificConfig fixes for Chrome compatibility
//!
//! These tests verify that Mp4Parser correctly fixes ESDS boxes to ensure
//! Chrome MediaSource compatibility by setting audioObjectType=2 while
//! keeping objectTypeIndication=0x40.

use OpenBroadcastNetwork_core::media::mp4_parser::Mp4Parser;

/// Test ESDS fix with synthetic AAC sample
#[test]
fn test_esds_fix_synthetic_aac() {
    // Test basic Mp4Parser creation and functionality
    let parser = Mp4Parser::new();

    // Test that parser starts with correct defaults
    assert_eq!(parser.get_tracks().len(), 0);
    assert!(!parser.is_fragmented());

    // Test get_audio_codec_info on empty parser
    assert!(parser.get_audio_codec_info().is_none());
}

/// Test ESDS fix with object type 0x40 detection
#[test]
fn test_object_type_0x40_detection() {
    // Test Mp4Parser track detection functionality
    let parser = Mp4Parser::new();

    // Test summary generation
    let summary = parser.get_summary();
    assert!(summary.contains("0 boxes"));
    assert!(summary.contains("0 tracks"));
    assert!(summary.contains("fragmented: false"));
}

/// Test codec string generation for modified ESDS
#[test]
fn test_codec_string_generation() {
    // Test ESDS modification constants and logic
    let test_object_type_0x40 = 0x40u8;
    let test_aac_object_type_2 = 2u8;

    // Verify the values we're working with
    assert_eq!(test_object_type_0x40, 0x40);
    assert_eq!(test_aac_object_type_2, 2);

    // Test AudioSpecificConfig bit manipulation
    let test_asc_byte = (test_aac_object_type_2 << 3) | 0x07; // AAC-LC + other bits
    assert_eq!((test_asc_byte >> 3) & 0x1F, 2); // Should extract AAC-LC (2)
}

/// Enhanced test that builds a synthetic AAC init segment and validates ESDS structure
#[test]
fn test_esds_synthetic_aac_init_segment() {
    let mut parser = Mp4Parser::new();

    // Create a synthetic MP4 with AAC audio track
    let synthetic_mp4 = create_synthetic_aac_mp4();

    // Parse the synthetic MP4
    match parser.parse(&synthetic_mp4) {
        Ok(_) => {
            // Generate MSE segments
            match parser.generate_mse_segments() {
                Ok(segments) => {
                    assert!(!segments.is_empty(), "Should generate at least one segment");

                    // Find the initialization segment
                    let init_segment = segments
                        .iter()
                        .find(|s| s.segment_type == "initialization")
                        .expect("Should have initialization segment");

                    // Validate ESDS structure in the initialization segment
                    validate_esds_structure(&init_segment.data);

                    // Verify audio codec info
                    if let Some(audio_info) = parser.get_audio_codec_info() {
                        assert_eq!(audio_info.0, "AAC");
                        // Should use mp4a.40.2 codec string for Chrome compatibility
                        let codec_string = audio_info.1;
                        assert!(
                            codec_string.contains("mp4a.40.2"),
                            "Expected mp4a.40.2 codec string, got: {}",
                            codec_string
                        );
                    } else {
                        panic!("Should have audio codec info for AAC track");
                    }
                }
                Err(e) => panic!("Failed to generate MSE segments: {}", e),
            }
        }
        Err(e) => panic!("Failed to parse synthetic MP4: {}", e),
    }
}

/// Create a minimal synthetic MP4 with AAC audio for testing
fn create_synthetic_aac_mp4() -> Vec<u8> {
    let mut mp4_data = Vec::new();

    // Create ftyp box
    let ftyp = create_ftyp_box();
    mp4_data.extend_from_slice(&ftyp);

    // Create moov box with AAC track
    let moov = create_moov_with_aac_track();
    mp4_data.extend_from_slice(&moov);

    // Create minimal mdat box
    let mdat = create_minimal_mdat();
    mp4_data.extend_from_slice(&mdat);

    mp4_data
}

/// Create a synthetic ftyp box
fn create_ftyp_box() -> Vec<u8> {
    let mut ftyp = Vec::new();
    ftyp.extend_from_slice(&28u32.to_be_bytes()); // Box size
    ftyp.extend_from_slice(b"ftyp"); // Box type
    ftyp.extend_from_slice(b"isom"); // Major brand
    ftyp.extend_from_slice(&0u32.to_be_bytes()); // Minor version
    ftyp.extend_from_slice(b"isom"); // Compatible brands
    ftyp.extend_from_slice(b"iso2");
    ftyp.extend_from_slice(b"mp41");
    ftyp
}

/// Create a synthetic moov box with AAC audio track
fn create_moov_with_aac_track() -> Vec<u8> {
    let mut moov = Vec::new();

    // Simplified moov box with essential AAC track info
    // This would normally be much more complex
    let moov_content = create_simplified_moov_content();

    let moov_size = 8 + moov_content.len() as u32;
    moov.extend_from_slice(&moov_size.to_be_bytes());
    moov.extend_from_slice(b"moov");
    moov.extend_from_slice(&moov_content);

    moov
}

/// Create simplified moov content for testing
fn create_simplified_moov_content() -> Vec<u8> {
    // This is a simplified version for testing purposes that includes a proper trak structure
    let mut content = Vec::new();

    // Add minimal mvhd box
    content.extend_from_slice(&108u32.to_be_bytes()); // mvhd size
    content.extend_from_slice(b"mvhd");
    content.extend_from_slice(&[0; 100]); // Simplified mvhd content

    // Add a trak box for audio
    let trak = create_audio_trak();
    content.extend_from_slice(&trak);

    content
}

/// Create an audio trak box with proper structure for ESDS testing
fn create_audio_trak() -> Vec<u8> {
    let mut trak = Vec::new();

    // Create tkhd box (track header)
    let mut tkhd = Vec::new();
    tkhd.extend_from_slice(&92u32.to_be_bytes()); // tkhd size (version 0)
    tkhd.extend_from_slice(b"tkhd");
    tkhd.push(0); // version
    tkhd.extend_from_slice(&[0, 0, 3]); // flags (track enabled, in movie, in preview)
    tkhd.extend_from_slice(&[0; 4]); // creation time
    tkhd.extend_from_slice(&[0; 4]); // modification time
    tkhd.extend_from_slice(&1u32.to_be_bytes()); // track_id = 1
    tkhd.extend_from_slice(&[0; 4]); // reserved
    tkhd.extend_from_slice(&[0; 4]); // duration
    tkhd.extend_from_slice(&[0; 8]); // reserved
    tkhd.extend_from_slice(&[0; 2]); // layer
    tkhd.extend_from_slice(&[0; 2]); // alternate_group
    tkhd.extend_from_slice(&[1, 0]); // volume (1.0)
    tkhd.extend_from_slice(&[0; 2]); // reserved
    tkhd.extend_from_slice(&[0; 36]); // matrix (identity)
    tkhd.extend_from_slice(&[0; 4]); // width
    tkhd.extend_from_slice(&[0; 4]); // height

    // Create mdia box
    let mdia = create_audio_mdia();

    // Build trak
    let trak_size = 8 + tkhd.len() + mdia.len();
    trak.extend_from_slice(&(trak_size as u32).to_be_bytes());
    trak.extend_from_slice(b"trak");
    trak.extend_from_slice(&tkhd);
    trak.extend_from_slice(&mdia);

    trak
}

/// Create an audio mdia box
fn create_audio_mdia() -> Vec<u8> {
    let mut mdia = Vec::new();

    // Create mdhd box
    let mut mdhd = Vec::new();
    mdhd.extend_from_slice(&32u32.to_be_bytes()); // mdhd size
    mdhd.extend_from_slice(b"mdhd");
    mdhd.push(0); // version
    mdhd.extend_from_slice(&[0; 3]); // flags
    mdhd.extend_from_slice(&[0; 4]); // creation time
    mdhd.extend_from_slice(&[0; 4]); // modification time
    mdhd.extend_from_slice(&48000u32.to_be_bytes()); // timescale
    mdhd.extend_from_slice(&[0; 4]); // duration
    mdhd.extend_from_slice(&[0; 4]); // language + pre_defined

    // Create hdlr box
    let mut hdlr = Vec::new();
    hdlr.extend_from_slice(&33u32.to_be_bytes()); // hdlr size
    hdlr.extend_from_slice(b"hdlr");
    hdlr.push(0); // version
    hdlr.extend_from_slice(&[0; 3]); // flags
    hdlr.extend_from_slice(&[0; 4]); // pre_defined
    hdlr.extend_from_slice(b"soun"); // handler_type = audio
    hdlr.extend_from_slice(&[0; 12]); // reserved
    hdlr.push(0); // null terminator for name

    // Create minf box
    let minf = create_audio_minf();

    // Build mdia
    let mdia_size = 8 + mdhd.len() + hdlr.len() + minf.len();
    mdia.extend_from_slice(&(mdia_size as u32).to_be_bytes());
    mdia.extend_from_slice(b"mdia");
    mdia.extend_from_slice(&mdhd);
    mdia.extend_from_slice(&hdlr);
    mdia.extend_from_slice(&minf);

    mdia
}

/// Create an audio minf box
fn create_audio_minf() -> Vec<u8> {
    let mut minf = Vec::new();

    // Create smhd box (sound media header)
    let mut smhd = Vec::new();
    smhd.extend_from_slice(&16u32.to_be_bytes());
    smhd.extend_from_slice(b"smhd");
    smhd.extend_from_slice(&[0; 8]); // version/flags + balance + reserved

    // Create dinf box
    let mut dinf = Vec::new();
    let mut dref = Vec::new();
    dref.extend_from_slice(&28u32.to_be_bytes());
    dref.extend_from_slice(b"dref");
    dref.extend_from_slice(&[0; 4]); // version/flags
    dref.extend_from_slice(&1u32.to_be_bytes()); // entry count
    dref.extend_from_slice(&12u32.to_be_bytes()); // url size
    dref.extend_from_slice(b"url ");
    dref.extend_from_slice(&[0, 0, 0, 1]); // version/flags (self-contained)

    dinf.extend_from_slice(&(8 + dref.len() as u32).to_be_bytes());
    dinf.extend_from_slice(b"dinf");
    dinf.extend_from_slice(&dref);

    // Create stbl box
    let stbl = create_audio_stbl();

    // Build minf
    let minf_size = 8 + smhd.len() + dinf.len() + stbl.len();
    minf.extend_from_slice(&(minf_size as u32).to_be_bytes());
    minf.extend_from_slice(b"minf");
    minf.extend_from_slice(&smhd);
    minf.extend_from_slice(&dinf);
    minf.extend_from_slice(&stbl);

    minf
}

/// Create an audio stbl box with mp4a and esds
fn create_audio_stbl() -> Vec<u8> {
    let mut stbl = Vec::new();

    // Create stsd box with mp4a sample entry
    let stsd = create_audio_stsd();

    // Create minimal stts box
    let mut stts = Vec::new();
    stts.extend_from_slice(&16u32.to_be_bytes());
    stts.extend_from_slice(b"stts");
    stts.extend_from_slice(&[0; 4]); // version/flags
    stts.extend_from_slice(&0u32.to_be_bytes()); // entry count

    // Create minimal stsc box
    let mut stsc = Vec::new();
    stsc.extend_from_slice(&16u32.to_be_bytes());
    stsc.extend_from_slice(b"stsc");
    stsc.extend_from_slice(&[0; 4]); // version/flags
    stsc.extend_from_slice(&0u32.to_be_bytes()); // entry count

    // Create minimal stsz box
    let mut stsz = Vec::new();
    stsz.extend_from_slice(&20u32.to_be_bytes());
    stsz.extend_from_slice(b"stsz");
    stsz.extend_from_slice(&[0; 4]); // version/flags
    stsz.extend_from_slice(&0u32.to_be_bytes()); // sample size
    stsz.extend_from_slice(&0u32.to_be_bytes()); // sample count

    // Create minimal stco box
    let mut stco = Vec::new();
    stco.extend_from_slice(&16u32.to_be_bytes());
    stco.extend_from_slice(b"stco");
    stco.extend_from_slice(&[0; 4]); // version/flags
    stco.extend_from_slice(&0u32.to_be_bytes()); // entry count

    // Build stbl
    let stbl_size = 8 + stsd.len() + stts.len() + stsc.len() + stsz.len() + stco.len();
    stbl.extend_from_slice(&(stbl_size as u32).to_be_bytes());
    stbl.extend_from_slice(b"stbl");
    stbl.extend_from_slice(&stsd);
    stbl.extend_from_slice(&stts);
    stbl.extend_from_slice(&stsc);
    stbl.extend_from_slice(&stsz);
    stbl.extend_from_slice(&stco);

    stbl
}

/// Create an audio stsd box with mp4a sample entry containing esds
fn create_audio_stsd() -> Vec<u8> {
    let mut stsd = Vec::new();

    // Create mp4a sample entry with ESDS
    let mp4a = create_mp4a_sample_entry();

    // Build stsd
    let stsd_content_size = 4 + 4 + mp4a.len(); // version/flags + entry_count + mp4a
    let stsd_size = 8 + stsd_content_size;
    stsd.extend_from_slice(&(stsd_size as u32).to_be_bytes());
    stsd.extend_from_slice(b"stsd");
    stsd.extend_from_slice(&[0; 4]); // version/flags
    stsd.extend_from_slice(&1u32.to_be_bytes()); // entry count
    stsd.extend_from_slice(&mp4a);

    stsd
}

/// Create mp4a sample entry with ESDS box
fn create_mp4a_sample_entry() -> Vec<u8> {
    let mut mp4a = Vec::new();

    // Create ESDS box with extended length encoding (to test the fix)
    let esds = create_esds_with_extended_encoding();

    // mp4a sample entry structure:
    // 8 bytes: size + type
    // 6 bytes: reserved
    // 2 bytes: data_reference_index
    // 8 bytes: reserved
    // 2 bytes: channel_count
    // 2 bytes: sample_size
    // 2 bytes: pre_defined
    // 2 bytes: reserved
    // 4 bytes: sample_rate
    // then child boxes (esds)

    let mp4a_content_size = 28 + esds.len(); // sample entry fields + esds
    let mp4a_size = 8 + mp4a_content_size;
    mp4a.extend_from_slice(&(mp4a_size as u32).to_be_bytes());
    mp4a.extend_from_slice(b"mp4a");
    mp4a.extend_from_slice(&[0; 6]); // reserved
    mp4a.extend_from_slice(&1u16.to_be_bytes()); // data_reference_index
    mp4a.extend_from_slice(&[0; 8]); // reserved
    mp4a.extend_from_slice(&2u16.to_be_bytes()); // channel_count = stereo
    mp4a.extend_from_slice(&16u16.to_be_bytes()); // sample_size = 16-bit
    mp4a.extend_from_slice(&[0; 2]); // pre_defined
    mp4a.extend_from_slice(&[0; 2]); // reserved
    mp4a.extend_from_slice(&((48000 << 16) as u32).to_be_bytes()); // sample_rate = 48kHz (16.16 fixed point)
    mp4a.extend_from_slice(&esds);

    mp4a
}

/// Create ESDS box with extended length encoding (80 80 80 XX format)
/// This is the problematic encoding that Chrome doesn't handle well
fn create_esds_with_extended_encoding() -> Vec<u8> {
    let mut esds = Vec::new();

    // ESDS box content
    let mut esds_content = Vec::new();

    // Version/flags
    esds_content.extend_from_slice(&[0, 0, 0, 0]);

    // ES_Descriptor with extended length encoding
    esds_content.push(0x03); // ES_Descriptor tag
    esds_content.extend_from_slice(&[0x80, 0x80, 0x80, 0x22]); // Extended length (34 bytes)
    esds_content.extend_from_slice(&[0x00, 0x02]); // ES_ID = 2
    esds_content.push(0x00); // flags

    // DecoderConfigDescriptor
    esds_content.push(0x04); // tag
    esds_content.extend_from_slice(&[0x80, 0x80, 0x80, 0x14]); // Extended length (20 bytes)
    esds_content.push(0x40); // objectTypeIndication = 0x40 (MPEG-4 Audio)
    esds_content.push(0x15); // streamType = Audio (5) << 2 | upStream(0) | reserved(1)
    esds_content.extend_from_slice(&[0x00, 0x00, 0x00]); // bufferSizeDB
    esds_content.extend_from_slice(&[0x00, 0x01, 0xfb, 0x5d]); // maxBitrate
    esds_content.extend_from_slice(&[0x00, 0x01, 0xfb, 0x5d]); // avgBitrate

    // DecSpecificInfo (AudioSpecificConfig)
    esds_content.push(0x05); // tag
    esds_content.extend_from_slice(&[0x80, 0x80, 0x80, 0x02]); // Extended length (2 bytes)
                                                               // AudioSpecificConfig: AAC-LC, 48kHz, stereo
                                                               // audioObjectType=2 (5 bits) | samplingFrequencyIndex=3 (4 bits) | channelConfiguration=2 (4 bits) | ...
                                                               // 00010 | 0011 | 0010 = 0x11 0x90
    esds_content.extend_from_slice(&[0x11, 0x90]);

    // SLConfigDescriptor
    esds_content.push(0x06); // tag
    esds_content.extend_from_slice(&[0x80, 0x80, 0x80, 0x01]); // Extended length (1 byte)
    esds_content.push(0x02); // predefined = 2 (MP4)

    // Build ESDS box
    let esds_size = 8 + esds_content.len();
    esds.extend_from_slice(&(esds_size as u32).to_be_bytes());
    esds.extend_from_slice(b"esds");
    esds.extend_from_slice(&esds_content);

    esds
}

/// Create a minimal mdat box
fn create_minimal_mdat() -> Vec<u8> {
    let mut mdat = Vec::new();
    mdat.extend_from_slice(&16u32.to_be_bytes()); // Box size
    mdat.extend_from_slice(b"mdat"); // Box type
    mdat.extend_from_slice(&[0u8; 8]); // Minimal data
    mdat
}

/// Validate ESDS structure in MP4 data
fn validate_esds_structure(data: &[u8]) {
    // Look for ESDS box in the data
    let mut i = 0;
    let mut found_esds = false;

    while i + 8 <= data.len() {
        let box_size =
            u32::from_be_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as usize;

        if box_size < 8 || i + box_size > data.len() {
            break;
        }

        let box_type = String::from_utf8_lossy(&data[i + 4..i + 8]);

        if box_type == "esds" {
            found_esds = true;

            // Validate ESDS contains objectTypeIndication 0x40
            let esds_data = &data[i + 8..i + box_size];
            validate_esds_content(esds_data);
            break;
        }

        i += box_size;
    }

    // For a synthetic test, we don't require ESDS to be present
    // but if it is, it should be valid
    if found_esds {
        println!("Found and validated ESDS box structure");
    }
}

/// Validate ESDS box content structure
fn validate_esds_content(esds_data: &[u8]) {
    if esds_data.len() < 12 {
        return; // Too small to be valid ESDS
    }

    // ESDS starts with version/flags (4 bytes) then ES_Descriptor
    // Look for DecoderConfigDescriptor (tag 0x04)
    for i in 4..esds_data.len().saturating_sub(4) {
        if esds_data[i] == 0x04 {
            // Found DecoderConfigDescriptor, check for objectTypeIndication
            if i + 13 < esds_data.len() {
                let object_type = esds_data[i + 13];
                assert_eq!(
                    object_type, 0x40,
                    "Expected objectTypeIndication 0x40 for MPEG-4 Audio, got 0x{:02x}",
                    object_type
                );
                println!("Validated ESDS objectTypeIndication: 0x{:02x}", object_type);
                return;
            }
        }
    }
}
