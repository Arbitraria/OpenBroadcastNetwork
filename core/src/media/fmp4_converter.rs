use std::io;

/// Proper fragmented MP4 converter for MSE compatibility
#[allow(dead_code)]
pub struct FragmentedMp4Converter {
    sequence_number: u32,
    base_media_decode_time: u64,
    track_id: u32,
}

impl FragmentedMp4Converter {
    /// Create a new converter with initial sequence number 1
    pub fn new() -> Self {
        Self {
            sequence_number: 1,
            base_media_decode_time: 0,
            track_id: 1,
        }
    }

    /// Create a proper fMP4 segment with moof + mdat structure
    pub fn create_fragment(
        &mut self,
        frames: &[FrameData],
        track_id: u32,
        timescale: u32,
    ) -> Result<Vec<u8>, io::Error> {
        let mut fragment = Vec::new();

        // Create moof (movie fragment)
        let moof = self.create_moof(frames, track_id, timescale)?;
        fragment.extend_from_slice(&moof);

        // Create mdat (media data)
        let mdat = self.create_mdat(frames)?;
        fragment.extend_from_slice(&mdat);

        // Update sequence number and decode time for next fragment
        self.sequence_number += 1;

        // Update base decode time for proper timestamp continuity
        let total_duration: u64 =
            frames.iter().map(|f| f.duration as u64).sum();
        self.base_media_decode_time += total_duration;

        Ok(fragment)
    }

    /// Create a proper fMP4 segment with sample flags and composition
    /// offsets for Chrome-compatible playback.
    pub fn create_fragment_v2(
        &mut self,
        frames: &[FrameData],
        track_id: u32,
        _timescale: u32,
    ) -> Result<Vec<u8>, io::Error> {
        let mut fragment = Vec::new();

        // Create moof with v2 trun (includes sample_flags + ctts)
        let moof = self.create_moof_v2(frames, track_id)?;
        fragment.extend_from_slice(&moof);

        // Create mdat
        let mdat = self.create_mdat_v2(frames)?;
        fragment.extend_from_slice(&mdat);

        // Update sequence number and decode time
        self.sequence_number += 1;
        let total_duration: u64 =
            frames.iter().map(|f| f.duration as u64).sum();
        self.base_media_decode_time += total_duration;

        Ok(fragment)
    }

    /// Create moof box with v2 trun (sample flags + composition offsets)
    fn create_moof_v2(
        &self,
        frames: &[FrameData],
        track_id: u32,
    ) -> Result<Vec<u8>, io::Error> {
        let mut moof_content = Vec::new();

        let mfhd = self.create_mfhd()?;
        moof_content.extend_from_slice(&mfhd);

        let traf = self.create_traf_v2(frames, track_id)?;
        moof_content.extend_from_slice(&traf);

        let mut moof = Vec::new();
        moof.extend_from_slice(
            &(8 + moof_content.len() as u32).to_be_bytes(),
        );
        moof.extend_from_slice(b"moof");
        moof.extend_from_slice(&moof_content);

        Ok(moof)
    }

    /// Create traf box with v2 trun
    fn create_traf_v2(
        &self,
        frames: &[FrameData],
        track_id: u32,
    ) -> Result<Vec<u8>, io::Error> {
        let mut traf_content = Vec::new();

        let tfhd = self.create_tfhd(track_id)?;
        traf_content.extend_from_slice(&tfhd);

        let tfdt = self.create_tfdt_auto()?;
        traf_content.extend_from_slice(&tfdt);

        let trun = self.create_trun_v2(frames)?;
        traf_content.extend_from_slice(&trun);

        let mut traf = Vec::new();
        traf.extend_from_slice(
            &(8 + traf_content.len() as u32).to_be_bytes(),
        );
        traf.extend_from_slice(b"traf");
        traf.extend_from_slice(&traf_content);

        Ok(traf)
    }

    /// Create tfdt with automatic version selection (v0 for 32-bit, v1 for
    /// 64-bit timestamps)
    fn create_tfdt_auto(&self) -> Result<Vec<u8>, io::Error> {
        if self.base_media_decode_time > u32::MAX as u64 {
            // Version 1: 64-bit base_media_decode_time
            let mut tfdt = Vec::new();
            tfdt.extend_from_slice(&20u32.to_be_bytes()); // size
            tfdt.extend_from_slice(b"tfdt");
            tfdt.push(1); // version 1
            tfdt.extend_from_slice(&[0, 0, 0]); // flags
            tfdt.extend_from_slice(
                &self.base_media_decode_time.to_be_bytes(),
            );
            Ok(tfdt)
        } else {
            self.create_tfdt()
        }
    }

    /// Create trun with sample_flags and composition time offsets
    fn create_trun_v2(
        &self,
        frames: &[FrameData],
    ) -> Result<Vec<u8>, io::Error> {
        let mut trun_content = Vec::new();

        // Version 1, flags: data-offset(0x01) | sample-duration(0x100)
        // | sample-size(0x200) | sample-flags(0x400)
        // | sample-composition-time-offset(0x800) = 0x000F01
        trun_content.push(1); // version 1
        trun_content.extend_from_slice(&[0x00, 0x0F, 0x01]);

        // Sample count
        trun_content.extend_from_slice(
            &(frames.len() as u32).to_be_bytes(),
        );

        // Data offset (from moof start to mdat payload)
        let data_offset = self.calculate_moof_size_v2(frames) + 8;
        trun_content.extend_from_slice(&(data_offset as i32).to_be_bytes());

        // Per-sample entries: duration(4) + size(4) + flags(4) + cts_offset(4)
        for frame in frames {
            trun_content.extend_from_slice(&frame.duration.to_be_bytes());
            trun_content.extend_from_slice(&frame.size.to_be_bytes());

            // Sample flags per ISO 14496-12
            // Keyframe: sample_depends_on=2 (independent) = 0x02000000
            // Non-keyframe: sample_depends_on=1 (dependent),
            //               sample_is_non_sync=1 = 0x01010000
            let flags: u32 = if frame.is_keyframe {
                0x02000000
            } else {
                0x01010000
            };
            trun_content.extend_from_slice(&flags.to_be_bytes());

            // Composition time offset (signed, version 1)
            trun_content.extend_from_slice(
                &frame.composition_offset.to_be_bytes(),
            );
        }

        let mut trun = Vec::new();
        trun.extend_from_slice(
            &(8 + trun_content.len() as u32).to_be_bytes(),
        );
        trun.extend_from_slice(b"trun");
        trun.extend_from_slice(&trun_content);

        Ok(trun)
    }

    /// Create mdat from FrameData slices
    fn create_mdat_v2(
        &self,
        frames: &[FrameData],
    ) -> Result<Vec<u8>, io::Error> {
        let mut mdat = Vec::new();
        let data_size: u32 = frames.iter().map(|f| f.size).sum();
        let box_size = 8 + data_size;
        mdat.extend_from_slice(&box_size.to_be_bytes());
        mdat.extend_from_slice(b"mdat");
        for frame in frames {
            mdat.extend_from_slice(&frame.data);
        }
        Ok(mdat)
    }

    /// Calculate moof size for v2 trun (16 bytes per sample instead of 8)
    fn calculate_moof_size_v2(&self, frames: &[FrameData]) -> u32 {
        let tfdt_size = if self.base_media_decode_time > u32::MAX as u64 {
            20 // version 1: 8-byte timestamp
        } else {
            16 // version 0: 4-byte timestamp
        };
        // moof(8) + mfhd(16) + traf(8) + tfhd(16) + tfdt + trun header(8)
        // + trun content(12 + N*16)
        8 + 16 + 8 + 16 + tfdt_size + 8 + 12 + (frames.len() as u32 * 16)
    }

    /// Create moof (movie fragment) box
    fn create_moof(
        &self,
        frames: &[FrameData],
        track_id: u32,
        timescale: u32,
    ) -> Result<Vec<u8>, io::Error> {
        let mut moof_content = Vec::new();

        // mfhd (movie fragment header)
        let mfhd = self.create_mfhd()?;
        moof_content.extend_from_slice(&mfhd);

        // traf (track fragment)
        let traf = self.create_traf(frames, track_id, timescale)?;
        moof_content.extend_from_slice(&traf);

        // Wrap in moof box
        let mut moof = Vec::new();
        moof.extend_from_slice(
            &(8 + moof_content.len() as u32).to_be_bytes(),
        );
        moof.extend_from_slice(b"moof");
        moof.extend_from_slice(&moof_content);

        Ok(moof)
    }

    /// Create mfhd (movie fragment header) box
    fn create_mfhd(&self) -> Result<Vec<u8>, io::Error> {
        let mut mfhd = Vec::new();

        // Box size and type
        mfhd.extend_from_slice(&16u32.to_be_bytes()); // size
        mfhd.extend_from_slice(b"mfhd"); // type

        // Version and flags
        mfhd.push(0); // version
        mfhd.extend_from_slice(&[0, 0, 0]); // flags

        // Sequence number
        mfhd.extend_from_slice(&self.sequence_number.to_be_bytes());

        Ok(mfhd)
    }

    /// Create traf (track fragment) box
    fn create_traf(
        &self,
        frames: &[FrameData],
        track_id: u32,
        _timescale: u32,
    ) -> Result<Vec<u8>, io::Error> {
        let mut traf_content = Vec::new();

        // tfhd (track fragment header)
        let tfhd = self.create_tfhd(track_id)?;
        traf_content.extend_from_slice(&tfhd);

        // tfdt (track fragment decode time)
        let tfdt = self.create_tfdt()?;
        traf_content.extend_from_slice(&tfdt);

        // trun (track run)
        let trun = self.create_trun(frames)?;
        traf_content.extend_from_slice(&trun);

        // Wrap in traf box
        let mut traf = Vec::new();
        traf.extend_from_slice(
            &(8 + traf_content.len() as u32).to_be_bytes(),
        );
        traf.extend_from_slice(b"traf");
        traf.extend_from_slice(&traf_content);

        Ok(traf)
    }

    /// Create tfhd (track fragment header) box
    fn create_tfhd(&self, track_id: u32) -> Result<Vec<u8>, io::Error> {
        let mut tfhd = Vec::new();

        // Box size and type
        tfhd.extend_from_slice(&16u32.to_be_bytes()); // size
        tfhd.extend_from_slice(b"tfhd"); // type

        // Version and flags
        tfhd.push(0); // version
                      // Flags: 0x020000 = default-base-is-moof
        tfhd.extend_from_slice(&[0x02, 0x00, 0x00]);

        // Track ID
        tfhd.extend_from_slice(&track_id.to_be_bytes());

        Ok(tfhd)
    }

    /// Create tfdt (track fragment decode time) box
    fn create_tfdt(&self) -> Result<Vec<u8>, io::Error> {
        let mut tfdt = Vec::new();

        // Box size and type
        tfdt.extend_from_slice(&16u32.to_be_bytes()); // size
        tfdt.extend_from_slice(b"tfdt"); // type

        // Version and flags
        tfdt.push(0); // version 0 (32-bit time)
        tfdt.extend_from_slice(&[0, 0, 0]); // flags

        // Base media decode time
        tfdt.extend_from_slice(&(self.base_media_decode_time as u32).to_be_bytes());

        Ok(tfdt)
    }

    /// Create trun (track run) box
    fn create_trun(
        &self,
        frames: &[FrameData],
    ) -> Result<Vec<u8>, io::Error> {
        let mut trun_content = Vec::new();

        // Version and flags
        trun_content.push(1); // version 1
        // Flags: 0x000301 = data-offset-present
        //   | sample-duration-present | sample-size-present
        trun_content.extend_from_slice(&[0x00, 0x03, 0x01]);

        // Sample count
        trun_content.extend_from_slice(
            &(frames.len() as u32).to_be_bytes(),
        );

        // Data offset (offset from moof start to mdat payload)
        // This will be: moof_size + 8 (mdat header)
        let data_offset = self.calculate_moof_size(frames) + 8;
        trun_content.extend_from_slice(
            &(data_offset as i32).to_be_bytes(),
        );

        // Sample entries (composition_offset ignored in v1 path)
        for frame in frames {
            // Sample duration
            trun_content.extend_from_slice(
                &frame.duration.to_be_bytes(),
            );
            // Sample size
            trun_content.extend_from_slice(&frame.size.to_be_bytes());
        }

        // Wrap in trun box
        let mut trun = Vec::new();
        trun.extend_from_slice(
            &(8 + trun_content.len() as u32).to_be_bytes(),
        );
        trun.extend_from_slice(b"trun");
        trun.extend_from_slice(&trun_content);

        Ok(trun)
    }

    /// Create mdat (media data) box
    fn create_mdat(
        &self,
        frames: &[FrameData],
    ) -> Result<Vec<u8>, io::Error> {
        let mut mdat = Vec::new();

        // Calculate total size
        let data_size: u32 = frames.iter().map(|f| f.size).sum();
        let box_size = 8 + data_size;

        // Box header
        mdat.extend_from_slice(&box_size.to_be_bytes());
        mdat.extend_from_slice(b"mdat");

        // Frame data
        for frame in frames {
            mdat.extend_from_slice(&frame.data);
        }

        Ok(mdat)
    }

    /// Calculate moof size for data offset
    fn calculate_moof_size(&self, frames: &[FrameData]) -> u32 {
        // moof header (8) + mfhd (16) + traf header (8) + tfhd (16)
        // + tfdt (16) + trun header (8) + trun content (12 + N * 8)
        8 + 16 + 8 + 16 + 16 + 8 + 12 + (frames.len() as u32 * 8)
    }

    /// Create a proper MSE initialization segment with ftyp + moov
    pub fn create_init_segment(
        &self,
        ftyp_data: &[u8],
        moov_data: &[u8],
        needs_mvex: bool,
    ) -> Result<Vec<u8>, io::Error> {
        let mut init_segment = Vec::new();

        // Copy ftyp
        init_segment.extend_from_slice(ftyp_data);

        // Process moov
        if needs_mvex {
            // Add mvex to moov for MSE compatibility
            let moov_with_mvex = self.add_mvex_to_moov(moov_data)?;
            init_segment.extend_from_slice(&moov_with_mvex);
        } else {
            // Already has mvex, use as-is
            init_segment.extend_from_slice(moov_data);
        }

        Ok(init_segment)
    }

    /// Add mvex box to moov for MSE compatibility
    fn add_mvex_to_moov(&self, moov_data: &[u8]) -> Result<Vec<u8>, io::Error> {
        // Parse moov to find where to insert mvex
        if moov_data.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid moov box",
            ));
        }

        let moov_size =
            u32::from_be_bytes([moov_data[0], moov_data[1], moov_data[2], moov_data[3]]);
        let moov_type = &moov_data[4..8];

        if moov_type != b"moov" {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Not a moov box"));
        }

        // Create mvex with trex for each track
        let mvex = self.create_mvex()?;

        // Reconstruct moov with mvex
        let new_moov_size = moov_size + mvex.len() as u32;
        let mut new_moov = Vec::new();

        // New size
        new_moov.extend_from_slice(&new_moov_size.to_be_bytes());
        // Type
        new_moov.extend_from_slice(b"moov");
        // Original moov content (without header)
        new_moov.extend_from_slice(&moov_data[8..]);
        // Append mvex
        new_moov.extend_from_slice(&mvex);

        Ok(new_moov)
    }

    /// Create mvex (movie extends) box
    fn create_mvex(&self) -> Result<Vec<u8>, io::Error> {
        let mut mvex_content = Vec::new();

        // Create trex for video track (track_id = 1)
        let trex = self.create_trex(1)?;
        mvex_content.extend_from_slice(&trex);

        // Create trex for audio track (track_id = 2)
        let trex2 = self.create_trex(2)?;
        mvex_content.extend_from_slice(&trex2);

        // Wrap in mvex box
        let mut mvex = Vec::new();
        mvex.extend_from_slice(&(8 + mvex_content.len() as u32).to_be_bytes());
        mvex.extend_from_slice(b"mvex");
        mvex.extend_from_slice(&mvex_content);

        Ok(mvex)
    }

    /// Create trex (track extends) box
    fn create_trex(&self, track_id: u32) -> Result<Vec<u8>, io::Error> {
        let mut trex = Vec::new();

        // Box size and type
        trex.extend_from_slice(&32u32.to_be_bytes()); // size
        trex.extend_from_slice(b"trex"); // type

        // Version and flags
        trex.push(0); // version
        trex.extend_from_slice(&[0, 0, 0]); // flags

        // Track ID
        trex.extend_from_slice(&track_id.to_be_bytes());

        // Default values
        trex.extend_from_slice(&1u32.to_be_bytes()); // default_sample_description_index
        trex.extend_from_slice(&0u32.to_be_bytes()); // default_sample_duration
        trex.extend_from_slice(&0u32.to_be_bytes()); // default_sample_size
        trex.extend_from_slice(&0u32.to_be_bytes()); // default_sample_flags

        Ok(trex)
    }
}

/// Frame data for proper fMP4 fragmentation with full metadata
pub struct FrameData {
    /// Raw frame data (NAL units or audio samples)
    pub data: Vec<u8>,
    /// Duration in timescale units
    pub duration: u32,
    /// Size in bytes
    pub size: u32,
    /// Whether this is a keyframe (sync sample)
    pub is_keyframe: bool,
    /// Composition time offset for B-frames
    pub composition_offset: i32,
}

impl FrameData {
    /// Create a new FrameData from raw bytes
    pub fn new(
        data: Vec<u8>,
        duration: u32,
        is_keyframe: bool,
        composition_offset: i32,
    ) -> Self {
        let size = data.len() as u32;
        Self {
            data,
            duration,
            size,
            is_keyframe,
            composition_offset,
        }
    }
}

impl From<Sample> for FrameData {
    fn from(sample: Sample) -> Self {
        Self {
            size: sample.size,
            duration: sample.duration,
            is_keyframe: sample.is_keyframe,
            data: sample.data,
            composition_offset: 0,
        }
    }
}

/// Sample data for fragmentation
///
/// Internal type. External consumers should use [`FrameData`] or
/// [`crate::media::segment::StreamSegment`].
pub struct Sample {
    /// Raw sample payload bytes
    pub data: Vec<u8>,
    /// Duration in timescale units
    pub duration: u32,
    /// Size in bytes
    pub size: u32,
    /// Whether this is a sync/key sample
    pub is_keyframe: bool,
}

impl Sample {
    /// Create a new sample; size is derived from data length
    pub fn new(data: Vec<u8>, duration: u32, is_keyframe: bool) -> Self {
        let size = data.len() as u32;
        Self {
            data,
            duration,
            size,
            is_keyframe,
        }
    }

    /// Convert to a unified StreamSegment with additional context
    ///
    /// The `timescale` parameter converts duration to microseconds.
    /// Typical values: 90000 for video (90kHz), 48000 for audio (48kHz).
    pub fn to_stream_segment(
        &self,
        stream_id: crate::media::segment::StreamId,
        sequence: u64,
        pts_us: u64,
        timescale: u32,
        media_type: crate::media::segment::MediaType,
    ) -> crate::media::segment::StreamSegment {
        crate::media::segment::StreamSegment::from_sample(
            self, stream_id, sequence, pts_us, timescale, media_type,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mfhd() {
        let converter = FragmentedMp4Converter::new();
        let mfhd = converter.create_mfhd().unwrap();

        assert_eq!(mfhd.len(), 16);
        assert_eq!(&mfhd[4..8], b"mfhd");
        assert_eq!(
            u32::from_be_bytes([mfhd[12], mfhd[13], mfhd[14], mfhd[15]]),
            1
        );
    }

    #[test]
    fn test_create_fragment() {
        let mut converter = FragmentedMp4Converter::new();

        let samples = vec![
            FrameData::new(vec![0x00, 0x01, 0x02, 0x03], 1000, true, 0),
            FrameData::new(vec![0x04, 0x05, 0x06, 0x07], 1000, false, 0),
        ];

        let fragment = converter.create_fragment(&samples, 1, 1000).unwrap();

        // Should have moof and mdat boxes
        assert!(fragment.len() > 16);

        // Check moof box
        let moof_type = std::str::from_utf8(&fragment[4..8]).unwrap();
        assert_eq!(moof_type, "moof");
    }
}
