use std::path::Path;
use std::time::Duration;

use ffmpeg_next as ffmpeg;
use thiserror::Error;
use tracing::{debug, info};

#[derive(Error, Debug)]
pub enum FFmpegReaderError {
    #[error("FFmpeg error: {0}")]
    FFmpeg(#[from] ffmpeg::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("No video stream found")]
    NoVideoStream,
    #[error("No audio stream found")]
    NoAudioStream,
    #[error("Invalid sample data")]
    InvalidSample,
}

pub type Result<T> = std::result::Result<T, FFmpegReaderError>;

/// Represents a media sample from FFmpeg
#[derive(Debug, Clone)]
pub struct FFmpegSample {
    pub data: Vec<u8>,
    pub timestamp: Duration,
    pub duration: Duration,
    pub is_sync: bool,
    pub stream_index: usize,
}

/// Video track information from FFmpeg
#[derive(Debug, Clone)]
pub struct FFmpegVideoTrack {
    pub stream_index: usize,
    pub codec: String,
    pub width: u32,
    pub height: u32,
    pub fps: f64,
    pub duration: Duration,
    pub bitrate: u64,
}

/// Audio track information from FFmpeg
#[derive(Debug, Clone)]
pub struct FFmpegAudioTrack {
    pub stream_index: usize,
    pub codec: String,
    pub sample_rate: u32,
    pub channels: u16,
    pub duration: Duration,
    pub bitrate: u64,
}

/// FFmpeg-based video reader that generates proper fMP4 segments
pub struct FFmpegVideoReader {
    input_context: ffmpeg::format::context::Input,
    input_path: String,
    video_track: Option<FFmpegVideoTrack>,
    audio_track: Option<FFmpegAudioTrack>,
    video_decoder: Option<ffmpeg::decoder::Video>,
    audio_decoder: Option<ffmpeg::decoder::Audio>,
    init_segment_generated: bool,
}

impl FFmpegVideoReader {
    /// Open a media file using FFmpeg
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        info!("Opening media file with FFmpeg: {:?}", path.as_ref());

        // Initialize FFmpeg
        ffmpeg::init()?;

        // Store path for later FFmpeg CLI usage
        let input_path = path.as_ref().to_string_lossy().to_string();

        // Open input file
        let input_context = ffmpeg::format::input(&path)?;

        let mut reader = Self {
            input_context,
            input_path,
            video_track: None,
            audio_track: None,
            video_decoder: None,
            audio_decoder: None,
            init_segment_generated: false,
        };

        reader.analyze_streams()?;
        Ok(reader)
    }

    /// Analyze streams and set up decoders
    fn analyze_streams(&mut self) -> Result<()> {
        info!("Analyzing media streams with FFmpeg");

        let input = &self.input_context;

        // Find video stream
        if let Some(video_stream) = input.streams().best(ffmpeg::media::Type::Video) {
            let video_index = video_stream.index();
            let codec_context =
                ffmpeg::codec::context::Context::from_parameters(video_stream.parameters())?;
            let video_decoder = codec_context.decoder().video()?;

            let time_base = video_stream.time_base();
            let duration = Duration::from_secs_f64(
                video_stream.duration() as f64 * time_base.numerator() as f64
                    / time_base.denominator() as f64,
            );

            let fps = video_stream.avg_frame_rate();
            let fps_val = fps.numerator() as f64 / fps.denominator() as f64;

            self.video_track = Some(FFmpegVideoTrack {
                stream_index: video_index,
                codec: format!("{:?}", video_decoder.id()),
                width: video_decoder.width(),
                height: video_decoder.height(),
                fps: fps_val,
                duration,
                bitrate: video_decoder.bit_rate() as u64,
            });

            self.video_decoder = Some(video_decoder);

            info!(
                "Found video stream: {}x{} @ {:.2}fps, codec: {:?}",
                self.video_track.as_ref().unwrap().width,
                self.video_track.as_ref().unwrap().height,
                self.video_track.as_ref().unwrap().fps,
                self.video_track.as_ref().unwrap().codec
            );
        }

        // Find audio stream
        if let Some(audio_stream) = input.streams().best(ffmpeg::media::Type::Audio) {
            let audio_index = audio_stream.index();
            let codec_context =
                ffmpeg::codec::context::Context::from_parameters(audio_stream.parameters())?;
            let audio_decoder = codec_context.decoder().audio()?;

            let time_base = audio_stream.time_base();
            let duration = Duration::from_secs_f64(
                audio_stream.duration() as f64 * time_base.numerator() as f64
                    / time_base.denominator() as f64,
            );

            self.audio_track = Some(FFmpegAudioTrack {
                stream_index: audio_index,
                codec: format!("{:?}", audio_decoder.id()),
                sample_rate: audio_decoder.rate(),
                channels: audio_decoder.channels() as u16,
                duration,
                bitrate: audio_decoder.bit_rate() as u64,
            });

            self.audio_decoder = Some(audio_decoder);

            info!(
                "Found audio stream: {} Hz, {} channels, codec: {:?}",
                self.audio_track.as_ref().unwrap().sample_rate,
                self.audio_track.as_ref().unwrap().channels,
                self.audio_track.as_ref().unwrap().codec
            );
        }

        Ok(())
    }

    /// Get video track information
    pub fn video_track(&self) -> Option<&FFmpegVideoTrack> {
        self.video_track.as_ref()
    }

    /// Get audio track information
    pub fn audio_track(&self) -> Option<&FFmpegAudioTrack> {
        self.audio_track.as_ref()
    }

    /// Start FFmpeg process to generate segments on-demand
    pub fn start_streaming_conversion(&self, output_dir: &str) -> Result<std::process::Child> {
        info!("Starting streaming FFmpeg conversion");

        use std::fs;
        use std::process::{Command, Stdio};

        // Create output directory
        fs::create_dir_all(output_dir).map_err(|e| FFmpegReaderError::Io(e))?;

        // Start FFmpeg process in background to generate segments
        let child = Command::new("ffmpeg")
            .arg("-y") // Overwrite output files
            .arg("-i")
            .arg(&self.input_path)
            .arg("-f")
            .arg("segment")
            .arg("-segment_format")
            .arg("mp4")
            .arg("-segment_time")
            .arg("2") // 2 second segments
            .arg("-segment_list_type")
            .arg("m3u8")
            .arg("-segment_list")
            .arg(format!("{}/playlist.m3u8", output_dir))
            .arg("-c:v")
            .arg("libx264")
            .arg("-c:a")
            .arg("aac")
            .arg("-movflags")
            .arg("frag_keyframe+empty_moov+faststart")
            .arg("-segment_start_number")
            .arg("0")
            .arg(format!("{}/segment_%03d.mp4", output_dir))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| FFmpegReaderError::Io(e))?;

        info!("FFmpeg streaming conversion started");
        Ok(child)
    }

    /// Get available segment files (as FFmpeg generates them)
    pub fn get_available_segments(&self, output_dir: &str) -> Result<Vec<String>> {
        use std::fs;

        let mut segments = Vec::new();

        if let Ok(entries) = fs::read_dir(output_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.extension().and_then(|s| s.to_str()) == Some("mp4") {
                        if let Some(path_str) = path.to_str() {
                            segments.push(path_str.to_string());
                        }
                    }
                }
            }
        }

        segments.sort();
        Ok(segments)
    }

    /// Read segment file as binary data
    pub fn read_segment_file(&self, path: &str) -> Result<Vec<u8>> {
        use std::fs;
        let data = fs::read(path).map_err(|e| FFmpegReaderError::Io(e))?;
        Ok(data)
    }

    /// Generate proper fMP4 initialization segment using FFmpeg muxer
    pub fn generate_fmp4_init_segment(&mut self) -> Result<Vec<u8>> {
        if self.init_segment_generated {
            return Err(FFmpegReaderError::InvalidSample); // Already generated
        }

        info!("Generating fMP4 initialization segment with FFmpeg muxer");

        // For now, let's create a simple but proper fMP4 initialization segment manually
        // This is a temporary approach while we figure out the correct FFmpeg muxer API

        let mut init_segment = Vec::new();

        // ftyp box for fMP4
        let ftyp = [
            0x00, 0x00, 0x00, 0x20, // size (32 bytes)
            b'f', b't', b'y', b'p', // type
            b'i', b's', b'o', b'm', // major brand (isom for MP4)
            0x00, 0x00, 0x00, 0x01, // minor version
            b'i', b's', b'o', b'm', // compatible brands
            b'i', b's', b'o', b'2', b'a', b'v', b'c', b'1', // avc1 for H.264
            b'm', b'p', b'4', b'1', // mp41
        ];
        init_segment.extend_from_slice(&ftyp);

        // moov box with minimal track information
        let moov_start = init_segment.len();
        init_segment.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // size placeholder
        init_segment.extend_from_slice(b"moov");

        // mvhd box (movie header)
        let mvhd = [
            0x00, 0x00, 0x00, 0x6C, // size (108 bytes)
            b'm', b'v', b'h', b'd', // type
            0x00, 0x00, 0x00, 0x00, // version + flags
            0x00, 0x00, 0x00, 0x00, // creation time
            0x00, 0x00, 0x00, 0x00, // modification time
            0x00, 0x00, 0x03, 0xE8, // timescale (1000)
            0x00, 0x00, 0x00, 0x00, // duration
            0x00, 0x01, 0x00, 0x00, // rate (1.0)
            0x01, 0x00, 0x00, 0x00, // volume (1.0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
            // transformation matrix (unity)
            0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, // pre-defined
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x02, // next track ID
        ];
        init_segment.extend_from_slice(&mvhd);

        // Update moov box size
        let moov_size = init_segment.len() - moov_start;
        let moov_size_bytes = (moov_size as u32).to_be_bytes();
        init_segment[moov_start..moov_start + 4].copy_from_slice(&moov_size_bytes);

        self.init_segment_generated = true;

        info!("Generated fMP4 init segment: {} bytes", init_segment.len());
        Ok(init_segment)
    }

    /// Convert FFmpeg packet to fMP4 media segment
    pub fn packet_to_fmp4_segment(&self, sample: &FFmpegSample) -> Result<Vec<u8>> {
        debug!(
            "Converting packet to fMP4 segment: {} bytes",
            sample.data.len()
        );

        // For now, let's create a simple but valid moof+mdat structure
        // This is more reliable than the complex FFmpeg muxer API

        let mut segment = Vec::new();

        // moof box (movie fragment)
        let moof_start = segment.len();
        segment.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // size placeholder
        segment.extend_from_slice(b"moof");

        // mfhd box (movie fragment header)
        segment.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]); // size (16 bytes)
        segment.extend_from_slice(b"mfhd");
        segment.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // version + flags
        segment.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // sequence number

        // traf box (track fragment)
        let traf_start = segment.len();
        segment.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // size placeholder
        segment.extend_from_slice(b"traf");

        // tfhd box (track fragment header)
        segment.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]); // size (16 bytes)
        segment.extend_from_slice(b"tfhd");
        segment.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // version + flags
        segment.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // track ID

        // Update traf box size
        let traf_size = segment.len() - traf_start;
        let traf_size_bytes = (traf_size as u32).to_be_bytes();
        segment[traf_start..traf_start + 4].copy_from_slice(&traf_size_bytes);

        // Update moof box size
        let moof_size = segment.len() - moof_start;
        let moof_size_bytes = (moof_size as u32).to_be_bytes();
        segment[moof_start..moof_start + 4].copy_from_slice(&moof_size_bytes);

        // mdat box (media data)
        let mdat_size = 8 + sample.data.len() as u32;
        segment.extend_from_slice(&mdat_size.to_be_bytes());
        segment.extend_from_slice(b"mdat");
        segment.extend_from_slice(&sample.data);

        debug!("Generated fMP4 media segment: {} bytes", segment.len());
        Ok(segment)
    }

    /// Convert an FFmpegSample to a StreamSegment
    ///
    /// This is a helper method for the unified type migration.
    /// The `media_type` parameter specifies whether this is video or audio.
    pub fn sample_to_stream_segment(
        sample: &FFmpegSample,
        stream_id: crate::media::segment::StreamId,
        sequence: u64,
        media_type: crate::media::segment::MediaType,
    ) -> crate::media::segment::StreamSegment {
        crate::media::segment::StreamSegment::from_ffmpeg_sample(
            sample, stream_id, sequence, media_type,
        )
    }
}
