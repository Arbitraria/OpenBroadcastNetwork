use std::path::Path;
use std::io::{self, Write};
use std::process::{Command, Stdio};
use tempfile::NamedTempFile;
use tracing::{info, debug, warn, error};

/// Transcoder for converting unsupported codecs to browser-compatible formats
pub struct MediaTranscoder {
    temp_dir: String,
}

impl MediaTranscoder {
    pub fn new() -> Self {
        Self {
            temp_dir: "/tmp".to_string(),
        }
    }

    /// Convert AC-3 audio to AAC while preserving video stream
    pub fn convert_ac3_to_aac<P: AsRef<Path>>(
        &self, 
        input_path: P,
        output_path: P
    ) -> Result<(), TranscoderError> {
        let input = input_path.as_ref();
        let output = output_path.as_ref();
        
        info!("Converting AC-3 audio to AAC: {} -> {}", 
              input.display(), output.display());
        
        // Use FFmpeg to convert AC-3 to AAC while copying video
        let mut cmd = Command::new("ffmpeg");
        cmd.args(&[
            "-i", input.to_str().unwrap(),
            "-c:v", "copy",           // Copy video stream as-is
            "-c:a", "aac",           // Convert audio to AAC
            "-b:a", "128k",          // Set AAC bitrate
            "-ac", "2",              // Force stereo output
            "-ar", "44100",          // Standard sample rate
            "-movflags", "frag_keyframe+empty_moov+faststart", // MSE-compatible
            "-y",                    // Overwrite output
            output.to_str().unwrap()
        ]);
        
        debug!("Running FFmpeg command: {:?}", cmd);
        
        let output_result = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(TranscoderError::Io)?;
        
        if !output_result.status.success() {
            let stderr = String::from_utf8_lossy(&output_result.stderr);
            error!("FFmpeg conversion failed: {}", stderr);
            return Err(TranscoderError::ConversionFailed(stderr.to_string()));
        }
        
        info!("Successfully converted AC-3 to AAC");
        Ok(())
    }

    /// Check if a file contains AC-3 audio that needs conversion
    pub fn needs_ac3_conversion<P: AsRef<Path>>(&self, path: P) -> Result<bool, TranscoderError> {
        let path_str = path.as_ref().to_str().unwrap();
        
        let output = Command::new("ffprobe")
            .args(&[
                "-v", "quiet",
                "-print_format", "json",
                "-show_streams",
                path_str
            ])
            .output()
            .map_err(TranscoderError::Io)?;
        
        if !output.status.success() {
            return Err(TranscoderError::ProbeError("ffprobe failed".to_string()));
        }
        
        let json_str = String::from_utf8_lossy(&output.stdout);
        
        // Simple check for AC-3 codec
        let has_ac3 = json_str.contains("\"codec_name\": \"ac3\"") || 
                     json_str.contains("\"codec_name\": \"eac3\"") ||
                     json_str.contains("\"codec_name\": \"ac-3\"");
        
        if has_ac3 {
            info!("Detected AC-3/E-AC-3 audio in {}", path_str);
        }
        
        Ok(has_ac3)
    }

    /// Create video-only version by removing all audio tracks
    pub fn create_video_only<P: AsRef<Path>>(
        &self,
        input_path: P,
        output_path: P
    ) -> Result<(), TranscoderError> {
        let input = input_path.as_ref();
        let output = output_path.as_ref();
        
        info!("Creating video-only version: {} -> {}", 
              input.display(), output.display());
        
        let mut cmd = Command::new("ffmpeg");
        cmd.args(&[
            "-i", input.to_str().unwrap(),
            "-c:v", "copy",           // Copy video stream as-is
            "-an",                   // Remove all audio tracks
            "-movflags", "frag_keyframe+empty_moov+faststart", // MSE-compatible
            "-y",                    // Overwrite output
            output.to_str().unwrap()
        ]);
        
        debug!("Running FFmpeg command: {:?}", cmd);
        
        let output_result = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(TranscoderError::Io)?;
        
        if !output_result.status.success() {
            let stderr = String::from_utf8_lossy(&output_result.stderr);
            error!("FFmpeg video-only conversion failed: {}", stderr);
            return Err(TranscoderError::ConversionFailed(stderr.to_string()));
        }
        
        info!("Successfully created video-only version");
        Ok(())
    }

    /// Convert any media file to MSE-compatible format
    pub fn convert_to_mse_compatible<P: AsRef<Path>>(
        &self,
        input_path: P,
        output_path: P,
        video_only: bool
    ) -> Result<(), TranscoderError> {
        let input = input_path.as_ref();
        let output = output_path.as_ref();
        
        info!("Converting to MSE-compatible format: {} -> {} (video_only: {})", 
              input.display(), output.display(), video_only);
        
        let mut cmd = Command::new("ffmpeg");
        cmd.args(&[
            "-i", input.to_str().unwrap(),
            "-c:v", "libx264",       // Ensure H.264 video
            "-profile:v", "main",    // H.264 Main profile
            "-level", "4.0",         // H.264 level 4.0
            "-pix_fmt", "yuv420p",   // Standard pixel format
        ]);
        
        if video_only {
            cmd.arg("-an");          // Remove audio
        } else {
            cmd.args(&[
                "-c:a", "aac",       // Convert audio to AAC
                "-b:a", "128k",      // AAC bitrate
                "-ac", "2",          // Stereo
                "-ar", "44100",      // Standard sample rate
            ]);
        }
        
        cmd.args(&[
            "-movflags", "frag_keyframe+empty_moov+faststart", // MSE-compatible flags
            "-f", "mp4",             // MP4 container
            "-y",                    // Overwrite output
            output.to_str().unwrap()
        ]);
        
        debug!("Running FFmpeg command: {:?}", cmd);
        
        let output_result = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(TranscoderError::Io)?;
        
        if !output_result.status.success() {
            let stderr = String::from_utf8_lossy(&output_result.stderr);
            error!("FFmpeg MSE conversion failed: {}", stderr);
            return Err(TranscoderError::ConversionFailed(stderr.to_string()));
        }
        
        info!("Successfully converted to MSE-compatible format");
        Ok(())
    }
}

impl Default for MediaTranscoder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TranscoderError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Conversion failed: {0}")]
    ConversionFailed(String),
    #[error("Probe error: {0}")]
    ProbeError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_transcoder_creation() {
        let transcoder = MediaTranscoder::new();
        assert_eq!(transcoder.temp_dir, "/tmp");
    }

    #[test]
    fn test_needs_ac3_conversion_with_nonexistent_file() {
        let transcoder = MediaTranscoder::new();
        let result = transcoder.needs_ac3_conversion("nonexistent.mp4");
        assert!(result.is_err());
    }
}